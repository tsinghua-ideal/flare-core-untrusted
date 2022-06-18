use core::panic::Location;
use std::any::{Any, TypeId};
use std::borrow::BorrowMut;
use std::cmp::{Ordering, Reverse};
use std::collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap};
use std::convert::TryInto;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::mem::forget;
use std::net::Ipv4Addr;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{
    atomic::{self, AtomicBool, AtomicUsize},
    mpsc::{sync_channel, Receiver, RecvError, SyncSender},
    Arc, RwLock, Weak,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::context::Context;
use crate::dependency::{DepInfo, Dependency};
use crate::env::{self, Env, BOUNDED_MEM_CACHE};
use crate::error::{Error, Result};
use crate::partial::{BoundedDouble, CountEvaluator, GroupedCountEvaluator, PartialResult};
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::scheduler::TaskContext;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::serialization_free::{Construct, Idx, SizeBuf};
use crate::split::Split;
use crate::utils::bounded_priority_queue::BoundedPriorityQueue;
use crate::utils::random::{BernoulliCellSampler, BernoulliSampler, PoissonSampler, RandomSampler};
use crate::{utils, Fn, SerArc, SerBox};

use aes_gcm::aead::{generic_array::GenericArray, Aead, NewAead};
use aes_gcm::Aes128Gcm;
use fasthash::MetroHasher;
use once_cell::sync::Lazy;
use ordered_float::OrderedFloat;
use rand::{Rng, SeedableRng};
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use sgx_types::*;

mod parallel_collection_rdd;
pub use parallel_collection_rdd::*;
mod cartesian_rdd;
pub use cartesian_rdd::*;
mod co_grouped_rdd;
pub use co_grouped_rdd::*;
mod coalesced_rdd;
pub use coalesced_rdd::*;
mod flatmapper_rdd;
mod mapper_rdd;
pub use flatmapper_rdd::*;
pub use mapper_rdd::*;
mod pair_rdd;
pub use pair_rdd::*;
mod partitionwise_sampled_rdd;
pub use partitionwise_sampled_rdd::*;
mod shuffled_rdd;
pub use shuffled_rdd::*;
mod map_partitions_rdd;
pub use map_partitions_rdd::*;
mod zip_rdd;
pub use zip_rdd::*;
mod union_rdd;
pub use union_rdd::*;

pub type ItemE = Vec<u8>;

static immediate_cout: bool = true;
pub static STAGE_LOCK: Lazy<StageLock> = Lazy::new(|| StageLock::new());
pub const MAX_ENC_BL: usize = 1024;

extern "C" {
    pub fn secure_execute_pre(
        eid: sgx_enclave_id_t,
        tid: u64,
        op_ids: *const u8,
        part_nums: *const u8,
        dep_info: DepInfo,
    ) -> sgx_status_t;
    pub fn secure_execute(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64,
        rdd_ids: *const u8,
        op_ids: *const u8,
        part_ids: *const u8,
        cache_meta: CacheMeta,
        dep_info: DepInfo,
        input: Input,
        captured_vars: *const u8,
    ) -> sgx_status_t;
    pub fn get_cnt_per_partition(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        op_id: OpId,
        part_id: usize,
    ) -> sgx_status_t;
    pub fn set_cnt_per_partition(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        part_id: usize,
        cnt_per_partition: usize,
    ) -> sgx_status_t;
    pub fn free_res_enc(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        input: *mut u8,
    ) -> sgx_status_t;
    pub fn priv_free_res_enc(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        input: *mut u8,
    ) -> sgx_status_t;
    pub fn get_sketch(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        p_buf: *mut u8,
        p_data_enc: *mut u8,
    ) -> sgx_status_t;
    pub fn clone_out(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        p_out: usize,
        p_data_enc: *mut u8,
    ) -> sgx_status_t;
    pub fn randomize_in_place(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        op_id: OpId,
        input: *const u8,
        seed: u64,
        is_some: u8,
        num: u64,
    ) -> sgx_status_t;
    pub fn set_sampler(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        with_replacement: u8,
        fraction: f64,
    ) -> sgx_status_t;
    pub fn etake(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        op_id: OpId,
        input: *const u8,
        should_take: usize,
        have_take: *mut usize,
    ) -> sgx_status_t;
    pub fn tail_compute(eid: sgx_enclave_id_t, retval: *mut usize, input: *mut u8) -> sgx_status_t;
    pub fn free_tail_info(eid: sgx_enclave_id_t, input: *mut u8) -> sgx_status_t;
}

#[no_mangle]
pub unsafe extern "C" fn sbrk_o(increment: usize) -> *mut c_void {
    libc::sbrk(increment as intptr_t)
}

#[no_mangle]
pub unsafe extern "C" fn ocall_cache_to_outside(
    rdd_id: usize,
    part_id: usize,
    data_ptr: usize,
) -> u8 {
    //need to clone from memory alloced by ucmalloc to memory alloced by default allocator
    Env::get()
        .cache_tracker
        .put_sdata((rdd_id, part_id), data_ptr as *mut u8, 0);
    0 //need to revise
}

#[no_mangle]
pub unsafe extern "C" fn ocall_cache_from_outside(rdd_id: usize, part_id: usize) -> usize {
    let res = Env::get().cache_tracker.get_sdata((rdd_id, part_id));
    match res {
        Some(val) => val,
        None => 0,
    }
}

pub fn default_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn wrapper_secure_execute_pre(op_ids: &Vec<OpId>, split_nums: &Vec<usize>, dep_info: DepInfo) {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let tid: u64 = thread::current().id().as_u64().into();
    let sgx_status = unsafe {
        secure_execute_pre(
            eid,
            tid,
            op_ids as *const Vec<OpId> as *const u8,
            split_nums as *const Vec<usize> as *const u8,
            dep_info,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
}

pub fn wrapper_secure_execute<T>(
    rdd_ids: &Vec<usize>,
    op_ids: &Vec<OpId>,
    part_ids: &Vec<usize>,
    cache_meta: CacheMeta,
    dep_info: DepInfo,
    data: &T,
    captured_vars: &HashMap<usize, Vec<Vec<u8>>>,
) -> usize
// return result_ptr
where
    T: Construct + Data,
{
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let mut result_bl_ptr: usize = 0;
    let tid: u64 = thread::current().id().as_u64().into();
    let input = Input::new(data);
    let sgx_status = unsafe {
        secure_execute(
            eid,
            &mut result_bl_ptr,
            tid,
            rdd_ids as *const Vec<usize> as *const u8,
            op_ids as *const Vec<OpId> as *const u8,
            part_ids as *const Vec<usize> as *const u8,
            cache_meta,
            dep_info,
            input,
            captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    result_bl_ptr
}

pub fn start_execute<T: Data>(acc_arg: AccArg, data: Vec<T>, tx: SyncSender<usize>) -> f64 {
    let mut wait = 0.0;
    let cache_meta = acc_arg.to_cache_meta();
    wrapper_secure_execute_pre(&acc_arg.op_ids, &acc_arg.split_nums, acc_arg.dep_info);
    let wait_now = Instant::now();
    acc_arg.get_enclave_lock();
    let wait_dur = wait_now.elapsed().as_nanos() as f64 * 1e-9;
    wait += wait_dur;
    let result_ptr = wrapper_secure_execute(
        &acc_arg.rdd_ids,
        &acc_arg.op_ids,
        &acc_arg.part_ids,
        cache_meta,
        acc_arg.dep_info,
        &data,
        &acc_arg.captured_vars,
    );
    tx.send(result_ptr).unwrap();
    wait
}

pub fn wrapper_get_cnt_per_partition(op_id: OpId, part_id: usize) -> usize {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let mut cnt_per_partition: usize = 0;
    let sgx_status = unsafe { get_cnt_per_partition(eid, &mut cnt_per_partition, op_id, part_id) };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    cnt_per_partition
}

pub fn wrapper_set_cnt_per_partition(op_id: OpId, part_id: usize, cnt_per_partition: usize) {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let sgx_status = unsafe { set_cnt_per_partition(eid, op_id, part_id, cnt_per_partition) };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
}

pub fn wrapper_action<T: Data>(data: Vec<T>, rdd_id: usize, op_id: OpId, is_local: bool) -> usize {
    let now = Instant::now();
    let tid: u64 = thread::current().id().as_u64().into();
    let captured_vars = HashMap::new();
    let rdd_ids = vec![rdd_id];
    let op_ids = vec![op_id];
    let part_ids = vec![0 as usize]; //placeholder
    let dep_info = if is_local {
        DepInfo::padding_new(4)
    } else {
        DepInfo::padding_new(3)
    };
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let input = Input::new_with(&data, usize::MAX);
    let mut result_ptr: usize = 0;
    let sgx_status = unsafe {
        secure_execute(
            eid,
            &mut result_ptr,
            tid,
            &rdd_ids as *const Vec<usize> as *const u8,
            &op_ids as *const Vec<OpId> as *const u8,
            &part_ids as *const Vec<usize> as *const u8,
            Default::default(), //meaningless
            dep_info,
            input,
            &captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("in aggregate, ecall {:?}s", dur);
    result_ptr
}

//This method should only be used if the resulting array is expected to be small,
pub fn wrapper_randomize_in_place<T>(
    op_id: OpId,
    input: &Vec<T>,
    seed: Option<u64>,
    num: u64,
) -> Vec<T>
where
    T: std::fmt::Debug
        + Clone
        + Serialize
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static,
{
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let (seed, is_some) = match seed {
        Some(seed) => (seed, 1),
        None => (0, 0),
    };

    let mut retval: usize = 0;
    let sgx_status = unsafe {
        randomize_in_place(
            eid,
            &mut retval,
            op_id,
            input as *const Vec<T> as *const u8,
            seed,
            is_some,
            num,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    let res = get_encrypted_data::<T>(op_id, DepInfo::padding_new(0), retval as *mut u8);
    *res
}

pub fn wrapper_set_sampler(op_id: OpId, with_replacement: bool, fraction: f64) {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let with_replacement = match with_replacement {
        true => 1,
        false => 0,
    };
    let sgx_status = unsafe { set_sampler(eid, op_id, with_replacement, fraction) };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
}

pub fn wrapper_take<T: Data>(op_id: OpId, input: &Vec<T>, should_take: usize) -> (Vec<T>, usize) {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let mut retval: usize = 0;
    let mut have_take: usize = 0;
    let sgx_status = unsafe {
        etake(
            eid,
            &mut retval,
            op_id,
            input as *const Vec<T> as *const u8,
            should_take,
            &mut have_take,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    let res = get_encrypted_data::<T>(op_id, DepInfo::padding_new(0), retval as *mut u8);
    (*res, have_take)
}

pub fn wrapper_tail_compute(tail_info: &mut TailCompInfo) {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let mut p_new_tail_info: usize = 0;
    let sgx_status = unsafe {
        tail_compute(
            eid,
            &mut p_new_tail_info,
            tail_info as *mut TailCompInfo as *mut u8,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    let new_tail_info_ = unsafe { Box::from_raw(p_new_tail_info as *mut u8 as *mut TailCompInfo) };
    let new_tail_info = new_tail_info_.clone();
    forget(new_tail_info_);

    let sgx_status = unsafe { free_tail_info(eid, p_new_tail_info as *mut u8) };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    };
    *tail_info = *new_tail_info;
}

pub fn get_encrypted_data<T>(op_id: OpId, dep_info: DepInfo, p_data_enc: *mut u8) -> Box<Vec<T>>
where
    T: std::fmt::Debug
        + Clone
        + Serialize
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static,
{
    let tid: u64 = thread::current().id().as_u64().into();
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    if immediate_cout {
        let res_ = unsafe { Box::from_raw(p_data_enc as *mut Vec<T>) };
        let res = res_.clone();
        forget(res_);
        let sgx_status = unsafe { free_res_enc(eid, op_id, dep_info, p_data_enc) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {}
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        res
    } else {
        let now = Instant::now();
        let size_buf_len = 1 << (7 + 10 + 10); //128M * 8B
        let size_buf = SizeBuf::new(size_buf_len);
        let size_buf_ptr = Box::into_raw(Box::new(size_buf));
        let sgx_status = unsafe {
            get_sketch(
                eid,
                op_id,
                dep_info,
                size_buf_ptr as *mut u8,
                p_data_enc, //shuffle write
            )
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => (),
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            }
        };

        let size_buf = unsafe { Box::from_raw(size_buf_ptr) };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("get sketch {:?}s", dur);
        let now = Instant::now();
        let mut v: Vec<T> = Vec::new();
        let mut idx = Idx::new();
        v.recv(&size_buf, &mut idx);
        let ptr_out = Box::into_raw(Box::new(v)) as *mut u8 as usize;
        let sgx_status = unsafe { clone_out(eid, op_id, dep_info, ptr_out, p_data_enc) };
        let v = unsafe { Box::from_raw(ptr_out as *mut u8 as *mut Vec<T>) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {}
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            }
        }
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("copy out {:?}s", dur);
        /*
        let now = Instant::now();
        let _v = bincode::serialize(&*v).unwrap();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("serialize {:?}", dur);
        let now = Instant::now();
        let _v = v.clone();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("clone {:?}", dur);
        */
        v
    }
}

pub fn move_data<T: Clone>(op_id: OpId, data: *mut u8) -> Box<Vec<T>> {
    let res_ = unsafe { Box::from_raw(data as *mut Vec<T>) };
    let res = res_.clone();
    forget(res_);
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();
    let sgx_status = unsafe {
        priv_free_res_enc(
            eid,
            op_id,
            DepInfo::padding_new(0), //default to 0, for cache should not appear at the end of stage
            data,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {}
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        }
    }
    res
}

#[inline(always)]
fn read_le_u64(input: &mut &[u8]) -> u64 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u64>());
    *input = rest;
    u64::from_le_bytes(int_bytes.try_into().unwrap())
}

#[inline(always)]
pub fn encrypt(pt: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.encrypt(nonce, pt).expect("encryption failure")
}

#[inline(always)]
pub fn ser_encrypt<T>(pt: &T) -> Vec<u8>
where
    T: ?Sized + serde::Serialize,
{
    encrypt(bincode::serialize(pt).unwrap().as_ref())
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: &[u8]) -> T
where
    T: serde::de::DeserializeOwned,
{
    bincode::deserialize(decrypt(ct).as_ref()).unwrap()
}

pub fn batch_encrypt<T: Data>(data: &[T]) -> Vec<ItemE> {
    data.chunks(MAX_ENC_BL)
        .map(|x| ser_encrypt(x))
        .collect::<Vec<_>>()
}

pub fn batch_decrypt<T: Data>(data_enc: &[ItemE]) -> Vec<T> {
    data_enc
        .iter()
        .map(|x| ser_decrypt::<Vec<T>>(x))
        .flatten()
        .collect::<Vec<_>>()
}

pub fn secure_compute_cached(
    acc_arg: &mut AccArg,
    cur_rdd_id: usize,
    cur_part_id: usize,
    tx: SyncSender<usize>,
) -> Vec<JoinHandle<()>> {
    let eid = Env::get()
        .enclave
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .geteid();

    let is_cached = Env::get().cache_tracker.scontain((cur_rdd_id, cur_part_id));
    let mut handles = Vec::new();
    if is_cached {
        acc_arg.set_cached_rdd_id(cur_rdd_id);
        // Compute based on cached values
        let rdd_ids = acc_arg.rdd_ids.clone();
        let op_ids = acc_arg.op_ids.clone();
        let part_ids = acc_arg.part_ids.clone();
        let split_nums = acc_arg.split_nums.clone();
        let cache_meta = acc_arg.to_cache_meta();
        let dep_info = acc_arg.dep_info;
        let eenter_lock = acc_arg.eenter_lock.clone();
        let captured_vars = acc_arg.captured_vars.clone();

        let handle = std::thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            wrapper_secure_execute_pre(&op_ids, &split_nums, dep_info);
            let mut result_ptr: usize = 0;
            while eenter_lock.compare_and_swap(false, true, atomic::Ordering::SeqCst) {
                //wait
            }
            let sgx_status = unsafe {
                secure_execute(
                    eid,
                    &mut result_ptr,
                    tid,
                    &rdd_ids as *const Vec<usize> as *const u8,
                    &op_ids as *const Vec<OpId> as *const u8,
                    &part_ids as *const Vec<usize> as *const u8,
                    cache_meta,
                    dep_info,
                    Input::build_from_ptr(0 as *const u8), //invalid pointer  TODO: send valid pointer
                    &captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
                )
            };
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {}
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                }
            };
            tx.send(result_ptr).unwrap();
        });
        handles.push(handle);
    } else {
        acc_arg.set_caching_rdd_id(cur_rdd_id);
    }
    handles
}

#[derive(Clone, Debug)]
pub struct AccArg {
    pub rdd_ids: Vec<usize>,
    pub op_ids: Vec<OpId>,
    pub part_ids: Vec<usize>,
    pub split_nums: Vec<usize>,
    pub dep_info: DepInfo,
    caching_rdd_id: usize,
    cached_rdd_id: usize,
    pub eenter_lock: Arc<AtomicBool>,
    pub captured_vars: HashMap<usize, Vec<Vec<u8>>>,
}

impl AccArg {
    pub fn new(dep_info: DepInfo, reduce_num: Option<usize>, eenter_lock: Arc<AtomicBool>) -> Self {
        let split_nums = match reduce_num {
            Some(reduce_num) => {
                assert!(dep_info.is_shuffle == 1);
                vec![reduce_num]
            }
            None => Vec::new(),
        };
        AccArg {
            rdd_ids: Vec::new(),
            op_ids: Vec::new(),
            part_ids: Vec::new(),
            split_nums,
            dep_info,
            caching_rdd_id: 0,
            cached_rdd_id: 0,
            eenter_lock,
            captured_vars: HashMap::new(),
        }
    }

    pub fn get_final_rdd_id(&self) -> usize {
        self.rdd_ids[0]
    }

    //set the to_be_cached rdd and return whether the set is successfully
    pub fn set_caching_rdd_id(&mut self, caching_rdd_id: usize) -> bool {
        if self.cached_rdd_id == caching_rdd_id {
            self.cached_rdd_id = 0;
        }
        match self.caching_rdd_id == 0 {
            true => {
                self.caching_rdd_id = caching_rdd_id;
                return true;
            }
            false => return false,
        }
    }

    //set the have_cached rdd and return whether the set is successfully
    pub fn set_cached_rdd_id(&mut self, cached_rdd_id: usize) -> bool {
        match self.cached_rdd_id == 0 {
            true => {
                self.cached_rdd_id = cached_rdd_id;
                return true;
            }
            false => return false,
        }
    }

    pub fn to_cache_meta(&self) -> CacheMeta {
        let len = self.rdd_ids.len();
        assert_eq!(len, self.op_ids.len());
        let mut caching_op_id = Default::default();
        let mut caching_part_id = Default::default();
        let mut cached_op_id = Default::default();
        let mut cached_part_id = Default::default();
        for idx in 0..len {
            if self.caching_rdd_id == self.rdd_ids[idx] {
                caching_op_id = self.op_ids[idx];
                caching_part_id = self.part_ids[idx];
            }
            if self.cached_rdd_id == self.rdd_ids[idx] {
                cached_op_id = self.op_ids[idx];
                cached_part_id = self.part_ids[idx];
            }
        }

        CacheMeta::new(
            self.caching_rdd_id,
            caching_op_id,
            caching_part_id,
            self.cached_rdd_id,
            cached_op_id,
            cached_part_id,
        )
    }

    pub fn insert_quadruple(
        &mut self,
        rdd_id: usize,
        op_id: OpId,
        part_id: usize,
        split_num: usize,
    ) {
        self.rdd_ids.push(rdd_id);
        self.op_ids.push(op_id);
        self.part_ids.push(part_id);
        self.split_nums.push(split_num);
    }

    pub fn is_caching_final_rdd(&self) -> bool {
        *self.part_ids.first().unwrap() != usize::MAX
            && *self.rdd_ids.first().unwrap() == self.caching_rdd_id
    }

    pub fn get_enclave_lock(&self) {
        while self
            .eenter_lock
            .compare_and_swap(false, true, atomic::Ordering::SeqCst)
        {
            //wait
        }
    }

    pub fn free_enclave_lock(&self) {
        assert_eq!(
            self.eenter_lock
                .compare_and_swap(true, false, atomic::Ordering::SeqCst),
            true
        );
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    caching_op_id: OpId,
    caching_part_id: usize,
    cached_rdd_id: usize,
    cached_op_id: OpId,
    cached_part_id: usize,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        caching_op_id: OpId,
        caching_part_id: usize,
        cached_rdd_id: usize,
        cached_op_id: OpId,
        cached_part_id: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            caching_op_id,
            caching_part_id,
            cached_rdd_id,
            cached_op_id,
            cached_part_id,
        }
    }

    pub fn get_triplet(&self) -> (usize, usize) {
        (self.caching_rdd_id, self.caching_part_id)
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct Input {
    data: usize,
    parallel_num: usize,
}

impl Input {
    pub fn new<T: Data>(data: &T) -> Self {
        let data = data as *const T as usize;
        Input {
            data,
            parallel_num: STAGE_LOCK.get_parall_num(),
        }
    }

    pub fn new_with<T: Data>(data: &T, parallel_num: usize) -> Self {
        let data = data as *const T as usize;
        Input { data, parallel_num }
    }

    pub fn build_from_ptr(data: *const u8) -> Self {
        Input {
            data: data as usize,
            parallel_num: STAGE_LOCK.get_parall_num(),
        }
    }
}

#[derive(Debug)]
pub struct StageLock {
    slock: AtomicBool,
    lock_holder_info: RwLock<LockHolderInfo>,
    //For result task, key.0 == key.1
    //For shuffle task, key.0 > key.1, key.0 is child rdd id and key.1 is parent rdd id, key.2 is identifier
    waiting_list: RwLock<BTreeMap<(usize, usize, usize), Vec<usize>>>,
    num_splits_mapping: RwLock<BTreeMap<(usize, usize, usize), usize>>,
}

#[derive(Debug)]
pub struct LockHolderInfo {
    cur_holder: (usize, usize, usize),
    num_cur_holders: usize,
    max_cur_holders: usize,
}

impl StageLock {
    pub fn new() -> Self {
        StageLock {
            slock: AtomicBool::new(false),
            lock_holder_info: RwLock::new(LockHolderInfo {
                cur_holder: (0, 0, 0),
                num_cur_holders: 0,
                max_cur_holders: 48,
            }),
            waiting_list: RwLock::new(BTreeMap::new()),
            num_splits_mapping: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn insert_stage(&self, rdd_id_pair: (usize, usize, usize), task_id: usize) {
        let mut waiting_list = self.waiting_list.write().unwrap();
        waiting_list
            .entry(rdd_id_pair)
            .or_insert(vec![])
            .push(task_id);
    }

    pub fn remove_stage(&self, rdd_id_pair: (usize, usize, usize), task_id: usize) {
        let mut waiting_list = self.waiting_list.write().unwrap();
        let mut remaining = 0;
        if let Some(task_ids) = waiting_list.get_mut(&rdd_id_pair) {
            if let Some(pos) = task_ids.iter().position(|x| *x == task_id) {
                task_ids.remove(pos);
            }
            remaining = task_ids.len();
        }
        if remaining == 0 {
            waiting_list.remove(&rdd_id_pair);
            self.num_splits_mapping
                .write()
                .unwrap()
                .remove(&rdd_id_pair);
        }
    }

    pub fn set_num_splits(&self, rdd_id_pair: (usize, usize, usize), num_splits: usize) {
        let mut num_splits_mapping = self.num_splits_mapping.write().unwrap();
        num_splits_mapping.insert(rdd_id_pair, num_splits);
    }

    pub fn get_parall_num(&self) -> usize {
        let num_splits_mapping = self.num_splits_mapping.read().unwrap();
        let info = self.lock_holder_info.read().unwrap();
        std::cmp::min(
            *num_splits_mapping.get(&info.cur_holder).unwrap(),
            info.max_cur_holders,
        )
    }

    pub fn get_stage_lock(&self, cur_rdd_id_pair: (usize, usize, usize)) {
        use atomic::Ordering::SeqCst;
        let mut flag = true;
        while flag {
            while self.slock.compare_and_swap(false, true, SeqCst) {
                //wait or bypass
                let mut info = self.lock_holder_info.write().unwrap();
                if cur_rdd_id_pair == info.cur_holder  //The case for bypass
                    && info.num_cur_holders < info.max_cur_holders
                {
                    info.num_cur_holders += 1;
                    return;
                }
            }
            if *self
                .waiting_list
                .read()
                .unwrap()
                .first_key_value()
                .unwrap()
                .0
                < cur_rdd_id_pair
            {
                self.slock.store(false, SeqCst);
            } else {
                let mut info = self.lock_holder_info.write().unwrap();
                info.cur_holder = cur_rdd_id_pair;
                info.num_cur_holders += 1;
                flag = false;
            }
        }
    }

    pub fn free_stage_lock(&self) {
        use atomic::Ordering::SeqCst;
        self.lock_holder_info.write().unwrap().num_cur_holders -= 1;
        let num_cur_holder = self.lock_holder_info.read().unwrap().num_cur_holders;
        if num_cur_holder == 0 {
            self.slock.compare_and_swap(true, false, SeqCst);
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Text<T, TE> {
    data: Option<T>,
    data_enc: Option<TE>,
    id: u64,
}

impl<T, TE> Text<T, TE>
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub fn new(data: Option<T>, data_enc: Option<TE>) -> Self {
        let loc = Location::caller();

        let file = loc.file();
        let line = loc.line();
        let num = 0;
        let id = OpId::new(file, line, num).h;

        Text { data, data_enc, id }
    }

    pub fn get_ct(&self) -> TE {
        self.data_enc.clone().unwrap()
    }

    pub fn get_ct_ref(&self) -> &TE {
        self.data_enc.as_ref().unwrap()
    }

    pub fn get_ct_mut(&mut self) -> &mut TE {
        self.data_enc.as_mut().unwrap()
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        let text = bincode::deserialize(&tail_info.get(self.id).unwrap()).unwrap();
        *self = text;
    }
}

impl<T> Text<T, Vec<u8>>
where
    T: Data,
{
    //Should be used for debug only
    pub fn get_pt(&self) -> T {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else {
            ser_decrypt(self.data_enc.as_ref().unwrap())
        }
    }
}

impl<T> Text<Vec<T>, Vec<Vec<u8>>>
where
    T: Data,
{
    //Should be used for debug only
    pub fn get_pt(&self) -> Vec<T> {
        if self.data.is_some() {
            self.data.clone().unwrap()
        } else {
            self.data_enc
                .as_ref()
                .unwrap()
                .iter()
                .map(|x| ser_decrypt::<Vec<T>>(x).into_iter())
                .flatten()
                .collect::<Vec<_>>()
        }
    }
}

impl<T, TE> Deref for Text<T, TE>
where
    T: Data,
    TE: Data,
{
    type Target = T;

    fn deref(&self) -> &T {
        self.data.as_ref().unwrap()
    }
}

impl<T, TE> DerefMut for Text<T, TE>
where
    T: Data,
    TE: Data,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.as_mut().unwrap()
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo { m: HashMap::new() }
    }

    pub fn insert<T, TE>(&mut self, text: &Text<T, TE>)
    where
        T: Data,
        TE: Data,
    {
        self.m.insert(text.id, bincode::serialize(text).unwrap());
    }

    pub fn remove(&mut self, id: u64) -> Vec<u8> {
        self.m.remove(&id).unwrap()
    }

    pub fn get(&self, id: u64) -> Option<Vec<u8>> {
        match self.m.get(&id) {
            Some(ser) => Some(ser.clone()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
    }
}

#[repr(C)]
#[derive(
    Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct OpId {
    h: u64,
}

impl OpId {
    pub fn new(file: &'static str, line: u32, num: usize) -> Self {
        let h = default_hash(&(file.to_string(), line, num));
        OpId { h }
    }
}

// Values which are needed for all RDDs
#[derive(Serialize, Deserialize)]
pub(crate) struct RddVals {
    pub id: usize,
    pub op_id: OpId,
    pub shuffle_ids: Vec<usize>,
    should_cache_: AtomicBool,
    secure: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub context: Weak<Context>,
}

impl RddVals {
    #[track_caller]
    pub fn new(sc: Arc<Context>, secure: bool) -> Self {
        let loc = Location::caller();
        RddVals {
            id: sc.new_rdd_id(),
            op_id: sc.new_op_id(loc),
            shuffle_ids: Vec::new(),
            should_cache_: AtomicBool::new(false),
            secure,
            context: Arc::downgrade(&sc),
        }
    }

    fn cache(&self) {
        self.should_cache_.store(true, atomic::Ordering::Relaxed);
    }

    fn should_cache(&self) -> bool {
        self.should_cache_.load(atomic::Ordering::Relaxed)
    }
}

// Due to the lack of HKTs in Rust, it is difficult to have collection of generic data with different types.
// Required for storing multiple RDDs inside dependencies and other places like Tasks, etc.,
// Refactored RDD trait into two traits one having RddBase trait which contains only non generic methods which provide information for dependency lists
// Another separate Rdd containing generic methods like map, etc.,
pub trait RddBase: Send + Sync + Serialize + Deserialize {
    fn cache(&self); //cache once temporarily
    fn should_cache(&self) -> bool;
    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe { Box::from_raw(ptr as *mut Vec<ItemE>) };
    }
    fn get_rdd_id(&self) -> usize;
    fn get_op_id(&self) -> OpId;
    fn get_op_ids(&self, op_ids: &mut Vec<OpId>);
    fn get_context(&self) -> Arc<Context>;
    fn get_op_name(&self) -> String {
        "unknown".to_owned()
    }
    fn register_op_name(&self, _name: &str) {
        log::debug!("couldn't register op name")
    }
    fn get_dependencies(&self) -> Vec<Dependency>;
    fn get_secure(&self) -> bool;
    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<ItemE>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }
    fn preferred_locations(&self, _split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        Vec::new()
    }
    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        None
    }
    fn splits(&self) -> Vec<Box<dyn Split>>;
    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }
    fn iterator_raw(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>>;
    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>>;
    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }
    fn is_pinned(&self) -> bool {
        false
    }
}

impl PartialOrd for dyn RddBase {
    fn partial_cmp(&self, other: &dyn RddBase) -> Option<Ordering> {
        Some(self.get_rdd_id().cmp(&other.get_rdd_id()))
    }
}

impl PartialEq for dyn RddBase {
    fn eq(&self, other: &dyn RddBase) -> bool {
        self.get_rdd_id() == other.get_rdd_id()
    }
}

impl Eq for dyn RddBase {}

impl Ord for dyn RddBase {
    fn cmp(&self, other: &dyn RddBase) -> Ordering {
        self.get_rdd_id().cmp(&other.get_rdd_id())
    }
}

impl<I: Rdd + ?Sized> RddBase for SerArc<I> {
    fn cache(&self) {
        (**self).get_rdd_base().cache();
    }
    fn should_cache(&self) -> bool {
        (**self).get_rdd_base().should_cache()
    }
    fn free_data_enc(&self, ptr: *mut u8) {
        (**self).free_data_enc(ptr);
    }
    fn get_rdd_id(&self) -> usize {
        (**self).get_rdd_base().get_rdd_id()
    }
    fn get_op_id(&self) -> OpId {
        (**self).get_rdd_base().get_op_id()
    }
    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        (**self).get_rdd_base().get_op_ids(op_ids)
    }
    fn get_context(&self) -> Arc<Context> {
        (**self).get_rdd_base().get_context()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        (**self).get_rdd_base().get_dependencies()
    }
    fn get_secure(&self) -> bool {
        (**self).get_rdd_base().get_secure()
    }
    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        (**self).move_allocation(value_ptr)
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        (**self).get_rdd_base().splits()
    }
    fn iterator_raw(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        (**self)
            .get_rdd_base()
            .iterator_raw(stage_id, split, acc_arg, tx)
    }
    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        (**self).get_rdd_base().iterator_any(split)
    }
}

impl<I: Rdd + ?Sized> Rdd for SerArc<I> {
    type Item = I::Item;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        (**self).get_rdd()
    }
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        (**self).get_rdd_base()
    }
    fn get_or_compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        (**self).get_or_compute(split)
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        (**self).compute(split)
    }
    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        (**self).secure_compute(stage_id, split, acc_arg, tx)
    }
}

// Rdd containing methods associated with processing
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn get_or_compute(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let cache_tracker = Env::get().cache_tracker.clone();
        Ok(cache_tracker.get_or_compute(self.get_rdd(), split))
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>>;

    fn iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if self.should_cache() {
            self.get_or_compute(split)
        } else {
            self.compute(split)
        }
    }

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>>;

    fn secure_iterator(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        dep_info: DepInfo,
        action_id: Option<OpId>,
    ) -> Result<Box<dyn Iterator<Item = ItemE>>> {
        let (tx, rx) = sync_channel(0);
        let rdd_id = self.get_rdd_id();
        let op_id = self.get_op_id();
        let part_id = split.get_index();
        let mut acc_arg = AccArg::new(dep_info, None, Arc::new(AtomicBool::new(false)));
        if let Some(action_id) = action_id {
            acc_arg.insert_quadruple(rdd_id, action_id, usize::MAX, usize::MAX);
        }
        let handles = self.secure_compute(stage_id, split, &mut acc_arg, tx)?;
        let result = match rx.recv() {
            Ok(received) => {
                let result = get_encrypted_data::<ItemE>(op_id, dep_info, received as *mut u8);
                acc_arg.free_enclave_lock();
                *result
            }
            Err(RecvError) => Vec::new(),
        };

        for handle in handles {
            handle.join().unwrap();
        }

        if acc_arg.is_caching_final_rdd() {
            let size = result.get_size();
            let data_ptr = Box::into_raw(Box::new(result.clone()));
            Env::get()
                .cache_tracker
                .put_sdata((rdd_id, part_id), data_ptr as *mut u8, size);
        }

        Ok(Box::new(result.into_iter()))
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
    #[track_caller]
    fn filter<F>(&self, predicate: F) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        F: SerFunc(&Self::Item) -> bool + Copy,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize,
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), filter_fn))
    }

    #[track_caller]
    fn map<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Self::Item) -> U,
        Self: Sized,
    {
        SerArc::new(MapperRdd::new(self.get_rdd(), f))
    }

    #[track_caller]
    fn flat_map<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        SerArc::new(FlatMapperRdd::new(self.get_rdd(), f))
    }

    /// Return a new RDD by applying a function to each partition of this RDD.
    #[track_caller]
    fn map_partitions<U: Data, F>(&self, func: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        let ignore_idx = Fn!(move |_index: usize,
                                   items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> { (func)(items) });
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), ignore_idx))
    }

    /// Return a new RDD by applying a function to each partition of this RDD,
    /// while tracking the index of the original partition.
    #[track_caller]
    fn map_partitions_with_index<U: Data, F>(&self, f: F) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(usize, Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        Self: Sized,
    {
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), f))
    }

    /// Return an RDD created by coalescing all elements within each partition into an array.
    #[allow(clippy::type_complexity)]
    #[track_caller]
    fn glom(&self) -> SerArc<dyn Rdd<Item = Vec<Self::Item>>>
    where
        Self: Sized,
    {
        let func = Fn!(
            |_index: usize, iter: Box<dyn Iterator<Item = Self::Item>>| Box::new(std::iter::once(
                iter.collect::<Vec<_>>()
            ))
                as Box<dyn Iterator<Item = Vec<Self::Item>>>
        );
        let rdd = MapPartitionsRdd::new(self.get_rdd(), Box::new(func));
        rdd.register_op_name("gloom");
        SerArc::new(rdd)
    }

    fn save_as_text_file(&self, path: String) -> Result<Vec<()>>
    where
        Self: Sized,
    {
        fn save<R: Data>(ctx: TaskContext, iter: Box<dyn Iterator<Item = R>>, path: String) {
            fs::create_dir_all(&path).unwrap();
            let id = ctx.split_id;
            let file_path = Path::new(&path).join(format!("part-{}", id));
            let f = fs::File::create(file_path).expect("unable to create file");
            let mut f = BufWriter::new(f);
            for item in iter {
                let line = format!("{:?}", item);
                f.write_all(line.as_bytes())
                    .expect("error while writing to file");
            }
        }
        let cl = Fn!(move |(ctx, (iter, _))| save::<Self::Item>(ctx, iter, path.to_string()));
        //TODO action_id
        self.get_context()
            .run_job_with_context(self.get_rdd(), None, cl)
    }

    fn reduce<F>(&self, f: F) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();
        let reduce_partition = Fn!(move |(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| {
            let acc = iter.reduce(&cf);
            match acc {
                None => vec![],
                Some(e) => vec![e],
            }
        });
        let now = Instant::now();
        let results = self
            .get_context()
            .run_job(self.get_rdd(), None, reduce_partition);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("mapper {:?}s", dur);
        let now = Instant::now();
        let result = Ok(results?.into_iter().flatten().reduce(f));
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("reducer {:?}s", dur);
        result
    }

    #[track_caller]
    fn secure_reduce<F>(&self, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let ctx = self.get_context();
        let loc = Location::caller();
        let action_id = ctx.new_op_id(loc);
        let cur_rdd_id = self.get_rdd_id();
        let cl = Fn!(move |(_, iter): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| {
            let data = iter.collect::<Vec<ItemE>>();
            let result_ptr = wrapper_action(data, cur_rdd_id, action_id, true);
            let partial_res = get_encrypted_data::<ItemE>(
                action_id,
                DepInfo::padding_new(4),
                result_ptr as *mut u8,
            );
            *partial_res
        });
        let data = self
            .get_context()
            .run_job(self.get_rdd(), Some(action_id), cl)?
            .into_iter()
            .flatten()
            .collect::<Vec<ItemE>>();
        let result_ptr = wrapper_action(data, self.get_rdd_id(), action_id, false);
        let mut temp =
            get_encrypted_data::<ItemE>(action_id, DepInfo::padding_new(3), result_ptr as *mut u8);
        Ok(Text::new(None, Some(temp.pop().unwrap())))
    }

    /// Aggregate the elements of each partition, and then the results for all the partitions, using a
    /// given associative function and a neutral "initial value". The function
    /// Fn(t1, t2) is allowed to modify t1 and return it as its result value to avoid object
    /// allocation; however, it should not modify t2.
    ///
    /// This behaves somewhat differently from fold operations implemented for non-distributed
    /// collections. This fold operation may be applied to partitions individually, and then fold
    /// those results into the final result, rather than apply the fold to each element sequentially
    /// in some defined ordering. For functions that are not commutative, the result may differ from
    /// that of a fold applied to a non-distributed collection.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated result of each partition for the `op`
    ///                  operator, and also the initial value for the combine results from different
    ///                  partitions for the `f` function - this will typically be the neutral
    ///                  element (e.g. `0` for summation)
    /// * `f` - a function used to both accumulate results within a partition and combine results
    ///                  from different partitions
    fn fold<F>(&self, init: Self::Item, f: F) -> Result<Self::Item>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cf = f.clone();
        let zero = init.clone();
        let reduce_partition = Fn!(move |(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| iter.fold(zero.clone(), &cf));
        let results = self
            .get_context()
            .run_job(self.get_rdd(), None, reduce_partition);
        Ok(results?.into_iter().fold(init, f))
    }

    #[track_caller]
    fn secure_fold<F>(&self, init: Self::Item, f: F) -> Result<Text<Self::Item, ItemE>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let ctx = self.get_context();
        let loc = Location::caller();
        let action_id = ctx.new_op_id(loc);
        let cur_rdd_id = self.get_rdd_id();
        let cl = Fn!(move |(_, iter): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| {
            let data = iter.collect::<Vec<ItemE>>();
            let result_ptr = wrapper_action(data, cur_rdd_id, action_id, true);
            let partial_res = get_encrypted_data::<ItemE>(
                action_id,
                DepInfo::padding_new(4),
                result_ptr as *mut u8,
            );
            *partial_res
        });
        let data = self
            .get_context()
            .run_job(self.get_rdd(), Some(action_id), cl)?
            .into_iter()
            .flatten()
            .collect::<Vec<ItemE>>();

        let result_ptr = wrapper_action(data, self.get_rdd_id(), action_id, false);
        let mut temp =
            get_encrypted_data::<ItemE>(action_id, DepInfo::padding_new(3), result_ptr as *mut u8);
        Ok(Text::new(None, Some(temp.pop().unwrap())))
    }

    /// Aggregate the elements of each partition, and then the results for all the partitions, using
    /// given combine functions and a neutral "initial value". This function can return a different result
    /// type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
    /// and one operation for merging two U's, as in Rust Iterator fold method. Both of these functions are
    /// allowed to modify and return their first argument instead of creating a new U to avoid memory
    /// allocation.
    ///
    /// # Arguments
    ///
    /// * `init` - an initial value for the accumulated result of each partition for the `seq_fn` function,
    ///                  and also the initial value for the combine results from
    ///                  different partitions for the `comb_fn` function - this will typically be the
    ///                  neutral element (e.g. `vec![]` for vector aggregation or `0` for summation)
    /// * `seq_fn` - a function used to accumulate results within a partition
    /// * `comb_fn` - an associative function used to combine results from different partitions
    fn aggregate<U: Data, SF, CF>(&self, init: U, seq_fn: SF, comb_fn: CF) -> Result<U>
    where
        Self: Sized,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        let zero = init.clone();
        let reduce_partition = Fn!(move |(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| iter.fold(zero.clone(), &seq_fn));
        let results = self
            .get_context()
            .run_job(self.get_rdd(), None, reduce_partition);
        Ok(results?.into_iter().fold(init, comb_fn))
    }

    #[track_caller]
    fn secure_aggregate<U, SF, CF>(
        &self,
        init: U,
        seq_fn: SF,
        comb_fn: CF,
    ) -> Result<Text<U, ItemE>>
    where
        Self: Sized,
        U: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
    {
        let ctx = self.get_context();
        let loc = Location::caller();
        let action_id = ctx.new_op_id(loc);
        let cur_rdd_id = self.get_rdd_id();
        let cl = Fn!(move |(_, iter): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| {
            let data = iter.collect::<Vec<ItemE>>();
            let result_ptr = wrapper_action(data, cur_rdd_id, action_id, true);
            let partial_res = get_encrypted_data::<ItemE>(
                action_id,
                DepInfo::padding_new(4),
                result_ptr as *mut u8,
            );
            *partial_res
        });

        let data = ctx
            .run_job(self.get_rdd(), None, cl)?
            .into_iter()
            .flatten()
            .collect::<Vec<ItemE>>();

        let result_ptr = wrapper_action(data, self.get_rdd_id(), action_id, false);
        let mut temp =
            get_encrypted_data::<ItemE>(action_id, DepInfo::padding_new(3), result_ptr as *mut u8);
        Ok(Text::new(None, Some(temp.pop().unwrap())))
    }

    /// Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    /// elements (a, b) where a is in `this` and b is in `other`.
    #[track_caller]
    fn cartesian<U>(
        &self,
        other: SerArc<dyn Rdd<Item = U>>,
    ) -> SerArc<dyn Rdd<Item = (Self::Item, U)>>
    where
        Self: Sized,
        U: Data,
    {
        SerArc::new(CartesianRdd::new(self.get_rdd(), other.get_rdd()))
    }

    /// Return a new RDD that is reduced into `num_partitions` partitions.
    ///
    /// This results in a narrow dependency, e.g. if you go from 1000 partitions
    /// to 100 partitions, there will not be a shuffle, instead each of the 100
    /// new partitions will claim 10 of the current partitions. If a larger number
    /// of partitions is requested, it will stay at the current number of partitions.
    ///
    /// However, if you're doing a drastic coalesce, e.g. to num_partitions = 1,
    /// this may result in your computation taking place on fewer nodes than
    /// you like (e.g. one node in the case of num_partitions = 1). To avoid this,
    /// you can pass shuffle = true. This will add a shuffle step, but means the
    /// current upstream partitions will be executed in parallel (per whatever
    /// the current partitioning is).
    ///
    /// # Notes
    ///
    /// With shuffle = true, you can actually coalesce to a larger number
    /// of partitions. This is useful if you have a small number of partitions,
    /// say 100, potentially with a few partitions being abnormally large. Calling
    /// coalesce(1000, shuffle = true) will result in 1000 partitions with the
    /// data distributed using a hash partitioner. The optional partition coalescer
    /// passed in must be serializable.
    #[track_caller]
    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        if shuffle {
            // Distributes elements evenly across output partitions, starting from a random partition.
            let distributed_partition = Fn!(
                move |index: usize, items: Box<dyn Iterator<Item = Self::Item>>| {
                    let mut hasher = MetroHasher::default();
                    index.hash(&mut hasher);
                    let mut rand = utils::random::get_default_rng_from_seed(hasher.finish());
                    let mut position = rand.gen_range(0, num_partitions);
                    Box::new(items.map(move |t| {
                        // Note that the hash code of the key will just be the key itself.
                        // The HashPartitioner will mod it with the number of total partitions.
                        position += 1;
                        (position, t)
                    })) as Box<dyn Iterator<Item = (usize, Self::Item)>>
                }
            );
            let map_steep: SerArc<dyn Rdd<Item = (usize, Self::Item)>> =
                SerArc::new(MapPartitionsRdd::new(self.get_rdd(), distributed_partition));
            let partitioner = Box::new(HashPartitioner::<usize>::new(num_partitions));
            self.get_context().add_num(1);
            let map_steep_part = map_steep.partition_by_key(partitioner);
            self.get_context().add_num(1);
            let coalesced =
                SerArc::new(CoalescedRdd::new(Arc::new(map_steep_part), num_partitions));
            self.get_context().add_num(1);
            coalesced
        } else {
            self.get_context().add_num(4);
            SerArc::new(CoalescedRdd::new(self.get_rdd(), num_partitions))
        }
    }

    fn collect(&self) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        let cl = Fn!(|(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| iter.collect::<Vec<Self::Item>>());
        let results = self.get_context().run_job(self.get_rdd(), None, cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::Item>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
    }

    #[track_caller]
    fn secure_collect(&self) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        let cl = Fn!(|(_, iter): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| iter.collect::<Vec<ItemE>>());
        let results = self.get_context().run_job(self.get_rdd(), None, cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<ItemE>| a + b.len());
        let result = results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            });

        Ok(Text::new(None, Some(result)))
    }

    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let context = self.get_context();
        let counting_func = Fn!(|(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| { iter.count() as u64 });
        Ok(context
            .run_job(self.get_rdd(), None, counting_func)?
            .into_iter()
            .sum())
    }

    #[track_caller]
    fn secure_count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let ctx = self.get_context();
        let loc = Location::caller();
        let action_id = ctx.new_op_id(loc);
        let action_id_c = action_id.clone();
        let cur_rdd_id = self.get_rdd_id();
        let cl = Fn!(move |(_, iter): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| {
            let data = iter.collect::<Vec<ItemE>>();
            let result_ptr = wrapper_action(data, cur_rdd_id, action_id_c, true);
            let partial_res = get_encrypted_data::<u64>(
                action_id,
                DepInfo::padding_new(4),
                result_ptr as *mut u8,
            );
            *partial_res
        });
        let data = self
            .get_context()
            .run_job(self.get_rdd(), None, cl)?
            .into_iter()
            .flatten()
            .collect::<Vec<u64>>();
        let res = data.into_iter().sum::<u64>();
        Ok(res)
    }

    /// Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
    #[track_caller]
    fn count_by_value(&self) -> SerArc<dyn Rdd<Item = (Self::Item, u64)>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        let mapped = self.map(Fn!(|x| (x, 1u64)));
        self.get_context().add_num(1);
        mapped.reduce_by_key(
            Box::new(Fn!(|(x, y)| x + y)) as Box<dyn Func((u64, u64)) -> u64>,
            self.number_of_splits(),
        )
    }

    /// Approximate version of `count_by_value`.
    ///
    /// # Arguments
    /// * `timeout` - maximum time to wait for the job, in milliseconds
    /// * `confidence` - the desired statistical confidence in the result
    fn count_by_value_aprox(
        &self,
        timeout: Duration,
        confidence: Option<f64>,
    ) -> Result<PartialResult<HashMap<Self::Item, BoundedDouble>>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        let confidence = if let Some(confidence) = confidence {
            confidence
        } else {
            0.95
        };
        assert!(0.0 <= confidence && confidence <= 1.0);

        let count_partition = Fn!(|(_ctx, (iter, _)): (
            TaskContext,
            (
                Box<dyn Iterator<Item = Self::Item>>,
                Box<dyn Iterator<Item = ItemE>>,
            )
        )|
         -> HashMap<Self::Item, usize> {
            let mut map = HashMap::new();
            iter.for_each(|e| {
                *map.entry(e).or_insert(0) += 1;
            });
            map
        });

        let evaluator = GroupedCountEvaluator::new(self.number_of_splits(), confidence);
        let rdd = self.get_rdd();
        rdd.register_op_name("count_by_value_approx");
        self.get_context()
            .run_approximate_job(count_partition, rdd, None, evaluator, timeout)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        let mapped = self.map(Fn!(|x| (Some(x), None)));
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Fn!(|(_x, y)| y), num_partitions);
        self.get_context().add_num(1);
        reduced_by_key.map(Fn!(|x: (Option<Self::Item>, Option<Self::Item>)| {
            let (x, _y) = x;
            x.unwrap()
        }))
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
    {
        self.distinct_with_num_partitions(self.number_of_splits())
    }

    /// Return the first element in this RDD.
    fn first(&self) -> Result<Self::Item>
    where
        Self: Sized,
    {
        if let Some(result) = self.take(1)?.into_iter().next() {
            Ok(result)
        } else {
            Err(Error::UnsupportedOperation("empty collection"))
        }
    }

    /// Return a new RDD that has exactly num_partitions partitions.
    ///
    /// Can increase or decrease the level of parallelism in this RDD. Internally, this uses
    /// a shuffle to redistribute data.
    ///
    /// If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
    /// which can avoid performing a shuffle.
    #[track_caller]
    fn repartition(&self, num_partitions: usize) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        self.coalesce(num_partitions, true)
    }

    /// Take the first num elements of the RDD. It works by first scanning one partition, and use the
    /// results from that partition to estimate the number of additional partitions needed to satisfy
    /// the limit.
    ///
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    fn take(&self, num: usize) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        // TODO: in original spark this is configurable; see rdd/RDD.scala:1397
        // Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
        const SCALE_UP_FACTOR: f64 = 2.0;
        if num == 0 {
            return Ok(vec![]);
        }
        let mut buf = vec![];
        let total_parts = self.number_of_splits() as u32;
        let mut parts_scanned = 0_u32;
        while buf.len() < num && parts_scanned < total_parts {
            // The number of partitions to try in this iteration. It is ok for this number to be
            // greater than total_parts because we actually cap it at total_parts in run_job.
            let mut num_parts_to_try = 1u32;
            let left = num - buf.len();
            if parts_scanned > 0 {
                // If we didn't find any rows after the previous iteration, quadruple and retry.
                // Otherwise, interpolate the number of partitions we need to try, but overestimate
                // it by 50%. We also cap the estimation in the end.
                let parts_scanned = f64::from(parts_scanned);
                num_parts_to_try = if buf.is_empty() {
                    (parts_scanned * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let num_parts_to_try =
                        (1.5 * left as f64 * parts_scanned / (buf.len() as f64)).ceil();
                    num_parts_to_try.min(parts_scanned * SCALE_UP_FACTOR) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partion =
                Fn!(move |(iter, _): (
                    Box<dyn Iterator<Item = Self::Item>>,
                    Box<dyn Iterator<Item = ItemE>>
                )| { iter.take(left).collect::<Vec<Self::Item>>() });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdd(),
                None,
                take_from_partion,
                partitions,
            )?;

            res.into_iter().for_each(|r| {
                let take = num - buf.len();
                buf.extend(r.into_iter().take(take));
            });

            parts_scanned += num_partitions;
        }

        Ok(buf)
    }

    #[track_caller]
    fn secure_take(&self, num: usize) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        const SCALE_UP_FACTOR: f64 = 2.0;
        let op_id = self.get_op_id();
        if num == 0 {
            return Ok(Text::new(None, Some(vec![])));
        }

        let mut buf = vec![];
        let mut count = 0;
        let total_parts = self.number_of_splits() as u32;
        let mut parts_scanned = 0_u32;
        while count < num && parts_scanned < total_parts {
            let mut num_parts_to_try = 1u32;
            let left = num - count;
            if parts_scanned > 0 {
                let parts_scanned = f64::from(parts_scanned);
                num_parts_to_try = if count == 0 {
                    (parts_scanned * SCALE_UP_FACTOR).ceil() as u32
                } else {
                    let num_parts_to_try =
                        (1.5 * left as f64 * parts_scanned / (count as f64)).ceil();
                    num_parts_to_try.min(parts_scanned * SCALE_UP_FACTOR) as u32
                };
            }

            let partitions: Vec<_> = (parts_scanned as usize
                ..total_parts.min(parts_scanned + num_parts_to_try) as usize)
                .collect();
            let num_partitions = partitions.len() as u32;
            let take_from_partition = Fn!(move |(_, iter): (
                Box<dyn Iterator<Item = Self::Item>>,
                Box<dyn Iterator<Item = ItemE>>
            )| {
                let data = iter.collect::<Vec<ItemE>>();
                let (partial, _) = wrapper_take(op_id, &data, left);
                partial
            });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdd(),
                None,
                take_from_partition,
                partitions,
            )?;
            res.into_iter().for_each(|r| {
                let should_take = num - count;
                if should_take != 0 {
                    let (mut temp, have_take) = wrapper_take(op_id, &r, should_take);
                    count += have_take;
                    buf.append(&mut temp);
                }
            });

            parts_scanned += num_partitions;
        }

        Ok(Text::new(None, Some(buf)))
    }

    /// Randomly splits this RDD with the provided weights.
    #[track_caller]
    fn random_split(
        &self,
        weights: Vec<f64>,
        seed: Option<u64>,
    ) -> Vec<SerArc<dyn Rdd<Item = Self::Item>>>
    where
        Self: Sized,
    {
        let sum: f64 = weights.iter().sum();
        assert!(
            weights.iter().all(|&x| x >= 0.0),
            format!("Weights must be nonnegative, but got {:?}", weights)
        );
        assert!(
            sum > 0.0,
            format!("Sum of weights must be positive, but got {:?}", weights)
        );

        let seed_val: u64 = seed.unwrap_or(rand::random::<u64>());

        let mut full_bounds = vec![0.0f64];
        let bounds = weights
            .into_iter()
            .map(|weight| weight / sum)
            .scan(0.0f64, |state, x| {
                *state = *state + x;
                Some(*state)
            });
        full_bounds.extend(bounds);

        let mut splitted_rdds: Vec<SerArc<dyn Rdd<Item = Self::Item>>> = Vec::new();

        for bound in full_bounds.windows(2) {
            let (lower_bound, upper_bound) = (bound[0], bound[1]);
            let func = Fn!(move |index: usize,
                                 partition: Box<dyn Iterator<Item = Self::Item>>|
                  -> Box<dyn Iterator<Item = Self::Item>> {
                let bcs = Arc::new(BernoulliCellSampler::new(lower_bound, upper_bound, false))
                    as Arc<dyn RandomSampler<Self::Item>>;

                let sampler_func = bcs.get_sampler(Some(seed_val + index as u64));

                Box::new(sampler_func(partition).into_iter())
            });
            //TODO decide whether to add_num
            let rdd = SerArc::new(MapPartitionsRdd::new(self.get_rdd(), func));
            splitted_rdds.push(rdd.clone());
        }

        splitted_rdds
    }

    /// Return a sampled subset of this RDD.
    ///
    /// # Arguments
    ///
    /// * `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    /// * `fraction` - expected size of the sample as a fraction of this RDD's size
    /// ** if without replacement: probability that each element is chosen; fraction must be [0, 1]
    /// ** if with replacement: expected number of times each element is chosen; fraction must be greater than or equal to 0
    /// * seed for the random number generator
    ///
    /// # Notes
    ///
    /// This is NOT guaranteed to provide exactly the fraction of the count of the given RDD.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    #[track_caller]
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let rdd = SerArc::new(PartitionwiseSampledRdd::new(self.get_rdd(), sampler, true));
        if rdd.get_secure() {
            wrapper_set_sampler(rdd.get_op_id(), with_replacement, fraction);
        }
        rdd
    }

    /// Return a fixed-size sampled subset of this RDD in an array.
    ///
    /// # Arguments
    ///
    /// `with_replacement` - can elements be sampled multiple times (replaced when sampled out)
    ///
    /// # Notes
    ///
    /// This method should only be used if the resulting array is expected to be small,
    /// as all the data is loaded into the driver's memory.
    ///
    /// Replacement requires extra allocations due to the nature of the used sampler (Poisson distribution).
    /// This implies a performance penalty but should be negligible unless fraction and the dataset are rather large.
    #[track_caller]
    fn take_sample(
        &self,
        with_replacement: bool,
        num: u64,
        seed: Option<u64>,
    ) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        const NUM_STD_DEV: f64 = 10.0f64;
        const REPETITION_GUARD: u8 = 100;
        // TODO: this could be const eval when the support is there for the necessary functions
        let max_sample_size = std::u64::MAX - (NUM_STD_DEV * (std::u64::MAX as f64).sqrt()) as u64;
        assert!(num <= max_sample_size);

        if num == 0 {
            return Ok(vec![]);
        }

        let initial_count = self.count()?;

        if initial_count == 0 {
            return Ok(vec![]);
        }

        // The original implementation uses java.util.Random which is a LCG pseudorng,
        // not cryptographically secure and some problems;
        // Here we choose Pcg64, which is a proven good performant pseudorng although without
        // strong cryptographic guarantees, which ain't necessary here.
        let mut rng = if let Some(seed) = seed {
            rand_pcg::Pcg64::seed_from_u64(seed)
        } else {
            // PCG with default specification state and stream params
            utils::random::get_default_rng()
        };

        if !with_replacement && num >= initial_count {
            let mut sample = self.collect()?;
            utils::randomize_in_place(&mut sample, &mut rng);
            self.get_context().add_num(1);
            Ok(sample)
        } else {
            let fraction = utils::random::compute_fraction_for_sample_size(
                num,
                initial_count,
                with_replacement,
            );
            let mut samples = self.sample(with_replacement, fraction).collect()?;

            // If the first sample didn't turn out large enough, keep trying to take samples;
            // this shouldn't happen often because we use a big multiplier for the initial size.
            let mut num_iters = 0;
            while samples.len() < num as usize && num_iters < REPETITION_GUARD {
                log::warn!(
                    "Needed to re-sample due to insufficient sample size. Repeat #{}",
                    num_iters
                );
                samples = self.sample(with_replacement, fraction).collect()?;
                num_iters += 1;
            }

            if num_iters >= REPETITION_GUARD {
                panic!("Repeated sampling {} times; aborting", REPETITION_GUARD)
            }

            utils::randomize_in_place(&mut samples, &mut rng);
            Ok(samples.into_iter().take(num as usize).collect::<Vec<_>>())
        }
    }

    #[track_caller]
    fn secure_take_sample(
        &self,
        with_replacement: bool,
        num: u64,
        seed: Option<u64>,
    ) -> Result<Text<Vec<Self::Item>, Vec<ItemE>>>
    where
        Self: Sized,
    {
        const NUM_STD_DEV: f64 = 10.0f64;
        const REPETITION_GUARD: u8 = 100;
        // TODO: this could be const eval when the support is there for the necessary functions
        let max_sample_size = std::u64::MAX - (NUM_STD_DEV * (std::u64::MAX as f64).sqrt()) as u64;
        assert!(num <= max_sample_size);

        if num == 0 {
            return Ok(Text::new(None, Some(vec![])));
        }

        let initial_count = self.secure_count()?;

        if initial_count == 0 {
            return Ok(Text::new(None, Some(vec![])));
        }

        if !with_replacement && num >= initial_count {
            let mut sample = self.secure_collect()?;
            *sample.get_ct_mut() = wrapper_randomize_in_place(
                self.get_op_id(),
                sample.get_ct_ref(),
                seed,
                initial_count,
            );
            self.get_context().add_num(1);
            Ok(sample)
        } else {
            let fraction = utils::random::compute_fraction_for_sample_size(
                num,
                initial_count,
                with_replacement,
            );

            let mut sample_rdd = self.sample(with_replacement, fraction);
            sample_rdd.cache();
            let mut samples_enc = sample_rdd.secure_collect()?;
            let mut len = sample_rdd.secure_count().unwrap();
            let mut num_iters = 0;
            while len < num && num_iters < REPETITION_GUARD {
                log::warn!(
                    "Needed to re-sample due to insufficient sample size. Repeat #{}",
                    num_iters,
                );
                sample_rdd = self.sample(with_replacement, fraction);
                sample_rdd.cache();
                samples_enc = sample_rdd.secure_collect()?;
                len = sample_rdd.secure_count().unwrap();
                num_iters += 1;
            }

            if num_iters >= REPETITION_GUARD {
                panic!("Repeated sampling {} times; aborting", REPETITION_GUARD)
            }

            *samples_enc.get_ct_mut() = wrapper_randomize_in_place(
                sample_rdd.get_op_id(),
                samples_enc.get_ct_ref(),
                seed,
                num,
            );
            Ok(samples_enc)
        }
    }

    /// Applies a function f to all elements of this RDD.
    fn for_each<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Self::Item),
        Self: Sized,
    {
        let func = Fn!(move |(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| iter.for_each(&func));
        self.get_context().run_job(self.get_rdd(), None, func)
    }

    /// Applies a function f to each partition of this RDD.
    fn for_each_partition<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>),
        Self: Sized + 'static,
    {
        let func = Fn!(move |(iter, _): (
            Box<dyn Iterator<Item = Self::Item>>,
            Box<dyn Iterator<Item = ItemE>>
        )| (&func)(iter));
        self.get_context().run_job(self.get_rdd(), None, func)
    }

    #[track_caller]
    fn union(&self, other: Arc<dyn Rdd<Item = Self::Item>>) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
    {
        SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            other,
        ]))
    }

    #[track_caller]
    fn zip<S>(&self, second: Arc<dyn Rdd<Item = S>>) -> SerArc<dyn Rdd<Item = (Self::Item, S)>>
    where
        Self: Clone,
        S: Data,
    {
        SerArc::new(ZippedPartitionsRdd::new(
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            second,
        ))
    }

    #[track_caller]
    fn intersection<T>(&self, other: Arc<T>) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        self.intersection_with_num_partitions(other, self.number_of_splits())
    }

    /// subtract function, same as the one found in apache spark
    /// example of subtract can be found in subtract.rs
    /// performs a full outer join followed by and intersection with self to get subtraction.
    #[track_caller]
    fn subtract<T>(&self, other: Arc<T>) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        self.subtract_with_num_partition(other, self.number_of_splits())
    }

    //Both should have consistent security guarantee (encrypted columns)
    #[track_caller]
    fn subtract_with_num_partition<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )))
            .clone();
        self.get_context().add_num(1);
        let mapped = self.map(Box::new(Fn!(|x| -> (Self::Item, Option<Self::Item>) {
            (x, None)
        })));
        self.get_context().add_num(1);
        let cogrouped = mapped.cogroup(
            other,
            Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        let mapped = cogrouped.map(Box::new(Fn!(|(x, (v1, v2)): (
            Self::Item,
            (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
        )|
         -> Option<Self::Item> {
            if (v1.len() >= 1) ^ (v2.len() >= 1) {
                Some(x)
            } else {
                None
            }
        })));
        self.get_context().add_num(1);
        let rdd = mapped.map_partitions(Box::new(Fn!(|iter: Box<
            dyn Iterator<Item = Option<Self::Item>>,
        >|
         -> Box<
            dyn Iterator<Item = Self::Item>,
        > {
            Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                as Box<dyn Iterator<Item = Self::Item>>
        })));
        self.get_context().add_num(1);
        let subtraction = self.intersection(Arc::new(rdd));
        (&*subtraction).register_op_name("subtraction");
        subtraction
    }

    #[track_caller]
    fn intersection_with_num_partitions<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = Self::Item>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        T: Rdd<Item = Self::Item> + Sized,
    {
        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )))
            .clone();
        self.get_context().add_num(1);
        let mapped = self.map(Box::new(Fn!(|x| -> (Self::Item, Option<Self::Item>) {
            (x, None)
        })));
        self.get_context().add_num(1);
        let cogrouped = mapped.cogroup(
            other,
            Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        let mapped = cogrouped.map(Box::new(Fn!(|(x, (v1, v2)): (
            Self::Item,
            (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
        )|
         -> Option<Self::Item> {
            if v1.len() >= 1 && v2.len() >= 1 {
                Some(x)
            } else {
                None
            }
        })));
        self.get_context().add_num(1);
        let rdd = mapped.map_partitions(Box::new(Fn!(|iter: Box<
            dyn Iterator<Item = Option<Self::Item>>,
        >|
         -> Box<
            dyn Iterator<Item = Self::Item>,
        > {
            Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                as Box<dyn Iterator<Item = Self::Item>>
        })));
        (&*rdd).register_op_name("intersection");
        rdd
    }

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the resulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    #[track_caller]
    fn group_by<K, F>(&self, func: F) -> SerArc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: Data + Hash + Eq,
        F: SerFunc(&Self::Item) -> K,
    {
        self.group_by_with_num_partitions(func, self.number_of_splits())
    }

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the resulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    #[track_caller]
    fn group_by_with_num_partitions<K, F>(
        &self,
        func: F,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: Data + Hash + Eq,
        F: SerFunc(&Self::Item) -> K,
    {
        let mapped = self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })));
        self.get_context().add_num(1);
        mapped.group_by_key(num_splits)
    }

    /// Return an RDD of grouped items. Each group consists of a key and a sequence of elements
    /// mapping to that key. The ordering of elements within each group is not guaranteed, and
    /// may even differ each time the resulting RDD is evaluated.
    ///
    /// # Notes
    ///
    /// This operation may be very expensive. If you are grouping in order to perform an
    /// aggregation (such as a sum or average) over each key, using `aggregate_by_key`
    /// or `reduce_by_key` will provide much better performance.
    #[track_caller]
    fn group_by_with_partitioner<K, F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, Vec<Self::Item>)>>
    where
        Self: Sized,
        K: Data + Hash + Eq,
        F: SerFunc(&Self::Item) -> K,
    {
        let mapped = self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })));
        self.get_context().add_num(1);
        mapped.group_by_key_using_partitioner(partitioner)
    }

    /// Approximate version of count() that returns a potentially incomplete result
    /// within a timeout, even if not all tasks have finished.
    ///
    /// The confidence is the probability that the error bounds of the result will
    /// contain the true value. That is, if count_approx were called repeatedly
    /// with confidence 0.9, we would expect 90% of the results to contain the
    /// true count. The confidence must be in the range [0,1] or an exception will
    /// be thrown.
    ///
    /// # Arguments
    /// * `timeout` - maximum time to wait for the job, in milliseconds
    /// * `confidence` - the desired statistical confidence in the result
    fn count_approx(
        &self,
        timeout: Duration,
        confidence: Option<f64>,
    ) -> Result<PartialResult<BoundedDouble>>
    where
        Self: Sized,
    {
        let confidence = if let Some(confidence) = confidence {
            confidence
        } else {
            0.95
        };
        assert!(0.0 <= confidence && confidence <= 1.0);

        let count_elements = Fn!(|(_ctx, (iter, _)): (
            TaskContext,
            (
                Box<dyn Iterator<Item = Self::Item>>,
                Box<dyn Iterator<Item = ItemE>>,
            )
        )|
         -> usize { iter.count() });

        let evaluator = CountEvaluator::new(self.number_of_splits(), confidence);
        let rdd = self.get_rdd();
        rdd.register_op_name("count_approx");
        self.get_context()
            .run_approximate_job(count_elements, rdd, None, evaluator, timeout)
    }

    /// Creates tuples of the elements in this RDD by applying `f`.
    #[track_caller]
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn Rdd<Item = (T, Self::Item)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        self.map(Fn!(move |k: Self::Item| -> (T, Self::Item) {
            let t = (func)(&k);
            (t, k)
        }))
    }

    /// Check if the RDD contains no elements at all. Note that an RDD may be empty even when it
    /// has at least 1 partition.
    fn is_empty(&self) -> bool
    where
        Self: Sized,
    {
        self.number_of_splits() == 0 || self.take(1).unwrap().len() == 0
    }

    /// Returns the max element of this RDD.
    fn max(&self) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Ord,
    {
        let max_fn = Fn!(|x: Self::Item, y: Self::Item| x.max(y));

        self.reduce(max_fn)
    }

    /// Returns the min element of this RDD.
    fn min(&self) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Ord,
    {
        let min_fn = Fn!(|x: Self::Item, y: Self::Item| x.min(y));
        self.reduce(min_fn)
    }

    /// Returns the first k (largest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [take_ordered](#take_ordered).
    /// # Notes
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    #[track_caller]
    fn top(&self, num: usize) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Ord,
    {
        let mapped = self.map(Fn!(|x| Reverse(x)));
        self.get_context().add_num(1);
        Ok(mapped.take_ordered(num)?.into_iter().map(|x| x.0).collect())
    }

    /// Returns the first k (smallest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [top()](#top).
    /// # Notes
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
    #[track_caller]
    fn take_ordered(&self, num: usize) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
        Self::Item: Data + Ord,
    {
        if num == 0 {
            Ok(vec![])
        } else {
            let first_k_func = Fn!(move |partition: Box<dyn Iterator<Item = Self::Item>>|
                -> Box<dyn Iterator<Item = BoundedPriorityQueue<Self::Item>>>  {
                    let mut queue = BoundedPriorityQueue::new(num);
                    partition.for_each(|item: Self::Item| queue.append(item));
                    Box::new(std::iter::once(queue))
            });

            let queue = self
                .map_partitions(first_k_func)
                .reduce(Fn!(
                    move |queue1: BoundedPriorityQueue<Self::Item>,
                          queue2: BoundedPriorityQueue<Self::Item>|
                          -> BoundedPriorityQueue<Self::Item> {
                        queue1.merge(queue2)
                    }
                ))?
                .ok_or_else(|| Error::Other)?
                as BoundedPriorityQueue<Self::Item>;

            Ok(queue.into())
        }
    }
}
