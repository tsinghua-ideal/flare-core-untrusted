use core::panic::Location;
use std::any::{Any, TypeId};
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher, HashMap};
use std::convert::TryInto;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufWriter, Write};
use std::mem::forget;
use std::net::Ipv4Addr;
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::{Arc, RwLock, Weak,
    atomic::{self, AtomicBool, AtomicUsize},
    mpsc::{sync_channel, SyncSender, Receiver} 
    };
use std::thread::{JoinHandle, self};
use std::time::{Duration, Instant};


use crate::context::Context;
use crate::dependency::{Dependency, DepInfo};
use crate::env::{self, BOUNDED_MEM_CACHE, Env};
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

use aes_gcm::Aes128Gcm;
use aes_gcm::aead::{Aead, NewAead, generic_array::GenericArray};
use fasthash::MetroHasher;
use once_cell::sync::Lazy;
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

type CT<T, TE> = Text<T, TE>;
pub type OText<T> = Text<T, T>; 

static immediate_cout: bool = true;
pub static STAGE_LOCK: Lazy<StageLock> = Lazy::new(|| StageLock::new());
pub const MAX_ENC_BL: usize = 1024;

extern "C" {
    pub fn secure_execute(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64,
        rdd_ids: *const u8,
        op_ids: *const u8,
        cache_meta: CacheMeta,
        dep_info: DepInfo,
        input: Input,
        captured_vars: *const u8,
    ) -> sgx_status_t;
    pub fn pre_merge(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64,
        op_id: OpId,
        dep_info: DepInfo,
        input: Input,
    ) -> sgx_status_t;
    pub fn exploit_spec_oppty(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64, 
        op_ids: *const u8,
        part_nums: *const u8,
        cache_meta: CacheMeta,
        dep_info: DepInfo,
        spec_identifier: *mut usize,  
    ) -> sgx_status_t;
    pub fn free_spec_seq(
        eid: sgx_enclave_id_t,
        input: *mut u8,
    ) -> sgx_status_t;
    pub fn spec_execute(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64, 
        spec_call_seq: usize,
        cache_meta: CacheMeta, 
        hash_ops: *mut u64,
    ) -> sgx_status_t;
    pub fn free_res_enc(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        input: *mut u8,
        is_spec: u8,
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
        is_spec: u8,
    ) -> sgx_status_t;
    pub fn clone_out(
        eid: sgx_enclave_id_t,
        op_id: OpId,
        dep_info: DepInfo,
        p_out: usize,
        p_data_enc: *mut u8,
        is_spec: u8,
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
    pub fn tail_compute(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        input: *mut u8,
    ) -> sgx_status_t;
    pub fn free_tail_info(
        eid: sgx_enclave_id_t,
        input: *mut u8, 
    ) -> sgx_status_t;
    pub fn probe_caching(
        eid: sgx_enclave_id_t,
        ret_val: *mut usize,
        rdd_id: usize,
        part: usize,
    ) -> sgx_status_t;
    pub fn finish_probe_caching(
        eid: sgx_enclave_id_t,
        cached_sub_parts: *mut u8,
    ) -> sgx_status_t;
}

#[no_mangle]
pub unsafe extern "C" fn sbrk_o(increment: usize) -> *mut c_void {
    libc::sbrk(increment as intptr_t)
}

#[no_mangle]
pub unsafe extern "C" fn ocall_cache_to_outside(rdd_id: usize,
    part_id: usize,
    sub_part_id: usize,
    data_ptr: usize,
) -> u8 {
    //need to clone from memory alloced by ucmalloc to memory alloced by default allocator
    Env::get().cache_tracker.put_sdata((rdd_id, part_id, sub_part_id), data_ptr as *mut u8, 0);
    0  //need to revise
}

#[no_mangle]
pub unsafe extern "C" fn ocall_cache_from_outside(rdd_id: usize,
    part_id: usize,
    sub_part_id: usize,
) -> usize {
    let res = Env::get().cache_tracker.get_sdata((rdd_id, part_id, sub_part_id));
    match res {
        Some(val) => val,
        None => 0,
    }
}

#[no_mangle]
pub unsafe extern "C" fn ocall_get_addr_map_len() -> usize {
    env::ADDR_MAP_LEN.load(atomic::Ordering::SeqCst)
}

pub fn default_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub fn wrapper_secure_execute<T>(
    rdd_ids: &Vec<usize>, 
    op_ids: &Vec<OpId>,
    cache_meta: CacheMeta,
    dep_info: DepInfo, 
    data: &T,
    lower: &mut Vec<usize>,
    upper: &mut Vec<usize>,
    upper_bound: &Vec<usize>,
    block_len: usize,
    to_set_usage: usize,
    captured_vars: &HashMap<usize, Vec<Vec<u8>>>,
) -> (usize, (f64, (f64, f64)))  //(result_ptr, (time_for_sub_part_computation, last_memory_usage_per_thread, max_memory_usage_per_thread)) 
where
    T: Construct + Data,
{
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut result_bl_ptr: usize = 0;
    let tid: u64 = thread::current().id().as_u64().into();
    let mut init_mem_usage = to_set_usage;
    let mut last_mem_usage = 0;
    let mut max_mem_usage = 0;
    let input = Input::new(data, lower, upper, upper_bound, block_len, &mut init_mem_usage, &mut last_mem_usage, &mut max_mem_usage);
    let now_comp = Instant::now();
    let sgx_status = unsafe {
        secure_execute(
            eid,
            &mut result_bl_ptr,
            tid,
            rdd_ids as *const Vec<usize> as *const u8,
            op_ids as *const Vec<OpId> as *const u8,
            cache_meta,
            dep_info,
            input,
            captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    let dur_comp = now_comp.elapsed().as_nanos() as f64 * 1e-9;
    (result_bl_ptr, (dur_comp, (last_mem_usage as f64, max_mem_usage as f64)))
}

pub fn dynamic_subpart_meta(
    time_comp: f64,
    max_mem_usage: f64,
    block_len_: &Arc<AtomicUsize>,
    slopes: &mut Vec<f64>,
    fresh_slope: &Arc<AtomicBool>,
    num_splits: usize,
) {
    let tid: u64 = thread::current().id().as_u64().into();
    let limit_per_partition = ((80*(1<<20)) as f64)/(num_splits as f64);  //about 80MB/num_splits
    let mut block_len = block_len_.load(atomic::Ordering::SeqCst);
    if fresh_slope.compare_and_swap(true, false, atomic::Ordering::SeqCst) {
        slopes.clear();   //for unique-partitioner-union, it may affect other cases but does not matter
    }
    let k = time_comp/(max_mem_usage/((1<<30) as f64));   // s/GB
    if slopes.is_empty() {
        slopes.push(k);
        block_len += 1;
    } else {
        let avg_k = slopes.iter().sum::<f64>()/(slopes.len() as f64);
        if k <= avg_k * 0.8 && max_mem_usage < limit_per_partition {
            slopes.clear();
            slopes.push(k);
            block_len += 1;
        } else if k <= avg_k * 1.2 && max_mem_usage < limit_per_partition {
            slopes.push(k);
            block_len += 1;
        } else if k <= avg_k * 1.5 && block_len > 1 {
            block_len -= 1;
        } else {
            block_len = (block_len + 1) / 2;
        }
    }
    //block_len = 1; //mark
    println!("tid: {:?}, limit_per_partition {:?}B, block_len: {:?}", tid, limit_per_partition, block_len);
    block_len_.store(block_len, atomic::Ordering::SeqCst);
}

pub fn wrapper_pre_merge<T: Data>(
    op_id: OpId,
    mut data: Vec<Vec<T>>,
    dep_info: DepInfo,
    num_splits: usize,
) -> Vec<Vec<T>> {
    let merge_factor = 16;
    fn ecall_pre_merge<T: Data>(op_id: OpId, data: Vec<Vec<T>>, dep_info: DepInfo, num_sub_part: usize, num_splits: usize) -> Vec<Vec<T>> {
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let tid: u64 = thread::current().id().as_u64().into();
        let mut lower = vec![0; num_sub_part];
        let mut upper = vec![1; num_sub_part];
        let upper_bound = data.iter().map(|sub_part| sub_part.len()).collect::<Vec<_>>();
        let mut result = Vec::new();
        let block_len = Arc::new(AtomicUsize::new(1));
        let mut slopes = Vec::new();
        let fresh_slope = Arc::new(AtomicBool::new(false));
        let mut to_set_usage = 0;
        while lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
            upper = upper.iter()
                .zip(upper_bound.iter())
                .map(|(l, ub)| std::cmp::min(*l, *ub))
                .collect::<Vec<_>>();
            let mut result_ptr: usize = 0;
            let mut init_mem_usage = to_set_usage;
            let mut last_mem_usage = 0;
            let mut max_mem_usage = 0;
            let input = Input::new(&data, &mut lower, &mut upper, &upper_bound, block_len.load(atomic::Ordering::SeqCst), &mut init_mem_usage, &mut last_mem_usage, &mut max_mem_usage);
            let now_comp = Instant::now();
            let sgx_status = unsafe {
                pre_merge(
                    eid,
                    &mut result_ptr,
                    tid,
                    op_id,
                    dep_info,
                    input,
                )
            };
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };
            let dur_comp = now_comp.elapsed().as_nanos() as f64 * 1e-9;
            dynamic_subpart_meta(dur_comp, max_mem_usage as f64, &block_len, &mut slopes, &fresh_slope, num_splits);
            let mut result_bl = get_encrypted_data::<Vec<T>>(op_id, dep_info, result_ptr as *mut u8, false);
            assert!(result_bl.len() == 1);
            result.append(&mut result_bl[0]);
            lower = lower.iter()
                .zip(upper_bound.iter())
                .map(|(l, ub)| std::cmp::min(*l, *ub))
                .collect::<Vec<_>>();
            to_set_usage = last_mem_usage;
        }
        vec![result]
    }
    let now = Instant::now();
    let mut n = data.len();
    let mut r = Vec::new();
    while n > merge_factor {
        let m = (n-1)/merge_factor+1;
        for i in 0..m {
            let start = i*merge_factor;
            let end = std::cmp::min((i+1)*merge_factor, n);
            r.append(&mut ecall_pre_merge(op_id, (&data[start..end]).to_vec(), dep_info, end-start, num_splits));    
        }
        data = r;
        n = data.len();
        r = Vec::new();
    
    }
    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
    println!("pre_merge took {:?}s", dur);
    data
}

pub fn wrapper_exploit_spec_oppty(
    op_ids: &Vec<OpId>,
    split_nums: &Vec<usize>,
    cache_meta: CacheMeta,
    dep_info: DepInfo
) -> Option<((Vec<usize>, Vec<OpId>), usize)> {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut retval: usize = 0;
    let mut spec_identifier = 0;
    let tid: u64 = thread::current().id().as_u64().into();
    let sgx_status = unsafe {
        exploit_spec_oppty(
            eid,
            &mut retval,
            tid,
            op_ids as *const Vec<OpId> as *const u8,
            split_nums as *const Vec<usize> as *const u8,
            cache_meta,
            dep_info,
            &mut spec_identifier
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    if retval != 0 {
        let res_ = unsafe{ Box::from_raw(retval as *mut (Vec<usize>, Vec<OpId>)) };
        let res = res_.clone();
        forget(res_);
        let sgx_status = unsafe {
            free_spec_seq(
                eid,
                retval as *mut u8,
            )
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        //return None;   //test non-spec case
        Some((*res, spec_identifier))
    } else {
        None
    }

}

pub fn wrapper_spec_execute(
    spec_call_seq: &Option<((Vec<usize>, Vec<OpId>), usize)>,
    cache_meta: CacheMeta,
) {
    if spec_call_seq.is_none() {
        println!("no speculative execution");
        return;
    }
    println!("begin speculative execution");
    let mut hash_ops: u64= 0;

    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut retval: usize = 0;
    let tid: u64 = thread::current().id().as_u64().into();
    let sgx_status = unsafe {
        spec_execute(
            eid,
            &mut retval,
            tid, 
            &spec_call_seq.as_ref().unwrap().0 as *const (Vec<usize>, Vec<OpId>) as usize,
            cache_meta,
            &mut hash_ops,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    //The first two args are not used in this case actually
    let res = get_encrypted_data::<Vec<u8>>(Default::default(), DepInfo::padding_new(1), retval as *mut u8, true);
    let key = (
        hash_ops,
        cache_meta.part_id,
        spec_call_seq.as_ref().unwrap().1,
    );

    if let Some(mut value) = env::SPEC_SHUFFLE_CACHE.get_mut(&key) {
        value.push(*res);
        return;
    }
    env::SPEC_SHUFFLE_CACHE.insert(key, vec![*res]);
}

pub fn wrapper_action<T: Data>(data: Vec<T>, rdd_id: usize, op_id: OpId) -> usize {
    let now = Instant::now();
    let tid: u64 = thread::current().id().as_u64().into();
    let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
    let rdd_ids = vec![rdd_id];
    let op_ids = vec![op_id];
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut init_mem_usage = 0;
    let mut last_mem_usage = 0;
    let mut max_mem_usage = 0;
    let input = Input::new(&data, &mut vec![0], &mut vec![data.len()], &vec![data.len()], usize::MAX, &mut init_mem_usage, &mut last_mem_usage, &mut max_mem_usage);
    let mut result_ptr: usize = 0;
    let sgx_status = unsafe {
        secure_execute(
            eid,
            &mut result_ptr,
            tid,
            &rdd_ids as *const Vec<usize> as *const u8,
            &op_ids as *const Vec<OpId> as *const u8,
            Default::default(),  //meaningless
            DepInfo::padding_new(3),   //3 is for reduce & fold & aggregate
            input,
            &captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
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
    T:  std::fmt::Debug 
        + Clone 
        + Serialize
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static 
{
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
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
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    let res = get_encrypted_data::<T>(op_id, DepInfo::padding_new(2), retval as *mut u8, false);
    *res
}

pub fn wrapper_set_sampler(op_id: OpId, with_replacement: bool, fraction: f64) {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let with_replacement = match with_replacement {
        true => 1,
        false => 0,
    };
    let sgx_status = unsafe {
        set_sampler(
            eid,
            op_id,
            with_replacement,
            fraction,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
}

pub fn wrapper_take<T: Data>(op_id: OpId, input: &Vec<T>, should_take: usize) -> (Vec<T>, usize) {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut retval: usize = 0;
    let mut have_take: usize = 0;
    let sgx_status = unsafe {
        etake(
            eid,
            &mut retval,
            op_id,
            input as *const Vec<T> as *const u8,
            should_take,
            &mut have_take
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    let res = get_encrypted_data::<T>(op_id, DepInfo::padding_new(2), retval as *mut u8, false);
    (*res, have_take)
}

pub fn wrapper_tail_compute(tail_info: &mut TailCompInfo) {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut p_new_tail_info: usize = 0;
    let sgx_status = unsafe {
        tail_compute(
            eid,
            &mut p_new_tail_info,
            tail_info as *mut TailCompInfo as *mut u8,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    let new_tail_info_ = unsafe{ Box::from_raw( p_new_tail_info as *mut u8 as *mut TailCompInfo) };
    let new_tail_info = new_tail_info_.clone();
    forget(new_tail_info_);

    let sgx_status = unsafe {
        free_tail_info(
            eid,
            p_new_tail_info as *mut u8,
        )
    };
    let _r = match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    *tail_info = *new_tail_info;
}

pub fn get_encrypted_data<T>(op_id: OpId, dep_info: DepInfo, p_data_enc: *mut u8, is_spec: bool) -> Box<Vec<T>> 
where
    T:  std::fmt::Debug 
        + Clone 
        + Serialize
        + serde::ser::Serialize
        + serde::de::DeserializeOwned
        + 'static
{
    let tid: u64 = thread::current().id().as_u64().into();
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let is_spec: u8 = match is_spec {
        false => 0,
        true => 1,
    };
    if immediate_cout {
        let res_ = unsafe{ Box::from_raw(p_data_enc as *mut Vec<T>) };
        let res = res_.clone();
        forget(res_);
        let sgx_status = unsafe { 
            free_res_enc(
                eid,
                op_id,
                dep_info,
                p_data_enc,
                is_spec,
            )
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
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
                p_data_enc,   //shuffle write
                is_spec,
            )
        };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => (),
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };

        let size_buf = unsafe{ Box::from_raw(size_buf_ptr) };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("get sketch {:?}s", dur);
        let now = Instant::now();
        let mut v: Vec<T> = Vec::new();
        let mut idx = Idx::new();
        v.recv(&size_buf, &mut idx);
        let ptr_out = Box::into_raw(Box::new(v)) as *mut u8 as usize; 
        let sgx_status = unsafe {
            clone_out(
                eid,
                op_id,
                dep_info,
                ptr_out,
                p_data_enc,
                is_spec,
            )
        };
        let v = unsafe { Box::from_raw(ptr_out as *mut u8 as *mut Vec<T>) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
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
    let res_ = unsafe{ Box::from_raw(data as *mut Vec<T>) };
    let res = res_.clone();
    forget(res_);
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let sgx_status = unsafe { 
        priv_free_res_enc(
            eid,
            op_id,
            DepInfo::padding_new(2), //default to 2, for cache should not appear at the end of stage
            data
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => {},
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
pub fn ser_encrypt<T>(pt: Vec<T>) -> Vec<u8> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut u8;
                Vec::from_raw_parts(ptr, len, cap)
            };
            encrypt(&rebuilt)
        },
        false => encrypt(bincode::serialize(&pt).unwrap().as_ref()),
    } 
}

#[inline(always)]
pub fn decrypt(ct: &[u8]) -> Vec<u8> {
    let key = GenericArray::from_slice(b"abcdefg hijklmn ");
    let cipher = Aes128Gcm::new(key);
    let nonce = GenericArray::from_slice(b"unique nonce");
    cipher.decrypt(nonce, ct).expect("decryption failure")
}

#[inline(always)]
pub fn ser_decrypt<T>(ct: Vec<u8>) -> Vec<T> 
where
    T: serde::ser::Serialize + serde::de::DeserializeOwned + 'static
{
    if ct.len() == 0 {
        return Vec::new();
    }
    match TypeId::of::<u8>() == TypeId::of::<T>() {
        true => {
            let pt = decrypt(&ct);
            let (ptr, len, cap) = pt.into_raw_parts();
            let rebuilt = unsafe {
                let ptr = ptr as *mut T;
                Vec::from_raw_parts(ptr, len, cap)
            };
            rebuilt
        },
        false => bincode::deserialize(decrypt(&ct).as_ref()).unwrap(),
    }
}

fn batch_encrypt<T, TE, FE>(mut data: Vec<T>, fe: FE) -> Vec<TE> 
where
    FE: Func(Vec<T>)->TE
{
    let mut len = data.len();
    let mut data_enc = Vec::with_capacity(len/MAX_ENC_BL+1);
    while len >= MAX_ENC_BL {
        len -= MAX_ENC_BL;
        let remain = data.split_off(MAX_ENC_BL);
        let input = data;
        data = remain;
        data_enc.push(fe(input));
    }
    if len != 0 {
        data_enc.push(fe(data));
    }
    data_enc
}

fn batch_decrypt<T, TE, FD>(data_enc: Vec<TE>, fd: FD) -> Vec<T> 
where
    FD: Func(TE)->Vec<T>
{
    let mut data = Vec::new();
    for block in data_enc {
        let mut pt = fd(block);
        data.append(&mut pt); //need to check security
    }
    data
}

//Return the cached or caching sub-partition number
pub fn cached_or_caching_in_enclave(rdd_id: usize, part: usize) -> BTreeSet<usize> {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let mut ret_val = 0;
    let sgx_status = unsafe {
        probe_caching(
            eid,
            &mut ret_val,
            rdd_id,
            part,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => (),
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    let cached_sub_parts_ = unsafe {
        Box::from_raw(ret_val as *mut u8 as *mut Vec<usize>)
    };
    let cached_sub_parts = cached_sub_parts_.clone();
    forget(cached_sub_parts_);
    let sgx_status = unsafe {
        finish_probe_caching(
            eid,
            ret_val as *mut u8,
        )
    };
    match sgx_status {
        sgx_status_t::SGX_SUCCESS => (),
        _ => {
            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
        },
    };
    cached_sub_parts.into_iter().collect()
}

pub fn secure_compute_cached(
    acc_arg: &mut AccArg,
    cur_rdd_id: usize, 
    tx: SyncSender<(usize, (usize, (f64, f64, usize)))>,
    captured_vars: HashMap<usize, Vec<Vec<u8>>>,
) -> Vec<JoinHandle<()>> {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let part_id = acc_arg.part_id;
    //check whether it is cached inside enclave
    let cached_sub_parts_in = cached_or_caching_in_enclave(cur_rdd_id, part_id);
    //check whether it is cached outside enclave
    let uncached_sub_parts = Env::get().cache_tracker
        .get_uncached_subpids(cur_rdd_id, part_id)
        .difference(&cached_sub_parts_in)
        .map(|x| *x)
        .collect::<BTreeSet<_>>();
    let mut cached_sub_parts_out = Env::get().cache_tracker
        .get_cached_subpids(cur_rdd_id, part_id)
        .difference(&cached_sub_parts_in)
        .map(|x| *x)
        .collect::<Vec<_>>();
    let mut cached_sub_parts = cached_sub_parts_in.into_iter().collect::<Vec<_>>();
    cached_sub_parts.append(&mut cached_sub_parts_out);
    // this cannot guarantee that cached data in enclave is used first
    /*
    cached_sub_parts = cached_sub_parts.union(&Env::get().cache_tracker.get_cached_subpids(cur_rdd_id, part_id))
        .map(|x| *x)
        .collect::<BTreeSet<_>>();
    */
    
    //println!("get_subpids {:?}, {:?}", cur_rdd_id, part_id);
    let subpids = Env::get().cache_tracker.get_subpids(cur_rdd_id, part_id);
    //println!("subpids = {:?}, cached = {:?}, uncached = {:?}", subpids, cached_sub_parts, uncached_sub_parts);
    assert_eq!(uncached_sub_parts.len() + cached_sub_parts.len(), subpids.len());

    acc_arg.cached_sub_parts = cached_sub_parts.clone().into_iter().collect::<BTreeSet<_>>();
    acc_arg.sub_parts_len = subpids.len();
    let mut handles = Vec::new();
    let cached_sub_parts_len = cached_sub_parts.len(); 
    if cached_sub_parts_len > 0 {
        acc_arg.set_cached_rdd_id(cur_rdd_id);
        // Compute based on cached values
        let rdd_ids = acc_arg.rdd_ids.clone();
        let op_ids = acc_arg.op_ids.clone();
        let split_nums = acc_arg.split_nums.clone();
        let mut cache_meta = acc_arg.to_cache_meta();
        let dep_info = acc_arg.dep_info;
        let eenter_lock = acc_arg.eenter_lock.clone();
        let acc_captured_size = acc_arg.acc_captured_size;

        let handle = std::thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            for (seq_id, sub_part) in cached_sub_parts.into_iter().enumerate() {
                let mut is_survivor = cached_sub_parts_len == subpids.len() &&  cached_sub_parts_len-1 == seq_id;
                //No need to get memory usage, for the sub-partition is fixed when using cached data
                let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                    &op_ids, 
                    &split_nums,
                    cache_meta, 
                    dep_info,
                );
                if spec_call_seq_ptr.is_some() {
                    is_survivor = true;
                }
                cache_meta.set_sub_part_id(sub_part);
                cache_meta.set_is_survivor(is_survivor);
                BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                let mut init_mem_usage = 0;
                let mut last_mem_usage = 0;
                let mut max_mem_usage = 0;
                let mut result_ptr: usize = 0;
                while eenter_lock.compare_and_swap(false, true, atomic::Ordering::SeqCst) {
                    //wait
                }
                let now_comp = Instant::now();
                let sgx_status = unsafe {
                    secure_execute(
                        eid,
                        &mut result_ptr,
                        tid,
                        &rdd_ids as *const Vec<usize> as *const u8,
                        &op_ids as *const Vec<OpId> as *const u8,
                        cache_meta,
                        dep_info,   
                        Input::padding(&mut init_mem_usage, &mut last_mem_usage, &mut max_mem_usage), //invalid pointer
                        &captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
                    )
                };
                match sgx_status {
                    sgx_status_t::SGX_SUCCESS => {},
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
                let dur_comp = now_comp.elapsed().as_nanos() as f64 * 1e-9;
                wrapper_spec_execute(
                    &spec_call_seq_ptr, 
                    cache_meta,
                    
                );
                tx.send((sub_part, (result_ptr, (dur_comp, max_mem_usage as f64, acc_captured_size)))).unwrap();
            }
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
    pub split_nums: Vec<usize>, 
    part_id: usize,
    pub dep_info: DepInfo,
    caching_rdd_id: usize,
    cached_rdd_id: usize,
    pub cached_sub_parts: BTreeSet<usize>,
    pub sub_parts_len: usize,
    pub eenter_lock: Arc<AtomicBool>,
    pub block_len: Arc<AtomicUsize>,
    pub cur_usage: Arc<AtomicUsize>,  //for transfer the cur memory usage in union thread to prev thread
    pub fresh_slope: Arc<AtomicBool>,
    pub acc_captured_size: usize,
}

impl AccArg {
    pub fn new(part_id: usize, dep_info: DepInfo, reduce_num: Option<usize>, eenter_lock: Arc<AtomicBool>, block_len: Arc<AtomicUsize>, cur_usage: Arc<AtomicUsize>, fresh_slope: Arc<AtomicBool>, acc_captured_size: usize) -> Self {
        let split_nums = match reduce_num {
            Some(reduce_num) => {
                assert!(dep_info.is_shuffle == 1);
                vec![reduce_num]
            },
            None => Vec::new(),
        };
        AccArg {
            rdd_ids: Vec::new(),
            op_ids: Vec::new(),
            split_nums,
            part_id,
            dep_info,
            caching_rdd_id: 0,
            cached_rdd_id: 0,
            cached_sub_parts: BTreeSet::new(),
            sub_parts_len: 0,
            eenter_lock,
            block_len,
            cur_usage,
            fresh_slope,
            acc_captured_size,
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
            },
            false => return false,
        }
    }

    //set the have_cached rdd and return whether the set is successfully
    pub fn set_cached_rdd_id(&mut self, cached_rdd_id: usize) -> bool {
        match self.cached_rdd_id == 0 {
            true => {
                self.cached_rdd_id = cached_rdd_id;
                return true;
            },
            false => return false,
        }
    }

    pub fn to_cache_meta(&self) -> CacheMeta {
        let len = self.rdd_ids.len();
        assert_eq!(len, self.op_ids.len());
        let mut caching_op_id = Default::default();
        let mut cached_op_id = Default::default();
        for idx in 0..len {
            if self.caching_rdd_id == self.rdd_ids[idx] {
                caching_op_id = self.op_ids[idx];
            }
            if self.cached_rdd_id == self.rdd_ids[idx] {
                cached_op_id = self.op_ids[idx];
            }
        }

        CacheMeta::new(
            self.caching_rdd_id,
            caching_op_id,
            self.cached_rdd_id,
            cached_op_id,
            self.part_id,
        )
    }

    pub fn totally_cached(&self) -> bool {
        let not_totally_cached = self.cached_sub_parts.len() < self.sub_parts_len ||
            self.sub_parts_len == 0;
        !not_totally_cached
    }

    pub fn cached(&self, subpid: &usize) -> bool {
        self.cached_sub_parts.contains(subpid)
    }

    pub fn insert_rdd_id(&mut self, rdd_id: usize) {
        self.rdd_ids.push(rdd_id);
    }

    pub fn insert_op_id(&mut self, op_id: OpId) {
        self.op_ids.push(op_id);
    }

    pub fn insert_split_num(&mut self, split_num: usize) {
        self.split_nums.push(split_num);
    }

    pub fn is_caching_final_rdd(&self) -> bool {
        self.rdd_ids[0] == self.caching_rdd_id
    }

    pub fn get_enclave_lock(&self) {
        while self.eenter_lock.compare_and_swap(false, true, atomic::Ordering::SeqCst) {
            //wait
        }
    }

    pub fn free_enclave_lock(&self) {
        assert_eq!(self.eenter_lock.compare_and_swap(true, false, atomic::Ordering::SeqCst), true);
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    caching_op_id: OpId,
    cached_rdd_id: usize,
    cached_op_id: OpId,
    part_id: usize,
    sub_part_id: usize, 
    is_survivor: u8,
}

impl CacheMeta {
    pub fn new(
        caching_rdd_id: usize,
        caching_op_id: OpId,
        cached_rdd_id: usize,
        cached_op_id: OpId,
        part_id: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            caching_op_id,
            cached_rdd_id,
            cached_op_id,
            part_id,
            sub_part_id: 0,
            is_survivor: 0,
        }
    }

    pub fn set_sub_part_id(&mut self, sub_part_id: usize) {
        self.sub_part_id = sub_part_id;
    }

    pub fn set_is_survivor(&mut self, is_survivor: bool) {
        match is_survivor {
            true => self.is_survivor = 1,
            false => self.is_survivor = 0,
        }
    }

    pub fn get_triplet(&self) -> (usize, usize, usize) {
        (
            self.caching_rdd_id,
            self.part_id,
            self.sub_part_id,    
        )
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Input {
    data: usize,
    lower: usize,
    upper: usize,
    upper_bound: usize,
    block_len: usize,
    init_mem_usage: usize,
    last_mem_usage: usize,
    max_mem_usage: usize,
}

impl Input {
    pub fn new<T: Data>(data: &T, 
        lower: &mut Vec<usize>, 
        upper: &mut Vec<usize>,
        upper_bound: &Vec<usize>,
        block_len: usize, 
        init_mem_usage: &mut usize, 
        last_mem_usage: &mut usize,
        max_mem_usage: &mut usize,
    ) -> Self {
        let data = data as *const T as usize;
        let lower = lower as *mut Vec<usize> as usize;
        let upper = upper as *mut Vec<usize> as usize;
        let upper_bound = upper_bound as *const Vec<usize> as usize;
        let init_mem_usage = init_mem_usage as *mut usize as usize;
        let last_mem_usage = last_mem_usage as *mut usize as usize;
        let max_mem_usage = max_mem_usage as *mut usize as usize;
        Input {
            data,
            lower,
            upper,
            upper_bound,
            block_len,
            init_mem_usage,
            last_mem_usage,
            max_mem_usage,
        }
    }

    pub fn build_from_ptr(data: *const u8, 
        lower: &mut Vec<usize>, 
        upper: &mut Vec<usize>,
        upper_bound: &Vec<usize>, 
        block_len: usize, 
        init_mem_usage: &mut usize,
        last_mem_usage: &mut usize,
        max_mem_usage: &mut usize,
    ) -> Self {
        let lower = lower as *mut Vec<usize> as usize;
        let upper = upper as *mut Vec<usize> as usize;
        let upper_bound = upper_bound as *const Vec<usize> as usize;
        let init_mem_usage = init_mem_usage as *mut usize as usize;
        let last_mem_usage = last_mem_usage as *mut usize as usize;
        let max_mem_usage = max_mem_usage as *mut usize as usize;
        Input {
            data: data as usize,
            lower,
            upper,
            upper_bound,
            block_len,
            init_mem_usage,
            last_mem_usage,
            max_mem_usage,
        }
    }

    pub fn padding(init_mem_usage: &mut usize, last_mem_usage: &mut usize, max_mem_usage: &mut usize) -> Self {
        let init_mem_usage = init_mem_usage as *mut usize as usize;
        let last_mem_usage = last_mem_usage as *mut usize as usize;
        let max_mem_usage = max_mem_usage as *mut usize as usize;
        Input {
            data: 0,
            lower: 0,
            upper: 0,
            upper_bound: 0,
            block_len: 0,
            init_mem_usage,
            last_mem_usage,
            max_mem_usage,
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
                max_cur_holders: 20,
            }),
            waiting_list: RwLock::new(BTreeMap::new()),
            num_splits_mapping: RwLock::new(BTreeMap::new()),
        }
    }
    
    pub fn insert_stage(&self, rdd_id_pair: (usize, usize, usize), task_id: usize) {
        let mut waiting_list = self.waiting_list.write().unwrap();
        waiting_list.entry(rdd_id_pair).or_insert(vec![]).push(task_id);
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
            self.num_splits_mapping.write().unwrap().remove(&rdd_id_pair);
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
            info.max_cur_holders
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
                    return
                }
            }
            if *self.waiting_list.read().unwrap().first_key_value().unwrap().0 < cur_rdd_id_pair {
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

#[derive(Default, Clone)]
pub struct Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    data_enc: TE,
    id: u64,
    bfe: Option<Box<dyn Func(T) -> TE>>,
    bfd: Option<Box<dyn Func(TE) -> T>>,
}

impl<T, TE> Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub fn new(data_enc: TE, bfe: Option<Box<dyn Func(T) -> TE>>, bfd: Option<Box<dyn Func(TE) -> T>>) -> Self {
        let loc = Location::caller(); 

        let file = loc.file();
        let line = loc.line();
        let num = 0;
        let id = OpId::new(
            file,
            line,
            num,
        ).h;

        Text {
            data_enc,
            id,
            bfe,
            bfd,
        }
    }

    pub fn get_ct(&self) -> TE {
        self.data_enc.clone()
    }

    pub fn update_from_tail_info(&mut self, tail_info: &TailCompInfo) {
        self.data_enc = tail_info.get(self.id).unwrap();
    }

    //For debug
    pub fn to_plain(&self) -> T {
        match &self.bfd {
            Some(bfd) => bfd(self.data_enc.clone()),
            None => {
                let data_enc = Box::new(self.data_enc.clone()) as Box<dyn Any>;
                *data_enc.downcast::<T>().unwrap()
            },
        }
    }

}

impl<T, TE> Deref for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    type Target = TE;

    fn deref(&self) -> &TE {
        &self.data_enc
    }
}

impl<T, TE> DerefMut for Text<T, TE> 
where
    T: Data,
    TE: Data,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data_enc
    }
}

#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct TailCompInfo {
    m: HashMap<u64, Vec<u8>>,
}

impl TailCompInfo {
    pub fn new() -> Self {
        TailCompInfo {
            m: HashMap::new(),
        }
    }

    pub fn insert<T, TE>(&mut self, text: &Text<T, TE>)
    where
        T: Data,
        TE: Data,
    {
        let ser = bincode::serialize(&text.data_enc).unwrap();
        self.m.insert(text.id, ser);
    }

    pub fn remove<TE: Data>(&mut self, id: u64) -> TE {
        let ser = self.m.remove(&id).unwrap();
        bincode::deserialize(&ser).unwrap()
    }

    pub fn get<TE: Data>(&self, id: u64) -> Option<TE> {
        match self.m.get(&id) {
            Some(ser) => Some(bincode::deserialize(&ser).unwrap()),
            None => None,
        }
    }

    pub fn clear(&mut self) {
        self.m.clear();
    }

}


#[repr(C)]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OpId {
    h: u64,
}

impl OpId {
    pub fn new(file: &'static str, line: u32, num: usize) -> Self {
        let h = default_hash(&(file.to_string(), line, num));
        OpId {
            h,
        }
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
    fn free_data_enc(&self, ptr: *mut u8);
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
    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize);
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
    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>>;
    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>>;
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
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

impl<I: RddE + ?Sized> RddBase for SerArc<I> {
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
    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>> {
        (**self).get_rdd_base().iterator_raw(split, acc_arg, tx)
    }    
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        (**self).get_rdd_base().iterator_any(split)
    }
}

impl<I: RddE + ?Sized> Rdd for SerArc<I> {
    type Item = I::Item;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        (**self).get_rdd()
    }
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        (**self).get_rdd_base()
    }
    fn get_or_compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        (**self).get_or_compute(split)
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        (**self).compute(split)
    }
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>> {
        (**self).secure_compute(split, acc_arg, tx)
    }

}

impl<I: RddE + ?Sized> RddE for SerArc<I> {
    type ItemE = I::ItemE;
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        (**self).get_rdde()
    }
    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        (**self).get_fe()
    }
    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        (**self).get_fd()
    }
    fn batch_encrypt(&self, data: Vec<Self::Item>) -> Vec<Self::ItemE> {
        (**self).batch_encrypt(data)
    }
    fn batch_decrypt(&self, data_enc: Vec<Self::ItemE>) -> Vec<Self::Item> {
        (**self).batch_decrypt(data_enc)
    }
}

// Rdd containing methods associated with processing
pub trait Rdd: RddBase + 'static {
    type Item: Data;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>;

    fn get_rdd_base(&self) -> Arc<dyn RddBase>;

    fn get_or_compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
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

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>>;
}

pub trait RddE: Rdd {
    type ItemE: Data;

    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>;

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE>;

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>>;

    fn batch_encrypt(&self, data: Vec<Self::Item>) -> Vec<Self::ItemE> {
        batch_encrypt(data, self.get_fe())
    }

    fn batch_decrypt(&self, data_enc: Vec<Self::ItemE>) -> Vec<Self::Item> {
        batch_decrypt(data_enc, self.get_fd())
    }

    fn secure_iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::ItemE>>> {
        let (tx, rx) = sync_channel(0);
        let rdd_id = self.get_rdd_id();
        let op_id = self.get_op_id();
        let part_id = split.get_index();
        let dep_info = DepInfo::padding_new(2);
        let mut acc_arg = AccArg::new(part_id, 
            dep_info, 
            None, 
            Arc::new(AtomicBool::new(false)), 
            Arc::new(AtomicUsize::new(1)), 
            Arc::new(AtomicUsize::new(0)), 
            Arc::new(AtomicBool::new(false)),
            0);
        STAGE_LOCK.get_stage_lock((rdd_id, rdd_id, 0));
        let handles = self.secure_compute(split, &mut acc_arg, tx)?;
        let caching = acc_arg.is_caching_final_rdd();

        let mut result = Vec::new();
        let mut slopes = Vec::new();
        for (sub_part_id, (received, (time_comp, max_mem_usage, acc_captured_size))) in rx {
            let mut result_bl = get_encrypted_data::<Self::ItemE>(op_id, dep_info, received as *mut u8, false);
            dynamic_subpart_meta(time_comp, max_mem_usage - acc_captured_size as f64, &acc_arg.block_len, &mut slopes, &acc_arg.fresh_slope, STAGE_LOCK.get_parall_num());
            acc_arg.free_enclave_lock();
            if caching {
                //collect result
                result.extend_from_slice(&result_bl);
                //cache
                let size = result_bl.get_size();
                let data_ptr = Box::into_raw(result_bl);
                Env::get().cache_tracker.put_sdata((rdd_id, part_id, sub_part_id), data_ptr as *mut u8, size);
            } else {
                result.append(result_bl.borrow_mut());
            }
        }
        for handle in handles {
            handle.join().unwrap();
        }
        STAGE_LOCK.free_stage_lock();
        Ok(Box::new(result.into_iter()))
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
    #[track_caller]
    fn filter<F>(&self, predicate: F) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        F: SerFunc(&Self::Item) -> bool + Copy,
        Self: Sized,
    {
        let filter_fn = Fn!(move |_index: usize,
                                  items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> {
            Box::new(items.filter(predicate))
        });
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), filter_fn, self.get_fe(), self.get_fd()))
    }

    #[track_caller]
    fn map<U: Data, UE: Data, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(Self::Item) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
        Self: Sized,
    {
        SerArc::new(MapperRdd::new(self.get_rdd(), f, fe, fd))
    }

    #[track_caller]
    fn flat_map<U: Data, UE: Data, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(Self::Item) -> Box<dyn Iterator<Item = U>>,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
        Self: Sized,
    {
        SerArc::new(FlatMapperRdd::new(self.get_rdd(), f, fe, fd))
    }

    /// Return a new RDD by applying a function to each partition of this RDD.
    #[track_caller]
    fn map_partitions<U: Data, UE: Data, F, FE, FD>(&self, func: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
        Self: Sized,
    {
        let ignore_idx = Fn!(move |_index: usize,
                                   items: Box<dyn Iterator<Item = Self::Item>>|
              -> Box<dyn Iterator<Item = _>> { (func)(items) });
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), ignore_idx, fe, fd))
    }

    /// Return a new RDD by applying a function to each partition of this RDD,
    /// while tracking the index of the original partition.
    #[track_caller]
    fn map_partitions_with_index<U: Data, UE: Data, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(usize, Box<dyn Iterator<Item = Self::Item>>) -> Box<dyn Iterator<Item = U>>,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
        Self: Sized,
    {
        SerArc::new(MapPartitionsRdd::new(self.get_rdd(), f, fe, fd))
    }

    /// Return an RDD created by coalescing all elements within each partition into an array.
    #[allow(clippy::type_complexity)]
    #[track_caller]
    fn glom(&self) -> SerArc<dyn RddE<Item = Vec<Self::Item>, ItemE = Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        let func = Fn!(
            |_index: usize, iter: Box<dyn Iterator<Item = Self::Item>>| Box::new(std::iter::once(
                iter.collect::<Vec<_>>()
            ))
                as Box<dyn Iterator<Item = Vec<Self::Item>>>
        );
        let fe = self.get_fe();
        let fd = self.get_fd();
        let fe_wrapper = Fn!(move |v: Vec<Vec<Self::Item>>| {
            v.into_iter().map(|x| (fe)(x)).collect::<Vec<Self::ItemE>>()
        });
        let fd_wrapper = Fn!(move |v: Vec<Self::ItemE>| {
            v.into_iter().map(|x| (fd)(x)).collect::<Vec<Vec<Self::Item>>>()
        });
        let rdd = MapPartitionsRdd::new(self.get_rdd(), Box::new(func), fe_wrapper, fd_wrapper);
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
        self.get_context().run_job_with_context(self.get_rdde(), cl)
    }

    fn reduce<F>(&self, f: F) -> Result<Option<Self::Item>>
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        // cloned cause we will use `f` later.
        let cf = f.clone();
        let reduce_partition = Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| {
            let acc = iter.reduce(&cf);
            match acc {
                None => vec![],
                Some(e) => vec![e],
            }
        });
        let now = Instant::now();
        let results = self.get_context().run_job(self.get_rdde(), reduce_partition);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("mapper {:?}s", dur);
        let now = Instant::now();
        let result = Ok(results?.into_iter().flatten().reduce(f));
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("reducer {:?}s", dur);
        result
    }

    #[track_caller]
    fn secure_reduce<F, UE, FE, FD>(&self, f: F, fe: FE, fd: FD) -> Result<CT<Vec<Self::Item>, Vec<UE>>>
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
    {

        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<Self::ItemE>>();
        let result_ptr = wrapper_action(data, self.get_rdd_id(), self.get_op_id());
        let temp = get_encrypted_data::<UE>(
            self.get_op_id(), 
            DepInfo::padding_new(3), 
            result_ptr as *mut u8,
            false,
        );
        /* Return type: Result<Option<UE>> */
        /*
        let result = match temp.is_empty() {
            true => None,
            false => Some(temp[0].clone()),
        };
        Ok(result)
        */
        let bfe = Box::new(Fn!(move |data | {
            batch_encrypt::<>(data, fe.clone())
        }));
    
        let bfd = Box::new(Fn!(move |data_enc | {
            batch_decrypt::<>(data_enc, fd.clone())
        }));
        Ok(Text::new(*temp, Some(bfe), Some(bfd)))
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
        let reduce_partition =
            Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.fold(zero.clone(), &cf));
        let results = self.get_context().run_job(self.get_rdde(), reduce_partition);
        Ok(results?.into_iter().fold(init, f))
    }

    #[track_caller]
    fn secure_fold<F, UE, FE, FD>(&self, init: Self::Item, f: F, fe: FE, fd: FD) -> Result<CT<Vec<Self::Item>, Vec<UE>>> 
    where
        Self: Sized,
        UE: Data,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
        FE: SerFunc(Vec<Self::Item>) -> UE,
        FD: SerFunc(UE) -> Vec<Self::Item>,
    {
        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<Self::ItemE>>();
       
        let result_ptr = wrapper_action(data, self.get_rdd_id(), self.get_op_id());
        let mut temp = get_encrypted_data::<UE>(
            self.get_op_id(), 
            DepInfo::padding_new(3), 
            result_ptr as *mut u8,
            false,
        );
        /* Return type: Result<UE> */
        /*
        //temp only contains one element
        Ok(temp.pop().unwrap())
        */
        let bfe = Box::new(Fn!(move |data| {
            batch_encrypt(data, fe.clone())
        }));
        let bfd = Box::new(Fn!(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        }));
        Ok(Text::new(*temp, Some(bfe), Some(bfd)))
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
        let reduce_partition =
            Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.fold(zero.clone(), &seq_fn));
        let results = self.get_context().run_job(self.get_rdde(), reduce_partition);
        Ok(results?.into_iter().fold(init, comb_fn))
    }

    #[track_caller]
    fn secure_aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD) -> Result<CT<Vec<U>, Vec<UE>>> 
    where
        Self: Sized,
        U: Data,
        UE: Data,
        SF: SerFunc(U, Self::Item) -> U,
        CF: SerFunc(U, U) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<_>>();
       
        let result_ptr = wrapper_action(data, self.get_rdd_id(), self.get_op_id());
        let mut temp = get_encrypted_data::<UE>(
            self.get_op_id(), 
            DepInfo::padding_new(3), 
            result_ptr as *mut u8,
            false,
        );
        /* Return type: Result<UE> */
        /*
        //temp only contains one element
        Ok(temp.pop().unwrap())
        */
        let bfe = Box::new(Fn!(move |data| {
            batch_encrypt(data, fe.clone())
        }));
        let bfd = Box::new(Fn!(move |data_enc| {
            batch_decrypt(data_enc, fd.clone())
        }));
        Ok(Text::new(*temp, Some(bfe), Some(bfd)))
    }

    /// Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    /// elements (a, b) where a is in `this` and b is in `other`.
    #[track_caller]
    fn cartesian<U, UE>(
        &self,
        other: SerArc<dyn RddE<Item = U, ItemE = UE>>,
    ) -> SerArc<dyn RddE<Item = (Self::Item, U), ItemE = (Self::ItemE, UE)>>
    where
        Self: Sized,
        U: Data,
        UE: Data,
    {
        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        let fe = Fn!(move |v: Vec<(Self::Item, U)>| {
            let (vx, vy): (Vec<Self::Item>, Vec<U>) = v.into_iter().unzip();
            ((fe0)(vx), (fe1)(vy))
        });
        let fd = Fn!(move |v: (Self::ItemE, UE)| {
            let (vx, vy) = v;
            (fd0)(vx).into_iter().zip((fd1)(vy).into_iter()).collect::<Vec<(Self::Item, U)>>()
        });
        SerArc::new(CartesianRdd::new(self.get_rdd(), other.get_rdd(), fe, fd))
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
    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
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
            let fe = self.get_fe();
            let fd = self.get_fd();
            let fe_wrapper_mpp = Fn!(move |v: Vec<(usize, Self::Item)>| {
                let (vx, vy): (Vec<usize>, Vec<Self::Item>) = v.into_iter().unzip();
                let ct_y = (fe)(vy);
                (vx, ct_y)
            });
            let fd_wrapper_mpp = Fn!(move |v: (Vec<usize>, Self::ItemE)| {
                let (vx, vy) = v;
                let pt_y = (fd)(vy); 
                vx.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
            });
            let map_steep: SerArc<dyn RddE<Item = (usize, Self::Item), ItemE = (Vec<usize>, Self::ItemE)>> =
                SerArc::new(MapPartitionsRdd::new(self.get_rdd(), distributed_partition, fe_wrapper_mpp, fd_wrapper_mpp));
            let partitioner = Box::new(HashPartitioner::<usize>::new(num_partitions));
            self.get_context().add_num(1);
            let map_steep_part = map_steep.partition_by_key(partitioner, self.get_fe(), self.get_fd());
            self.get_context().add_num(1);
            let coalesced = SerArc::new(CoalescedRdd::new(
                Arc::new(map_steep_part),
                num_partitions,
                self.get_fe(),
                self.get_fd(),
            ));
            self.get_context().add_num(1);
            coalesced
        } else {
            self.get_context().add_num(4);
            SerArc::new(CoalescedRdd::new(self.get_rdd(), num_partitions, self.get_fe(), self.get_fd()))
        }
    }

    fn collect(&self) -> Result<Vec<Self::Item>>
    where
        Self: Sized,
    {
        let cl =
            Fn!(|(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::Item>>());
        let results = self.get_context().run_job(self.get_rdde(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::Item>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
    }

    #[track_caller]
    fn secure_collect(&self) -> Result<CT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        let cl =
            Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let results = self.get_context().run_job(self.get_rdde(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::ItemE>| a + b.len());
        let result = results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            });

        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(Fn!(move |data| batch_encrypt(data, fe.clone())));
        let bfd = Box::new(Fn!(move |data_enc| batch_decrypt(data_enc, fd.clone())));
        Ok(Text::new(result, Some(bfe), Some(bfd)))
    }

    fn count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let context = self.get_context();
        let counting_func =
            Fn!(|(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| { iter.count() as u64 });
        Ok(context
            .run_job(self.get_rdde(), counting_func)?
            .into_iter()
            .sum())
    }

    fn secure_count(&self) -> Result<u64>
    where
        Self: Sized,
    {
        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<_>>();
        let result_ptr = wrapper_action(data, self.get_rdd_id(), self.get_op_id());
        /*
        let mut temp = get_encrypted_data::<u64>(self.get_rdd_id(), 3, result_ptr as *mut u8);
        //temp only contains one element
        Ok(temp.pop().unwrap())
        */
        Ok(result_ptr as u64)
    }   


    /// Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
    /// TODO fe and fd should be drived from RddE automatically, not act as input
    #[track_caller]
    fn count_by_value(&self) -> SerArc<dyn RddE<Item = (Self::Item, u64), ItemE = (Self::ItemE, Vec<u8>)>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(Self::Item, u64)>| {
            let (vx, vy): (Vec<Self::Item>, Vec<u64>) = v.into_iter().unzip();
            let ct_x = (fe_c)(vx);
            (ct_x, vy)
        });
        let fd_c = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Self::ItemE, Vec<u64>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx); 
            pt_x.into_iter().zip(vy.into_iter()).collect::<Vec<_>>()
        });

        let fe = self.get_fe();
        let fe_wrapper_rd = Fn!(move |v: Vec<(Self::Item, u64)>| {
            let (vx, vy): (Vec<Self::Item>, Vec<u64>) = v.into_iter().unzip();
            let ct_x = (fe)(vx);
            let ct_y = ser_encrypt(vy);
            (ct_x, ct_y)
        });
        let fd = self.get_fd();
        let fd_wrapper_rd = Fn!(move |v: (Self::ItemE, Vec<u8>)| {
            let (vx, vy) = v;
            let pt_x = (fd)(vx);
            let pt_y: Vec<u64> = ser_decrypt(vy);
            assert_eq!(pt_x.len(), pt_y.len());
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });
        //TODO modify reduce_by_key
        let mapped = self.map(Fn!(|x| (x, 1u64)), fe_wrapper_mp, fd_wrapper_mp);
        self.get_context().add_num(1);
        mapped.reduce_by_key(
            Box::new(Fn!(|(x, y)| x + y)) as Box<dyn Func((u64, u64))->u64>,
            self.number_of_splits(),
            fe_wrapper_rd,
            fd_wrapper_rd,
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
                Box<dyn Iterator<Item = Self::ItemE>>,
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
        let rdd = self.get_rdde();
        rdd.register_op_name("count_by_value_approx");
        self.get_context()
            .run_approximate_job(count_partition, rdd, evaluator, timeout)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct_with_num_partitions(
        &self,
        num_partitions: usize,
    ) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp0 = Fn!(move |v: Vec<(Option<Self::Item>, Option<Self::Item>)>| {
            let (vx, vy): (Vec<Option<Self::Item>>, Vec<Option<Self::Item>>) = v.into_iter().unzip();
            let ct_x = (fe_c)(vx.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>());
            (ct_x, vy)
        }); 
        let fd_c = self.get_fd();
        let fd_wrapper_mp0 = Fn!(move |v: (Self::ItemE, Vec<Option<Self::Item>>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx).into_iter().map(|x| Some(x));
            pt_x.zip(vy.into_iter()).collect::<Vec<_>>()
        });
        let fe_wrapper_rd = fe_wrapper_mp0.clone();
        let fd_wrapper_rd = fd_wrapper_mp0.clone();
        let fe = self.get_fe();       
        let fe_wrapper_mp1 = Fn!(move |v: Vec<Self::Item>| {
            let ct = (fe)(v);
            ct
        });
        let fd = self.get_fd();
        let fd_wrapper_mp1 = Fn!(move |v: Self::ItemE| {
            let pt = (fd)(v);
            pt
        });

        let mapped = self.map(
            Fn!(|x| (Some(x), None)), 
            fe_wrapper_mp0, 
            fd_wrapper_mp0
        );
        self.get_context().add_num(1);
        let reduced_by_key = mapped.reduce_by_key(Fn!(|(_x, y)| y),
            num_partitions,
            fe_wrapper_rd,
            fd_wrapper_rd);
        self.get_context().add_num(1);
        reduced_by_key.map(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        }), fe_wrapper_mp1, fd_wrapper_mp1)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
    #[track_caller]
    fn distinct(&self) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
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
    fn repartition(&self, num_partitions: usize) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
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
            let take_from_partion = Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| {
                iter.take(left).collect::<Vec<Self::Item>>()
            });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdde(),
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
    fn secure_take(&self, num: usize) -> Result<CT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        const SCALE_UP_FACTOR: f64 = 2.0;
        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(Fn!(move |data | {
            batch_encrypt::<>(data, fe.clone())
        }));
    
        let bfd = Box::new(Fn!(move |data_enc | {
            batch_decrypt::<>(data_enc, fd.clone())
        }));
        if num == 0 {
            return Ok(Text::new(vec![], Some(bfe), Some(bfd)));
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
            let take_from_partition = Fn!(move |(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| {
                iter.collect::<Vec<Self::ItemE>>()
            });

            let res = self.get_context().run_job_with_partitions(
                self.get_rdde(),
                take_from_partition,
                partitions,
            )?;
            res.into_iter().for_each(|r| {
                let should_take = num - count;
                if should_take != 0 {
                    let (mut temp, have_take) = wrapper_take(self.get_op_id(), &r, should_take);
                    count += have_take;
                    buf.append(&mut temp);
                }
            });

            parts_scanned += num_partitions;
        }

        Ok(Text::new(buf, Some(bfe), Some(bfd)))
    }

    /// Randomly splits this RDD with the provided weights.
    #[track_caller]
    fn random_split(
        &self,
        weights: Vec<f64>,
        seed: Option<u64>,
    ) -> Vec<SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>>
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

        let mut splitted_rdds: Vec<SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>> = Vec::new();

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
            let rdd = SerArc::new(MapPartitionsRdd::new(self.get_rdd(), func, self.get_fe(), self.get_fd()));
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
    fn sample(&self, with_replacement: bool, fraction: f64) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
    {
        assert!(fraction >= 0.0);

        let sampler = if with_replacement {
            Arc::new(PoissonSampler::new(fraction, true)) as Arc<dyn RandomSampler<Self::Item>>
        } else {
            Arc::new(BernoulliSampler::new(fraction)) as Arc<dyn RandomSampler<Self::Item>>
        };
        let rdd = SerArc::new(PartitionwiseSampledRdd::new(self.get_rdd(), sampler, true, self.get_fe(), self.get_fd()));
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
    ) -> Result<CT<Vec<Self::Item>, Vec<Self::ItemE>>>
    where
        Self: Sized,
    {
        const NUM_STD_DEV: f64 = 10.0f64;
        const REPETITION_GUARD: u8 = 100;
        // TODO: this could be const eval when the support is there for the necessary functions
        let max_sample_size = std::u64::MAX - (NUM_STD_DEV * (std::u64::MAX as f64).sqrt()) as u64;
        assert!(num <= max_sample_size);

        let fe = self.get_fe();
        let fd = self.get_fd();
        let bfe = Box::new(Fn!(move |data| batch_encrypt(data, fe.clone())));
        let bfd = Box::new(Fn!(move |data_enc| batch_decrypt(data_enc, fd.clone())));

        if num == 0 {
            return Ok(Text::new(vec![], Some(bfe.clone()), Some(bfd.clone())));
        }

        let initial_count = self.secure_count()?;

        if initial_count == 0 {
            return Ok(Text::new(vec![], Some(bfe.clone()), Some(bfd.clone())));
        }

        if !with_replacement && num >= initial_count {
            let mut sample = self.secure_collect()?;
            *sample = wrapper_randomize_in_place(self.get_op_id(), &sample, seed, initial_count);
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
            while len < num  && num_iters < REPETITION_GUARD {
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

            *samples_enc = wrapper_randomize_in_place(sample_rdd.get_op_id(), &samples_enc, seed, num);
            Ok(samples_enc)
        }
    }

    /// Applies a function f to all elements of this RDD.
    fn for_each<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Self::Item),
        Self: Sized,
    {
        let func = Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.for_each(&func));
        self.get_context().run_job(self.get_rdde(), func)
    }

    /// Applies a function f to each partition of this RDD.
    fn for_each_partition<F>(&self, func: F) -> Result<Vec<()>>
    where
        F: SerFunc(Box<dyn Iterator<Item = Self::Item>>),
        Self: Sized + 'static,
    {
        let func = Fn!(move |(iter, _): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| (&func)(iter));
        self.get_context().run_job(self.get_rdde(), func)
    }

    #[track_caller]
    fn union(
        &self,
        other: Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>,
    ) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
    {
        SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>,
            other,
        ]))
    }

    #[track_caller]
    fn zip<S: Data>(
        &self,
        second: Arc<dyn Rdd<Item = S>>,
    ) -> SerArc<dyn Rdd<Item = (Self::Item, S)>>
    where
        Self: Clone,
    {
        SerArc::new(ZippedPartitionsRdd::<Self::Item, S>::new(
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>,
            second,
        ))
    }

    #[track_caller]
    fn intersection<T>(&self, other: Arc<T>) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
        T: RddE<Item = Self::Item, ItemE = Self::ItemE> + Sized,
    {
        self.intersection_with_num_partitions(other, self.number_of_splits())
    }

    /// subtract function, same as the one found in apache spark 
    /// example of subtract can be found in subtract.rs
    /// performs a full outer join followed by and intersection with self to get subtraction.
    #[track_caller]
    fn subtract<T>(&self, other: Arc<T>) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
        T: RddE<Item = Self::Item, ItemE = Self::ItemE> + Sized,
    {
        self.subtract_with_num_partition(other, self.number_of_splits())
    }
    
    //Both should have consistent security guarantee (encrypted columns)
    #[track_caller]
    fn subtract_with_num_partition<T>(
        &self,
        other: Arc<T>,
        num_splits: usize,
    ) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
        T: RddE<Item = Self::Item, ItemE = Self::ItemE> + Sized,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp0 = Fn!(move |v: Vec<(Self::Item, Option<Self::Item>)>| {
            let (vx, vy): (Vec<Self::Item>, Vec<Option<Self::Item>>)= v.into_iter().unzip();
            let ct_x = (fe_c)(vx);
            (ct_x, vy)
        });
        let fd_c = self.get_fd();
        let fd_wrapper_mp0 = Fn!(move |v: (Self::ItemE, Vec<Option<Self::Item>>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx); 
            pt_x.into_iter().zip(vy.into_iter()).collect::<Vec<_>>()
        });

        let fe_c = self.get_fe();
        let fe_wrapper_jn = Fn!(move |v: Vec<(Self::Item, (Vec<Option<Self::Item>>, Vec<Option<Self::Item>>))>| {
            let (vx, vy): (Vec<Self::Item>, Vec<(Vec<Option<Self::Item>>, Vec<Option<Self::Item>>)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<Vec<Option<Self::Item>>>, Vec<Vec<Option<Self::Item>>>) = vy.into_iter().unzip();
            let ct_x = (fe_c)(vx);
            (ct_x, (vy, vz))
        });
        let fd_c = self.get_fd();
        let fd_wrapper_jn = Fn!(move |v: (Self::ItemE, (Vec<Vec<Option<Self::Item>>>, Vec<Vec<Option<Self::Item>>>))| {
            let (vx, (vy, vz)) = v;
            let pt_x = (fd_c)(vx); 
            pt_x.into_iter()
                .zip(vy.into_iter()
                    .zip(vz.into_iter())
                ).collect::<Vec<_>>()
        }); 

        //TODO may need to be revised
        let fe_wrapper_mp1 = Fn!(move |v: Vec<Option<Self::Item>>| {
            encrypt::<>(bincode::serialize(&v).unwrap().as_ref())
        });
        let fd_wrapper_mp1 = Fn!(move |v: Vec<u8>| {
            bincode::deserialize::<Vec<Option<Self::Item>>>(decrypt::<>(v.as_ref()).as_ref()).unwrap()
        });

        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0.clone(), fd_wrapper_mp0.clone())
            .clone();
        self.get_context().add_num(1);
        let mapped = self
            .map(Box::new(Fn!(
                |x| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0, fd_wrapper_mp0);
        self.get_context().add_num(1);
        let cogrouped = mapped.cogroup(
                other,
                fe_wrapper_jn,
                fd_wrapper_jn,
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
            })), fe_wrapper_mp1, fd_wrapper_mp1);
        self.get_context().add_num(1);
        let rdd = mapped.map_partitions(Box::new(Fn!(|iter: Box<
                dyn Iterator<Item = Option<Self::Item>>,
            >|
             -> Box<
                dyn Iterator<Item = Self::Item>,
            > {
                Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                    as Box<dyn Iterator<Item = Self::Item>>
            })), self.get_fe(), self.get_fd());
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
    ) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Clone,
        Self::Item: Data + Eq + Hash,
        Self::ItemE: Data + Eq + Hash,
        T: RddE<Item = Self::Item, ItemE = Self::ItemE> + Sized,
    {
        let fe_c = self.get_fe();
        let fe_wrapper_mp0 = Fn!(move |v: Vec<(Self::Item, Option<Self::Item>)>| {
            let (vx, vy): (Vec<Self::Item>, Vec<Option<Self::Item>>) = v.into_iter().unzip();
            let ct_x = (fe_c)(vx);
            (ct_x, vy)
        });
        let fd_c = self.get_fd();
        let fd_wrapper_mp0 = Fn!(move |v: (Self::ItemE, Vec<Option<Self::Item>>)| {
            let (vx, vy) = v;
            let pt_x = (fd_c)(vx); 
            pt_x.into_iter().zip(vy.into_iter()).collect::<Vec<_>>()
        });

        let fe_c = self.get_fe();
        let fe_wrapper_jn = Fn!(move |v: Vec<(Self::Item, (Vec<Option<Self::Item>>, Vec<Option<Self::Item>>))>| {
            let (vx, vy): (Vec<Self::Item>, Vec<(Vec<Option<Self::Item>>, Vec<Option<Self::Item>>)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<Vec<Option<Self::Item>>>, Vec<Vec<Option<Self::Item>>>) = vy.into_iter().unzip();
            let ct_x = (fe_c)(vx);
            (ct_x, (vy, vz))
        });
        let fd_c = self.get_fd();
        let fd_wrapper_jn = Fn!(move |v: (Self::ItemE, (Vec<Vec<Option<Self::Item>>>, Vec<Vec<Option<Self::Item>>>))| {
            let (vx, (vy, vz)) = v;
            let pt_x = (fd_c)(vx); 
            pt_x.into_iter()
                .zip(vy.into_iter()
                    .zip(vz.into_iter())
                ).collect::<Vec<_>>()
        }); 

        //TODO may need to be revised
        let fe_wrapper_mp1 = Fn!(move |v: Vec<Option<Self::Item>>| {
            encrypt::<>(bincode::serialize(&v).unwrap().as_ref())
        });
        let fd_wrapper_mp1 = Fn!(move |v: Vec<u8>| {
            bincode::deserialize::<Vec<Option<Self::Item>>>(decrypt::<>(v.as_ref()).as_ref()).unwrap()
        });

        let other = other
            .map(Box::new(Fn!(
                |x: Self::Item| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0.clone(), fd_wrapper_mp0.clone())
            .clone();
        self.get_context().add_num(1);
        let mapped = self
            .map(Box::new(Fn!(
                |x| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0, fd_wrapper_mp0);
        self.get_context().add_num(1);
        let cogrouped = mapped.cogroup(
                other,
                fe_wrapper_jn,
                fd_wrapper_jn,
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
            })), fe_wrapper_mp1, fd_wrapper_mp1);
        self.get_context().add_num(1);
        let rdd = mapped.map_partitions(Box::new(Fn!(|iter: Box<
                dyn Iterator<Item = Option<Self::Item>>,
            >|
             -> Box<
                dyn Iterator<Item = Self::Item>,
            > {
                Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                    as Box<dyn Iterator<Item = Self::Item>>
            })), self.get_fe(), self.get_fd());
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
    fn group_by<K, F>(&self, func: F) -> SerArc<dyn RddE<Item = (K, Vec<Self::Item>), ItemE = (Vec<u8>, Vec<Self::ItemE>)>>
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
    ) -> SerArc<dyn RddE<Item = (K, Vec<Self::Item>), ItemE = (Vec<u8>, Vec<Self::ItemE>)>>
    where
        Self: Sized,
        K: Data + Hash + Eq,
        F: SerFunc(&Self::Item) -> K,
    {
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(K, Self::Item)>| {
            let len = v.len();
            let (vx, vy): (Vec<K>, Vec<Self::Item>) = v.into_iter().unzip();
            let ct_x = ser_encrypt(vx);
            let ct_y = (fe)(vy);
            (ct_x, ct_y)
        });
        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Vec<u8>, Self::ItemE)| {
            let (vx, vy) = v;
            let pt_x: Vec<K> = ser_decrypt(vx);
            let pt_y = (fd)(vy);
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });

        let fe = self.get_fe();
        let fe_wrapper_gb = Fn!(move |v: Vec<(K, Vec<Self::Item>)>| {
            let (vx, vy): (Vec<K>, Vec<Vec<Self::Item>>) = v.into_iter().unzip();
            let mut ct_y = Vec::with_capacity(vy.len());
            for y in vy {
                ct_y.push((fe)(y));
            }
            let ct_x = ser_encrypt(vx);
            (ct_x, ct_y)
        });
        let fd = self.get_fd();
        let fd_wrapper_gb = Fn!(move |v: (Vec<u8>, Vec<Self::ItemE>)| {
            let (vx, vy) = v;
            let mut pt_y = Vec::with_capacity(vy.len());
            for y in vy {
                pt_y.push((fd)(y));
            }
            let pt_x = ser_decrypt(vx);
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });

        let mapped = self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })), fe_wrapper_mp, fd_wrapper_mp);
        self.get_context().add_num(1);
        mapped.group_by_key(fe_wrapper_gb, fd_wrapper_gb, num_splits)
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
    ) -> SerArc<dyn RddE<Item = (K, Vec<Self::Item>), ItemE = (Vec<u8>, Vec<Self::ItemE>)>>
    where
        Self: Sized,
        K: Data + Hash + Eq,
        F: SerFunc(&Self::Item) -> K,
    {
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(K, Self::Item)>| {
            let len = v.len();
            let (vx, vy): (Vec<K>, Vec<Self::Item>) = v.into_iter().unzip();
            let ct_x = ser_encrypt(vx);
            let ct_y = (fe)(vy);
            (ct_x, ct_y)
        });
        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Vec<u8>, Self::ItemE)| {
            let (vx, vy) = v;
            let pt_x: Vec<K> = ser_decrypt(vx);
            let pt_y = (fd)(vy);
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });

        let fe = self.get_fe();
        let fe_wrapper_gb = Fn!(move |v: Vec<(K, Vec<Self::Item>)>| {
            let (vx, vy): (Vec<K>, Vec<Vec<Self::Item>>) = v.into_iter().unzip();
            let mut ct_y = Vec::with_capacity(vy.len());
            for y in vy {
                ct_y.push((fe)(y));
            }
            let ct_x = ser_encrypt(vx);
            (ct_x, ct_y)
        });
        let fd = self.get_fd();
        let fd_wrapper_gb = Fn!(move |v: (Vec<u8>, Vec<Self::ItemE>)| {
            let (vx, vy) = v;
            let mut pt_y = Vec::with_capacity(vy.len());
            for y in vy {
                pt_y.push((fd)(y));
            }
            let pt_x = ser_decrypt(vx);
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });

        let mapped = self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })), fe_wrapper_mp, fd_wrapper_mp);
        self.get_context().add_num(1);
        mapped.group_by_key_using_partitioner(fe_wrapper_gb, fd_wrapper_gb, partitioner)
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
                Box<dyn Iterator<Item = Self::ItemE>>,
            )
        )|
         -> usize { iter.count() });

        let evaluator = CountEvaluator::new(self.number_of_splits(), confidence);
        let rdd = self.get_rdde();
        rdd.register_op_name("count_approx");
        self.get_context()
            .run_approximate_job(count_elements, rdd, evaluator, timeout)
    }

    /// Creates tuples of the elements in this RDD by applying `f`.
    #[track_caller]
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn RddE<Item = (T, Self::Item), ItemE = (Vec<u8>, Self::ItemE)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(T, Self::Item)>| {
            let len = v.len();
            let (vx, vy): (Vec<T>, Vec<Self::Item>) = v.into_iter().unzip();
            let ct_x = ser_encrypt(vx);
            let ct_y = (fe)(vy);
            (ct_x, ct_y)
        });

        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Vec<u8>, Self::ItemE)| {
            let (vx, vy) = v;
            let pt_x: Vec<T> = ser_decrypt(vx);
            let pt_y = (fd)(vy);
            assert_eq!(pt_x.len(), pt_y.len());
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });
       
        self.map(Fn!(move |k: Self::Item| -> (T, Self::Item) {
            let t = (func)(&k);
            (t, k)
        }), fe_wrapper_mp, fd_wrapper_mp)
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
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<Reverse<Self::Item>>| {
            (fe)(v.into_iter().map(|x| x.0).collect::<Vec<_>>())
        });
        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: Self::ItemE| {
            (fd)(v).into_iter().map(|x| Reverse(x)).collect::<Vec<_>>()
        });

        let mapped = self.map(Fn!(|x| Reverse(x)), fe_wrapper_mp, fd_wrapper_mp);
        self.get_context().add_num(1);
        Ok(mapped.take_ordered(num)?
            .into_iter()
            .map(|x| x.0)
            .collect())
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

            let fe_wrapper_mpp = Fn!( move |pt: Vec<BoundedPriorityQueue<Self::Item>>| {
                encrypt::<>(bincode::serialize(&pt).unwrap().as_ref())
            });

            let fd_wrapper_mpp = Fn!( move |ct: Vec<u8>| {
                bincode::deserialize::<Vec<BoundedPriorityQueue<Self::Item>>>(decrypt::<>(ct.as_ref()).as_ref()).unwrap()
            });

            let queue = self
                .map_partitions(first_k_func, fe_wrapper_mpp, fd_wrapper_mpp)
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

pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}

impl<T, I> Reduce<T> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T,
    {
        self.next().map(|first| self.fold(first, f))
    }
}
