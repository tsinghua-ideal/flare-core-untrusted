use std::any::TypeId;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs;
use std::hash::Hash;
use std::io::{BufWriter, Write};
use std::mem::forget;
use std::net::Ipv4Addr;
use std::path::Path;
use std::slice;
use std::sync::{Arc, Weak,
    atomic::{self, AtomicBool},
    mpsc::{sync_channel, SyncSender, Receiver} 
    };
use std::thread::{JoinHandle, self};
use std::time::{Duration, Instant};


use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{RDDB_MAP, Env};
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
use lazy_static::lazy_static;
use parking_lot::Mutex;
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

pub const MAX_ENC_BL: usize = 1000;
static immediate_cout: bool = true;
lazy_static! {
    pub static ref EENTER_LOCK: Arc<std::sync::Mutex<bool>> = Arc::new(std::sync::Mutex::new(false));
}

extern "C" {
    pub fn secure_executing(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        tid: u64,
        rdd_ids: *const u8,
        cache_meta: CacheMeta,
        is_shuffle: u8,
        input: *mut u8,
        captured_vars: *const u8,
    ) -> sgx_status_t;
    pub fn free_res_enc(
        eid: sgx_enclave_id_t,
        op_id: usize,
        is_shuffle: u8,
        input: *mut u8,  
    ) -> sgx_status_t;
    pub fn priv_free_res_enc(
        eid: sgx_enclave_id_t,
        op_id: usize,
        is_shuffle: u8,
        input: *mut u8,  
    ) -> sgx_status_t;
    pub fn get_sketch(
        eid: sgx_enclave_id_t,
        rdd_id: usize,
        is_shuffle: u8,
        p_buf: *mut u8,
        p_data_enc: *mut u8,
    ) -> sgx_status_t;
    pub fn clone_out(
        eid: sgx_enclave_id_t,
        rdd_id: usize,
        is_shuffle: u8,
        p_out: usize,
        p_data_enc: *mut u8,
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
    Env::get().cache_tracker.put_sdata((rdd_id, part_id, sub_part_id), data_ptr as *mut u8);
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


pub fn get_encrypted_data<T>(rdd_id: usize, is_shuffle: u8, p_data_enc: *mut u8) -> Box<Vec<T>> 
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
    if immediate_cout {
        let op_id = RDDB_MAP.map_id(rdd_id);
        let res_ = unsafe{ Box::from_raw(p_data_enc as *mut Vec<T>) };
        let res = res_.clone();
        forget(res_);
        let sgx_status = unsafe { 
            free_res_enc(
                eid,
                op_id,
                is_shuffle,
                p_data_enc,
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
                rdd_id,
                is_shuffle,
                size_buf_ptr as *mut u8,
                p_data_enc,   //shuffle write
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
                rdd_id,
                is_shuffle,
                ptr_out,
                p_data_enc,
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

pub fn move_data<T: Clone>(op_id: usize, data: *mut u8) -> Box<Vec<T>> {
    let res_ = unsafe{ Box::from_raw(data as *mut Vec<T>) };
    let res = res_.clone();
    forget(res_);
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    let sgx_status = unsafe { 
        priv_free_res_enc(
            eid,
            op_id,
            0, //default to 0, for cache should not appear at the end of stage
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

//Return the cached or caching sub-partition number
pub fn cached_or_caching_in_enclave(rdd_id: usize, part: usize) -> HashSet<usize> {
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
    cached_sub_parts.into_iter().collect::<HashSet<_>>()
}

pub fn secure_compute_cached(
    acc_arg: &mut AccArg,
    cur_rdd_id: usize, 
    part_id: usize, 
    tx: SyncSender<usize>,
    captured_vars: HashMap<usize, Vec<u8>>,
) -> Vec<JoinHandle<()>> {
    let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
    //check whether it is cached inside enclave
    let mut cached_sub_parts = cached_or_caching_in_enclave(cur_rdd_id, part_id);
    //check whether it is cached outside enclave
    let mut uncached_sub_parts = Env::get().cache_tracker.get_uncached_subpids(cur_rdd_id, part_id);
    uncached_sub_parts = uncached_sub_parts.difference(&cached_sub_parts)
        .map(|x| *x)
        .collect::<HashSet<_>>();
    cached_sub_parts = cached_sub_parts.union(&Env::get().cache_tracker.get_cached_subpids(cur_rdd_id, part_id))
        .map(|x| *x)
        .collect::<HashSet<_>>();
    println!("get_subpids {:?}, {:?}", cur_rdd_id, part_id);
    let subpids = Env::get().cache_tracker.get_subpids(cur_rdd_id, part_id);
    println!("subpids = {:?}, cached = {:?}, uncached = {:?}", subpids, cached_sub_parts, uncached_sub_parts);
    assert_eq!(uncached_sub_parts.len() + cached_sub_parts.len(), subpids.len());

    acc_arg.cached_sub_parts = cached_sub_parts.clone();
    acc_arg.sub_parts_len = subpids.len();
    let mut handles = Vec::new();
    if cached_sub_parts.len() > 0 {
        acc_arg.step_cached(true);
        // Compute based on cached values
        let rdd_ids = acc_arg.rdd_ids.clone();
        let caching_rdd_id = acc_arg.caching_rdd_id;
        let steps_to_caching = acc_arg.steps_to_caching;
        let steps_to_cached = acc_arg.steps_to_cached;
        let mut cache_meta = CacheMeta::new(caching_rdd_id, cur_rdd_id, part_id, steps_to_caching, steps_to_cached);
        let is_shuffle = acc_arg.is_shuffle;

        let handle = std::thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            for sub_part in cached_sub_parts {
                cache_meta.set_sub_part_id(sub_part);
                let data_ptr: usize = 0;  //invalid pointer
                let mut result_ptr: usize = 0;
                while *EENTER_LOCK.lock().unwrap() {
                    //wait
                }
                let sgx_status = unsafe {
                    secure_executing(
                        eid,
                        &mut result_ptr,
                        tid,
                        &rdd_ids as *const Vec<(usize, usize)> as *const u8, 
                        cache_meta,
                        is_shuffle,   
                        data_ptr as *mut u8 ,
                        &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                    )
                };
                *EENTER_LOCK.lock().unwrap() =  true;
                match sgx_status {
                    sgx_status_t::SGX_SUCCESS => {},
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
                tx.send(result_ptr).unwrap();
            }
        });
        handles.push(handle);
    } else {
        acc_arg.step_cached(false);
        acc_arg.set_caching_rdd_id(cur_rdd_id);
    }
    handles
}


#[derive(Clone)]
pub struct AccArg {
    pub rdd_ids: Vec<(usize, usize)>,
    cur_cont_seg: usize, 
    pub is_shuffle: u8,
    pub caching_rdd_id: usize,
    pub steps_to_caching: usize,
    pub steps_to_cached: usize,
    pub step_caching_end: bool,
    pub step_cached_end: bool,
    pub cached_sub_parts: HashSet<usize>,
    pub sub_parts_len: usize,
}

impl AccArg {
    pub fn new(final_rdd_id: usize, is_shuffle: u8, steps_to_caching: usize, steps_to_cached: usize) -> Self {
        AccArg {
            rdd_ids: vec![(final_rdd_id, final_rdd_id)],
            cur_cont_seg: 0,
            is_shuffle,
            caching_rdd_id: 0,
            steps_to_caching,
            steps_to_cached,
            step_caching_end: false,
            step_cached_end: false,
            cached_sub_parts: HashSet::new(),
            sub_parts_len: 0,
        }
    }

    //set the to_be_cached rdd and return whether the set is successfully
    pub fn set_caching_rdd_id(&mut self, caching_rdd_id: usize) -> bool {
        match self.caching_rdd_id == 0 {
            true => {
                self.caching_rdd_id = caching_rdd_id;
                return true;
            },
            false => return false,
        }
    }

    fn step_caching(&mut self, should_cache: bool) {
        let b = should_cache || self.step_caching_end;
        match b {
            true => self.step_caching_end = true,
            false => self.steps_to_caching += 1,
        }
        println!("step_caching step = {:?}, end = {:?}", self.steps_to_caching, self.step_caching_end);
    }

    fn step_cached(&mut self, have_cache: bool) {
        let b = have_cache || self.step_cached_end;
        match b {
            true => self.step_cached_end = true,
            false => self.steps_to_cached += 1,
        }
        println!("step_cachd step = {:?}, end = {:?}", self.steps_to_cached, self.step_cached_end);
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
        let cur = &mut self.cur_cont_seg;
        let lower = &mut self.rdd_ids[*cur].1;
        if rdd_id == *lower - 1 || rdd_id == *lower {
            *lower = rdd_id;
        } else {
            self.rdd_ids.push((rdd_id, rdd_id));
            *cur += 1;
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CacheMeta {
    caching_rdd_id: usize,
    cached_rdd_id: usize,
    part_id: usize,
    sub_part_id: usize, 
    steps_to_caching: usize, 
    steps_to_cached: usize,
}

impl CacheMeta {
    pub fn new(caching_rdd_id: usize,
        cached_rdd_id: usize,
        part_id: usize,
        steps_to_caching: usize,
        steps_to_cached: usize,
    ) -> Self {
        CacheMeta {
            caching_rdd_id,
            cached_rdd_id,
            part_id,
            sub_part_id: 0, 
            steps_to_caching,
            steps_to_cached,
        }
    }

    pub fn set_sub_part_id(&mut self, sub_part_id: usize) {
        self.sub_part_id = sub_part_id;
    }
}

// Values which are needed for all RDDs
#[derive(Serialize, Deserialize)]
pub(crate) struct RddVals {
    pub id: usize,
    pub dependencies: Vec<Dependency>,
    should_cache_: AtomicBool,
    secure: bool,
    #[serde(skip_serializing, skip_deserializing)]
    pub context: Weak<Context>,
}

impl RddVals {
    pub fn new(sc: Arc<Context>, secure: bool) -> Self {
        RddVals {
            id: sc.new_rdd_id(),
            dependencies: Vec::new(),
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
    fn get_context(&self) -> Arc<Context>;
    fn get_op_name(&self) -> String {
        "unknown".to_owned()
    }
    fn register_op_name(&self, _name: &str) {
        log::debug!("couldn't register op name")
    }
    fn get_dependencies(&self) -> Vec<Dependency>;
    fn get_secure(&self) -> bool;
    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>>;
    fn insert_ecall_id(&self);
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
    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>>;
    // Analyse whether this is required or not. It requires downcasting while executing tasks which could hurt performance.
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>>;
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
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
    fn get_context(&self) -> Arc<Context> {
        (**self).get_rdd_base().get_context()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        (**self).get_rdd_base().get_dependencies()
    }
    fn get_secure(&self) -> bool {
        (**self).get_rdd_base().get_secure()
    }
    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
        (**self).get_rdd_base().get_ecall_ids()
    }
    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        (**self).move_allocation(value_ptr)
    }
    fn insert_ecall_id(&self) {
        (**self).get_rdd_base().insert_ecall_id();
    }
    fn splits(&self) -> Vec<Box<dyn Split>> {
        (**self).get_rdd_base().splits()
    }
    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        (**self).get_rdd_base().iterator_raw(split, acc_arg, tx)
    }    
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
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
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
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

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>>;
}

pub trait RddE: Rdd {
    type ItemE: Data;

    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>;

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE>;

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>>;

    fn batch_encrypt(&self, mut data: Vec<Self::Item>) -> Vec<Self::ItemE> {
        let mut len = data.len();
        let mut data_enc = Vec::with_capacity(len);
        while len >= MAX_ENC_BL {
            len -= MAX_ENC_BL;
            let remain = data.split_off(MAX_ENC_BL);
            let input = data;
            data = remain;
            data_enc.push(self.get_fe()(input));
        }
        if len != 0 {
            data_enc.push(self.get_fe()(data));
        }
        
        data_enc
    }

    fn batch_decrypt(&self, data_enc: Vec<Self::ItemE>) -> Vec<Self::Item> {
        let mut data = Vec::new();
        for block in data_enc {
            let mut pt = self.get_fd()(block);
            data.append(&mut pt); //need to check security
        }
        data
    }

    fn secure_iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::ItemE>>> {
        let (tx, rx) = sync_channel(0);
        let mut acc_arg = AccArg::new(self.get_rdd_id(), 0, 0, 0);
        let handles = self.secure_compute(split, &mut acc_arg, tx)?;

        let mut result = Vec::new();
        for received in rx {
            println!("in secure_iterator");
            let mut result_bl = get_encrypted_data::<Self::ItemE>(self.get_rdd_id(), 0, received as *mut u8);
            *EENTER_LOCK.lock().unwrap() = false;
            result.append(result_bl.borrow_mut());
        }
        for handle in handles {
            handle.join().unwrap();
        }
        Ok(Box::new(result.into_iter()))
    }

    /// Return a new RDD containing only the elements that satisfy a predicate.
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

    fn map<U: Data, UE: Data, F, FE, FD>(&self, f: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(Self::Item) -> U,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
        Self: Sized,
    {
        SerArc::new(MapperRdd::new(self.get_rdd(), f, fe, fd))
    }

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

    fn secure_reduce<F>(&self, f: F) -> Result<Option<Self::ItemE>> 
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {

        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<Self::ItemE>>();
       
        let data_ptr = Box::into_raw(Box::new(data));
        let now = Instant::now();
        let tid: u64 = thread::current().id().as_u64().into();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.get_rdd_id();
        let rdd_ids = vec![(cur_rdd_id, cur_rdd_id)];
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let mut result_ptr: usize = 0;
        let sgx_status = unsafe {
            secure_executing(
                eid,
                &mut result_ptr,
                tid,
                &rdd_ids as *const Vec<(usize, usize)> as *const u8,
                CacheMeta::new(0, 0, 0, 0, 0),  //meaningless
                3,   //3 is for reduce & fold
                data_ptr as *mut u8 ,
                &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
            )
        };
        let _data = unsafe{ Box::from_raw(data_ptr) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in reduce, ecall {:?}s", dur);
        let temp = get_encrypted_data::<Self::ItemE>(self.get_rdd_id(), 3, result_ptr as *mut u8);
        let result = match temp.is_empty() {
            true => None,
            false => Some(temp[0].clone()),
        };
        Ok(result)
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

    fn secure_fold<F>(&self, init: Self::Item, f: F) -> Result<Self::ItemE> 
    where
        Self: Sized,
        F: SerFunc(Self::Item, Self::Item) -> Self::Item,
    {
        let cl = Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let data = self.get_context().run_job(self.get_rdde(), cl)?
            .into_iter().flatten().collect::<Vec<Self::ItemE>>();
       
        let data_ptr = Box::into_raw(Box::new(data));
        let now = Instant::now();
        let tid: u64 = thread::current().id().as_u64().into();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.get_rdd_id();
        let rdd_ids = vec![(cur_rdd_id, cur_rdd_id)];
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let mut result_ptr: usize = 0;
        let sgx_status = unsafe {
            secure_executing(
                eid,
                &mut result_ptr,
                tid,
                &rdd_ids as *const Vec<(usize, usize)> as *const u8,  //shuffle rdd id
                CacheMeta::new(0, 0, 0, 0, 0),  //meaningless
                3,   //3 is for reduce & fold
                data_ptr as *mut u8 ,
                &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
            )
        };
        let _data = unsafe{ Box::from_raw(data_ptr) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in fold, ecall {:?}s", dur);
        let mut temp = get_encrypted_data::<Self::ItemE>(self.get_rdd_id(), 3, result_ptr as *mut u8);
        //temp only contains one element
        Ok(temp.pop().unwrap())
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

    //
    fn secure_aggregate<U, UE, SF, CF, FE, FD>(&self, init: U, seq_fn: SF, comb_fn: CF, fe: FE, fd: FD) -> Result<UE>
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
       
        let data_ptr = Box::into_raw(Box::new(data));
        let now = Instant::now();
        let tid: u64 = thread::current().id().as_u64().into();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.get_rdd_id();
        let rdd_ids = vec![(cur_rdd_id, cur_rdd_id)];
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let mut result_ptr: usize = 0;
        let sgx_status = unsafe {
            secure_executing(
                eid,
                &mut result_ptr,
                tid,
                &rdd_ids as *const Vec<(usize, usize)> as *const u8,  //shuffle rdd id
                CacheMeta::new(0, 0, 0, 0, 0),  //meaningless
                3,   //3 is for reduce & fold
                data_ptr as *mut u8 ,
                &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
            )
        };
        let _data = unsafe{ Box::from_raw(data_ptr) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in aggregate, ecall {:?}s", dur);
        let mut temp = get_encrypted_data::<UE>(self.get_rdd_id(), 3, result_ptr as *mut u8);
        //temp only contains one element
        Ok(temp.pop().unwrap())
    }

    /// Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of
    /// elements (a, b) where a is in `this` and b is in `other`.
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
    fn coalesce(&self, num_partitions: usize, shuffle: bool) -> SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>
    where
        Self: Sized,
    {
        if shuffle {
            // Distributes elements evenly across output partitions, starting from a random partition.
            use std::hash::Hasher;
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
            SerArc::new(CoalescedRdd::new(
                Arc::new(map_steep.partition_by_key(partitioner, self.get_fe(), self.get_fd())),
                num_partitions,
                self.get_fe(),
                self.get_fd(),
            ))
        } else {
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

    fn secure_collect(&self) -> Result<Vec<Self::ItemE>>
    where
        Self: Sized,
    {
        let cl =
            Fn!(|(_, iter): (Box<dyn Iterator<Item = Self::Item>>, Box<dyn Iterator<Item = Self::ItemE>>)| iter.collect::<Vec<Self::ItemE>>());
        let results = self.get_context().run_job(self.get_rdde(), cl)?;
        let size = results.iter().fold(0, |a, b: &Vec<Self::ItemE>| a + b.len());
        Ok(results
            .into_iter()
            .fold(Vec::with_capacity(size), |mut acc, v| {
                acc.extend(v);
                acc
            }))
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
       
        let data_ptr = Box::into_raw(Box::new(data));
        let now = Instant::now();
        let tid: u64 = thread::current().id().as_u64().into();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.get_rdd_id();
        let rdd_ids = vec![(cur_rdd_id, cur_rdd_id)];
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let mut result_ptr: usize = 0;
        let sgx_status = unsafe {
            secure_executing(
                eid,
                &mut result_ptr,
                tid,
                &rdd_ids as *const Vec<(usize, usize)> as *const u8,  //shuffle rdd id
                CacheMeta::new(0, 0, 0, 0, 0),  //meaningless
                3,   //3 is for reduce & fold
                data_ptr as *mut u8 ,
                &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
            )
        };
        let _data = unsafe{ Box::from_raw(data_ptr) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in aggregate, ecall {:?}s", dur);
        /*
        let mut temp = get_encrypted_data::<u64>(self.get_rdd_id(), 3, result_ptr as *mut u8);
        //temp only contains one element
        Ok(temp.pop().unwrap())
        */
        Ok(result_ptr as u64)
    }   


    /// Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.
    /// TODO fe and fd should be drived from RddE automatically, not act as input
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
        self.map(Fn!(|x| (x, 1u64)), fe_wrapper_mp, fd_wrapper_mp)
        .reduce_by_key(
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

        self.map(Box::new(Fn!(|x| (Some(x), None)))
            as Box<
                dyn Func(Self::Item) -> (Option<Self::Item>, Option<Self::Item>),
            >, fe_wrapper_mp0, fd_wrapper_mp0)
        .reduce_by_key(Box::new(Fn!(|(_x, y)| y)),
            num_partitions,
            fe_wrapper_rd,
            fd_wrapper_rd)
        .map(Box::new(Fn!(|x: (
            Option<Self::Item>,
            Option<Self::Item>
        )| {
            let (x, _y) = x;
            x.unwrap()
        })), fe_wrapper_mp1, fd_wrapper_mp1)
    }

    /// Return a new RDD containing the distinct elements in this RDD.
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

    /// Randomly splits this RDD with the provided weights.
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
        SerArc::new(PartitionwiseSampledRdd::new(self.get_rdd(), sampler, true, self.get_fe(), self.get_fd()))
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

    fn union(
        &self,
        other: Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>,
    ) -> Result<SerArc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>>
    where
        Self: Clone,
    {
        Ok(SerArc::new(Context::union(&[
            Arc::new(self.clone()) as Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>>,
            other,
        ])?))
    }

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
        let rdd = self
            .map(Box::new(Fn!(
                |x| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0, fd_wrapper_mp0)
            .cogroup(
                other,
                fe_wrapper_jn,
                fd_wrapper_jn,
                Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
            ).map(Box::new(Fn!(|(x, (v1, v2)): (
                Self::Item,
                (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
            )|
             -> Option<Self::Item> {
                if (v1.len() >= 1) ^ (v2.len() >= 1) {
                    Some(x)
                } else {
                    None
                }
            })), fe_wrapper_mp1, fd_wrapper_mp1)
            .map_partitions(Box::new(Fn!(|iter: Box<
                dyn Iterator<Item = Option<Self::Item>>,
            >|
             -> Box<
                dyn Iterator<Item = Self::Item>,
            > {
                Box::new(iter.filter(|x| x.is_some()).map(|x| x.unwrap()))
                    as Box<dyn Iterator<Item = Self::Item>>
            })), self.get_fe(), self.get_fd());

        let subtraction = self.intersection(Arc::new(rdd));
        (&*subtraction).register_op_name("subtraction");
        subtraction
    }

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
        let rdd = self
            .map(Box::new(Fn!(
                |x| -> (Self::Item, Option<Self::Item>) { (x, None) }
            )), fe_wrapper_mp0, fd_wrapper_mp0)
            .cogroup(
                other,
                fe_wrapper_jn,
                fd_wrapper_jn,
                Box::new(HashPartitioner::<Self::Item>::new(num_splits)) as Box<dyn Partitioner>,
            )
            .map(Box::new(Fn!(|(x, (v1, v2)): (
                Self::Item,
                (Vec::<Option<Self::Item>>, Vec::<Option<Self::Item>>)
            )|
             -> Option<Self::Item> {
                if v1.len() >= 1 && v2.len() >= 1 {
                    Some(x)
                } else {
                    None
                }
            })), fe_wrapper_mp1, fd_wrapper_mp1)
            .map_partitions(Box::new(Fn!(|iter: Box<
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

        self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })), fe_wrapper_mp, fd_wrapper_mp)
        .group_by_key(fe_wrapper_gb, fd_wrapper_gb, num_splits)
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

        self.map(Box::new(Fn!(move |val: Self::Item| -> (K, Self::Item) {
            let key = (func)(&val);
            (key, val)
        })), fe_wrapper_mp, fd_wrapper_mp)
        .group_by_key_using_partitioner(fe_wrapper_gb, fd_wrapper_gb, partitioner)
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
    fn key_by<T, F>(&self, func: F) -> SerArc<dyn RddE<Item = (Self::Item, T), ItemE = (Self::ItemE, Vec<u8>)>>
    where
        Self: Sized,
        T: Data,
        F: SerFunc(&Self::Item) -> T,
    {
        let fe = self.get_fe();
        let fe_wrapper_mp = Fn!(move |v: Vec<(Self::Item, T)>| {
            let len = v.len();
            let (vx, vy): (Vec<Self::Item>, Vec<T>) = v.into_iter().unzip();
            let ct_x = (fe)(vx);
            let ct_y = ser_encrypt(vy);
            (ct_x, ct_y)
        });

        let fd = self.get_fd();
        let fd_wrapper_mp = Fn!(move |v: (Self::ItemE, Vec<u8>)| {
            let (vx, vy) = v;
            let pt_x = (fd)(vx);
            let pt_y: Vec<T> = ser_decrypt(vy);
            assert_eq!(pt_x.len(), pt_y.len());
            pt_x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
        });
       
        self.map(Fn!(move |k: Self::Item| -> (Self::Item, T) {
            let t = (func)(&k);
            (k, t)
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

        Ok(self
            .map(Fn!(|x| Reverse(x)), fe_wrapper_mp, fd_wrapper_mp)
            .take_ordered(num)?
            .into_iter()
            .map(|x| x.0)
            .collect())
    }

    /// Returns the first k (smallest) elements from this RDD as defined by the specified
    /// Ord<T> and maintains ordering. This does the opposite of [top()](#top).
    /// # Notes
    /// This method should only be used if the resulting array is expected to be small, as
    /// all the data is loaded into the driver's memory.
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
