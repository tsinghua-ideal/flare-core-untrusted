//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::collections::HashMap;
use std::sync::{Arc, mpsc::SyncSender, Weak};
use std::time::Instant;
use std::thread::{JoinHandle, self};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{BOUNDED_MEM_CACHE, RDDB_MAP, Env};
use crate::error::{Error, Result};
use crate::rdd::*;
use crate::serialization_free::Construct;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use sgx_types::*;

/// A collection of objects which can be sliced into partitions with a partitioning function.
pub trait Chunkable<D>
where
    D: Data,
{
    fn slice_with_set_parts(self, parts: usize) -> Vec<Arc<Vec<D>>>;

    fn slice(self) -> Vec<Arc<Vec<D>>>
    where
        Self: Sized,
    {
        let as_many_parts_as_cpus = num_cpus::get();
        self.slice_with_set_parts(as_many_parts_as_cpus)
    }
}

#[derive(Serialize, Deserialize, Clone)]
enum DataForm<T, TE> {
    Plaintext(Vec<Arc<Vec<T>>>),
    Ciphertext(Vec<Arc<Vec<TE>>>),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: usize,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: usize, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            index,
            values,
        }
    }
    // Lot of unnecessary cloning is there. Have to refactor for better performance
    fn iterator(&self) -> Box<dyn Iterator<Item = T>> {
        let data = self.values.clone();
        let len = data.len();
        Box::new((0..len).map(move |i| data[i].clone()))
    }

    fn secure_iterator(&self, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> JoinHandle<()> {
        let data = self.values.clone();  
        let len = data.len();
        let data = (0..len).map(move |i| data[i].clone()).collect::<Vec<T>>();
        let data_size = data.get_aprox_size() / len;
        let part_id = self.get_index();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.rdd_id;
        acc_arg.insert_rdd_id(cur_rdd_id);
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        //sub-partition
        let mut acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            let mut sub_part_id = 0;
            let mut cache_meta = CacheMeta::new(acc_arg.caching_rdd_id,
                0,   //indicate it cannot find the cached data
                part_id, 
                acc_arg.steps_to_caching,
                acc_arg.steps_to_cached,
            );
            let block_len = (1 << (10+10)) / data_size;  //each block: 1MB
            let mut cur = 0;
            while cur < len {
                let next = match cur + block_len > len {
                    true => len,
                    false => cur + block_len,
                };
                
                //currently, all sub_partitions are not cached as long as coming to this rdd
                if !acc_arg.cached(&sub_part_id) {  
                    cache_meta.set_sub_part_id(sub_part_id);
                    println!("insert_subpid {:?}, {:?}, {:?}", cache_meta.caching_rdd_id, part_id, sub_part_id);
                    BOUNDED_MEM_CACHE.insert_subpid(cache_meta.caching_rdd_id, part_id, sub_part_id);
                    let now = Instant::now();
                    let block = Box::new((&data[cur..next]).to_vec());
                    let block_ptr = Box::into_raw(block);
                    let mut result_bl_ptr: usize = 0;
                    while *EENTER_LOCK.lock().unwrap() {
                        //wait
                    }
                    println!("rdd ids(before secure_executing) {:?}", acc_arg.rdd_ids);
                    let sgx_status = unsafe {
                        secure_executing(
                            eid,
                            &mut result_bl_ptr,
                            tid,
                            &acc_arg.rdd_ids as *const Vec<(usize, usize)> as *const u8,
                            cache_meta,
                            acc_arg.is_shuffle,  
                            block_ptr as *mut u8,
                            &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                        )
                    };
                    *EENTER_LOCK.lock().unwrap() =  true;

                    let _block = unsafe{ Box::from_raw(block_ptr) };
                    let _r = match sgx_status {
                        sgx_status_t::SGX_SUCCESS => {},
                        _ => {
                            panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                        },
                    };
                    tx.send(result_bl_ptr).unwrap();
                    let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                    println!("in ParallelCollectionRdd, num of sub_partition {:?}, compute {:?}", sub_part_id, dur);
                }
                sub_part_id += 1;
                cur = next;  
            }
        });
        handle 
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T, TE> {
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: DataForm<T, TE>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T, TE, FE, FD> 
where
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T, TE>>,
    fe: FE,
    fd: FD,
}

impl<T, TE, FE, FD> Clone for ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, TE, FE, FD> ParallelCollection<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    pub fn new<I, IE>(context: Arc<Context>, data: I, data_enc: IE, fe: FE, fd: FD, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
        IE: IntoIterator<Item = TE>,
    {
        let data: Vec<_> = data.into_iter().collect();
        let data_len = data.len();
        let data_enc: Vec<_> = data_enc.into_iter().collect();
        let data_enc_len = data_enc.len();
        let mut secure = false;
        if data_len > 0 && data_enc_len > 0 {
            panic!("Input invalid! Only one form (pt or ct) needs to provide!")
        }
        if data_enc_len > 0 {
            secure = true;
        }
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                context: Arc::downgrade(&context),
                vals: Arc::new(RddVals::new(context.clone(), secure)), //chain of security
                ecall_ids: Arc::new(Mutex::new(Vec::new())),
                splits_: ParallelCollection::<T, TE, FE, FD>::slice(data, data_enc, num_slices),
                num_slices, //field init shorthand
            }),
            fe,
            fd,
        }
    }

    /*
    pub fn from_chunkable<C>(context: Arc<Context>, data: C, secure: bool) -> Self
    where
        C: Chunkable<T>,
    {
        let splits_ = data.slice();
        let rdd_vals = ParallelCollectionVals {
            context: Arc::downgrade(&context),
            vals: Arc::new(RddVals::new(context.clone(), secure)),
            ecall_ids: Arc::new(Mutex::new(Vec::new())),
            num_slices: splits_.len(),
            splits_,
        };
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(rdd_vals),
        }
    }
    */

    fn slice(data: Vec<T>, data_enc: Vec<TE>, num_slices: usize) -> DataForm<T, TE> {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data_len = data.len();
            let data_enc_len = data_enc.len();
            if data_len > 0 {
                let mut end = ((slice_count + 1) * data_len) / num_slices;
                let mut output = Vec::new();
                let mut tmp = Vec::new();
                let mut iter_count = 0;
                for i in data {
                    if iter_count < end {
                        tmp.push(i);
                        iter_count += 1;
                    } else {
                        slice_count += 1;
                        end = ((slice_count + 1) * data_len) / num_slices;
                        output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                        tmp.push(i);
                        iter_count += 1;
                    }
                }
                output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                DataForm::Plaintext(output)
            } else if data_enc_len > 0 {
                let mut end = ((slice_count + 1) * data_enc_len) / num_slices;
                let mut output = Vec::new();
                let mut tmp = Vec::new();
                let mut iter_count = 0;
                for i in data_enc {
                    if iter_count < end {
                        tmp.push(i);
                        iter_count += 1;
                    } else {
                        slice_count += 1;
                        end = ((slice_count + 1) * data_enc_len) / num_slices;
                        output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                        tmp.push(i);
                        iter_count += 1;
                    }
                }
                output.push(Arc::new(tmp.drain(..).collect::<Vec<_>>()));
                DataForm::Ciphertext(output)
            } else {
                panic!("Neither plaintext nor ciphertext is provided")
            }
        }
    }

}

impl<K, V, KE, VE, FE, FD> RddBase for ParallelCollection<(K, V), (KE, VE), FE, FD> 
where
    K: Data,
    V: Data,
    KE: Data,
    VE: Data,
    FE: SerFunc(Vec<(K, V)>) -> (KE, VE),
    FD: SerFunc((KE, VE)) -> Vec<(K, V)>, 
{

}

impl<T, TE, FE, FD> RddBase for ParallelCollection<T, TE, FE, FD>
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
{
    fn cache(&self) {
        self.rdd_vals.vals.cache();
        RDDB_MAP.insert(
            self.get_rdd_id(), 
            self.get_rdd_base()
        );
    }

    fn should_cache(&self) -> bool {
        self.rdd_vals.vals.should_cache()
    }

    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe {
            Box::from_raw(ptr as *mut Vec<TE>)
        };
    }

    fn get_rdd_id(&self) -> usize {
        self.rdd_vals.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.rdd_vals.vals.context.upgrade().unwrap()
    }

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.rdd_vals.vals.dependencies.clone()
    }
    
    fn get_secure(&self) -> bool {
        self.rdd_vals.vals.secure
    }

    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
        self.rdd_vals.ecall_ids.clone()
    }
    fn insert_ecall_id(&self) {
        if self.rdd_vals.vals.secure {
            self.rdd_vals.ecall_ids.lock().push(self.rdd_vals.vals.id);
        }
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<TE>(self.get_rdd_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        match self.rdd_vals.splits_.clone() {
            DataForm::Plaintext(splits) => {
                (0..self.rdd_vals.num_slices)
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id,
                        i,
                        splits[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>()
            },
            DataForm::Ciphertext(splits) => {
                (0..self.rdd_vals.num_slices)
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id,
                        i,
                        splits[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>()
            },
        }

    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.num_slices
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<T, TE, FE, FD> Rdd for ParallelCollection<T, TE, FE, FD>
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
{
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        })
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            Ok(s.iterator())
        } else {
            Err(Error::DowncastFailure("ParallelCollectionSplit<T>"))
        }
    }

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<TE>>() {
            Ok(vec![s.secure_iterator(acc_arg, tx)])
        } else {
            Err(Error::DowncastFailure("ParallelCollectionSplit<TE>"))
        }
    }

}

impl<T, TE, FE, FD> RddE for ParallelCollection<T, TE, FE, FD>
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
{
    type ItemE = TE;
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Self::ItemE>    
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE)->Vec<Self::Item>>
    } 
}
