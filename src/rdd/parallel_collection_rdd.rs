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
    op_id: OpId,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: usize, op_id: OpId, index: usize, values: Arc<Vec<T>>) -> Self {
        ParallelCollectionSplit {
            rdd_id,
            op_id,
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

    fn secure_iterator(&self, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> JoinHandle<()> {
        let data = self.values.clone();  
        let len = data.len();
        if len == 0 {
            return std::thread::spawn(|| {});
        }
        let data = (0..len).map(move |i| data[i].clone()).collect::<Vec<T>>();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());

        //sub-partition
        let acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let now = Instant::now();
            let mut wait = 0.0;
            let mut sub_part_id = 0;
            let mut cache_meta = acc_arg.to_cache_meta();
            let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                &acc_arg.op_ids,
                &acc_arg.split_nums,
                cache_meta, 
                acc_arg.dep_info,
            );
            let mut is_survivor = spec_call_seq_ptr.is_some();
            let mut cur = 0;
            let mut to_set_usage = 0;
            while cur < len {
                let wait_now = Instant::now();
                acc_arg.get_enclave_lock();
                let wait_dur = wait_now.elapsed().as_nanos() as f64 * 1e-9;
                wait += wait_dur;

                //update block_len
                let block_len = acc_arg.block_len.load(atomic::Ordering::SeqCst);
                let cur_usage = acc_arg.cur_usage.load(atomic::Ordering::SeqCst);
                if cur_usage != 0 {
                    to_set_usage = cur_usage;
                }
                let next = match cur + block_len > len {
                    true => len,
                    false => cur + block_len,
                };
                is_survivor = is_survivor || next == len;
                //currently, all sub_partitions are not cached as long as coming to this rdd
                if !acc_arg.cached(&sub_part_id) {
                    cache_meta.set_sub_part_id(sub_part_id);
                    cache_meta.set_is_survivor(is_survivor);
                    BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                    let (result_bl_ptr, (time_comp, mem_usage)) = wrapper_secure_execute(
                        &acc_arg.rdd_ids,
                        &acc_arg.op_ids,
                        cache_meta,
                        acc_arg.dep_info,
                        &data,
                        &mut vec![cur],
                        &mut vec![next],
                        &vec![len],
                        block_len,
                        to_set_usage,
                        &captured_vars,
                    );
                    wrapper_spec_execute(
                        &spec_call_seq_ptr, 
                        cache_meta,
                    );
                    match acc_arg.dep_info.is_shuffle == 0 {
                        true => tx.send((sub_part_id, (result_bl_ptr, (time_comp, mem_usage.0, acc_arg.acc_captured_size)))).unwrap(),
                        false => tx.send((sub_part_id, (result_bl_ptr, (time_comp, mem_usage.1, acc_arg.acc_captured_size)))).unwrap(),
                    };
                    to_set_usage = match spec_call_seq_ptr.is_none() {
                        true => mem_usage.0 as usize,
                        false => 0,
                    }
                }
                sub_part_id += 1;
                cur = next; 
            }
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in parallel collection rdd, total {:?}***", dur);
        });
        handle 
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T, TE> {
    vals: Arc<RddVals>,
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
    #[track_caller]
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

    fn get_op_id(&self) -> OpId {
        self.rdd_vals.vals.op_id
    }

    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        op_ids.push(self.get_op_id());
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
        Vec::new()
    }
    
    fn get_secure(&self) -> bool {
        self.rdd_vals.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<TE>(self.get_op_id(), value_ptr);
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
                        self.rdd_vals.vals.op_id,
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
                        self.rdd_vals.vals.op_id,
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

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
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

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64, usize)))>) -> Result<Vec<JoinHandle<()>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<TE>>() {
            let cur_rdd_id = self.get_rdd_id();
            let cur_op_id = self.get_op_id();
            let cur_split_num = self.number_of_splits();
            acc_arg.insert_rdd_id(cur_rdd_id);
            acc_arg.insert_op_id(cur_op_id);
            acc_arg.insert_split_num(cur_split_num);

            let captured_vars = Env::get().captured_vars.lock().unwrap().clone();
            let should_cache = self.should_cache();
            if should_cache {
                let mut handles = secure_compute_cached(
                    acc_arg, 
                    cur_rdd_id, 
                    tx.clone(),
                    captured_vars,
                );
    
                if !acc_arg.totally_cached() {
                    acc_arg.set_caching_rdd_id(cur_rdd_id);
                    handles.append(&mut vec![s.secure_iterator(acc_arg, tx)]);
                }
                Ok(handles)     
            } else {
                Ok(vec![s.secure_iterator(acc_arg, tx)])
            }
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
