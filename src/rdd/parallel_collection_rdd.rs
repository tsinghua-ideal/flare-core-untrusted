//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::collections::HashMap;
use std::sync::{mpsc::SyncSender, Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{Env, BOUNDED_MEM_CACHE, RDDB_MAP};
use crate::error::{Error, Result};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::serialization_free::Construct;
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
enum DataForm<T> {
    Plaintext(Vec<Arc<Vec<T>>>),
    Ciphertext(Vec<Arc<Vec<ItemE>>>),
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

    fn secure_iterator(&self, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> JoinHandle<()> {
        let data = self.values.clone();
        let len = data.len();
        if len == 0 {
            return std::thread::spawn(|| {});
        }
        let data = (0..len).map(move |i| data[i].clone()).collect::<Vec<T>>();
        //sub-partition
        let acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let now = Instant::now();
            let wait = start_execute(acc_arg, data, tx);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            log::debug!("***in parallel collection rdd, total {:?}***", dur);
        });
        handle
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: DataForm<T>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T> Clone for ParallelCollection<T>
where
    T: Data,
{
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T> ParallelCollection<T>
where
    T: Data,
{
    #[track_caller]
    pub fn new<I, IE>(context: Arc<Context>, data: I, data_enc: IE, num_slices: usize) -> Self
    where
        I: IntoIterator<Item = T>,
        IE: IntoIterator<Item = ItemE>,
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
                splits_: ParallelCollection::<T>::slice(data, data_enc, num_slices),
                num_slices, //field init shorthand
            }),
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

    fn slice(data: Vec<T>, data_enc: Vec<ItemE>, num_slices: usize) -> DataForm<T> {
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

impl<K, V> RddBase for ParallelCollection<(K, V)>
where
    K: Data,
    V: Data,
{
}

impl<T> RddBase for ParallelCollection<T>
where
    T: Data,
{
    fn cache(&self) {
        self.rdd_vals.vals.cache();
        RDDB_MAP.insert(self.get_rdd_id(), self.get_rdd_base());
    }

    fn should_cache(&self) -> bool {
        self.rdd_vals.vals.should_cache()
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

    fn splits(&self) -> Vec<Box<dyn Split>> {
        match self.rdd_vals.splits_.clone() {
            DataForm::Plaintext(splits) => (0..self.rdd_vals.num_slices)
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id,
                        self.rdd_vals.vals.op_id,
                        i,
                        splits[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>(),
            DataForm::Ciphertext(splits) => (0..self.rdd_vals.num_slices)
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id,
                        self.rdd_vals.vals.op_id,
                        i,
                        splits[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>(),
        }
    }

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.num_slices
    }

    fn iterator_raw(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }

    default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<T> Rdd for ParallelCollection<T>
where
    T: Data,
{
    type Item = T;
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
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

    fn secure_compute(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<ItemE>>() {
            let cur_rdd_id = self.get_rdd_id();
            let cur_op_id = self.get_op_id();
            let cur_part_id = split.get_index();
            let cur_split_num = self.number_of_splits();
            acc_arg.insert_quadruple(cur_rdd_id, cur_op_id, cur_part_id, cur_split_num);

            let should_cache = self.should_cache();
            if should_cache {
                let mut handles =
                    secure_compute_cached(acc_arg, cur_rdd_id, cur_part_id, tx.clone());

                if handles.is_empty() {
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
