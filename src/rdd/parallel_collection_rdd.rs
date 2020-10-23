//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::Env;
use crate::error::Result;
use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
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

extern "C" {
    fn secure_executing(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        id: usize,
        is_shuffle: u8,
        input: *mut u8,
        captured_vars: *const u8,
    ) -> sgx_status_t;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ParallelCollectionSplit<T> {
    rdd_id: i64,
    index: usize,
    values: Arc<Vec<T>>,
}

impl<T: Data> Split for ParallelCollectionSplit<T> {
    fn get_index(&self) -> usize {
        self.index
    }
}

impl<T: Data> ParallelCollectionSplit<T> {
    fn new(rdd_id: i64, index: usize, values: Arc<Vec<T>>) -> Self {
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

    fn secure_iterator(&self, id: usize) -> Vec<usize> {
        let data = self.values.clone();  
        let len = data.len();
        let data = (0..len).map(move |i| data[i].clone()).collect::<Vec<T>>();
        let data_size = std::mem::size_of::<T>();  //may need revising when the type of element is not trivial
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        
        //sub-partition
        let block_len = (1 << (10+10)) / data_size;  //each block: 1MB
        let mut cur = 0;
        let mut result_ptr = Vec::new();
        while cur < len {
            let next = match cur + block_len > len {
                true => len,
                false => cur + block_len,
            };
            let block = Box::new((&data[cur..next]).to_vec());
            let block_ptr = Box::into_raw(block);
            let mut result_bl_ptr: usize = 0;
            let now = Instant::now();
            let sgx_status = unsafe {
                secure_executing(
                    Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                    &mut result_bl_ptr,
                    id,
                    0,   //false
                    block_ptr as *mut u8,
                    &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                )
            };
            let block = unsafe{ Box::from_raw(block_ptr) };
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };
            result_ptr.push(result_bl_ptr);
           
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in ParallelCollectionRdd, compute {:?}", dur);
            cur = next;  
        }
        result_ptr  
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T, TE> {
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Arc<Vec<T>>>,
    splits_enc_: Vec<Arc<Vec<TE>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T, TE> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T, TE>>,
}

impl<T: Data, TE: Data> Clone for ParallelCollection<T, TE> {
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data, TE: Data> ParallelCollection<T, TE> {
    pub fn new<I, IE>(context: Arc<Context>, data: I, data_enc: IE, num_slices: usize, secure: bool) -> Self
    where
        I: IntoIterator<Item = T>,
        IE: IntoIterator<Item = TE>,
    { 
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                context: Arc::downgrade(&context),
                vals: Arc::new(RddVals::new(context.clone(), secure)), //chain of security
                ecall_ids: Arc::new(Mutex::new(Vec::new())),
                splits_: ParallelCollection::<T, TE>::slice(data, num_slices),
                splits_enc_: ParallelCollection::<T, TE>::slice_enc(data_enc, num_slices),
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

    fn slice<I>(data: I, num_slices: usize) -> Vec<Arc<Vec<T>>>
    where
        I: IntoIterator<Item = T>,
    {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data: Vec<_> = data.into_iter().collect();
            let data_len = data.len();
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
            output
        }
    }

    fn slice_enc<IE>(data_enc: IE, num_slices: usize) -> Vec<Arc<Vec<TE>>>
    where
        IE: IntoIterator<Item = TE>,
    {
        if num_slices < 1 {
            panic!("Number of slices should be greater than or equal to 1");
        } else {
            let mut slice_count = 0;
            let data_enc: Vec<_> = data_enc.into_iter().collect();
            let data_len = data_enc.len();
            let mut end = ((slice_count + 1) * data_len) / num_slices;
            let mut output = Vec::new();
            let mut tmp = Vec::new();
            let mut iter_count = 0;
            for i in data_enc {
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
            output
        }
    }

}

impl<K: Data, V: Data, TE: Data> RddBase for ParallelCollection<(K, V), TE> {
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any parallel collection",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<T: Data, TE: Data> RddBase for ParallelCollection<T, TE> {
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

    fn splits(&self) -> Vec<Box<dyn Split>> {
        if self.get_secure() {
            (0..self.rdd_vals.splits_.len())
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id as i64,
                        i,
                        self.rdd_vals.splits_enc_[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>()
        } else {
            (0..self.rdd_vals.splits_.len())
                .map(|i| {
                    Box::new(ParallelCollectionSplit::new(
                        self.rdd_vals.vals.id as i64,
                        i,
                        self.rdd_vals.splits_[i as usize].clone(),
                    )) as Box<dyn Split>
                })
                .collect::<Vec<Box<dyn Split>>>()
        }
    }

    fn number_of_splits(&self) -> usize {
        if self.get_secure() {
            self.rdd_vals.splits_.len()
        } else {
            self.rdd_vals.splits_enc_.len()
        }
    }

    fn iterator_raw(&self, split: Box<dyn Split>) -> Vec<usize> {
        self.secure_compute(split, self.get_rdd_id())
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

impl<T: Data, TE: Data> Rdd for ParallelCollection<T, TE> {
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
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }

    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<usize> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<TE>>() {
            s.secure_iterator(id)
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }

}
