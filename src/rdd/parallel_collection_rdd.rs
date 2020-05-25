//! This module implements parallel collection RDD for dividing the input collection for parallel processing.
use std::sync::{Arc, Weak};

use crate::context::Context;
use crate::env::Env;
use crate::dependency::Dependency;
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
        input: *const u8,
        input_idx: *const usize,
        idx_len: usize,
        output: *mut u8,
        output_idx: *mut usize,
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

    fn secure_iterator<D: Data>(&self, id: usize) -> Vec<Vec<u8>> {
        let data = self.values.clone();  
        let len = data.len();
        let data = (0..len).map(move |i| data[i].clone()).collect::<Vec<T>>();
        let data_size = std::mem::size_of::<D>();  //may need revising when the type of element is not trivial

        let cap = 1 << (7+10+10);  //128MB
        
        //it's needed without sub-partition
        //let cap = cap << 5; 
        
        //sub-partition
        let block_len = (1 << (5+10+10)) / data_size;  //each block: 32MB
        let mut cur = 0;
        let mut ser_result = Vec::new();
        while cur < len {
            let next = match cur + block_len > len {
                true => len,
                false => cur + block_len,
            };
            
            //In future, the serialized_data is off-the-shelf, 
            //the only thing that needs to do is sub-partition to serialized_block
            let ser_block: Vec<u8> = bincode::serialize(&data[cur..next]).unwrap();
            let ser_block_idx: Vec<usize> = vec![ser_block.len()];
            let mut ser_result_bl = Vec::<u8>::with_capacity(cap);
            let mut ser_result_bl_idx = Vec::<usize>::with_capacity(1);
            let mut retval = 1;
            let sgx_status = unsafe {
                secure_executing(
                    Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                    &mut retval,
                    id,
                    0,   //false
                    ser_block.as_ptr() as *const u8,
                    ser_block_idx.as_ptr() as *const usize,
                    ser_block_idx.len(),
                    ser_result_bl.as_mut_ptr() as *mut u8,
                    ser_result_bl_idx.as_mut_ptr() as *mut usize,
                )
            };
            unsafe {
                ser_result_bl_idx.set_len(retval);
                ser_result_bl.set_len(ser_result_bl_idx[retval-1]);
            }
            assert!(ser_result_bl_idx.len()==1 
                    && ser_result_bl.len()==*ser_result_bl_idx.last().unwrap());
            //log::info!("retval = {}, ser_result_bl = {:?}", retval, ser_result_bl);
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };
            ser_result_bl.shrink_to_fit();
            ser_result.push(ser_result_bl);
            
            cur = next;  
        }
        ser_result  
    }
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollectionVals<T> {
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    splits_: Vec<Arc<Vec<T>>>,
    num_slices: usize,
}

#[derive(Serialize, Deserialize)]
pub struct ParallelCollection<T> {
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    rdd_vals: Arc<ParallelCollectionVals<T>>,
}

impl<T: Data> Clone for ParallelCollection<T> {
    fn clone(&self) -> Self {
        ParallelCollection {
            name: Mutex::new(self.name.lock().clone()),
            rdd_vals: self.rdd_vals.clone(),
        }
    }
}

impl<T: Data> ParallelCollection<T> {
    pub fn new<I>(context: Arc<Context>, data: I, num_slices: usize, secure: bool) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        ParallelCollection {
            name: Mutex::new("parallel_collection".to_owned()),
            rdd_vals: Arc::new(ParallelCollectionVals {
                context: Arc::downgrade(&context),
                vals: Arc::new(RddVals::new(context.clone(), secure)), //chain of security
                ecall_ids: Arc::new(Mutex::new(Vec::new())),
                splits_: ParallelCollection::slice(data, num_slices),
                num_slices, //field init shorthand
            }),
        }
    }

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
}

impl<K: Data, V: Data> RddBase for ParallelCollection<(K, V)> {
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

impl<T: Data> RddBase for ParallelCollection<T> {
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

    fn number_of_splits(&self) -> usize {
        self.rdd_vals.splits_.len()
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

impl<T: Data> Rdd for ParallelCollection<T> {
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

    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<Vec<u8>> {
        if let Some(s) = split.downcast_ref::<ParallelCollectionSplit<T>>() {
            s.secure_iterator::<Self::Item>(id)
        } else {
            panic!(
                "Got split object from different concrete type other than ParallelCollectionSplit"
            )
        }
    }

}
