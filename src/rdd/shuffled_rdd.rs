use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::env::Env;
use crate::dependency::{Dependency, ShuffleDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use parking_lot::Mutex;
use sgx_types::*;

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

#[derive(Clone, Serialize, Deserialize)]
struct ShuffledRddSplit {
    index: usize,
}

impl ShuffledRddSplit {
    fn new(index: usize) -> Self {
        ShuffledRddSplit { index }
    }
}

impl Split for ShuffledRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Serialize, Deserialize)]
pub struct ShuffledRdd<K: Data + Eq + Hash, V: Data, C: Data> {
    #[serde(with = "serde_traitobject")]
    parent: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    shuffle_id: usize,
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Clone for ShuffledRdd<K, V, C> {
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            ecall_ids: self.ecall_ids.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> ShuffledRdd<K, V, C> {
    pub(crate) fn new(
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let ctx = parent.get_context();
        let secure = parent.get_secure();   //temp
        let ecall_ids = parent.get_ecall_ids();
        let shuffle_id = ctx.new_shuffle_id();
        let mut vals = RddVals::new(ctx, secure);

        vals.dependencies
            .push(Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::new(
                    shuffle_id,
                    false,
                    parent.get_rdd_base(),
                    aggregator.clone(),
                    part.clone(),
                ),
            )));
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            ecall_ids,
            part,
            shuffle_id,
        }
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> RddBase for ShuffledRdd<K, V, C> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
        self.ecall_ids.clone()
    }

    fn insert_ecall_id(&self) {
        if self.vals.secure {
            self.ecall_ids.lock().push(self.vals.id);
        }
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.part.get_num_of_partitions())
            .map(|x| Box::new(ShuffledRddSplit::new(x)) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }

    fn iterator_ser(&self, split: Box<dyn Split>) -> Vec<Vec<u8>> {
        self.secure_compute(split, self.get_rdd_id())
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }

    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside cogroup iterator_any shuffledrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K: Data + Eq + Hash, V: Data, C: Data> Rdd for ShuffledRdd<K, V, C> {
    type Item = (K, C);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        log::debug!("compute inside shuffled rdd");
        let now = Instant::now();

        let fut = ShuffleFetcher::fetch::<K, C>(self.shuffle_id, split.get_index());
        let mut combiners: HashMap<K, Option<C>> = HashMap::new();
        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
            if let Some(old_c) = combiners.get_mut(&k) {
                let old = old_c.take().unwrap();
                let input = ((old, c),);
                let output = self.aggregator.merge_combiners.call(input);
                *old_c = Some(output);
            } else {
                combiners.insert(k, Some(c));
            }
        }

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd shuffle read {:?}", dur);
        Ok(Box::new(
            combiners.into_iter().map(|(k, v)| (k, v.unwrap())),
        ))
    }
    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<Vec<u8>> {
        //TODO K, V both need encryption?
        log::debug!("compute inside shuffled rdd");

        let now = Instant::now();
        let fut = ShuffleFetcher::secure_fetch(self.shuffle_id, split.get_index());
        let ser_data_set = futures::executor::block_on(fut).unwrap();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, fetch {:?}", dur);

        let now = Instant::now();
        let cap = 1 << (2+10+10+10);   //4G  
        let mut ser_data = Vec::<u8>::with_capacity(cap);
        let mut ser_data_idx = Vec::<usize>::with_capacity(cap>>3);
        let mut idx: usize = 0;
        for (i, mut ser_data_bl) in ser_data_set.into_iter().enumerate() {
            idx += ser_data_bl.len();
            ser_data.append(&mut ser_data_bl);
            ser_data_idx.push(idx);
        } 
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, merge ser data {:?}", dur);

        let now = Instant::now();
        let mut ser_result = Vec::<u8>::with_capacity(cap);
        let mut ser_result_idx = Vec::<usize>::with_capacity(cap>>3);
        let mut retval = cap;
        let sgx_status = unsafe {
            secure_executing(
                Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                &mut retval,
                self.get_rdd_id(),  //shuffle rdd id
                1,   //is_shuffle = true
                ser_data.as_ptr() as *const u8,
                ser_data_idx.as_ptr() as *const usize,
                ser_data_idx.len(),
                ser_result.as_mut_ptr() as *mut u8,
                ser_result_idx.as_mut_ptr() as *mut usize,
            )
        };
        unsafe {
            ser_result_idx.set_len(retval);
            ser_result.set_len(ser_result_idx[retval-1]);
        }
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        ser_result_idx.shrink_to_fit();
        ser_result.shrink_to_fit();
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, shuffle read {:?}", dur);
        
        log::debug!("finish shuffle read");

        let now = Instant::now();
        let ser_data = ser_result;
        let ser_data_idx = ser_result_idx;
       
        let cap = 1 << (7+10+10);  //128MB
        let mut ser_result: Vec<Vec<u8>> = Vec::new();
        let mut pre_idx: usize = 0;

        for idx in ser_data_idx {
            let ser_block = &ser_data[pre_idx..idx];
            let ser_block_idx: Vec<usize> = vec![ser_block.len()];
            let mut ser_result_bl = Vec::<u8>::with_capacity(cap);
            let mut ser_result_bl_idx = Vec::<usize>::with_capacity(1);
            let mut retval = 1;
            let sgx_status = unsafe {
                secure_executing(
                    Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                    &mut retval,
                    id,
                    0,
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
            //log::info!("retval = {}, result = {:?}, len = {}, cap = {}", retval, serialized_result_bl, serialized_result_bl.len(), serialized_result_bl.capacity());
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };
            ser_result_bl.shrink_to_fit();
            ser_result.push(ser_result_bl);

            pre_idx = idx;
        }
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, compute {:?}", dur);

        ser_result        

    }
}
