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
        input: *mut u8,
        captured_vars: *const u8,
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

    fn iterator_raw(&self, split: Box<dyn Split>) -> Vec<usize> {
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
    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<usize> {
        //TODO K, V both need encryption?
        log::debug!("compute inside shuffled rdd");

        let now = Instant::now();
        //let fut = ShuffleFetcher::secure_fetch(self.shuffle_id, split.get_index());
        let fut = ShuffleFetcher::fetch::<K, C>(self.shuffle_id, split.get_index());
        let buckets: Vec<(K, C)> = futures::executor::block_on(fut).unwrap().into_iter().collect();
        let data_ptr = Box::into_raw(Box::new(buckets));
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, fetch {:?}", dur);
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new()); 
        let now = Instant::now();
        let mut result_ptr: usize = 0;
        let sgx_status = unsafe {
            secure_executing(
                Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                &mut result_ptr,
                self.get_rdd_id(),  //shuffle rdd id
                1,   //is_shuffle = true
                data_ptr as *mut u8, 
                &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
            )
        };
        let buckets = unsafe{ Box::from_raw(data_ptr) };
        match sgx_status {
            sgx_status_t::SGX_SUCCESS => {},
            _ => {
                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
            },
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd, shuffle read {:?}", dur);
        
        log::debug!("finish shuffle read");

        let data = unsafe{ Box::from_raw(result_ptr as *mut u8 as *mut Vec<Self::Item>) };
        let data_size = std::mem::size_of::<Self::Item>();
        let len = data.len();

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
