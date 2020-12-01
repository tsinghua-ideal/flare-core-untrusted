use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::{JoinHandle, self};
use std::time::Instant;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::env::Env;
use crate::dependency::{Dependency, ShuffleDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::{Rdd, RddBase, RddE, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
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
        tid: u64,
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
pub struct ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    parent: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    shuffle_id: usize,
    fe: FE,
    fd: FD,
}

impl<K, V, C, KE, CE, FE, FD> Clone for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone, 
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            ecall_ids: self.ecall_ids.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone, 
{
    pub(crate) fn new(
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let ctx = parent.get_context();
        let secure = parent.get_secure();   //temp
        let ecall_ids = parent.get_ecall_ids();
        let shuffle_id = ctx.new_shuffle_id();
        let mut vals = RddVals::new(ctx, secure);

        vals.dependencies
            .push(Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::<_, _, _, KE, CE>::new(
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
            fe,
            fd,
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> RddBase for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
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

    fn iterator_raw(&self, split: Box<dyn Split>, tx: SyncSender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        self.secure_compute(split, self.get_rdd_id(), tx, is_shuffle)
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
}

impl<K, V, C, KE, CE, FE, FD> Rdd for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
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
    fn secure_compute(&self, split: Box<dyn Split>, id: usize, tx: SyncSender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        let fut = ShuffleFetcher::secure_fetch::<KE, CE>(self.shuffle_id, split.get_index());
        let bucket: Vec<Vec<(KE, CE)>> = futures::executor::block_on(fut)?.into_iter().collect();  // bucket per subpartition
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_id = self.get_rdd_id();

        let handle = thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            let now = Instant::now();
            let mut blocks = Vec::new(); 
            let num_subpar = bucket.len();
            let chunk_size = 1000;   //num of entries = 1000*num_subpar
            let mut iter_vec = Vec::new();
            for idx_subpar in 0..num_subpar {
                iter_vec.push(bucket[idx_subpar].chunks(chunk_size));        
            }
                        
            while !iter_vec.is_empty() {
                //get block (memory inefficient)
                blocks.clear();
                let mut empty_idxs = Vec::new();
                for (idx, iter) in iter_vec.iter_mut().enumerate() {
                    let nxt = iter.next();
                    if nxt.is_none() {
                        empty_idxs.push(idx);
                        continue;
                    }
                    let slice = nxt.unwrap();
                    blocks.push(slice.to_vec());
                }
                empty_idxs.reverse();
                for idx in empty_idxs {
                    iter_vec.remove(idx);
                }
                
                let block_ptr = Box::into_raw(Box::new(blocks));
                
                let mut result_bl_ptr: usize = 0; 
                let sgx_status = unsafe {
                    secure_executing(
                        Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                        &mut result_bl_ptr,
                        cur_id,  //shuffle rdd id
                        tid,
                        2,   //shuffle read
                        block_ptr as *mut u8, 
                        &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                    )
                };
                blocks = *unsafe{ Box::from_raw(block_ptr) };
                let _r = match sgx_status {
                    sgx_status_t::SGX_SUCCESS => {},
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
                
                // this block is in enclave, cannot access
                let block_ptr = result_bl_ptr as *mut u8;
                result_bl_ptr = 0;
                let sgx_status = unsafe {
                    secure_executing(
                        Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                        &mut result_bl_ptr,
                        id,
                        tid,
                        is_shuffle,  
                        block_ptr,
                        &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                    )
                };
                match sgx_status {
                    sgx_status_t::SGX_SUCCESS => {},
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
                tx.send(result_bl_ptr).unwrap();
            }  
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in ShuffledRdd, shuffle read + narrow {:?}", dur);  
        });
        Ok(handle)
    }
}

impl<K, V, C, KE, CE, FE, FD> RddE for ShuffledRdd<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>,
{
    type ItemE = (KE, CE);
    
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
