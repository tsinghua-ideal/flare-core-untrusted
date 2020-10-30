use crate::aggregator::Aggregator;
use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::RddBase;
use crate::serializable_traits::Data;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::forget;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sgx_types::*;

// Revise if enum is good choice. Considering enum since down casting one trait object to another trait object is difficult.
#[derive(Clone, Serialize, Deserialize)]
pub enum Dependency {
    #[serde(with = "serde_traitobject")]
    NarrowDependency(Arc<dyn NarrowDependencyTrait>),
    #[serde(with = "serde_traitobject")]
    ShuffleDependency(Arc<dyn ShuffleDependencyTrait>),
}

pub trait NarrowDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_parents(&self, partition_id: usize) -> Vec<usize>;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
}

#[derive(Serialize, Deserialize, Clone)]
pub struct OneToOneDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
}

impl OneToOneDependency {
    pub fn new(rdd_base: Arc<dyn RddBase>) -> Self {
        OneToOneDependency { rdd_base }
    }
}

impl NarrowDependencyTrait for OneToOneDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        vec![partition_id]
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

/// Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct RangeDependency {
    #[serde(with = "serde_traitobject")]
    rdd_base: Arc<dyn RddBase>,
    /// the start of the range in the child RDD
    out_start: usize,
    /// the start of the range in the parent RDD
    in_start: usize,
    /// the length of the range
    length: usize,
}

impl RangeDependency {
    pub fn new(
        rdd_base: Arc<dyn RddBase>,
        in_start: usize,
        out_start: usize,
        length: usize,
    ) -> Self {
        RangeDependency {
            rdd_base,
            in_start,
            out_start,
            length,
        }
    }
}

impl NarrowDependencyTrait for RangeDependency {
    fn get_parents(&self, partition_id: usize) -> Vec<usize> {
        if partition_id >= self.out_start && partition_id < self.out_start + self.length {
            vec![partition_id - self.out_start + self.in_start]
        } else {
            Vec::new()
        }
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }
}

pub trait ShuffleDependencyTrait: Serialize + Deserialize + Send + Sync {
    fn get_shuffle_id(&self) -> usize;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn is_shuffle(&self) -> bool;
    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String;
}

impl PartialOrd for dyn ShuffleDependencyTrait {
    fn partial_cmp(&self, other: &dyn ShuffleDependencyTrait) -> Option<Ordering> {
        Some(self.get_shuffle_id().cmp(&other.get_shuffle_id()))
    }
}

impl PartialEq for dyn ShuffleDependencyTrait {
    fn eq(&self, other: &dyn ShuffleDependencyTrait) -> bool {
        self.get_shuffle_id() == other.get_shuffle_id()
    }
}

impl Eq for dyn ShuffleDependencyTrait {}

impl Ord for dyn ShuffleDependencyTrait {
    fn cmp(&self, other: &dyn ShuffleDependencyTrait) -> Ordering {
        self.get_shuffle_id().cmp(&other.get_shuffle_id())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ShuffleDependency<K, V, C, KE, CE>
where 
    K: Data, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    pub shuffle_id: usize,
    pub is_cogroup: bool,
    #[serde(with = "serde_traitobject")]
    pub rdd_base: Arc<dyn RddBase>,
    #[serde(with = "serde_traitobject")]
    pub aggregator: Arc<Aggregator<K, V, C>>,
    #[serde(with = "serde_traitobject")]
    pub partitioner: Box<dyn Partitioner>,
    is_shuffle: bool,
    _marker_ke: PhantomData<KE>,
    _marker_ce: PhantomData<CE>,
}

impl<K, V, C, KE, CE> ShuffleDependency<K, V, C, KE, CE> 
where
    K: Data, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd_base: Arc<dyn RddBase>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
    ) -> Self {
        ShuffleDependency {
            shuffle_id,
            is_cogroup,
            rdd_base,
            aggregator,
            partitioner,
            is_shuffle: true,
            _marker_ke: PhantomData,
            _marker_ce: PhantomData,
        }
    }
}

impl<K, V, C, KE, CE> ShuffleDependencyTrait for ShuffleDependency<K, V, C, KE, CE> 
where 
    K: Data + Eq + Hash, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
{
    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    fn do_shuffle_task(&self, rdd_base: Arc<dyn RddBase>, partition: usize) -> String {
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );
        let split = rdd_base.splits()[partition].clone();
        log::debug!("split index: {}", split.get_index());
        log::debug!("rdd id {:?}, secure: {:?}", rdd_base.get_rdd_id(), rdd_base.get_secure());
        if rdd_base.get_secure() {
            let now = Instant::now();
            let rdd_id = rdd_base.get_rdd_id();
            let data_ptr = rdd_base.iterator_raw(split).unwrap();
            //let data = rdd_base.iterator_any(split).unwrap().collect::<Vec<_>>();
            //println!("data = {:?}", data);
            let captured_vars = std::mem::replace(&mut *env::Env::get().captured_vars.lock().unwrap(), HashMap::new());
            let num_output_splits = self.partitioner.get_num_of_partitions();
            let mut buckets: Vec<Vec<(KE, CE)>> = (0..num_output_splits)
                .map(|_| Vec::new())
                .collect::<Vec<_>>();

            for block_ptr in data_ptr { 
                let now = Instant::now();
                let mut buckets_bl_ptr: usize = 0;
                log::debug!("enter enclave");
                let sgx_status = unsafe {
                    secure_executing(
                        env::Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                        &mut buckets_bl_ptr,
                        rdd_id,
                        1,   //is_shuffle = true
                        block_ptr as *mut u8,
                        &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                    )
                };
                match sgx_status {
                    sgx_status_t::SGX_SUCCESS => (),
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                log::debug!("in dependency, rdd_id {:?}, shuffle write {:?}", rdd_id, dur);
                let buckets_bl = unsafe{ Box::from_raw(buckets_bl_ptr as *mut u8 as *mut Vec<Vec<(KE, CE)>>) };
                
                for (i, mut bucket) in buckets_bl.into_iter().enumerate() {
                    buckets[i].append(&mut bucket); 
                }
                log::debug!("success!");
            }
           
            for (i, bucket) in buckets.into_iter().enumerate() {
                let ser_bytes = bincode::serialize(&bucket).unwrap();
                env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
            }
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in dependency, total {:?}", dur);
            env::Env::get().shuffle_manager.get_server_uri()    
        } else {
            let iter = if self.is_cogroup {
                rdd_base.cogroup_iterator_any(split)
            } else {
                rdd_base.iterator_any(split.clone())
            };

            let now = Instant::now();
            
            let aggregator = self.aggregator.clone();
            let num_output_splits = self.partitioner.get_num_of_partitions();
            log::debug!("is cogroup rdd: {}", self.is_cogroup);
            log::debug!("number of output splits: {}", num_output_splits);
            let partitioner = self.partitioner.clone();
            let mut buckets: Vec<HashMap<K, C>> = (0..num_output_splits)
                .map(|_| HashMap::new())
                .collect::<Vec<_>>();
            log::debug!(
                "before iterating while executing shuffle map task for partition #{}",
                partition
            );

            for (count, i) in iter.unwrap().enumerate() {
                let b = i.into_any().downcast::<(K, V)>().unwrap();
                let (k, v) = *b;
                if count == 0 {
                    log::debug!(
                        "iterating inside dependency map task after downcasting: key: {:?}, value: {:?}",
                        k,
                        v
                    );
                }
                let bucket_id = partitioner.get_partition(&k);
                let bucket = &mut buckets[bucket_id];
                if let Some(old_v) = bucket.get_mut(&k) {
                    let input = ((old_v.clone(), v),);
                    let output = aggregator.merge_value.call(input);
                    *old_v = output;
                } else {
                    bucket.insert(k, aggregator.create_combiner.call((v,)));
                }
            }

            for (i, bucket) in buckets.into_iter().enumerate() {
                let set: Vec<(K, C)> = bucket.into_iter().collect();
                let ser_bytes = bincode::serialize(&set).unwrap();
                log::debug!(
                    "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}: {:?}",
                    i,
                    self.shuffle_id,
                    partition,
                    set.get(0)
                );
                env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
            }
            log::debug!(
                "returning shuffle address for shuffle task #{}",
                self.shuffle_id
            );
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            log::info!("in dependency, shuffle write {:?}", dur);
            env::Env::get().shuffle_manager.get_server_uri()
        }
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
