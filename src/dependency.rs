use crate::aggregator::Aggregator;
use crate::env;
use crate::partitioner::Partitioner;
use crate::rdd::{
    default_hash, free_res_enc, get_encrypted_data, wrapper_secure_execute, AccArg, ItemE, OpId,
    RddBase,
};
use crate::serializable_traits::Data;
use crate::shuffle::ShuffleFetcher;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use sgx_types::*;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{
    atomic,
    mpsc::{self, RecvError},
    Arc,
};
use std::time::{Duration, Instant};

#[repr(C)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct DepInfo {
    pub is_shuffle: u8,
    pub identifier: usize,
    pub parent_rdd_id: usize,
    pub child_rdd_id: usize,
    parent_op_id: OpId,
    child_op_id: OpId,
}

impl DepInfo {
    pub fn new(
        is_shuffle: u8,
        identifier: usize,
        parent_rdd_id: usize,
        child_rdd_id: usize,
        parent_op_id: OpId,
        child_op_id: OpId,
    ) -> Self {
        DepInfo {
            is_shuffle,
            identifier,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            child_op_id,
        }
    }

    //This for shuffle read or narrow
    pub fn padding_new(is_shuffle: u8) -> Self {
        DepInfo {
            is_shuffle,
            identifier: 0,
            parent_rdd_id: 0,
            child_rdd_id: 0,
            parent_op_id: Default::default(),
            child_op_id: Default::default(),
        }
    }

    pub fn dep_type(&self) -> u8 {
        self.is_shuffle
    }
}

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
    /// the start of the range in the parent RDD
    in_start: usize,
    /// the start of the range in the child RDD
    out_start: usize,
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
    fn get_dep_info(&self) -> DepInfo;
    fn get_shuffle_id(&self) -> usize;
    fn get_rdd_base(&self) -> Arc<dyn RddBase>;
    fn is_shuffle(&self) -> bool;
    fn do_shuffle_task(
        &self,
        stage_id: usize,
        rdd_base: Arc<dyn RddBase>,
        partition: usize,
    ) -> String;
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
pub(crate) struct ShuffleDependency<K, V, C>
where
    K: Data,
    V: Data,
    C: Data,
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
    identifier: usize,
    parent_rdd_id: usize,
    child_rdd_id: usize,
    parent_op_id: OpId,
    child_op_id: OpId,
}

impl<K, V, C> ShuffleDependency<K, V, C>
where
    K: Data,
    V: Data,
    C: Data,
{
    pub fn new(
        shuffle_id: usize,
        is_cogroup: bool,
        rdd_base: Arc<dyn RddBase>,
        aggregator: Arc<Aggregator<K, V, C>>,
        partitioner: Box<dyn Partitioner>,
        identifier: usize,
        child_rdd_id: usize,
        child_op_id: OpId,
    ) -> Self {
        let parent_rdd_id = rdd_base.get_rdd_id();
        let parent_op_id = rdd_base.get_op_id();
        ShuffleDependency {
            shuffle_id,
            is_cogroup,
            rdd_base,
            aggregator,
            partitioner,
            is_shuffle: true,
            identifier,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            child_op_id,
        }
    }
}

impl<K, V, C> ShuffleDependencyTrait for ShuffleDependency<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
{
    fn get_dep_info(&self) -> DepInfo {
        DepInfo::new(
            1,
            self.identifier,
            self.parent_rdd_id,
            self.child_rdd_id,
            self.parent_op_id,
            self.child_op_id,
        )
    }

    fn get_shuffle_id(&self) -> usize {
        self.shuffle_id
    }

    fn is_shuffle(&self) -> bool {
        self.is_shuffle
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        self.rdd_base.clone()
    }

    fn do_shuffle_task(
        &self,
        stage_id: usize,
        rdd_base: Arc<dyn RddBase>,
        partition: usize,
    ) -> String {
        log::debug!(
            "executing shuffle task #{} for partition #{}",
            self.shuffle_id,
            partition
        );

        log::debug!(
            "rdd id {:?}, secure: {:?}",
            rdd_base.get_rdd_id(),
            rdd_base.get_secure()
        );
        if rdd_base.get_secure() {
            let mut op_ids = vec![self.child_op_id];
            rdd_base.get_op_ids(&mut op_ids);
            let hash_ops = default_hash(&op_ids);
            let mut dep_info = self.get_dep_info();
            let key = (hash_ops, partition, dep_info.identifier);

            println!("in denepdency, key = {:?}, ops = {:?}", key, op_ids);
            let now = Instant::now();
            let (tx, rx) = mpsc::sync_channel(0);
            let mut acc_arg = AccArg::new(
                dep_info,
                Some(self.partitioner.get_num_of_partitions()),
                Arc::new(atomic::AtomicBool::new(false)),
            );

            let split = rdd_base.splits()[partition].clone();
            log::debug!("split index: {}", split.get_index());
            let handles = rdd_base
                .iterator_raw(stage_id, split, &mut acc_arg, tx)
                .unwrap();

            let num_output_splits = self.partitioner.get_num_of_partitions();
            let buckets = match rx.recv() {
                Ok((buckets_ptr, marks_ptr)) => {
                    assert_eq!(marks_ptr, 0usize);
                    if self.aggregator.is_default {
                        let mut info = get_encrypted_data::<ItemE, Vec<ItemE>>(
                            rdd_base.get_op_id(),
                            dep_info,
                            buckets_ptr,
                            marks_ptr,
                        )
                        .0;
                        acc_arg.free_enclave_lock();
                        let num_invalids = info.pop().unwrap();
                        let data_ptr = info.pop().unwrap();
                        let num_invalids = futures::executor::block_on(ShuffleFetcher::fetch_sync(
                            (stage_id, 0, rdd_base.number_of_splits()),
                            partition,
                            num_invalids,
                        ))
                        .unwrap();
                        //finish the shuffle
                        acc_arg.get_enclave_lock();
                        dep_info.is_shuffle = 11;
                        let (data_ptr, marks_ptr) = wrapper_secure_execute(
                            stage_id,
                            &vec![rdd_base.get_rdd_id()],
                            &vec![rdd_base.get_op_id()],
                            &vec![partition],
                            Default::default(),
                            dep_info,
                            &(data_ptr, num_invalids),
                            &Vec::<ItemE>::new(),
                            &HashMap::new(),
                        );

                        assert_eq!(marks_ptr, 0);
                        let mut buckets = get_encrypted_data::<Vec<ItemE>, ItemE>(
                            rdd_base.get_op_id(),
                            dep_info,
                            data_ptr,
                            marks_ptr,
                        )
                        .0;
                        acc_arg.free_enclave_lock();
                        buckets.resize(num_output_splits, Vec::new());
                        buckets
                    } else {
                        let mut buckets = get_encrypted_data::<Vec<ItemE>, Vec<ItemE>>(
                            rdd_base.get_op_id(),
                            dep_info,
                            buckets_ptr,
                            marks_ptr,
                        )
                        .0;
                        acc_arg.free_enclave_lock();
                        buckets.resize(num_output_splits, Vec::new());
                        buckets
                    }
                }
                Err(RecvError) => vec![Vec::new(); num_output_splits],
            };

            for handle in handles {
                handle.join().unwrap();
            }
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            log::info!("in dependency, shuffle write {:?}", dur);
            for (i, bucket) in buckets.into_iter().enumerate() {
                let ser_bytes = bincode::serialize(&bucket).unwrap();
                env::SHUFFLE_CACHE.insert((self.shuffle_id, partition, i), ser_bytes);
            }

            env::Env::get().shuffle_manager.get_server_uri()
        } else {
            let split = rdd_base.splits()[partition].clone();
            log::debug!("split index: {}", split.get_index());
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

            for (count, i) in iter
                .unwrap()
                .into_any()
                .downcast::<Vec<(K, V)>>()
                .unwrap()
                .into_iter()
                .enumerate()
            {
                let (k, v) = i;
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
                    "shuffle dependency map task set from bucket #{} in shuffle id #{}, partition #{}",
                    i,
                    self.shuffle_id,
                    partition,
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
