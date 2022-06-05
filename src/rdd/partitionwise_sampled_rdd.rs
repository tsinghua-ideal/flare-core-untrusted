use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{Env, RDDB_MAP};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use crate::utils::random::RandomSampler;
use crate::Fn;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::sync::{mpsc::SyncSender, Arc};
use std::thread::JoinHandle;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T>
where
    T: Data,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    sampler: Arc<dyn RandomSampler<T>>,
    preserves_partitioning: bool,
}

impl<T> PartitionwiseSampledRdd<T>
where
    T: Data,
{
    #[track_caller]
    pub(crate) fn new(
        prev: Arc<dyn Rdd<Item = T>>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);

        PartitionwiseSampledRdd {
            prev,
            vals,
            sampler,
            preserves_partitioning,
        }
    }
}

impl<T> Clone for PartitionwiseSampledRdd<T>
where
    T: Data,
{
    fn clone(&self) -> Self {
        PartitionwiseSampledRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            sampler: self.sampler.clone(),
            preserves_partitioning: self.preserves_partitioning,
        }
    }
}

impl<T> RddBase for PartitionwiseSampledRdd<T>
where
    T: Data,
{
    fn cache(&self) {
        self.vals.cache();
        RDDB_MAP.insert(self.get_rdd_id(), self.get_rdd_base());
    }

    fn should_cache(&self) -> bool {
        self.vals.should_cache()
    }

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_id(&self) -> OpId {
        self.vals.op_id
    }

    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        op_ids.push(self.get_op_id());
        if !self.should_cache() {
            self.prev.get_op_ids(op_ids);
        }
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::NarrowDependency(Arc::new(
            OneToOneDependency::new(self.prev.get_rdd_base()),
        ))]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        if self.preserves_partitioning {
            self.prev.partitioner()
        } else {
            None
        }
    }

    fn iterator_raw(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(stage_id, split, acc_arg, tx)
    }

    default fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }

    default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<T, V> RddBase for PartitionwiseSampledRdd<(T, V)>
where
    T: Data,
    V: Data,
{
}

impl<T> Rdd for PartitionwiseSampledRdd<T>
where
    T: Data,
{
    type Item = T;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let sampler_func = self.sampler.get_sampler(None);
        let iter = self.prev.iterator(split)?;
        Ok(Box::new(sampler_func(iter).into_iter()) as Box<dyn Iterator<Item = T>>)
    }

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_part_id = split.get_index();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_quadruple(cur_rdd_id, cur_op_id, cur_part_id, cur_split_num);
        let should_cache = self.should_cache();
        if should_cache {
            let mut handles = secure_compute_cached(acc_arg, cur_rdd_id, cur_part_id, tx.clone());

            if handles.is_empty() {
                acc_arg.set_caching_rdd_id(cur_rdd_id);
                handles.append(&mut self.prev.secure_compute(stage_id, split, acc_arg, tx)?);
            }
            Ok(handles)
        } else {
            self.prev.secure_compute(stage_id, split, acc_arg, tx)
        }
    }
}
