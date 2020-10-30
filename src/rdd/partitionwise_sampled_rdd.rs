use crate::Fn;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::{Rdd, RddE, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use crate::utils::random::RandomSampler;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use parking_lot::Mutex;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(with = "serde_traitobject")]
    sampler: Arc<dyn RandomSampler<T>>,
    preserves_partitioning: bool,
    fe: FE,
    fd: FD,
}

impl<T, TE, FE, FD> PartitionwiseSampledRdd<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    pub(crate) fn new(
        prev: Arc<dyn Rdd<Item = T>>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
        fe: FE,
        fd: FD,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        let ecall_ids = prev.get_ecall_ids();

        PartitionwiseSampledRdd {
            prev,
            vals,
            ecall_ids,
            sampler,
            preserves_partitioning,
            fe,
            fd,
        }
    }
}

impl<T, TE, FE, FD> Clone for PartitionwiseSampledRdd<T, TE, FE, FD> 
where 
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> Vec<TE> + Clone,
    FD: Func(Vec<TE>) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        PartitionwiseSampledRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            ecall_ids: self.ecall_ids.clone(),
            sampler: self.sampler.clone(),
            preserves_partitioning: self.preserves_partitioning,
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, TE, FE, FD> RddBase for PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>, 
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

    fn iterator_raw(&self, split: Box<dyn Split>) -> Result<Vec<usize>> {
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
        log::debug!("inside PartitionwiseSampledRdd iterator_any");
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }
}

impl<T, V, TE, VE, FE, FD> RddBase for PartitionwiseSampledRdd<(T, V), (TE, VE), FE, FD> 
where 
    T: Data,
    V: Data,
    TE: Data,
    VE: Data,
    FE: SerFunc(Vec<(T, V)>) -> Vec<(TE, VE)>,
    FD: SerFunc(Vec<(TE, VE)>) -> Vec<(T, V)>, 
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside PartitionwiseSampledRdd cogroup_iterator_any",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<T, TE, FE, FD> Rdd for PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>, 
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
    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Result<Vec<usize>> {
        self.prev.secure_compute(split, id)
    }

}

impl<T, TE, FE, FD> RddE for PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> Vec<TE>,
    FD: SerFunc(Vec<TE>) -> Vec<T>, 
{
    type ItemE = TE;
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>>    
    }

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>>
    } 
}
