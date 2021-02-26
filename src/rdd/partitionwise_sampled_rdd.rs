use crate::Fn;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{RDDB_MAP, Env};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use crate::utils::random::RandomSampler;
use serde_derive::{Deserialize, Serialize};
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::JoinHandle;
use parking_lot::Mutex;

#[derive(Serialize, Deserialize)]
pub struct PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
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
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    #[track_caller]
    pub(crate) fn new(
        prev: Arc<dyn Rdd<Item = T>>,
        sampler: Arc<dyn RandomSampler<T>>,
        preserves_partitioning: bool,
        fe: FE,
        fd: FD,
    ) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);

        PartitionwiseSampledRdd {
            prev,
            vals,
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
    FE: Func(Vec<T>) -> TE + Clone,
    FD: Func(TE) -> Vec<T> + Clone,
{
    fn clone(&self) -> Self {
        PartitionwiseSampledRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
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
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
{
    fn cache(&self) {
        self.vals.cache();
        RDDB_MAP.insert(
            self.get_rdd_id(), 
            self.get_rdd_base()
        );
    }

    fn should_cache(&self) -> bool {
        self.vals.should_cache()
    }

    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe {
            Box::from_raw(ptr as *mut Vec<TE>)
        };
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

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<TE>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
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

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
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
    FE: SerFunc(Vec<(T, V)>) -> (TE, VE),
    FD: SerFunc((TE, VE)) -> Vec<(T, V)>, 
{

}

impl<T, TE, FE, FD> Rdd for PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
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
    
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_rdd_id(cur_rdd_id);
        acc_arg.insert_op_id(cur_op_id);
        acc_arg.insert_split_num(cur_split_num);
        let captured_vars = Env::get().captured_vars.lock().unwrap().clone();
        let should_cache = self.should_cache();
        if should_cache {
            let mut handles = secure_compute_cached(
                acc_arg, 
                cur_rdd_id, 
                tx.clone(),
                captured_vars,
            );

            if !acc_arg.totally_cached() {
                acc_arg.set_caching_rdd_id(cur_rdd_id);
                handles.append(&mut self.prev.secure_compute(split, acc_arg, tx)?);
            }
            Ok(handles)     
        } else {
            self.prev.secure_compute(split, acc_arg, tx)
        }
    }

}

impl<T, TE, FE, FD> RddE for PartitionwiseSampledRdd<T, TE, FE, FD> 
where
    T: Data,
    TE: Data,
    FE: SerFunc(Vec<T>) -> TE,
    FD: SerFunc(TE) -> Vec<T>, 
{
    type ItemE = TE;
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
