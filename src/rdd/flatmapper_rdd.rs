use std::sync::{mpsc::SyncSender, Arc};
use std::thread::JoinHandle;

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{Env, RDDB_MAP};
use crate::error::Result;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct FlatMapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
}

impl<T, U, F> Clone for FlatMapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
        }
    }
}

impl<T, U, F> FlatMapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        FlatMapperRdd { prev, vals, f }
    }
}

impl<T, U, F> RddBase for FlatMapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
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
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<T, V, U, F> RddBase for FlatMapperRdd<T, (V, U), F>
where
    T: Data,
    V: Data,
    U: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)>>,
{
}

impl<T, U, F> Rdd for FlatMapperRdd<T, U, F>
where
    T: Data,
    U: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(self.prev.iterator(split)?.flat_map(f)))
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
        let captured_vars = self.f.get_ser_captured_var();
        if !captured_vars.is_empty() {
            acc_arg.captured_vars.insert(cur_rdd_id, captured_vars);
        }
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
