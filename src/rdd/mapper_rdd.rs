use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{
    atomic::{AtomicBool, Ordering::SeqCst},
    mpsc::SyncSender,
    Arc,
};
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
pub struct MapperRdd<T: Data, U: Data, F>
where
    F: Func(T) -> U + Clone,
{
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
}

// Can't derive clone automatically
impl<T: Data, U: Data, F> Clone for MapperRdd<T, U, F>
where
    F: Func(T) -> U + Clone,
{
    fn clone(&self) -> Self {
        MapperRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
        }
    }
}

impl<T: Data, U: Data, F> MapperRdd<T, U, F>
where
    F: Func(T) -> U + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        MapperRdd {
            name: Mutex::new("map".to_owned()),
            prev,
            vals,
            f,
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T: Data, U: Data, F> RddBase for MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U,
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

    fn get_op_name(&self) -> String {
        self.name.lock().to_owned()
    }

    fn register_op_name(&self, name: &str) {
        let own_name = &mut *self.name.lock();
        *own_name = name.to_owned();
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::NarrowDependency(Arc::new(
            OneToOneDependency::new(self.prev.get_rdd_base()),
        ))]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
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
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T, V, U, F> RddBase for MapperRdd<T, (V, U), F>
where
    T: Data,
    V: Data,
    U: Data,
    F: SerFunc(T) -> (V, U),
{
}

impl<T: Data, U: Data, F> Rdd for MapperRdd<T, U, F>
where
    F: SerFunc(T) -> U,
{
    type Item = U;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        Ok(Box::new(self.prev.iterator(split)?.map(self.f.clone())))
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
