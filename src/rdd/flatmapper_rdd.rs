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
pub struct FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<T, U, UE, F, FE, FD> Clone for FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        FlatMapperRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<T, U, UE, F, FE, FD> FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: Func(T) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<U>) -> UE + Clone,
    FD: Func(UE) -> Vec<U> + Clone,
{
    #[track_caller]
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        FlatMapperRdd {
            prev,
            vals,
            f,
            fe,
            fd,
        }
    }
}

impl<T, U, UE, F, FE, FD> RddBase for FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    fn cache(&self) {
        self.vals.cache();
        RDDB_MAP.insert(self.get_rdd_id(), self.get_rdd_base());
    }

    fn should_cache(&self) -> bool {
        self.vals.should_cache()
    }

    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe { Box::from_raw(ptr as *mut Vec<UE>) };
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
        let value = move_data::<UE>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_raw(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }

    default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<T, V, U, VE, UE, F, FE, FD> RddBase for FlatMapperRdd<T, (V, U), (VE, UE), F, FE, FD>
where
    T: Data,
    V: Data,
    U: Data,
    VE: Data,
    UE: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = (V, U)>>,
    FE: SerFunc(Vec<(V, U)>) -> (VE, UE),
    FD: SerFunc((VE, UE)) -> Vec<(V, U)>,
{
}

impl<T, U, UE, F, FE, FD> Rdd for FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
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
                handles.append(&mut self.prev.secure_compute(split, acc_arg, tx)?);
            }
            Ok(handles)
        } else {
            self.prev.secure_compute(split, acc_arg, tx)
        }
    }
}

impl<T, U, UE, F, FE, FD> RddE for FlatMapperRdd<T, U, UE, F, FE, FD>
where
    T: Data,
    U: Data,
    UE: Data,
    F: SerFunc(T) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<U>) -> UE,
    FD: SerFunc(UE) -> Vec<U>,
{
    type ItemE = UE;
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>) -> Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>) -> Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE) -> Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE) -> Vec<Self::Item>>
    }
}
