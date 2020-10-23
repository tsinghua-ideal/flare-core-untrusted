use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::sync::{atomic::AtomicBool, atomic::Ordering::SeqCst, Arc};

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::env::Env;
use crate::rdd::{Rdd, RddBase, RddVals};
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MapperRdd<T: Data, U: Data, UE: Data, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> Vec<UE> + Clone, 
    FD: Func(Vec<UE>) -> Vec<U> + Clone,
{
    #[serde(skip_serializing, skip_deserializing)]
    name: Mutex<String>,
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = T>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    f: F,
    pinned: AtomicBool,
    _marker_t: PhantomData<T>, // phantom data is necessary because of type parameter T
    _marker_ue: PhantomData<UE>,
    _marker_fe: PhantomData<FE>,
    _marker_fd: PhantomData<FD>,
}

// Can't derive clone automatically
impl<T: Data, U: Data, UE: Data, F, FE, FD> Clone for MapperRdd<T, U, UE, F, FE, FD>
where
    F: Func(T) -> U + Clone,
    FE: Func(Vec<U>) -> Vec<UE> + Clone,
    FD: Func(Vec<UE>) -> Vec<U> + Clone,
{
    fn clone(&self) -> Self {
        MapperRdd {
            name: Mutex::new(self.name.lock().clone()),
            prev: self.prev.clone(),
            ecall_ids: self.ecall_ids.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
            pinned: AtomicBool::new(self.pinned.load(SeqCst)),
            _marker_t: PhantomData,
            _marker_ue: PhantomData,
            _marker_fe: PhantomData,
            _marker_fd: PhantomData,
        }
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> MapperRdd<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
{
    pub(crate) fn new(prev: Arc<dyn Rdd<Item = T>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        let ecall_ids = prev.get_ecall_ids();
        MapperRdd {
            name: Mutex::new("map".to_owned()),
            prev,
            ecall_ids,
            vals,
            f,
            pinned: AtomicBool::new(false),
            _marker_t: PhantomData,
            _marker_ue: PhantomData,
            _marker_fe: PhantomData,
            _marker_fd: PhantomData,
        }
    }

    pub(crate) fn pin(self) -> Self {
        self.pinned.store(true, SeqCst);
        self
    }
}

impl<T: Data, U: Data, UE: Data, F, FE, FD> RddBase for MapperRdd<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
{
    fn get_rdd_id(&self) -> usize {
        self.vals.id
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

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        self.prev.preferred_locations(split)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }

    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_raw(&self, split: Box<dyn Split>) -> Vec<usize> {
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
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
        ))
    }

    fn is_pinned(&self) -> bool {
        self.pinned.load(SeqCst)
    }
}

impl<T: Data, V: Data, U: Data, UE: Data, F, FE, FD> RddBase for MapperRdd<T, (V, U), UE, F, FE, FD>
where
    F: SerFunc(T) -> (V, U),
    FE: SerFunc(Vec<(V, U)>) -> Vec<UE>,  //need check
    FD: SerFunc(Vec<UE>) -> Vec<(V, U)>,  //need check
{
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any maprdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<T: Data, U: Data, UE: Data, F: 'static, FE: 'static, FD: 'static> Rdd for MapperRdd<T, U, UE, F, FE, FD>
where
    F: SerFunc(T) -> U,
    FE: SerFunc(Vec<U>) -> Vec<UE>,
    FD: SerFunc(Vec<UE>) -> Vec<U>,
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

    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<usize> {
        let captured_vars = self.f.get_ser_captured_var();
        if !captured_vars.is_empty() {
            Env::get().captured_vars
                .lock()
                .unwrap()
                .insert(self.get_rdd_id(), captured_vars);
        }
        println!("captured_vars: {:?}", *Env::get().captured_vars.lock().unwrap());
        self.prev.secure_compute(split, id)
    }
     
}
