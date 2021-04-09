use itertools::{iproduct, Itertools};

use crate::{context::Context};
use crate::dependency::Dependency;
use crate::env::{Env, RDDB_MAP};
use crate::error::{Error, Result};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::JoinHandle;
use parking_lot::Mutex;

#[derive(Clone, Serialize, Deserialize)]
struct CartesianSplit {
    idx: usize,
    s1_idx: usize,
    s2_idx: usize,
    #[serde(with = "serde_traitobject")]
    s1: Box<dyn Split>,
    #[serde(with = "serde_traitobject")]
    s2: Box<dyn Split>,
}

impl Split for CartesianSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct CartesianRdd<T, U, TE, UE, FE, FD> 
where 
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    rdd1: Arc<dyn Rdd<Item = T>>,
    #[serde(with = "serde_traitobject")]
    rdd2: Arc<dyn Rdd<Item = U>>,
    fe: FE,
    fd: FD,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T, U, TE, UE, FE, FD> CartesianRdd<T, U, TE, UE, FE, FD> 
where 
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    #[track_caller]
    pub(crate) fn new(
        rdd1: Arc<dyn Rdd<Item = T>>,
        rdd2: Arc<dyn Rdd<Item = U>>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let vals = Arc::new(RddVals::new(rdd1.get_context(), rdd1.get_secure()));
        let num_partitions_in_rdd2 = rdd2.number_of_splits();
        CartesianRdd {
            vals,
            rdd1,
            rdd2,
            fe,
            fd,
            num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T, U, TE, UE, FE, FD>  Clone for CartesianRdd<T, U, TE, UE, FE, FD> 
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    fn clone(&self) -> Self {
        CartesianRdd {
            vals: self.vals.clone(),
            rdd1: self.rdd1.clone(),
            rdd2: self.rdd2.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
            num_partitions_in_rdd2: self.num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T, U, TE, UE, FE, FD> RddBase for CartesianRdd<T, U, TE, UE, FE, FD> 
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: SerFunc(Vec<(T, U)>) -> (TE, UE),
    FD: SerFunc((TE, UE)) -> Vec<(T, U)>,
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
            Box::from_raw(ptr as *mut Vec<(TE, UE)>)
        };
    }

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_id(&self) -> OpId {
        self.vals.op_id
    }

    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        todo!()
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        todo!()
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<(TE, UE)>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        // create the cross product split
        let mut array =
            Vec::with_capacity(self.rdd1.number_of_splits() + self.rdd2.number_of_splits());
        for (s1, s2) in iproduct!(self.rdd1.splits().iter(), self.rdd2.splits().iter()) {
            let s1_idx = s1.get_index();
            let s2_idx = s2.get_index();
            let idx = s1_idx * self.num_partitions_in_rdd2 + s2_idx;
            array.push(Box::new(CartesianSplit {
                idx,
                s1_idx,
                s2_idx,
                s1: s1.clone(),
                s2: s2.clone(),
            }) as Box<dyn Split>);
        }
        array
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
    }
}

impl<T, U, TE, UE, FE, FD> Rdd for CartesianRdd<T, U, TE, UE, FE, FD> 
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: SerFunc(Vec<(T, U)>) -> (TE, UE),
    FD: SerFunc((TE, UE)) -> Vec<(T, U)>,
{
    type Item = (T, U);
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
    where
        Self: Sized,
    {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let current_split = split
            .downcast::<CartesianSplit>()
            .or(Err(Error::DowncastFailure("CartesianSplit")))?;

        let iter1 = self.rdd1.iterator(current_split.s1)?;
        // required because iter2 must be clonable:
        let iter2: Vec<_> = self.rdd2.iterator(current_split.s2)?.collect();
        Ok(Box::new(iter1.cartesian_product(iter2.into_iter())))
    }

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        //TODO
        Err(Error::UnsupportedOperation("Unsupported secure_compute"))
    }

}

impl<T, U, TE, UE, FE, FD> RddE for CartesianRdd<T, U, TE, UE, FE, FD> 
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: SerFunc(Vec<(T, U)>) -> (TE, UE),
    FD: SerFunc((TE, UE)) -> Vec<(T, U)>,
{
    type ItemE = (TE, UE);
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