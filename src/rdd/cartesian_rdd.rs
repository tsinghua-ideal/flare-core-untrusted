use itertools::{iproduct, Itertools};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{Env, RDDB_MAP};
use crate::error::{Error, Result};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::{mpsc::SyncSender, Arc};
use std::thread::JoinHandle;

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
pub struct CartesianRdd<T, U>
where
    T: Data,
    U: Data,
{
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    rdd1: Arc<dyn Rdd<Item = T>>,
    #[serde(with = "serde_traitobject")]
    rdd2: Arc<dyn Rdd<Item = U>>,
    num_partitions_in_rdd2: usize,
    _marker_t: PhantomData<T>,
    _market_u: PhantomData<U>,
}

impl<T, U> CartesianRdd<T, U>
where
    T: Data,
    U: Data,
{
    #[track_caller]
    pub(crate) fn new(rdd1: Arc<dyn Rdd<Item = T>>, rdd2: Arc<dyn Rdd<Item = U>>) -> Self {
        let vals = Arc::new(RddVals::new(rdd1.get_context(), rdd1.get_secure()));
        let num_partitions_in_rdd2 = rdd2.number_of_splits();
        CartesianRdd {
            vals,
            rdd1,
            rdd2,
            num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T, U> Clone for CartesianRdd<T, U>
where
    T: Data,
    U: Data,
{
    fn clone(&self) -> Self {
        CartesianRdd {
            vals: self.vals.clone(),
            rdd1: self.rdd1.clone(),
            rdd2: self.rdd2.clone(),
            num_partitions_in_rdd2: self.num_partitions_in_rdd2,
            _marker_t: PhantomData,
            _market_u: PhantomData,
        }
    }
}

impl<T, U> RddBase for CartesianRdd<T, U>
where
    T: Data,
    U: Data,
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

    fn iterator_raw(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(stage_id, split, acc_arg, tx)
    }

    default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<T, U> Rdd for CartesianRdd<T, U>
where
    T: Data,
    U: Data,
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

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        //TODO
        Err(Error::UnsupportedOperation("Unsupported secure_compute"))
    }
}
