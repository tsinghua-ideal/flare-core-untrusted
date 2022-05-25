use std::cmp::min;
use std::marker::PhantomData;
use std::sync::{mpsc::SyncSender, Arc};
use std::thread::JoinHandle;

use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{Env, RDDB_MAP};
use crate::error::{Error, Result};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct ZippedPartitionsSplit {
    fst_idx: usize,
    sec_idx: usize,
    idx: usize,

    #[serde(with = "serde_traitobject")]
    fst_split: Box<dyn Split>,
    #[serde(with = "serde_traitobject")]
    sec_split: Box<dyn Split>,
}

impl Split for ZippedPartitionsSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    first: Arc<dyn RddE<Item = T, ItemE = TE>>,
    #[serde(with = "serde_traitobject")]
    second: Arc<dyn RddE<Item = U, ItemE = UE>>,
    vals: Arc<RddVals>,
    fe: FE,
    fd: FD,
    _marker_t: PhantomData<(TE, UE)>,
}

impl<T, U, TE, UE, FE, FD> Clone for ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    fn clone(&self) -> Self {
        ZippedPartitionsRdd {
            first: self.first.clone(),
            second: self.second.clone(),
            vals: self.vals.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
            _marker_t: PhantomData,
        }
    }
}

impl<T, U, TE, UE, FE, FD> ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: Func(Vec<(T, U)>) -> (TE, UE) + Clone,
    FD: Func((TE, UE)) -> Vec<(T, U)> + Clone,
{
    #[track_caller]
    pub fn new(
        first: Arc<dyn RddE<Item = T, ItemE = TE>>,
        second: Arc<dyn RddE<Item = U, ItemE = UE>>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let vals = RddVals::new(first.get_context(), first.get_secure()); //temp
        let vals = Arc::new(vals);

        ZippedPartitionsRdd {
            first,
            second,
            vals,
            fe,
            fd,
            _marker_t: PhantomData,
        }
    }

    fn secure_compute_prev(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let current_split = split
            .downcast::<ZippedPartitionsSplit>()
            .or(Err(Error::DowncastFailure("ZippedPartitionsSplit")))?;

        let dep_info = DepInfo::padding_new(0);
        let fst = self
            .first
            .secure_iterator(current_split.fst_split.clone(), dep_info.clone(), None)?
            .collect::<Vec<_>>();
        let sec = self
            .second
            .secure_iterator(current_split.sec_split.clone(), dep_info, None)?
            .collect::<Vec<_>>();

        let data = self.secure_zip((fst, sec), acc_arg);
        let acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let now = Instant::now();
            let wait = start_execute(acc_arg, data, tx);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in zipped rdd, compute, total {:?}***", dur);
        });
        Ok(vec![handle])
    }

    fn secure_zip(&self, data: (Vec<TE>, Vec<UE>), acc_arg: &mut AccArg) -> Vec<(TE, UE)> {
        acc_arg.get_enclave_lock();
        let cur_rdd_ids = vec![self.vals.id];
        let cur_op_ids = vec![self.vals.op_id];
        let cur_part_ids = vec![*acc_arg.part_ids.last().unwrap()];
        let dep_info = DepInfo::padding_new(2);

        let result_ptr = wrapper_secure_execute(
            &cur_rdd_ids,
            &cur_op_ids,
            &cur_part_ids,
            Default::default(),
            dep_info,
            &data,
            &acc_arg.captured_vars,
        );
        let result = get_encrypted_data::<(TE, UE)>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
        acc_arg.free_enclave_lock();
        *result
    }
}

impl<T, U, TE, UE, FE, FD> RddBase for ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
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
        RDDB_MAP.insert(self.get_rdd_id(), self.get_rdd_base());
    }

    fn should_cache(&self) -> bool {
        self.vals.should_cache()
    }

    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe { Box::from_raw(ptr as *mut Vec<(TE, UE)>) };
    }

    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_op_id(&self) -> OpId {
        self.vals.op_id
    }

    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        op_ids.push(self.get_op_id());
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![
            Dependency::NarrowDependency(Arc::new(OneToOneDependency::new(
                self.first.get_rdd_base(),
            ))),
            Dependency::NarrowDependency(Arc::new(OneToOneDependency::new(
                self.second.get_rdd_base(),
            ))),
        ]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        let value = move_data::<(TE, UE)>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut arr = Vec::with_capacity(min(
            self.first.number_of_splits(),
            self.second.number_of_splits(),
        ));

        for (fst, sec) in self.first.splits().iter().zip(self.second.splits().iter()) {
            let fst_idx = fst.get_index();
            let sec_idx = sec.get_index();

            arr.push(Box::new(ZippedPartitionsSplit {
                fst_idx,
                sec_idx,
                idx: fst_idx,
                fst_split: fst.clone(),
                sec_split: sec.clone(),
            }) as Box<dyn Split>)
        }
        arr
    }

    fn number_of_splits(&self) -> usize {
        self.splits().len()
    }

    fn iterator_raw(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }

    fn cogroup_iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        self.iterator_any(split)
    }
}

impl<T, U, TE, UE, FE, FD> Rdd for ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
where
    T: Data,
    U: Data,
    TE: Data,
    UE: Data,
    FE: SerFunc(Vec<(T, U)>) -> (TE, UE),
    FD: SerFunc((TE, UE)) -> Vec<(T, U)>,
{
    type Item = (T, U);

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let current_split = split
            .downcast::<ZippedPartitionsSplit>()
            .or(Err(Error::DowncastFailure("ZippedPartitionsSplit")))?;

        let fst_iter = self.first.iterator(current_split.fst_split.clone())?;
        let sec_iter = self.second.iterator(current_split.sec_split.clone())?;
        Ok(Box::new(fst_iter.zip(sec_iter)))
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

        let should_cache = self.should_cache();
        if should_cache {
            let mut handles = secure_compute_cached(acc_arg, cur_rdd_id, cur_part_id, tx.clone());

            if handles.is_empty() {
                acc_arg.set_caching_rdd_id(cur_rdd_id);
                handles.append(&mut self.secure_compute_prev(split, acc_arg, tx)?);
            }
            Ok(handles)
        } else {
            self.secure_compute_prev(split, acc_arg, tx)
        }
    }

    fn iterator(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        self.compute(split.clone())
    }
}

impl<T, U, TE, UE, FE, FD> RddE for ZippedPartitionsRdd<T, U, TE, UE, FE, FD>
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

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>) -> Self::ItemE> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>) -> Self::ItemE>
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE) -> Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Self::ItemE) -> Vec<Self::Item>>
    }
}
