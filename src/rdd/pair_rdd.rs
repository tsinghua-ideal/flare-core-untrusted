use std::hash::Hash;
use std::sync::{mpsc::SyncSender, Arc};
use std::thread::JoinHandle;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{Env, RDDB_MAP};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::co_grouped_rdd::CoGroupedRdd;
use crate::rdd::shuffled_rdd::ShuffledRdd;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};

// Trait containing pair rdd methods. No need of implicit conversion like in Spark version.
pub trait PairRdd<K, V>: Rdd<Item = (K, V)> + Send + Sync
where
    K: Data + Eq + Hash,
    V: Data,
{
    #[track_caller]
    fn combine_by_key<C: Data>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, C)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        SerArc::new(ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
        ))
    }

    #[track_caller]
    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    #[track_caller]
    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, Vec<V>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner)
    }

    #[track_caller]
    fn reduce_by_key<F>(&self, func: F, num_splits: usize) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
    }

    #[track_caller]
    fn reduce_by_key_using_partitioner<F>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, V)>>
    where
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let create_combiner = Box::new(Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner)
    }

    #[track_caller]
    fn values(&self) -> SerArc<dyn Rdd<Item = V>>
    where
        Self: Sized,
    {
        SerArc::new(ValuesRdd::new(self.get_rdd()))
    }

    #[track_caller]
    fn map_values<U, F>(&self, f: F) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        Self: Sized,
        F: SerFunc(V) -> U + Clone,
        U: Data,
    {
        SerArc::new(MappedValuesRdd::new(self.get_rdd(), f))
    }

    #[track_caller]
    fn flat_map_values<U, F>(&self, f: F) -> SerArc<dyn Rdd<Item = (K, U)>>
    where
        Self: Sized,
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        U: Data,
    {
        SerArc::new(FlatMappedValuesRdd::new(self.get_rdd(), f))
    }

    #[track_caller]
    fn join<W>(
        &self,
        other: SerArc<dyn Rdd<Item = (K, W)>>,
        num_splits: usize,
    ) -> SerArc<dyn Rdd<Item = (K, (V, W))>>
    where
        W: Data + Default,
    {
        let f = Fn!(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });

        let cogrouped = self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        cogrouped.flat_map_values(Box::new(f))
    }

    #[track_caller]
    fn cogroup<W>(
        &self,
        other: SerArc<dyn Rdd<Item = (K, W)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn Rdd<Item = (K, (Vec<V>, Vec<W>))>>
    where
        W: Data,
    {
        SerArc::new(CoGroupedRdd::new(
            self.get_rdd(),
            other.get_rdd(),
            partitioner,
        ))
    }

    #[track_caller]
    fn partition_by_key(&self, partitioner: Box<dyn Partitioner>) -> SerArc<dyn Rdd<Item = V>> {
        // Guarantee the number of partitions by introducing a shuffle phase
        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
            partitioner,
        );
        // Flatten the results of the combined partitions
        let flattener = Fn!(|grouped: (K, Vec<V>)| {
            let (_key, values) = grouped;
            let iter: Box<dyn Iterator<Item = _>> = Box::new(values.into_iter());
            iter
        });
        self.get_context().add_num(1);
        shuffle_steep.flat_map(flattener)
    }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K, V, T> PairRdd<K, V> for T
where
    T: Rdd<Item = (K, V)>,
    K: Data + Eq + Hash,
    V: Data,
{
}

impl<K, V, T> PairRdd<K, V> for SerArc<T>
where
    T: Rdd<Item = (K, V)>,
    K: Data + Eq + Hash,
    V: Data,
{
}

#[derive(Serialize, Deserialize)]
pub struct ValuesRdd<K, V>
where
    K: Data,
    V: Data,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
}

impl<K, V> Clone for ValuesRdd<K, V>
where
    K: Data,
    V: Data,
{
    fn clone(&self) -> Self {
        ValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
        }
    }
}

impl<K, V> ValuesRdd<K, V>
where
    K: Data,
    V: Data,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        ValuesRdd { prev, vals }
    }
}

impl<K, V> RddBase for ValuesRdd<K, V>
where
    K: Data,
    V: Data,
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

    default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<K, V> Rdd for ValuesRdd<K, V>
where
    K: Data,
    V: Data,
{
    type Item = V;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        Ok(Box::new(self.prev.iterator(split)?.map(|(k, v)| v)))
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

#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
}

impl<K, V, U, F> Clone for MappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
        }
    }
}

impl<K, V, U, F> MappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> U + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        MappedValuesRdd { prev, vals, f }
    }
}

impl<K, V, U, F> RddBase for MappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> U,
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

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<K, V, U, F> Rdd for MappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> U,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
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

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    f: F,
}

impl<K, V, U, F> Clone for FlatMappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            f: self.f.clone(),
        }
    }
}

impl<K, V, U, F> FlatMappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        FlatMappedValuesRdd { prev, vals, f }
    }
}

impl<K, V, U, F> RddBase for FlatMappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
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

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<K, V, U, F> Rdd for FlatMappedValuesRdd<K, V, U, F>
where
    K: Data,
    V: Data,
    U: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
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
