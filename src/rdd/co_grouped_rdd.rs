use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::slice::Chunks;
use std::sync::{
    mpsc::{self, SyncSender},
    Arc,
};
use std::thread::{self, JoinHandle};

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::env::{Env, BOUNDED_MEM_CACHE, RDDB_MAP};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::serialization_free::Construct;
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use sgx_types::*;

#[derive(Clone, Serialize, Deserialize)]
enum CoGroupSplitDep {
    NarrowCoGroupSplitDep {
        #[serde(with = "serde_traitobject")]
        rdd: Arc<dyn RddBase>,
        #[serde(with = "serde_traitobject")]
        split: Box<dyn Split>,
    },
    ShuffleCoGroupSplitDep {
        shuffle_id: usize,
    },
}

#[derive(Clone, Serialize, Deserialize)]
struct CoGroupSplit {
    index: usize,
    deps: Vec<CoGroupSplitDep>,
}

impl CoGroupSplit {
    fn new(index: usize, deps: Vec<CoGroupSplitDep>) -> Self {
        CoGroupSplit { index, deps }
    }
}

impl Hasher for CoGroupSplit {
    fn finish(&self) -> u64 {
        self.index as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        for i in bytes {
            self.write_u8(*i);
        }
    }
}

impl Split for CoGroupSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct CoGroupedRdd<K, V, W>
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
{
    pub(crate) vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd0: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd1: Arc<dyn Rdd<Item = (K, W)>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) part: Box<dyn Partitioner>,
}

impl<K, V, W> CoGroupedRdd<K, V, W>
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
{
    #[track_caller]
    pub fn new(
        rdd0: Arc<dyn Rdd<Item = (K, V)>>,
        rdd1: Arc<dyn Rdd<Item = (K, W)>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let context = rdd0.get_context();
        let secure = rdd0.get_secure() || rdd1.get_secure(); //
        let mut vals = RddVals::new(context.clone(), secure);

        if !rdd0
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            vals.shuffle_ids.push(context.new_shuffle_id());
        }

        if !rdd1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            vals.shuffle_ids.push(context.new_shuffle_id());
        }

        let vals = Arc::new(vals);
        CoGroupedRdd {
            vals,
            rdd0,
            rdd1,
            part,
        }
    }

    fn secure_compute_prev(
        &self,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut deps = split.clone().deps;
            let mut num_sub_part = vec![0, 0]; //kv, kw
            let mut kv = (Vec::new(), Vec::new());
            match deps.remove(0) {
                //rdd0
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let (tx, rx) = mpsc::sync_channel(0);
                    let part_id = split.get_index();
                    let mut acc_arg_cg = AccArg::new(
                        DepInfo::padding_new(0),
                        None,
                        Arc::new(AtomicBool::new(false)),
                    );
                    let handles = rdd.iterator_raw(split, &mut acc_arg_cg, tx)?; //TODO need sorted
                    let mut kv_0 = Vec::new();

                    for received in rx {
                        let result_bl = get_encrypted_data::<ItemE>(
                            rdd.get_op_id(),
                            acc_arg_cg.dep_info,
                            received as *mut u8,
                        );
                        acc_arg_cg.free_enclave_lock();
                        kv_0.push(*result_bl);
                    }

                    if acc_arg_cg.is_caching_final_rdd() {
                        let size = kv_0.get_size();
                        let data_ptr = Box::into_raw(Box::new(kv_0.clone()));
                        Env::get().cache_tracker.put_sdata(
                            (rdd.get_rdd_id(), part_id),
                            data_ptr as *mut u8,
                            size,
                        );
                    }
                    for sub_part in kv_0 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[0] += 1;
                            kv.0.push(sub_part);
                        }
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    //TODO need revision if fe & fd of group_by is passed
                    let fut = ShuffleFetcher::secure_fetch(shuffle_id, split.get_index());
                    let kv_1 = futures::executor::block_on(fut)?
                        .into_iter()
                        .collect::<Vec<_>>();
                    for sub_part in kv_1 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[0] += 1;
                            kv.1.push(sub_part);
                        }
                    }
                }
            };

            let mut kw = (Vec::new(), Vec::new());
            match deps.remove(0) {
                //rdd1
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let (tx, rx) = mpsc::sync_channel(0);
                    let part_id = split.get_index();
                    let mut acc_arg_cg = AccArg::new(
                        DepInfo::padding_new(0),
                        None,
                        Arc::new(AtomicBool::new(false)),
                    );
                    let handles = rdd.iterator_raw(split, &mut acc_arg_cg, tx)?; //TODO need sorted
                    let mut kw_0 = Vec::new();

                    for received in rx {
                        let result_bl = get_encrypted_data::<ItemE>(
                            rdd.get_op_id(),
                            acc_arg_cg.dep_info,
                            received as *mut u8,
                        );
                        acc_arg_cg.free_enclave_lock();
                        kw_0.push(*result_bl);
                    }

                    if acc_arg_cg.is_caching_final_rdd() {
                        let size = kw_0.get_size();
                        let data_ptr = Box::into_raw(Box::new(kw_0.clone()));
                        Env::get().cache_tracker.put_sdata(
                            (rdd.get_rdd_id(), part_id),
                            data_ptr as *mut u8,
                            size,
                        );
                    }
                    for sub_part in kw_0 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[1] += 1;
                            kw.0.push(sub_part);
                        }
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    //TODO need revision if fe & fd of group_by is passed
                    let fut = ShuffleFetcher::secure_fetch(shuffle_id, split.get_index());
                    let kw_1 = futures::executor::block_on(fut)?
                        .into_iter()
                        .collect::<Vec<_>>();
                    for sub_part in kw_1 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[1] += 1;
                            kw.1.push(sub_part);
                        }
                    }
                }
            }
            let data = (kv.0, kv.1, kw.0, kw.1);
            if num_sub_part[0] == 0 || num_sub_part[1] == 0 {
                return Ok(Vec::new());
            }
            //shuffle read
            let now = Instant::now();
            let data = self.secure_shuffle_read(data, acc_arg);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("***in co grouped rdd, shuffle read, total {:?}***", dur);

            let acc_arg = acc_arg.clone();
            let handle = std::thread::spawn(move || {
                let now = Instant::now();
                let wait = start_execute(acc_arg, data, tx);
                let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
                println!("***in co grouped rdd, compute, total {:?}***", dur);
            });
            Ok(vec![handle])
        } else {
            Err(Error::DowncastFailure(
                "Got split object from different concrete type other than CoGroupSplit",
            ))
        }
    }

    fn secure_shuffle_read(
        &self,
        buckets: (
            Vec<Vec<ItemE>>,
            Vec<Vec<Vec<ItemE>>>,
            Vec<Vec<ItemE>>,
            Vec<Vec<Vec<ItemE>>>,
        ),
        acc_arg: &mut AccArg,
    ) -> Vec<ItemE> {
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
            &buckets,
            &acc_arg.captured_vars,
        );

        let mut result =
            get_encrypted_data::<ItemE>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
        acc_arg.free_enclave_lock();
        *result
    }
}

impl<K, V, W> RddBase for CoGroupedRdd<K, V, W>
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
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
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        let cur_rdd_id = self.vals.id;
        let cur_op_id = self.vals.op_id;
        let rdd0 = &self.rdd0;
        let rdd1 = &self.rdd1;
        let part = self.part.clone();
        let mut shuffle_ids = self.vals.shuffle_ids.clone();
        let mut deps = Vec::new();

        if rdd0
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            let rdd_base = rdd0.get_rdd_base();
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(rdd_base)) as Arc<dyn NarrowDependencyTrait>,
            ));
        } else {
            let aggr = Arc::new(Aggregator::<K, V, _>::default());
            let rdd_base = rdd0.get_rdd_base();
            log::debug!("creating aggregator inside cogrouprdd");
            deps.push(Dependency::ShuffleDependency(
                //TODO need revision if fe & fd of group_by is passed
                Arc::new(ShuffleDependency::new(
                    shuffle_ids.remove(0),
                    true,
                    rdd_base,
                    aggr,
                    part.clone(),
                    0,
                    cur_rdd_id,
                    cur_op_id,
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
        }

        if rdd1
            .partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any))
        {
            let rdd_base = rdd1.get_rdd_base();
            deps.push(Dependency::NarrowDependency(
                Arc::new(OneToOneDependency::new(rdd_base)) as Arc<dyn NarrowDependencyTrait>,
            ));
        } else {
            let aggr = Arc::new(Aggregator::<K, W, _>::default());
            let rdd_base = rdd1.get_rdd_base();
            log::debug!("creating aggregator inside cogrouprdd");
            deps.push(Dependency::ShuffleDependency(
                //TODO need revision if fe & fd of group_by is passed
                Arc::new(ShuffleDependency::new(
                    shuffle_ids.remove(0),
                    true,
                    rdd_base,
                    aggr,
                    part.clone(),
                    1,
                    cur_rdd_id,
                    cur_op_id,
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
        }
        deps
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::new();
        let mut rdds = Vec::new();
        rdds.push(self.rdd0.get_rdd_base());
        rdds.push(self.rdd1.get_rdd_base());
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                rdds.iter()
                    .enumerate()
                    .map(|(i, r)| match &self.get_dependencies()[i] {
                        Dependency::ShuffleDependency(s) => {
                            CoGroupSplitDep::ShuffleCoGroupSplitDep {
                                shuffle_id: s.get_shuffle_id(),
                            }
                        }
                        _ => CoGroupSplitDep::NarrowCoGroupSplitDep {
                            rdd: r.clone().into(),
                            split: r.splits()[i].clone(),
                        },
                    })
                    .collect(),
            )) as Box<dyn Split>)
        }
        splits
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        let part = self.part.clone() as Box<dyn Partitioner>;
        Some(part)
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
        let res = self.iterator(split)?.collect::<Vec<_>>();
        /*
        Ok(Box::new(res.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
        */
        Ok(Box::new(res) as Box<dyn AnyData>)
    }
}

impl<K, V, W> Rdd for CoGroupedRdd<K, V, W>
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
{
    type Item = (K, (Vec<V>, Vec<W>));
    //type Item = (K, Vec<Vec<Box<dyn AnyData>>>);
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    #[allow(clippy::type_complexity)]
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut agg: HashMap<K, (Vec<V>, Vec<W>)> = HashMap::new();
            let mut deps = split.clone().deps;

            match deps.remove(0) {
                //deps[0]
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    log::debug!("inside iterator CoGroupedRdd narrow dep");
                    for i in rdd
                        .iterator_any(split)?
                        .into_any()
                        .downcast::<Vec<(K, V)>>()
                        .unwrap()
                        .into_iter()
                    {
                        log::debug!(
                            "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                            i
                        );
                        let (k, v) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .0
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                    let fut = ShuffleFetcher::fetch::<K, Vec<V>>(shuffle_id, split.get_index());
                    for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                        let temp = agg.entry(k).or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.0.push(v);
                        }
                    }
                }
            };

            match deps.remove(0) {
                //deps[1]
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    log::debug!("inside iterator CoGroupedRdd narrow dep");
                    for i in rdd
                        .iterator_any(split)?
                        .into_any()
                        .downcast::<Vec<(K, W)>>()
                        .unwrap()
                        .into_iter()
                    {
                        log::debug!(
                            "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                            i
                        );
                        let (k, v) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .1
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                    let fut = ShuffleFetcher::fetch::<K, Vec<W>>(shuffle_id, split.get_index());
                    for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                        let temp = agg.entry(k).or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.1.push(v);
                        }
                    }
                }
            };

            Ok(Box::new(agg.into_iter()))
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
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
}
