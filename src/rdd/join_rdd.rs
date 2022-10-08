use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{
    mpsc::{self, RecvError, SyncSender},
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
use crate::map_output_tracker::GetServerUriReq;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::serialization_free::Construct;
use crate::shuffle::{ShuffleError, ShuffleFetcher};
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use sgx_types::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct JoinedRdd<K, V, W>
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

impl<K, V, W> JoinedRdd<K, V, W>
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
        JoinedRdd {
            vals,
            rdd0,
            rdd1,
            part,
        }
    }

    fn secure_compute_prev(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut has_shuffle = false;
            let mut deps = split.clone().deps;
            let mut kv = ((Vec::new(), Vec::new()), Vec::new());
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
                    let handles = rdd.iterator_raw(stage_id, split, &mut acc_arg_cg, tx)?;
                    match rx.recv() {
                        Ok((data_ptr, marks_ptr)) => {
                            kv.0 = get_encrypted_data::<ItemE, ItemE>(
                                rdd.get_op_id(),
                                acc_arg_cg.dep_info,
                                data_ptr,
                                marks_ptr,
                            );
                            acc_arg_cg.free_enclave_lock();
                        }
                        Err(RecvError) => {
                            kv.0 = Default::default();
                        }
                    }
                    if acc_arg_cg.is_caching_final_rdd() {
                        let size = kv.0.get_size();
                        let (data_ptr, marks_ptr) = (
                            Box::into_raw(Box::new(kv.0 .0.clone())),
                            Box::into_raw(Box::new(kv.0 .1.clone())),
                        );
                        Env::get().cache_tracker.put_sdata(
                            (rdd.get_rdd_id(), part_id),
                            (data_ptr as *mut u8 as usize, marks_ptr as *mut u8 as usize),
                            size,
                        );
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    //TODO need to sort kv.0
                    unimplemented!();
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    has_shuffle = true;
                    let fut = ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
                        GetServerUriReq::PrevStage(shuffle_id),
                        split.get_index(),
                        usize::MAX,
                    );
                    kv.1 = futures::executor::block_on(fut)?
                        .into_iter()
                        .filter(|x| !x.is_empty())
                        .collect::<Vec<_>>();
                }
            };

            let mut kw = ((Vec::new(), Vec::new()), Vec::new());
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
                    let handles = rdd.iterator_raw(stage_id, split, &mut acc_arg_cg, tx)?;
                    match rx.recv() {
                        Ok((data_ptr, marks_ptr)) => {
                            kw.0 = get_encrypted_data::<ItemE, ItemE>(
                                rdd.get_op_id(),
                                acc_arg_cg.dep_info,
                                data_ptr,
                                marks_ptr,
                            );
                            acc_arg_cg.free_enclave_lock();
                        }
                        Err(RecvError) => {
                            kw.0 = Default::default();
                        }
                    }
                    if acc_arg_cg.is_caching_final_rdd() {
                        let size = kw.0.get_size();
                        let (data_ptr, marks_ptr) = (
                            Box::into_raw(Box::new(kw.0 .0.clone())),
                            Box::into_raw(Box::new(kw.0 .1.clone())),
                        );
                        Env::get().cache_tracker.put_sdata(
                            (rdd.get_rdd_id(), part_id),
                            (data_ptr as *mut u8 as usize, marks_ptr as *mut u8 as usize),
                            size,
                        );
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    //TODO need to sort kw.0
                    unimplemented!();
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    has_shuffle = true;
                    let fut = ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
                        GetServerUriReq::PrevStage(shuffle_id),
                        split.get_index(),
                        usize::MAX,
                    );
                    kw.1 = futures::executor::block_on(fut)?
                        .into_iter()
                        .filter(|x| !x.is_empty())
                        .collect::<Vec<_>>();
                }
            }

            //some parameters
            let cur_rdd_ids = vec![self.vals.id];
            let cur_op_ids = vec![self.vals.op_id];
            let reduce_id = *acc_arg.part_ids.last().unwrap();
            let cur_part_ids = vec![reduce_id];
            //due to rdds like union, we only need partial executor info registered for the stage id
            let part_id_offset = if acc_arg.part_ids[0] == usize::MAX {
                acc_arg.part_ids[1] - acc_arg.part_ids.last().unwrap()
            } else {
                acc_arg.part_ids[0] - acc_arg.part_ids.last().unwrap()
            };
            let num_splits = *acc_arg.split_nums.last().unwrap();
            let part_group = (stage_id, part_id_offset, num_splits);

            //TODO: what if has_shuffle is false?
            let data = (kv.0, kv.1, kw.0, kw.1);

            //need split_num fix first
            wrapper_secure_execute_pre(&acc_arg.op_ids, &acc_arg.split_nums, acc_arg.dep_info);
            //shuffle read
            let now = Instant::now();
            let (data, marks) = secure_shuffle_read(
                stage_id,
                data,
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                part_group,
                acc_arg,
            );
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("***in co grouped rdd, shuffle read, total {:?}***", dur);

            let acc_arg = acc_arg.clone();
            let handle = std::thread::spawn(move || {
                let now = Instant::now();
                let wait = start_execute(stage_id, acc_arg, data, marks, tx);
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
}

impl<K, V, W> RddBase for JoinedRdd<K, V, W>
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
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(stage_id, split, acc_arg, tx)
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

impl<K, V, W> Rdd for JoinedRdd<K, V, W>
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
{
    type Item = (K, (V, W));
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
                    log::debug!("inside iterator JoinedRdd narrow dep");
                    for i in rdd
                        .iterator_any(split)?
                        .into_any()
                        .downcast::<Vec<(K, V)>>()
                        .unwrap()
                        .into_iter()
                    {
                        log::debug!("inside iterator JoinedRdd narrow dep iterator any: {:?}", i);
                        let (k, v) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .0
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator JoinedRdd shuffle dep, agg: {:?}", agg);
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
                    log::debug!("inside iterator JoinedRdd narrow dep");
                    for i in rdd
                        .iterator_any(split)?
                        .into_any()
                        .downcast::<Vec<(K, W)>>()
                        .unwrap()
                        .into_iter()
                    {
                        log::debug!("inside iterator JoinedRdd narrow dep iterator any: {:?}", i);
                        let (k, v) = i;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new()))
                            .1
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator JoinedRdd shuffle dep, agg: {:?}", agg);
                    let fut = ShuffleFetcher::fetch::<K, Vec<W>>(shuffle_id, split.get_index());
                    for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                        let temp = agg.entry(k).or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.1.push(v);
                        }
                    }
                }
            };

            Ok(Box::new(agg.into_iter().flat_map(|(k, (vs, ws))| {
                vs.into_iter().flat_map(move |v| {
                    let k = k.clone();
                    ws.clone()
                        .into_iter()
                        .map(move |w| (k.clone(), (v.clone(), w)))
                })
            })))
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
    }

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_part_id = split.get_index();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_quadruple(cur_rdd_id, cur_op_id, cur_part_id, cur_split_num);

        let should_cache = self.should_cache();
        if should_cache {
            let mut handles =
                secure_compute_cached(stage_id, acc_arg, cur_rdd_id, cur_part_id, tx.clone());

            if handles.is_empty() {
                acc_arg.set_caching_rdd_id(cur_rdd_id);
                handles.append(&mut self.secure_compute_prev(stage_id, split, acc_arg, tx)?);
            }
            Ok(handles)
        } else {
            self.secure_compute_prev(stage_id, split, acc_arg, tx)
        }
    }
}

fn secure_shuffle_read(
    stage_id: usize,
    buckets: (
        (Vec<ItemE>, Vec<ItemE>),
        Vec<Vec<ItemE>>,
        (Vec<ItemE>, Vec<ItemE>),
        Vec<Vec<ItemE>>,
    ),
    cur_rdd_ids: &Vec<usize>,
    cur_op_ids: &Vec<OpId>,
    cur_part_ids: &Vec<usize>,
    part_group: (usize, usize, usize),
    acc_arg: &AccArg,
) -> (Vec<ItemE>, Vec<ItemE>) {
    let reduce_id = cur_part_ids[0];
    let num_splits = part_group.2;
    let mut tmp_captured_var = HashMap::new();

    //obliv_filter_stage3 + obliv_agg_stage1
    let (part_set, buckets_set) = {
        let buckets = buckets; //move
        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(20);
        let (data_ptr, marks_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &buckets,
            &Vec::<ItemE>::new(),
            &HashMap::new(),
        );

        //part contains table A and table B
        let (part_set, buckets_set) = get_encrypted_data::<Vec<ItemE>, Vec<Vec<ItemE>>>(
            cur_op_ids[0],
            dep_info,
            data_ptr,
            marks_ptr,
        );
        acc_arg.free_enclave_lock();

        //bucket id/tid/blocks
        let mut reorgan_buckets_set = vec![vec![Vec::new(), Vec::new()]; part_group.2];

        for (tid, buckets) in buckets_set.into_iter().enumerate() {
            for (bid, bucket) in buckets.into_iter().enumerate() {
                reorgan_buckets_set[bid][tid] = bucket;
            }
        }

        for (i, bucket_set) in reorgan_buckets_set.into_iter().enumerate() {
            //bucket from A and bucket from B are both in bucket_set
            let ser_bytes = bincode::serialize(&bucket_set).unwrap();
            env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes);
        }
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();
        let fut = ShuffleFetcher::secure_fetch::<Vec<Vec<ItemE>>>(
            GetServerUriReq::CurStage(part_group),
            reduce_id,
            usize::MAX,
        );
        //tid/input id/blocks
        let mut buckets_set = vec![Vec::new(); 2];

        for bucket_set in futures::executor::block_on(fut).unwrap() {
            for (tid, bucket) in bucket_set.into_iter().enumerate() {
                if !bucket.is_empty() {
                    buckets_set[tid].push(bucket);
                }
            }
        }
        (part_set, buckets_set)
    };

    let (data, part, acc_col_misc, acc_col_rem, alpha, beta, n_out_prime) = {
        let buckets_set = buckets_set;
        //obliv_agg_stage_2 + obliv_group_by_stage1 + obliv_join_stage1 + obliv_join_stage2
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&part_group).unwrap()],
        );
        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(21);
        let (data_ptr, meta_data_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &buckets_set,
            &Vec::<ItemE>::new(),
            &tmp_captured_var,
        );

        let (mut data_set, meta_data) =
            get_encrypted_data::<Vec<ItemE>, u8>(cur_op_ids[0], dep_info, data_ptr, meta_data_ptr);
        acc_arg.free_enclave_lock();
        let (alpha, beta, n_out_prime): (Vec<usize>, Vec<usize>, usize) =
            bincode::deserialize(&meta_data).unwrap();
        let acc_col_misc = data_set.pop().unwrap().pop().unwrap(); //Enc(acc_col, num_bins, idx_last)
        let acc_col_rem = data_set.pop().unwrap().pop().unwrap();
        let data = data_set.pop().unwrap();
        let part = data_set.pop().unwrap();
        assert!(data_set.is_empty());
        (
            data,
            part,
            acc_col_misc,
            acc_col_rem,
            alpha,
            beta,
            n_out_prime,
        )
    };

    let (acc_col_rem, mut acc_col_rec) = {
        let acc_col_rem = futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            acc_col_rem,
        ))
        .unwrap();
        //clear the sort cache
        env::SORT_CACHE.retain(|k, _| k.0 != part_group);
        //sync again to avoid other threads fetching the old value
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();

        //obliv_join_stage_3 pre, i.e., assign remaining bins
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&n_out_prime).unwrap()],
        );
        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(22);
        let (data_ptr, meta_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &acc_col_rem,
            &Vec::<ItemE>::new(),
            &tmp_captured_var,
        );

        assert_eq!(meta_ptr, 0);
        let (mut data_set, _) =
            get_encrypted_data::<ItemE, ItemE>(cur_op_ids[0], dep_info, data_ptr, meta_ptr);
        acc_arg.free_enclave_lock();

        let acc_col_rem = data_set.pop().unwrap(); //acc_col_rem owned by the partition
        let acc_col_rec = data_set.pop().unwrap(); //received acc_col, currently it is only valid for partition 0
        assert!(data_set.is_empty());
        (acc_col_rem, acc_col_rec)
    };

    let mut last_bin_loc_rec = Vec::new();
    if reduce_id != 0 {
        let fut = ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
            GetServerUriReq::CurStage(part_group),
            reduce_id,
            (reduce_id + num_splits - 1) % num_splits,
        );
        let mut fut_res = futures::executor::block_on(fut);
        //the exchanging info may not be prepared
        while fut_res.is_err() {
            match fut_res {
                Ok(_) => unreachable!(),
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(5));
                    fut_res =
                        futures::executor::block_on(ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
                            GetServerUriReq::CurStage(part_group),
                            reduce_id,
                            (reduce_id + num_splits - 1) % num_splits,
                        ));
                }
            }
        }
        let mut info = fut_res.unwrap().collect::<Vec<_>>().remove(0);
        last_bin_loc_rec = info.pop().unwrap();
        acc_col_rec = info.pop().unwrap();
        assert!(info.is_empty());
    }

    //obliv_join_stage3
    let (acc_col, num_bins, mut join_cnt) = {
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&part_group).unwrap()],
        );

        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(23);
        let (data_ptr, join_cnt) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &vec![
                acc_col_misc,
                acc_col_rem.clone(),
                acc_col_rec,
                last_bin_loc_rec,
            ],
            &Vec::<ItemE>::new(),
            &tmp_captured_var,
        );

        assert!(
            reduce_id == num_splits - 1 && join_cnt != 0
                || reduce_id < num_splits - 1 && join_cnt == 0
        );
        let (mut data_set, _) =
            get_encrypted_data::<ItemE, ItemE>(cur_op_ids[0], dep_info, data_ptr, 0);
        acc_arg.free_enclave_lock();

        let num_bins = data_set.pop().unwrap();
        last_bin_loc_rec = data_set.pop().unwrap(); //last_bin_loc_own serves as last_bin_loc_rec of next partition
        let acc_col = data_set.pop().unwrap();
        acc_col_rec = data_set.pop().unwrap();
        assert!(data_set.is_empty());
        (acc_col, num_bins, join_cnt)
    };

    if reduce_id < num_splits - 1 {
        //send info (last_bin_loc_rec and acc_col_rec) to the next worker
        let ser_bytes = bincode::serialize(&vec![acc_col_rec, last_bin_loc_rec.clone()]).unwrap();
        env::SORT_CACHE.insert(
            (part_group, reduce_id, (reduce_id + 1) % num_splits),
            ser_bytes.clone(),
        );
        //wait for join_cnt
        assert_eq!(join_cnt, 0);
        let fut = ShuffleFetcher::secure_fetch::<usize>(
            GetServerUriReq::CurStage(part_group),
            reduce_id,
            num_splits - 1,
        );
        let mut fut_res = futures::executor::block_on(fut);
        while fut_res.is_err() {
            match fut_res {
                Ok(_) => unreachable!(),
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(5));
                    fut_res = futures::executor::block_on(ShuffleFetcher::secure_fetch::<usize>(
                        GetServerUriReq::CurStage(part_group),
                        reduce_id,
                        num_splits - 1,
                    ));
                }
            }
        }
        join_cnt = fut_res.unwrap().collect::<Vec<_>>().remove(0);
    } else {
        //broadcast the join_cnt
        let ser_bytes = bincode::serialize(&join_cnt).unwrap();
        for i in 0..num_splits - 1 {
            env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes.clone());
        }
    }
    assert_ne!(join_cnt, 0);

    //obliv_join_stage4
    let buckets_set = {
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&part_group).unwrap()],
        );

        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(24);
        let (data_ptr, marks_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &(part, data, acc_col, num_bins, last_bin_loc_rec, acc_col_rem),
            &Vec::<ItemE>::new(),
            &tmp_captured_var,
        );
        assert_eq!(marks_ptr, 0);

        let (mut buckets_a, _) =
            get_encrypted_data::<Vec<ItemE>, ItemE>(cur_op_ids[0], dep_info, data_ptr, marks_ptr);
        acc_arg.free_enclave_lock();
        let buckets_b = buckets_a.split_off(num_splits);
        assert_eq!(buckets_b.len(), buckets_a.len());

        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();

        for (i, bucket_set) in buckets_a
            .into_iter()
            .zip(buckets_b.into_iter())
            .map(|(a, b)| vec![a, b])
            .enumerate()
        {
            //bucket from A and bucket from B are both in bucket_set
            let ser_bytes = bincode::serialize(&bucket_set).unwrap();
            env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes);
        }
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();

        let fut = ShuffleFetcher::secure_fetch::<Vec<Vec<ItemE>>>(
            GetServerUriReq::CurStage(part_group),
            reduce_id,
            usize::MAX,
        );
        //tid/input id/blocks
        let mut buckets_set = vec![Vec::new(); 2];

        for bucket_set in futures::executor::block_on(fut).unwrap() {
            for (tid, bucket) in bucket_set.into_iter().enumerate() {
                if !bucket.is_empty() {
                    buckets_set[tid].push(bucket);
                }
            }
        }
        buckets_set
    };

    //obliv_join_stage5
    let buckets = {
        let part_set = part_set;
        let buckets_set = buckets_set;
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&(part_group, beta, n_out_prime)).unwrap()],
        );
        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(25);
        let (data_ptr, marks_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &part_set,
            &buckets_set,
            &tmp_captured_var,
        );

        assert_eq!(marks_ptr, 0);
        let (mut buckets, _) =
            get_encrypted_data::<Vec<ItemE>, ItemE>(cur_op_ids[0], dep_info, data_ptr, marks_ptr);
        acc_arg.free_enclave_lock();
        buckets.resize(part_group.2, Vec::new());
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();
        for (i, bucket) in buckets.into_iter().enumerate() {
            let ser_bytes = bincode::serialize(&bucket).unwrap();
            env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes);
        }
        futures::executor::block_on(ShuffleFetcher::fetch_sync(
            part_group,
            reduce_id,
            Vec::new(),
        ))
        .unwrap();
        let fut = ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
            GetServerUriReq::CurStage(part_group),
            reduce_id,
            usize::MAX,
        );
        futures::executor::block_on(fut)
            .unwrap()
            .filter(|x| !x.is_empty())
            .collect::<Vec<_>>()
    };

    //obliv_join_stage_6
    {
        let buckets = buckets;
        let padding_len = (join_cnt - 1) / n_out_prime + 1 + alpha[0] * alpha[1];
        tmp_captured_var.insert(
            cur_rdd_ids[0],
            vec![bincode::serialize(&(padding_len)).unwrap()],
        );

        acc_arg.get_enclave_lock();
        let dep_info = DepInfo::padding_new(2);
        let (data_ptr, marks_ptr) = wrapper_secure_execute(
            stage_id,
            cur_rdd_ids,
            cur_op_ids,
            cur_part_ids,
            Default::default(),
            dep_info,
            &buckets,
            &Vec::<ItemE>::new(),
            &tmp_captured_var,
        );

        //the invalid values have not been filtered.
        assert_ne!(marks_ptr, 0);
        let (data, marks) =
            get_encrypted_data::<ItemE, ItemE>(cur_op_ids[0], dep_info, data_ptr, marks_ptr);
        acc_arg.free_enclave_lock();
        (data, marks)
    }
}
