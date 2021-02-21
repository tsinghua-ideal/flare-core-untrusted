use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::slice::Chunks;
use std::sync::{Arc, mpsc::{self, SyncSender}};
use std::thread::{JoinHandle, self};

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::env::{BOUNDED_MEM_CACHE, RDDB_MAP, Env};
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
pub struct CoGroupedRdd<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone, 
{
    pub(crate) vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd0: Arc<dyn RddE<Item = (K, V), ItemE = (KE, VE)>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd1: Arc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
    pub(crate) fe: FE,
    pub(crate) fd: FD,
    #[serde(with = "serde_traitobject")]
    pub(crate) part: Box<dyn Partitioner>,
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> CoGroupedRdd<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)) + Clone, 
    FD: Func((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone, 
{
    #[track_caller]
    pub fn new(
        rdd0: Arc<dyn RddE<Item = (K, V), ItemE = (KE, VE)>>,
        rdd1: Arc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>, 
        fe: FE,
        fd: FD,
        part: Box<dyn Partitioner>
    ) -> Self {
        let context = rdd0.get_context();
        let secure = rdd0.get_secure() || rdd1.get_secure() ;  //
        let mut vals = RddVals::new(context.clone(), secure);

        if !rdd0.partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any)) 
        {
            vals.shuffle_ids.push(context.new_shuffle_id());
        }

        if !rdd1.partitioner()
            .map_or(false, |p| p.equals(&part as &dyn Any)) 
        {
            vals.shuffle_ids.push(context.new_shuffle_id());
        }

        let vals = Arc::new(vals);
        CoGroupedRdd {
            vals,
            rdd0,
            rdd1,
            fe,
            fd,
            part,
        }
    }

    fn secure_compute_prev(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
            let cur_rdd_id = self.vals.id;
            let cur_op_id = self.vals.op_id;
            let rdd_ids = vec![cur_rdd_id];
            let op_ids = vec![cur_op_id];
            let mut deps = split.clone().deps;
            let mut num_sub_part = vec![0, 0]; //kv, kw
            let mut upper_bound = Vec::new();
            let mut kv = (Vec::new(), Vec::new());
            match deps.remove(0) {   //rdd1
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let (tx, rx) = mpsc::sync_channel(0);
                    let mut acc_arg_cg = AccArg::new(split.get_index(), DepInfo::padding_new(01));
                    let handles = rdd.iterator_raw(split, &mut acc_arg_cg, tx)?;  //TODO need sorted
                    for received in rx {
                        let result_bl = get_encrypted_data::<(KE, VE)>(rdd.get_op_id(), acc_arg_cg.dep_info, received as *mut u8, false);
                        let result_bl_len = result_bl.len();
                        if result_bl_len > 0 {
                            num_sub_part[0] += 1;
                            upper_bound.push(result_bl_len);
                            kv.0.push(*result_bl);
                        } 
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    //TODO need revision if fe & fd of group_by is passed 
                    let fut = ShuffleFetcher::secure_fetch::<KE, Vec<u8>>(shuffle_id, split.get_index());
                    let kv_1 = futures::executor::block_on(fut)?.into_iter().collect::<Vec<_>>();
                    for sub_part in kv_1 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[0] += 1;
                            upper_bound.push(sub_part_len);
                            kv.1.push(sub_part);
                        }
                    }
                }
            };
            
            let mut kw = (Vec::new(), Vec::new());
            match deps.remove(0) {    //rdd0
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let (tx, rx) = mpsc::sync_channel(0);
                    let mut acc_arg_cg = AccArg::new(split.get_index(), DepInfo::padding_new(01));
                    let handles = rdd.iterator_raw(split, &mut acc_arg_cg, tx)?;  //TODO need sorted
                    for received in rx {
                        let result_bl = get_encrypted_data::<(KE, WE)>(rdd.get_op_id(), acc_arg_cg.dep_info, received as *mut u8, false);
                        let result_bl_len = result_bl.len();
                        if result_bl_len > 0 {
                            num_sub_part[1] += 1;
                            upper_bound.push(result_bl_len);
                            kw.0.push(*result_bl);
                        }
                    }
                    for handle in handles {
                        handle.join().unwrap();
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    //TODO need revision if fe & fd of group_by is passed 
                    let fut = ShuffleFetcher::secure_fetch::<KE, Vec<u8>>(shuffle_id, split.get_index());
                    let kw_1 = futures::executor::block_on(fut)?.into_iter().collect::<Vec<_>>();
                    for sub_part in kw_1 {
                        let sub_part_len = sub_part.len();
                        if sub_part_len > 0 {
                            num_sub_part[1] += 1;
                            upper_bound.push(sub_part_len);
                            kw.1.push(sub_part);
                        }
                    }
                }
            }
            let data = (kv.0, kv.1, kw.0, kw.1);
            if num_sub_part[0] == 0 || num_sub_part[1] == 0 {
                return Ok(Vec::new());
            }

            let acc_arg = acc_arg.clone();
            let handle = std::thread::spawn(move || {
                get_stage_lock();
                let now = Instant::now();
                let mut wait = 0.0; 
                let mut lower = vec![0; num_sub_part[0] + num_sub_part[1]];
                let mut upper = vec![1; num_sub_part[0] + num_sub_part[1]];
                
                let mut sub_part_id = 0;
                let mut cache_meta = acc_arg.to_cache_meta();

                while lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
                    let mut is_survivor = false; //tmp 
                    if !acc_arg.cached(&sub_part_id) {  //don't support partial cache now, for the lower and upper is not remembered
                        //TODO: get lower of the last cached data
                        upper = upper.iter()
                            .zip(upper_bound.iter())
                            .map(|(l, ub)| std::cmp::min(*l, *ub))
                            .collect::<Vec<_>>();
                        let (mut result_bl_ptr, swait) = wrapper_secure_execute(
                            &rdd_ids,
                            &op_ids,
                            cache_meta, //the cache_meta should not be used, this execution does not go to compute(), where cache-related operation is
                            DepInfo::padding_new(20), //shuffle read and no encryption for result
                            &data,
                            &mut lower,
                            &mut upper,
                            BLOCK_SIZE,
                            &captured_vars,
                        );
                        wait += swait;
                        let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                            &acc_arg.op_ids,
                            cache_meta, 
                            acc_arg.dep_info,
                        );
                        if spec_call_seq_ptr != 0 {
                            is_survivor = true;
                        }
                        cache_meta.set_sub_part_id(sub_part_id);
                        cache_meta.set_is_survivor(is_survivor);
                        BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                        // this block is in enclave, cannot access
                        let block_ptr = result_bl_ptr as *const u8;
                        let input = Input::build_from_ptr(block_ptr, &mut vec![0], &mut vec![usize::MAX], BLOCK_SIZE);
                        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
                        result_bl_ptr = 0;
                        let tid: u64 = thread::current().id().as_u64().into();
                        let sgx_status = unsafe {
                            secure_execute(
                                eid,
                                &mut result_bl_ptr,
                                tid,
                                &acc_arg.rdd_ids as *const Vec<usize> as *const u8,
                                &acc_arg.op_ids as *const Vec<OpId> as *const u8,
                                cache_meta,
                                acc_arg.dep_info,  
                                input,
                                &captured_vars as *const HashMap<usize, Vec<Vec<u8>>> as *const u8,
                            )
                        };
                        match sgx_status {
                            sgx_status_t::SGX_SUCCESS => {},
                            _ => {
                                panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                            },
                        };
                        wrapper_spec_execute(
                            spec_call_seq_ptr, 
                            cache_meta,
                        );
                        tx.send(result_bl_ptr).unwrap();
                        lower = lower.iter()
                            .zip(upper_bound.iter())
                            .map(|(l, ub)| std::cmp::min(*l, *ub))
                            .collect::<Vec<_>>();
                    }
                    sub_part_id += 1;
                }
                free_stage_lock();
                let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
                println!("***in co grouped rdd, total {:?}***", dur);  
                
            });
            Ok(vec![handle])

        } else {
            Err(Error::DowncastFailure("Got split object from different concrete type other than CoGroupSplit"))
        }
    }

}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> RddBase for CoGroupedRdd<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>,
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
            Box::from_raw(ptr as *mut Vec<(KE, (CE, DE))>)
        };
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

        if rdd0.partitioner()
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
                Arc::new(ShuffleDependency::<_, _, _, KE, Vec<u8>>::new(
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

        if rdd1.partitioner()
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
                Arc::new(ShuffleDependency::<_, _, _, KE, Vec<u8>>::new(
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

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<(KE, (CE, DE))>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::new();
        let mut rdds = Vec::new();
        rdds.push(self.rdd0.get_rdd_base());
        rdds.push(self.rdd1.get_rdd_base());
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                rdds
                    .iter()
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

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        let res = self.iterator(split)?;
        Ok(Box::new(res.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> Rdd for CoGroupedRdd<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>,
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

            match deps.remove(0) {  //deps[0]
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    log::debug!("inside iterator CoGroupedRdd narrow dep");
                    for i in rdd.iterator_any(split)? {
                        log::debug!(
                            "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                            i
                        );
                        let b = i
                            .into_any()
                            .downcast::<(K, V)>()
                            .unwrap();
                        let (k, v) = *b;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new())).0
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                    let fut = ShuffleFetcher::fetch::<K, Vec<V>>(
                        shuffle_id,
                        split.get_index(),
                    );
                    for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                        let temp = agg.entry(k).or_insert_with(|| (Vec::new(), Vec::new()));
                        for v in c {
                            temp.0.push(v);
                        }
                    }
                }
            };

            match deps.remove(0) {  //deps[1]
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    log::debug!("inside iterator CoGroupedRdd narrow dep");
                    for i in rdd.iterator_any(split)? {
                        log::debug!(
                            "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                            i
                        );
                        let b = i
                            .into_any()
                            .downcast::<(K, W)>()
                            .unwrap();
                        let (k, v) = *b;
                        agg.entry(k)
                            .or_insert_with(|| (Vec::new(), Vec::new())).1
                            .push(v)
                    }
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                    let fut = ShuffleFetcher::fetch::<K, Vec<W>>(
                        shuffle_id,
                        split.get_index(),
                    );
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
    
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        acc_arg.insert_rdd_id(cur_rdd_id);
        acc_arg.insert_op_id(cur_op_id);

        let captured_vars = Env::get().captured_vars.lock().unwrap().clone();
        let should_cache = self.should_cache();
        if should_cache {
            let mut handles = secure_compute_cached(
                acc_arg, 
                cur_rdd_id, 
                tx.clone(),
                captured_vars,
            );

            if !acc_arg.totally_cached() {
                acc_arg.set_caching_rdd_id(cur_rdd_id);
                handles.append(&mut self.secure_compute_prev(split, acc_arg, tx)?);
            }
            Ok(handles)     
        } else {
            self.secure_compute_prev(split, acc_arg, tx)
        }
    }

}

impl<K, V, W, KE, VE, WE, CE, DE, FE, FD> RddE for CoGroupedRdd<K, V, W, KE, VE, WE, CE, DE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data + Eq + Hash,
    VE: Data,
    CE: Data,
    WE: Data,
    DE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
    FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
    type ItemE = (KE, (CE, DE));
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
