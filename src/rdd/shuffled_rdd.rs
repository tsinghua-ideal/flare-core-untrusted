use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::{JoinHandle, self};
use std::time::Instant;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::env::{BOUNDED_MEM_CACHE, RDDB_MAP, Env};
use crate::dependency::{Dependency, ShuffleDependency};
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use parking_lot::Mutex;
use sgx_types::*;

#[derive(Clone, Serialize, Deserialize)]
struct ShuffledRddSplit {
    index: usize,
}

impl ShuffledRddSplit {
    fn new(index: usize) -> Self {
        ShuffledRddSplit { index }
    }
}

impl Split for ShuffledRddSplit {
    fn get_index(&self) -> usize {
        self.index
    }
}

#[derive(Serialize, Deserialize)]
pub struct ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    parent: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    shuffle_id: usize,
    fe: FE,
    fd: FD,
}

impl<K, V, C, KE, CE, FE, FD> Clone for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone, 
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, C, KE, CE, FE, FD> ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: Func(Vec<(K, C)>) -> (KE, CE) + Clone,
    FD: Func((KE, CE)) -> Vec<(K, C)> + Clone, 
{
    #[track_caller]
    pub(crate) fn new(
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> Self {
        let ctx = parent.get_context();
        let secure = parent.get_secure();   //temp
        let shuffle_id = ctx.new_shuffle_id();
        let mut vals = RddVals::new(ctx, secure);
        vals.shuffle_ids.push(shuffle_id);
        let vals = Arc::new(vals);
        ShuffledRdd {
            parent,
            aggregator,
            vals,
            part,
            shuffle_id,
            fe,
            fd,
        }
    }

    fn secure_compute_prev(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        let part_id = split.get_index();
        let fut = ShuffleFetcher::secure_fetch::<KE, CE>(self.shuffle_id, part_id);
        let bucket: Vec<Vec<(KE, CE)>> = futures::executor::block_on(fut)?.into_iter().filter(|sub_part| sub_part.len() > 0).collect();  // bucket per subpartition
        //pre_merge
        let parent_rdd_id = self.parent.get_rdd_id();
        let child_rdd_id = self.vals.id;
        let parent_op_id = self.parent.get_op_id();
        let dep_info = DepInfo::new(
            1,
            0,
            parent_rdd_id,
            child_rdd_id,
            parent_op_id,
            self.vals.op_id,
        );
        println!("bucket size before pre_merge: {:?}", bucket.get_size());
        acc_arg.get_enclave_lock();
        let bucket = wrapper_pre_merge(parent_op_id, bucket, dep_info, STAGE_LOCK.get_parall_num());
        acc_arg.free_enclave_lock();
        println!("bucket size after pre_merge: {:?}", bucket.get_size());
        //
        let num_sub_part = bucket.len();
        let upper_bound = bucket.iter().map(|sub_part| sub_part.len()).collect::<Vec<_>>();
        if num_sub_part == 0 {
            return Ok(Vec::new());
        }

        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());

        let acc_arg = acc_arg.clone();
        let handle = thread::spawn(move || {
            let now = Instant::now();
            let mut wait = 0.0;
            let mut lower = vec![0; num_sub_part];
            let mut upper = vec![1; num_sub_part];
            let mut sub_part_id = 0;
            let mut cache_meta = acc_arg.to_cache_meta();
            let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                &acc_arg.op_ids,
                cache_meta,
                acc_arg.dep_info,
            );
            let mut is_survivor = spec_call_seq_ptr.is_some();
            let mut to_set_usage = 0;
            while lower.iter().zip(upper_bound.iter()).filter(|(l, ub)| l < ub).count() > 0 {
                let wait_now = Instant::now();
                acc_arg.get_enclave_lock();
                let wait_dur = wait_now.elapsed().as_nanos() as f64 * 1e-9;
                wait += wait_dur;
                //update block_len
                let block_len = acc_arg.block_len.load(atomic::Ordering::SeqCst);
                let cur_usage = acc_arg.cur_usage.load(atomic::Ordering::SeqCst);
                if cur_usage != 0 {
                    to_set_usage = cur_usage;
                }
                if !acc_arg.cached(&sub_part_id) {                        //don't support partial cache now, for the lower and upper is not remembered
                    cache_meta.set_sub_part_id(sub_part_id);
                    cache_meta.set_is_survivor(is_survivor);
                    BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                    //TODO: get lower of the last cached data
                    upper = upper.iter()
                        .zip(upper_bound.iter())
                        .map(|(l, ub)| std::cmp::min(*l, *ub))
                        .collect::<Vec<_>>();
                    let (result_bl_ptr, (time_comp, mem_usage)) = wrapper_secure_execute(
                        &acc_arg.rdd_ids,
                        &acc_arg.op_ids,
                        &acc_arg.split_nums,
                        cache_meta,
                        acc_arg.dep_info,
                        &bucket,
                        &mut lower,
                        &mut upper,
                        block_len,
                        to_set_usage,
                        &captured_vars,
                    );
                    wrapper_spec_execute(
                        &spec_call_seq_ptr, 
                        cache_meta, 
                    );
                    match acc_arg.dep_info.is_shuffle == 0 {
                        true => tx.send((sub_part_id, (result_bl_ptr, (time_comp, mem_usage.0)))).unwrap(),
                        false => tx.send((sub_part_id, (result_bl_ptr, (time_comp, mem_usage.1)))).unwrap(),
                    };
                    lower = lower.iter()
                        .zip(upper_bound.iter())
                        .map(|(l, ub)| std::cmp::min(*l, *ub))
                        .collect::<Vec<_>>();
                    to_set_usage = match spec_call_seq_ptr.is_none() {
                        true => mem_usage.0 as usize,
                        false => 0,
                    }
                }
                sub_part_id += 1;
            }
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in shuffled rdd, total {:?}***", dur);  
        });
        Ok(vec![handle])
    } 

}

impl<K, V, C, KE, CE, FE, FD> RddBase for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
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
            Box::from_raw(ptr as *mut Vec<(KE, CE)>)
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
        vec![Dependency::ShuffleDependency(Arc::new(
                ShuffleDependency::<_, _, _, KE, CE>::new(
                    self.shuffle_id,
                    false,
                    self.parent.get_rdd_base(),
                    self.aggregator.clone(),
                    self.part.clone(),
                    0,
                    cur_rdd_id,
                    cur_op_id,
                ),
            ))]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<(KE, CE)>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        (0..self.part.get_num_of_partitions())
            .map(|x| Box::new(ShuffledRddSplit::new(x)) as Box<dyn Split>)
            .collect()
    }

    fn number_of_splits(&self) -> usize {
        self.part.get_num_of_partitions()
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        Some(self.part.clone())
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
    }
}

impl<K, V, C, KE, CE, FE, FD> Rdd for ShuffledRdd<K, V, C, KE, CE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>, 
{
    type Item = (K, C);

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        log::debug!("compute inside shuffled rdd");
        let now = Instant::now();

        let fut = ShuffleFetcher::fetch::<K, C>(self.shuffle_id, split.get_index());
        let mut combiners: HashMap<K, Option<C>> = HashMap::new();
        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
            if let Some(old_c) = combiners.get_mut(&k) {
                let old = old_c.take().unwrap();
                let input = ((old, c),);
                let output = self.aggregator.merge_combiners.call(input);
                *old_c = Some(output);
            } else {
                combiners.insert(k, Some(c));
            }
        }

        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("in ShuffledRdd shuffle read {:?}", dur);
        Ok(Box::new(
            combiners.into_iter().map(|(k, v)| (k, v.unwrap())),
        ))
    }
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_rdd_id(cur_rdd_id);
        acc_arg.insert_op_id(cur_op_id);
        acc_arg.insert_split_num(cur_split_num);

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

impl<K, V, C, KE, CE, FE, FD> RddE for ShuffledRdd<K, V, C, KE, CE, FE, FD>
where
    K: Data + Eq + Hash, 
    V: Data, 
    C: Data,
    KE: Data,
    CE: Data,
    FE: SerFunc(Vec<(K, C)>) -> (KE, CE),
    FD: SerFunc((KE, CE)) -> Vec<(K, C)>,
{
    type ItemE = (KE, CE);
    
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