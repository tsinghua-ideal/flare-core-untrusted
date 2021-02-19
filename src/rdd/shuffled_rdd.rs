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

    fn secure_compute_prev(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        let part_id = split.get_index();
        let fut = ShuffleFetcher::secure_fetch::<KE, CE>(self.shuffle_id, part_id);
        let mut bucket: Vec<Vec<(KE, CE)>> = futures::executor::block_on(fut)?.into_iter().collect();  // bucket per subpartition
        let mut data_size = 0;
        let mut num_sub_part = 0;
        for sub_part in &bucket {
            if sub_part.len() > 0 {
                data_size += sub_part.get_aprox_size() / sub_part.len();
                num_sub_part += 1;
            }
        }
        if num_sub_part == 0 {
            return Ok(Vec::new());
        }
        let avg_iteme_size = data_size / num_sub_part;
        
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.vals.id;
        let cur_op_id = self.vals.op_id;
        let rdd_ids = vec![cur_rdd_id];
        let op_ids = vec![cur_op_id];

        let acc_arg = acc_arg.clone();
        let handle = thread::spawn(move || {
            let now = Instant::now();
            let block_len = ((1 << 10+10) - 1) / (avg_iteme_size * num_sub_part) + 1;  //each block: 1MB
            let mut block = Vec::new();
            let mut remaining_in_enclave = false;
            let mut has_block = step_forward(&mut block, block_len, &mut bucket);
            let mut sub_part_id = 0;
            let mut cache_meta = acc_arg.to_cache_meta();
            while has_block || remaining_in_enclave {
                let mut is_survivor = !has_block;
                if !acc_arg.cached(&sub_part_id) {
                    let mut result_bl_ptr = wrapper_secure_execute(
                        &rdd_ids,
                        &op_ids,
                        cache_meta,
                        DepInfo::padding_new(20),   //shuffle read
                        Box::new(block), 
                        &captured_vars,
                    );
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
                    let block_ptr = result_bl_ptr as *mut u8;
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
                            block_ptr,
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
                        0 as *mut u8,
                    );
                    tx.send(result_bl_ptr).unwrap();
                }
                sub_part_id += 1;
                remaining_in_enclave = has_block;
                block = Vec::new();
                has_block = step_forward(&mut block, block_len, &mut bucket);
            }  
            let dur = now.elapsed().as_nanos() as f64 * 1e-9;
            println!("in ShuffledRdd, shuffle read + narrow {:?}", dur);  
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

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
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

fn step_forward<KE, CE>(
    block: &mut Vec<Vec<(KE, CE)>>, 
    block_len: usize,
    buckets: &mut Vec<Vec<(KE, CE)>>, 
) -> bool
where
    KE: Data,
    CE: Data,
{
    fn fill_block<T: Data>(block: &mut Vec<Vec<T>>, source: &mut Vec<Vec<T>>, block_len: usize) {
        for sub_part in source {
            let remain_len = sub_part.len();
            if remain_len == 0 {
                continue;
            }
            let len = std::cmp::min(remain_len, block_len);
            let mut remain = sub_part.split_off(len);
            std::mem::swap(&mut remain, sub_part);
            block.push(remain);
        }
    }
    fill_block(block, buckets, block_len);
    !block.is_empty()
}