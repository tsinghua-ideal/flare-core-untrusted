use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{mpsc::SyncSender, Arc};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, ShuffleDependency};
use crate::env::{Env, BOUNDED_MEM_CACHE, RDDB_MAP};
use crate::error::Result;
use crate::map_output_tracker::GetServerUriReq;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::shuffle::{ShuffleError, ShuffleFetcher};
use crate::split::Split;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
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
pub struct ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
{
    #[serde(with = "serde_traitobject")]
    parent: Arc<dyn Rdd<Item = (K, V)>>,
    #[serde(with = "serde_traitobject")]
    aggregator: Arc<Aggregator<K, V, C>>,
    vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    part: Box<dyn Partitioner>,
    shuffle_id: usize,
}

impl<K, V, C> Clone for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
{
    fn clone(&self) -> Self {
        ShuffledRdd {
            parent: self.parent.clone(),
            aggregator: self.aggregator.clone(),
            vals: self.vals.clone(),
            part: self.part.clone(),
            shuffle_id: self.shuffle_id,
        }
    }
}

impl<K, V, C> ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
{
    #[track_caller]
    pub(crate) fn new(
        parent: Arc<dyn Rdd<Item = (K, V)>>,
        aggregator: Arc<Aggregator<K, V, C>>,
        part: Box<dyn Partitioner>,
    ) -> Self {
        let ctx = parent.get_context();
        let secure = parent.get_secure(); //temp
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
        }
    }

    fn secure_compute_prev(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let part_id = split.get_index();
        let fut = ShuffleFetcher::secure_fetch(
            GetServerUriReq::PrevStage(self.shuffle_id),
            part_id,
            usize::MAX,
        );
        let buckets: Vec<Vec<ItemE>> = futures::executor::block_on(fut)?
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(); // bucket per subpartition

        //column sort
        let now = Instant::now();
        let data = self.secure_column_sort_first(stage_id, buckets, acc_arg);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("***in shuffled rdd, first column sort, total {:?}***", dur);

        //shuffle read
        let now = Instant::now();
        let data = self.secure_shuffle_read(stage_id, data, acc_arg);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("***in shuffled rdd, shuffle read, total {:?}***", dur);

        //column sort again to filter
        let now = Instant::now();
        let data = self.secure_column_sort_second(stage_id, data, acc_arg);
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("***in shuffled rdd, second column sort, total {:?}***", dur);

        let acc_arg = acc_arg.clone();
        let handle = thread::spawn(move || {
            let now = Instant::now();
            let wait = start_execute(acc_arg, data, tx);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in shuffled rdd, compute, total {:?}***", dur);
        });
        Ok(vec![handle])
    }

    fn secure_column_sort_first(
        &self,
        stage_id: usize,
        buckets: Vec<Vec<ItemE>>,
        acc_arg: &mut AccArg,
    ) -> Vec<ItemE> {
        //need split_num fix first
        wrapper_secure_execute_pre(&acc_arg.op_ids, &acc_arg.split_nums, acc_arg.dep_info);
        //count the cnt_per_partition
        {
            let cur_rdd_ids = vec![self.vals.id];
            let cur_op_ids = vec![self.vals.op_id];
            let reduce_id = *acc_arg.part_ids.last().unwrap();
            let cur_part_ids = vec![reduce_id];

            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(20);
            let invalid_ptr = wrapper_secure_execute(
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                Default::default(),
                dep_info,
                &buckets,
                &acc_arg.captured_vars,
            );
            assert_eq!(invalid_ptr, 0);
            acc_arg.free_enclave_lock();
        }
        println!("count the cnt_per_partition");
        //step 3 - 8
        let data = ShuffleFetcher::fetch_sort(buckets, stage_id, acc_arg, vec![21, 22, 23]);
        data
    }

    fn secure_column_sort_second(
        &self,
        stage_id: usize,
        data: Vec<ItemE>,
        acc_arg: &mut AccArg,
    ) -> Vec<ItemE> {
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
        //step 1: sort + step 2: shuffle (transpose)
        let fetched_data = {
            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(25);
            let result_ptr = wrapper_secure_execute(
                &cur_rdd_ids,
                &cur_op_ids,
                &cur_part_ids,
                Default::default(),
                dep_info,
                &data,
                &acc_arg.captured_vars,
            );
            let mut buckets =
                get_encrypted_data::<Vec<ItemE>>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
            acc_arg.free_enclave_lock();
            buckets.resize(num_splits, Vec::new());
            futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();

            for (i, bucket) in buckets.into_iter().enumerate() {
                let ser_bytes = bincode::serialize(&bucket).unwrap();
                log::debug!(
                    "during step 2. bucket #{} in stage id #{}, partition #{}: {:?}",
                    i,
                    stage_id,
                    reduce_id,
                    bucket
                );
                env::SORT_CACHE.insert((part_group, reduce_id, i), ser_bytes);
            }
            futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();
            let fut = ShuffleFetcher::secure_fetch(
                GetServerUriReq::CurStage(part_group),
                reduce_id,
                usize::MAX,
            );
            futures::executor::block_on(fut)
                .unwrap()
                .filter(|x| !x.is_empty())
                .collect::<Vec<_>>()
        };
        log::debug!(
            "step 2 finished. partition = {:?}, data = {:?}",
            reduce_id,
            fetched_data
        );
        //step 3 - 8
        let data = ShuffleFetcher::fetch_sort(fetched_data, stage_id, acc_arg, vec![26, 27, 28]);
        data
    }

    fn secure_shuffle_read(
        &self,
        stage_id: usize,
        data: Vec<ItemE>,
        acc_arg: &mut AccArg,
    ) -> Vec<ItemE> {
        //the current implementation is not fully oblivious
        //because Opaque doe not design for general oblivious group_by
        //but aggregate should be oblivious
        let reduce_id = *acc_arg.part_ids.last().unwrap();
        let part_id_offset = if acc_arg.part_ids[0] == usize::MAX {
            acc_arg.part_ids[1] - acc_arg.part_ids.last().unwrap()
        } else {
            acc_arg.part_ids[0] - acc_arg.part_ids.last().unwrap()
        };
        let num_splits = *acc_arg.split_nums.last().unwrap();
        let part_group = (stage_id, part_id_offset, num_splits);

        let cur_rdd_ids = vec![self.vals.id];
        let cur_op_ids = vec![self.vals.op_id];
        let cur_part_ids = vec![reduce_id];

        //first aggregate
        let mut agg_data = if data.is_empty() {
            Vec::new()
        } else {
            acc_arg.get_enclave_lock();
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

            let result =
                get_encrypted_data::<ItemE>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
            acc_arg.free_enclave_lock();
            *result
        };

        //sync in order to clear the sort cache
        futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();
        //clear the sort cache
        env::SORT_CACHE.retain(|k, _| k.0 != part_group);
        //sync again to avoid clearing the just-written value
        futures::executor::block_on(ShuffleFetcher::fetch_sync(part_group)).unwrap();

        if reduce_id != 0 {
            let fut = ShuffleFetcher::secure_fetch(
                GetServerUriReq::CurStage(part_group),
                reduce_id,
                (reduce_id + num_splits - 1) % num_splits,
            );
            let mut fut_res = futures::executor::block_on(fut);
            //the aggregate info may not be prepared
            while fut_res.is_err() {
                match fut_res {
                    Ok(_) => unreachable!(),
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(5));
                        fut_res = futures::executor::block_on(ShuffleFetcher::secure_fetch(
                            GetServerUriReq::CurStage(part_group),
                            reduce_id,
                            (reduce_id + num_splits - 1) % num_splits,
                        ));
                    }
                }
            }
            let mut received_agg_info = fut_res.unwrap().collect::<Vec<_>>();
            //aggregate again
            if !received_agg_info.is_empty() {
                let sup_data = received_agg_info.remove(0);
                assert!(received_agg_info.is_empty());
                if agg_data.is_empty() {
                    agg_data = sup_data;
                } else if !sup_data.is_empty() {
                    let mut tmp_captured_var = HashMap::new();
                    tmp_captured_var.insert(cur_rdd_ids[0], sup_data);

                    acc_arg.get_enclave_lock();
                    let dep_info = DepInfo::padding_new(24);
                    let result_ptr = wrapper_secure_execute(
                        &cur_rdd_ids,
                        &cur_op_ids,
                        &cur_part_ids,
                        Default::default(),
                        dep_info,
                        &agg_data,
                        &tmp_captured_var,
                    );

                    let result =
                        get_encrypted_data::<ItemE>(cur_op_ids[0], dep_info, result_ptr as *mut u8);
                    acc_arg.free_enclave_lock();
                    agg_data = *result;
                }
            }
        }

        //send aggregate info to the next worker
        if reduce_id != num_splits - 1 {
            let last_kc = agg_data.pop();
            if last_kc.is_some() {
                let ser_bytes = bincode::serialize(&vec![last_kc.unwrap()]).unwrap();
                env::SORT_CACHE.insert(
                    (part_group, reduce_id, (reduce_id + 1) % num_splits),
                    ser_bytes.clone(),
                );
            } else {
                env::SORT_CACHE.insert(
                    (part_group, reduce_id, (reduce_id + 1) % num_splits),
                    bincode::serialize(&Vec::<ItemE>::new()).unwrap(),
                );
            }
        }

        agg_data
    }
}

impl<K, V, C> RddBase for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
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
        vec![Dependency::ShuffleDependency(Arc::new(
            ShuffleDependency::new(
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

    fn iterator_raw(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<usize>,
    ) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(stage_id, split, acc_arg, tx)
    }

    fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any shuffledrdd",);
        Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
    }
}

impl<K, V, C> Rdd for ShuffledRdd<K, V, C>
where
    K: Data + Eq + Hash,
    V: Data,
    C: Data,
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
                handles.append(&mut self.secure_compute_prev(stage_id, split, acc_arg, tx)?);
            }
            Ok(handles)
        } else {
            self.secure_compute_prev(stage_id, split, acc_arg, tx)
        }
    }
}
