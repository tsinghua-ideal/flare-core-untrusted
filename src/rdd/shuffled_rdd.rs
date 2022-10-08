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
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let part_id = split.get_index();
        let fut = ShuffleFetcher::secure_fetch::<Vec<ItemE>>(
            GetServerUriReq::PrevStage(self.shuffle_id),
            part_id,
            usize::MAX,
        );
        let buckets: Vec<Vec<ItemE>> = futures::executor::block_on(fut)?
            .into_iter()
            .filter(|x| !x.is_empty())
            .collect(); // bucket per subpartition

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

        //need split_num fix first
        wrapper_secure_execute_pre(&acc_arg.op_ids, &acc_arg.split_nums, acc_arg.dep_info);
        //shuffle read
        let now = Instant::now();
        let (data, marks) = secure_shuffle_read(
            stage_id,
            buckets,
            &cur_rdd_ids,
            &cur_op_ids,
            &cur_part_ids,
            part_group,
            acc_arg,
            self.aggregator.is_default,
        );
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("***in shuffled rdd, shuffle read, total {:?}***", dur);

        let acc_arg = acc_arg.clone();
        let handle = thread::spawn(move || {
            let now = Instant::now();
            let wait = start_execute(stage_id, acc_arg, data, marks, tx);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in shuffled rdd, compute, total {:?}***", dur);
        });
        Ok(vec![handle])
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
        tx: SyncSender<(usize, usize)>,
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
    buckets: Vec<Vec<ItemE>>,
    cur_rdd_ids: &Vec<usize>,
    cur_op_ids: &Vec<OpId>,
    cur_part_ids: &Vec<usize>,
    part_group: (usize, usize, usize),
    acc_arg: &AccArg,
    is_aggregator_default: bool,
) -> (Vec<ItemE>, Vec<ItemE>) {
    let reduce_id = cur_part_ids[0];
    let mut tmp_captured_var = HashMap::new();

    if is_aggregator_default {
        //obliv_filter_stage3 + obliv_agg_stage1
        let (part, mut buckets) = {
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

            let (part, mut buckets) = get_encrypted_data::<ItemE, Vec<ItemE>>(
                cur_op_ids[0],
                dep_info,
                data_ptr,
                marks_ptr,
            );
            acc_arg.free_enclave_lock();
            buckets.resize(part_group.2, Vec::new());
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
            (
                part,
                futures::executor::block_on(fut)
                    .unwrap()
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>(),
            )
        };

        //obliv_agg_stage_2 + obliv_group_by_stage1 + obliv_group_by_stage2 + obliv_group_by_stage3
        let (buckets, alpha, beta, n_out_prime) = {
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
                &buckets,
                &Vec::<ItemE>::new(),
                &tmp_captured_var,
            );

            let (mut buckets, meta_data) = get_encrypted_data::<Vec<ItemE>, u8>(
                cur_op_ids[0],
                dep_info,
                data_ptr,
                meta_data_ptr,
            );
            acc_arg.free_enclave_lock();
            let (alpha, beta, n_out_prime): (usize, usize, usize) =
                bincode::deserialize(&meta_data).unwrap();
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
            (
                futures::executor::block_on(fut)
                    .unwrap()
                    .filter(|x| !x.is_empty())
                    .collect::<Vec<_>>(),
                alpha,
                beta,
                n_out_prime,
            )
        };

        //obliv_group_by_stage_4
        let buckets = {
            tmp_captured_var.insert(
                cur_rdd_ids[0],
                vec![bincode::serialize(&(part_group, beta, n_out_prime)).unwrap()],
            );
            acc_arg.get_enclave_lock();
            let dep_info = DepInfo::padding_new(22);
            let (data_ptr, marks_ptr) = wrapper_secure_execute(
                stage_id,
                cur_rdd_ids,
                cur_op_ids,
                cur_part_ids,
                Default::default(),
                dep_info,
                &part,
                &buckets,
                &tmp_captured_var,
            );

            assert_eq!(marks_ptr, 0);
            let (mut buckets, _) = get_encrypted_data::<Vec<ItemE>, ItemE>(
                cur_op_ids[0],
                dep_info,
                data_ptr,
                marks_ptr,
            );
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

        //obliv_group_by_stage_5
        {
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
                &HashMap::new(),
            );

            //the invalid values have not been filtered.
            assert_ne!(marks_ptr, 0);
            let (data, marks) =
                get_encrypted_data::<ItemE, ItemE>(cur_op_ids[0], dep_info, data_ptr, marks_ptr);
            acc_arg.free_enclave_lock();
            (data, marks)
        }
    } else {
        //obliv_agg_stage2
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
            &HashMap::new(),
        );

        let (data, marks) =
            get_encrypted_data::<ItemE, ItemE>(cur_op_ids[0], dep_info, data_ptr, marks_ptr);
        acc_arg.free_enclave_lock();
        //the invalid values have been filtered.
        assert!(marks.is_empty());
        (data, marks)
    }
}
