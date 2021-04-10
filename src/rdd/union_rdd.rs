use std::net::Ipv4Addr;
use std::sync::{Arc, mpsc::{sync_channel, TryRecvError ,SyncSender}};
use std::thread::JoinHandle;

use itertools::{Itertools, MinMaxResult};
use serde_derive::{Deserialize, Serialize};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, RangeDependency};
use crate::env::{BOUNDED_MEM_CACHE, RDDB_MAP, Env};
use crate::error::{Error, Result};
use crate::partitioner::Partitioner;
use crate::rdd::union_rdd::UnionVariants::{NonUniquePartitioner, PartitionerAware};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use parking_lot::Mutex;

#[derive(Clone, Serialize, Deserialize)]
struct UnionSplit<T: 'static, TE: 'static> {
    /// index of the partition
    idx: usize,
    /// the parent RDD this partition refers to
    rdd: SerArc<dyn RddE<Item = T, ItemE = TE>>,
    /// index of the parent RDD this partition refers to
    parent_rdd_index: usize,
    /// index of the partition within the parent RDD this partition refers to
    parent_rdd_split_index: usize,
}

impl<T, TE> UnionSplit<T, TE> 
where
    T: Data,
    TE: Data,
{
    fn parent_partition(&self) -> Box<dyn Split> {
        self.rdd.splits()[self.parent_rdd_split_index].clone()
    }
}

impl<T, TE> Split for UnionSplit<T, TE> 
where 
    T: Data,
    TE: Data,
{
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionerAwareUnionSplit {
    idx: usize,
}

impl PartitionerAwareUnionSplit {
    fn parents<'a, T: Data, TE: Data>(
        &'a self,
        rdds: &'a [SerArc<dyn RddE<Item = T, ItemE = TE>>],
    ) -> impl Iterator<Item = Box<dyn Split>> + 'a {
        rdds.iter().map(move |rdd| rdd.splits()[self.idx].clone())
    }
}

impl Split for PartitionerAwareUnionSplit {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Serialize, Deserialize)]
pub struct UnionRdd<T: 'static, TE: 'static>(UnionVariants<T, TE>);

impl<T, TE> UnionRdd<T, TE>
where
    T: Data,
    TE: Data,
{
    #[track_caller]
    pub(crate) fn new(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> Self {
        UnionRdd(UnionVariants::new(rdds))
    }

    fn secure_compute_prev(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => {
                let (tx_un, rx_un) = sync_channel(0);
                let tx = tx.clone();
                let part = &*split
                    .downcast::<UnionSplit<T, TE>>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))?;
                let split = part.parent_partition();
                let parent = &rdds[part.parent_rdd_index];
                let part_id = split.get_index();
                let mut acc_arg_un = AccArg::new(part_id, 
                    DepInfo::padding_new(0), 
                    None, 
                    acc_arg.eenter_lock.clone(),
                    acc_arg.block_len.clone(),
                    acc_arg.cur_usage.clone(),
                    acc_arg.fresh_slope.clone(),
                );
                let handle_uns = parent.secure_compute(split, &mut acc_arg_un, tx_un.clone())?; 
                
                let acc_arg = acc_arg.clone();
                let handle = std::thread::spawn(move || {
                    let tid: u64 = thread::current().id().as_u64().into();
                    let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
                    
                    let mut sub_part_id = 0;
                    let mut cache_meta = acc_arg.to_cache_meta();
                    let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                        &acc_arg.op_ids, 
                        cache_meta, 
                        acc_arg.dep_info,
                    );
                    let mut is_survivor = spec_call_seq_ptr != 0;
                    let mut r = rx_un.recv();
                    while r.is_ok() {
                        let (_, (ptr, (time_comp, cur_mem_usage))) = r.unwrap();
                        wrapper_register_mem_usage(cur_mem_usage as usize);
                        //The last connected one will survive in cache
                        let r_next = rx_un.try_recv();
                        is_survivor = is_survivor || r_next == Err(TryRecvError::Disconnected);
                        if !acc_arg.cached(&sub_part_id) {
                            cache_meta.set_sub_part_id(sub_part_id);
                            cache_meta.set_is_survivor(is_survivor);
                            BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                            let mut init_mem_usage = 0;
                            let mut max_mem_usage = 0;
                            let input = Input::build_from_ptr(ptr as *const u8, &mut vec![0], &mut vec![usize::MAX], usize::MAX, &mut init_mem_usage, &mut max_mem_usage);
                            let mut result_bl_ptr: usize = 0; 
                            let now_comp = Instant::now();
                            let sgx_status = unsafe {
                                secure_execute(
                                    eid,
                                    &mut result_bl_ptr,
                                    tid,
                                    &acc_arg.rdd_ids as *const Vec<usize> as *const u8,
                                    &acc_arg.op_ids as *const Vec<OpId> as *const u8, 
                                    &acc_arg.split_nums as *const Vec<usize> as *const u8,
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
                            let dur_comp = now_comp.elapsed().as_nanos() as f64 * 1e-9;
                            wrapper_spec_execute(
                                spec_call_seq_ptr, 
                                cache_meta, 
                            );
                            let cur_usage = wrapper_revoke_mem_usage(true);
                            acc_arg.cur_usage.store(cur_usage, atomic::Ordering::SeqCst);
                            match acc_arg.dep_info.is_shuffle == 0 {
                                true => tx.send((sub_part_id, (result_bl_ptr, (time_comp + dur_comp, cur_usage as f64)))).unwrap(),
                                false => tx.send((sub_part_id, (result_bl_ptr, (time_comp + dur_comp, max_mem_usage as f64)))).unwrap(),
                            };
                        }
                        sub_part_id += 1;
                        if r_next.is_ok() {  // this branch is unreachable actually
                            r = Ok(r_next.unwrap());
                        } else {
                            r = rx_un.recv();
                        }
                    }
                    for handle_un in handle_uns {
                        handle_un.join().unwrap();
                    }
                });
                Ok(vec![handle])
            },
            PartitionerAware { rdds, .. } => {
                let split = split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("PartitionerAwareUnionSplit")))?;
                let iter = rdds
                    .iter()
                    .zip(split.parents(&rdds))
                    .collect::<Vec<_>>();
                let last_idx = iter.len() - 1;
                let mut handles = Vec::new();
                let union_lock = Arc::new(AtomicBool::new(false));
                for (idx, (rdd, p)) in iter.into_iter().enumerate() {
                    let part_id = p.get_index();
                    let rdd = rdd.clone();
                    let acc_arg = acc_arg.clone();
                    let tx = tx.clone();
                    let union_lock = union_lock.clone();
                    let handle = std::thread::spawn(move || {
                        while union_lock.compare_and_swap(false, true, atomic::Ordering::SeqCst) {
                            //wait
                        }
                        //refresh block_len, because parent rdds may have different lineage
                        let (tx_un, rx_un) = sync_channel(0);
                        let block_len = acc_arg.block_len.clone();
                        let cur_usage = acc_arg.cur_usage.clone();
                        let fresh_slope = acc_arg.fresh_slope.clone();
                        block_len.store(1, atomic::Ordering::SeqCst);
                        cur_usage.store(0, atomic::Ordering::SeqCst);
                        fresh_slope.store(true, atomic::Ordering::SeqCst);
                        let mut acc_arg_un = AccArg::new(part_id, 
                            DepInfo::padding_new(0), 
                            None,
                            acc_arg.eenter_lock.clone(),
                            block_len,
                            cur_usage, 
                            fresh_slope
                        );
                        let handle_uns = rdd.secure_compute(p.clone(), &mut acc_arg_un, tx_un).unwrap();
                        let tid: u64 = thread::current().id().as_u64().into();
                        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
                        let mut sub_part_id = 0;
                        let mut cache_meta = acc_arg.to_cache_meta();
                        let spec_call_seq_ptr = wrapper_exploit_spec_oppty(
                            &acc_arg.op_ids, 
                            cache_meta, 
                            acc_arg.dep_info,
                        );
                        let mut is_survivor = spec_call_seq_ptr != 0;
                        let mut r = rx_un.recv();
                        while r.is_ok() {
                            let (_, (ptr, (time_comp, cur_mem_usage))) = r.unwrap();
                            wrapper_register_mem_usage(cur_mem_usage as usize);
                            //The last connected one will survive in cache
                            let r_next = rx_un.try_recv();
                            is_survivor = is_survivor || (r_next == Err(TryRecvError::Disconnected) && idx == last_idx);
                            if !acc_arg.cached(&sub_part_id) {
                                cache_meta.set_sub_part_id(sub_part_id);
                                cache_meta.set_is_survivor(is_survivor);
                                BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                                let mut init_mem_usage = 0;
                                let mut max_mem_usage = 0;
                                let input = Input::build_from_ptr(ptr as *const u8, &mut vec![0], &mut vec![usize::MAX], usize::MAX, &mut init_mem_usage, &mut max_mem_usage);
                                let mut result_bl_ptr: usize = 0; 
                                let now_comp = Instant::now();
                                let sgx_status = unsafe {
                                    secure_execute(
                                        eid,
                                        &mut result_bl_ptr,
                                        tid,
                                        &acc_arg.rdd_ids as *const Vec<usize> as *const u8,
                                        &acc_arg.op_ids as *const Vec<OpId> as *const u8, 
                                        &acc_arg.split_nums as *const Vec<usize> as *const u8,
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
                                let dur_comp = now_comp.elapsed().as_nanos() as f64 * 1e-9;
                                wrapper_spec_execute(
                                    spec_call_seq_ptr, 
                                    cache_meta,
                                );
                                let cur_usage = wrapper_revoke_mem_usage(true);
                                acc_arg.cur_usage.store(cur_usage, atomic::Ordering::SeqCst);
                                match acc_arg.dep_info.is_shuffle == 0 {
                                    true => tx.send((sub_part_id, (result_bl_ptr, (time_comp + dur_comp, cur_usage as f64)))).unwrap(),
                                    false => tx.send((sub_part_id, (result_bl_ptr, (time_comp + dur_comp, max_mem_usage as f64)))).unwrap(),
                                };
                            }
                            sub_part_id += 1;
                            if r_next.is_ok() {  // this branch is unreachable actually
                                r = Ok(r_next.unwrap());
                            } else {
                                r = rx_un.recv();
                            }
                        }
                        for handle_un in handle_uns {
                            handle_un.join().unwrap();
                        }
                        assert_eq!(union_lock.compare_and_swap(true, false, atomic::Ordering::SeqCst), true);
                    });
                    handles.push(handle)
                }
                Ok(handles)
            }
        }
    }
}

impl<T: Data, TE: Data> Clone for UnionRdd<T, TE> {
    fn clone(&self) -> Self {
        UnionRdd(self.0.clone())
    }
}

#[derive(Serialize, Deserialize)]
enum UnionVariants<T: 'static, TE: 'static> {
    NonUniquePartitioner {
        rdds: Vec<SerArc<dyn RddE<Item = T, ItemE = TE>>>,
        vals: Arc<RddVals>,
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    PartitionerAware {
        rdds: Vec<SerArc<dyn RddE<Item = T, ItemE = TE>>>,
        vals: Arc<RddVals>,
        #[serde(with = "serde_traitobject")]
        part: Box<dyn Partitioner>,
    },
}

impl<T: Data, TE: Data> Clone for UnionVariants<T, TE> {
    fn clone(&self) -> Self {
        match self {
            NonUniquePartitioner { rdds, vals, .. } => NonUniquePartitioner {
                rdds: rdds.clone(),
                vals: vals.clone(),
            },
            PartitionerAware {
                rdds, vals, part, ..
            } => PartitionerAware {
                rdds: rdds.clone(),
                vals: vals.clone(),
                part: part.clone(),
            },
        }
    }
}

impl<T: Data, TE: Data> UnionVariants<T, TE> {
    #[track_caller]
    fn new(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> Self {
        let context = rdds[0].get_context();
        let secure = rdds[0].get_secure();  //temp
        let mut vals = RddVals::new(context, secure);

        let mut pos = 0;
        let final_rdds: Vec<_> = rdds.iter().map(|rdd| rdd.clone().into()).collect();

        if !UnionVariants::has_unique_partitioner(rdds) {
            let vals = Arc::new(vals);
            log::debug!("inside unique partitioner constructor");
            NonUniquePartitioner {
                rdds: final_rdds,
                vals,
            }
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner).unwrap();
            log::debug!("inside partition aware constructor");
            let vals = Arc::new(vals);
            PartitionerAware {
                rdds: final_rdds,
                vals,
                part,
            }
        }
    }

    fn has_unique_partitioner(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> bool {
        rdds.iter()
            .map(|p| p.partitioner())
            .try_fold(None, |prev: Option<Box<dyn Partitioner>>, p| {
                if let Some(partitioner) = p {
                    if let Some(prev_partitioner) = prev {
                        if prev_partitioner.equals((&*partitioner).as_any()) {
                            // only continue in case both partitioners are the same
                            Ok(Some(partitioner))
                        } else {
                            Err(())
                        }
                    } else {
                        // first element
                        Ok(Some(partitioner))
                    }
                } else {
                    Err(())
                }
            })
            .is_ok()
    }

    fn current_pref_locs<'a>(
        &'a self,
        rdd: Arc<dyn RddBase>,
        split: &dyn Split,
        context: Arc<Context>,
    ) -> impl Iterator<Item = std::net::Ipv4Addr> + 'a {
        context
            .get_preferred_locs(rdd, split.get_index())
            .into_iter()
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<T>)->TE> {
        match self {
            NonUniquePartitioner { rdds, .. } => rdds[0].get_fe(),
            PartitionerAware { rdds, .. } => rdds[0].get_fe(),
        }
    }

    fn get_fd(&self) -> Box<dyn Func(TE)->Vec<T>> {
        match self {
            NonUniquePartitioner { rdds, .. } => rdds[0].get_fd(),
            PartitionerAware { rdds, .. } => rdds[0].get_fd(),
        }
    }

}

impl<T: Data, TE: Data> RddBase for UnionRdd<T, TE> {
    fn cache(&self) {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.cache(),
            PartitionerAware { vals, .. } => vals.cache(),
        }
        RDDB_MAP.insert(
            self.get_rdd_id(), 
            self.get_rdd_base()
        );
    }

    fn should_cache(&self) -> bool {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.should_cache(),
            PartitionerAware { vals, .. } => vals.should_cache(),
        }
    }

    fn free_data_enc(&self, ptr: *mut u8) {
        let _data_enc = unsafe {
            Box::from_raw(ptr as *mut Vec<TE>)
        };
    }

    fn get_rdd_id(&self) -> usize {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.id,
            PartitionerAware { vals, .. } => vals.id,
        }
    }

    fn get_op_id(&self) -> OpId {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.op_id,
            PartitionerAware { vals, .. } => vals.op_id,
        }
    }

    fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
        //speculative execution doesn't go across union rdd
        op_ids.push(self.get_op_id());
    }

    fn get_op_name(&self) -> String {
        "union".to_owned()
    }

    fn get_context(&self) -> Arc<Context> {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.context.upgrade().unwrap(),
            PartitionerAware { vals, .. } => vals.context.upgrade().unwrap(),
        }
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        let mut pos = 0;
        let rdds = match &self.0 {
            NonUniquePartitioner { rdds, .. } => 
                rdds.iter().map(|rdd| rdd.clone().into()).collect::<Vec<_>>(),
            PartitionerAware { rdds, .. } => 
                rdds.iter().map(|rdd| rdd.clone().into()).collect::<Vec<_>>(),
        };

        if !UnionVariants::has_unique_partitioner(&rdds) {
            let deps = rdds
                .iter()
                .map(|rdd| {
                    let rdd_base = rdd.get_rdd_base();
                    let num_parts = rdd_base.number_of_splits();
                    let dep = Dependency::NarrowDependency(Arc::new(RangeDependency::new(
                        rdd_base, 0, pos, num_parts,
                    )));
                    pos += num_parts;
                    dep
                })
                .collect();
            deps
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner).unwrap();
            log::debug!("inside partition aware constructor");
            let deps = rdds
                .iter()
                .map(|x| {
                    Dependency::NarrowDependency(
                        Arc::new(OneToOneDependency::new(x.get_rdd_base()))
                            as Arc<dyn NarrowDependencyTrait>,
                    )
                })
                .collect();
            deps
        }
    }

    fn get_secure(&self) -> bool {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.secure,
            PartitionerAware { vals, .. } => vals.secure,
        }
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<TE>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        match &self.0 {
            NonUniquePartitioner { .. } => Vec::new(),
            PartitionerAware { rdds, .. } => {
                log::debug!(
                    "finding preferred location for PartitionerAwareUnionRdd, partition {}",
                    split.get_index()
                );

                let split = &*split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))
                    .unwrap();

                let locations =
                    rdds.iter()
                        .zip(split.parents(rdds.as_slice()))
                        .map(|(rdd, part)| {
                            let parent_locations = self.0.current_pref_locs(
                                rdd.get_rdd_base(),
                                &*part,
                                self.get_context(),
                            );
                            log::debug!("location of {} partition {} = {}", 1, 2, 3);
                            parent_locations
                        });

                // find the location that maximum number of parent partitions prefer
                let location = match locations.flatten().minmax_by_key(|loc| *loc) {
                    MinMaxResult::MinMax(_, max) => Some(max),
                    MinMaxResult::OneElement(e) => Some(e),
                    MinMaxResult::NoElements => None,
                };

                log::debug!(
                    "selected location for PartitionerAwareRdd, partition {} = {:?}",
                    split.get_index(),
                    location
                );

                location.into_iter().collect()
            }
        }
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => rdds
                .iter()
                .enumerate()
                .flat_map(|(rdd_idx, rdd)| {
                    rdd.splits()
                        .into_iter()
                        .enumerate()
                        .map(move |(split_idx, _split)| (rdd_idx, rdd, split_idx))
                })
                .enumerate()
                .map(|(idx, (rdd_idx, rdd, s_idx))| {
                    Box::new(UnionSplit {
                        idx,
                        rdd: rdd.clone(),
                        parent_rdd_index: rdd_idx,
                        parent_rdd_split_index: s_idx,
                    }) as Box<dyn Split>
                })
                .collect(),
            PartitionerAware { part, .. } => {
                let num_partitions = part.get_num_of_partitions();
                (0..num_partitions)
                    .map(|idx| Box::new(PartitionerAwareUnionSplit { idx }) as Box<dyn Split>)
                    .collect()
            }
        }
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any union_rdd",);
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
    }

    fn partitioner(&self) -> Option<Box<dyn Partitioner>> {
        match &self.0 {
            NonUniquePartitioner { .. } => None,
            PartitionerAware { part, .. } => Some(part.clone()),
        }
    }
}

impl<T: Data, TE: Data> Rdd for UnionRdd<T, TE> {
    type Item = T;

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(UnionRdd(self.0.clone())) as Arc<dyn RddBase>
    }

    fn get_rdd(&self) -> Arc<dyn Rdd<Item = T>> {
        Arc::new(UnionRdd(self.0.clone())) as Arc<dyn Rdd<Item = T>>
    }

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = T>>> {
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => {
                let part = &*split
                    .downcast::<UnionSplit<T, TE>>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))?;
                let parent = &rdds[part.parent_rdd_index];
                parent.iterator(part.parent_partition())
            }
            PartitionerAware { rdds, .. } => {
                let split = split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("PartitionerAwareUnionSplit")))?;
                let iter: Result<Vec<_>> = rdds
                    .iter()
                    .zip(split.parents(&rdds))
                    .map(|(rdd, p)| rdd.iterator(p.clone()))
                    .collect();
                Ok(Box::new(iter?.into_iter().flatten()))
            }
        }
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

impl<T, TE> RddE for UnionRdd<T, TE>
where
    T: Data,
    TE: Data,
{
    type ItemE = TE;
    
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(UnionRdd(self.0.clone())) as Arc<dyn RddE<Item = T, ItemE = TE>>
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Self::ItemE> {
        self.0.get_fe()
    }

    fn get_fd(&self) -> Box<dyn Func(Self::ItemE)->Vec<Self::Item>> {
        self.0.get_fd()
    }
}