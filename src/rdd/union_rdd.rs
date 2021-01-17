use std::net::Ipv4Addr;
use std::sync::{Arc, mpsc::{sync_channel, SyncSender}};
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
    pub(crate) fn new(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> Result<Self> {
        Ok(UnionRdd(UnionVariants::new(rdds)?))
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
        ecall_ids: Arc<Mutex<Vec<usize>>>,
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    PartitionerAware {
        rdds: Vec<SerArc<dyn RddE<Item = T, ItemE = TE>>>,
        vals: Arc<RddVals>,
        ecall_ids: Arc<Mutex<Vec<usize>>>,
        #[serde(with = "serde_traitobject")]
        part: Box<dyn Partitioner>,
    },
}

impl<T: Data, TE: Data> Clone for UnionVariants<T, TE> {
    fn clone(&self) -> Self {
        match self {
            NonUniquePartitioner { rdds, vals, ecall_ids, .. } => NonUniquePartitioner {
                rdds: rdds.clone(),
                vals: vals.clone(),
                ecall_ids: ecall_ids.clone(),
            },
            PartitionerAware {
                rdds, vals, ecall_ids, part, ..
            } => PartitionerAware {
                rdds: rdds.clone(),
                vals: vals.clone(),
                ecall_ids: ecall_ids.clone(),
                part: part.clone(),
            },
        }
    }
}

impl<T: Data, TE: Data> UnionVariants<T, TE> {
    fn new(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> Result<Self> {
        let context = rdds[0].get_context();
        let secure = rdds[0].get_secure();  //temp
        let mut vals = RddVals::new(context, secure);

        let mut pos = 0;
        let final_rdds: Vec<_> = rdds.iter().map(|rdd| rdd.clone().into()).collect();

        if !UnionVariants::has_unique_partitioner(rdds) {
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
            vals.dependencies = deps;
            let vals = Arc::new(vals);
            log::debug!("inside unique partitioner constructor");
            Ok(NonUniquePartitioner {
                rdds: final_rdds,
                vals,
                ecall_ids: rdds[0].get_ecall_ids(),
            })
        } else {
            let part = rdds[0].partitioner().ok_or(Error::LackingPartitioner)?;
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
            vals.dependencies = deps;
            let vals = Arc::new(vals);
            Ok(PartitionerAware {
                rdds: final_rdds,
                vals,
                ecall_ids: rdds[0].get_ecall_ids(),
                part,
            })
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
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.dependencies.clone(),
            PartitionerAware { vals, .. } => vals.dependencies.clone(),
        }
    }

    fn get_secure(&self) -> bool {
        match &self.0 {
            NonUniquePartitioner { vals, .. } => vals.secure,
            PartitionerAware { vals, .. } => vals.secure,
        }
    }

    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
        match &self.0 {
            NonUniquePartitioner { ecall_ids, .. } => ecall_ids.clone(),
            PartitionerAware { ecall_ids, .. } => ecall_ids.clone(),
        }
    }

    fn insert_ecall_id(&self) {
        match &self.0 {
            NonUniquePartitioner { vals, ecall_ids, .. } =>         
                if vals.secure {
                    ecall_ids.lock().push(vals.id);
                }
            PartitionerAware { vals, ecall_ids, .. } =>
                if vals.secure {
                    ecall_ids.lock().push(vals.id);
                }
        }
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<TE>(self.get_rdd_id(), value_ptr);
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

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any union_rdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|x| Box::new(x) as Box<dyn AnyData>),
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

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        let part_id = split.get_index();
        let cur_rdd_id = self.get_rdd_id();
        acc_arg.insert_rdd_id(cur_rdd_id);

        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        match &self.0 {
            NonUniquePartitioner { rdds, .. } => {
                let (tx_un, rx_un) = sync_channel(0);
                let tx = tx.clone();
                let part = &*split
                    .downcast::<UnionSplit<T, TE>>()
                    .or(Err(Error::DowncastFailure("UnionSplit")))?;
                let parent = &rdds[part.parent_rdd_index];
                let rdd_id = parent.get_rdd_id();
                let mut acc_arg_un = AccArg::new(rdd_id, 0, 0, 0);
                let handle_uns = parent.secure_compute(part.parent_partition(), &mut acc_arg_un, tx_un.clone())?; 
                
                let acc_arg = acc_arg.clone();
                let handle = std::thread::spawn(move || {
                    let tid: u64 = thread::current().id().as_u64().into();
                    let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
                    
                    let mut sub_part_id = 0;
                    let mut cache_meta = CacheMeta::new(acc_arg.caching_rdd_id,
                        0,   //indicate it cannot find the cached data
                        part_id, 
                        acc_arg.steps_to_caching,
                        acc_arg.steps_to_cached,
                    );
                    for received in rx_un {
                        if !acc_arg.cached(&sub_part_id) {
                            cache_meta.set_sub_part_id(sub_part_id);
                            BOUNDED_MEM_CACHE.insert_subpid(cache_meta.caching_rdd_id, part_id, sub_part_id);
                            let mut result_bl_ptr: usize = 0; 
                            let _sgx_status = unsafe {
                                secure_executing(
                                    eid,
                                    &mut result_bl_ptr,
                                    tid,
                                    &acc_arg.rdd_ids as *const Vec<(usize, usize)> as *const u8, 
                                    cache_meta,
                                    acc_arg.is_shuffle, 
                                    received as *mut u8, 
                                    &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                                )
                            };
                            tx.send(result_bl_ptr).unwrap();
                        }
                        sub_part_id += 1;
                    }
                    for handle_un in handle_uns {
                        handle_un.join().unwrap();
                    }
                });
                Ok(vec![handle])
            },
            PartitionerAware { rdds, .. } => {
                let (tx_un, rx_un) = sync_channel(0);
                let split = split
                    .downcast::<PartitionerAwareUnionSplit>()
                    .or(Err(Error::DowncastFailure("PartitionerAwareUnionSplit")))?;
                let iter = rdds
                    .iter()
                    .zip(split.parents(&rdds))
                    .collect::<Vec<_>>();

                let mut handle_uns = Vec::new(); 
                for (rdd, p) in iter {
                    let rdd_id = rdd.get_rdd_id();
                    let mut acc_arg_un = AccArg::new(rdd_id, 0, 0, 0);
                    handle_uns.append(&mut rdd.secure_compute(p.clone(), &mut acc_arg_un, tx_un.clone())?);
                }

                let acc_arg = acc_arg.clone();
                let handle = std::thread::spawn(move || { 
                    let tid: u64 = thread::current().id().as_u64().into();
                    let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
                    let mut sub_part_id = 0;
                    let mut cache_meta = CacheMeta::new(acc_arg.caching_rdd_id,
                        0,   //indicate it cannot find the cached data
                        part_id, 
                        acc_arg.steps_to_caching,
                        acc_arg.steps_to_cached,
                    );
                    for received in rx_un {
                        if !acc_arg.cached(&sub_part_id) {
                            cache_meta.set_sub_part_id(sub_part_id);
                            BOUNDED_MEM_CACHE.insert_subpid(cache_meta.caching_rdd_id, part_id, sub_part_id);
                            let mut result_bl_ptr: usize = 0; 
                            let _sgx_status = unsafe {
                                secure_executing(
                                    eid,
                                    &mut result_bl_ptr,
                                    tid,
                                    &acc_arg.rdd_ids as *const Vec<(usize, usize)> as *const u8, 
                                    cache_meta,
                                    acc_arg.is_shuffle, 
                                    received as *mut u8, 
                                    &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                                )
                            };
                            tx.send(result_bl_ptr).unwrap();
                        }
                        sub_part_id += 1;
                    }

                    for handle_un in handle_uns {
                        handle_un.join().unwrap();
                    }
                });
                Ok(vec![handle])
            }
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