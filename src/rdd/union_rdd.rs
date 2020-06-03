use std::net::Ipv4Addr;
use std::sync::Arc;

use itertools::{Itertools, MinMaxResult};
use serde_derive::{Deserialize, Serialize};

use crate::context::Context;
use crate::dependency::{Dependency, NarrowDependencyTrait, OneToOneDependency, RangeDependency};
use crate::error::{Error, Result};
use crate::partitioner::Partitioner;
use crate::rdd::union_rdd::UnionVariants::{NonUniquePartitioner, PartitionerAware};
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::split::Split;
use parking_lot::Mutex;

#[derive(Clone, Serialize, Deserialize)]
struct UnionSplit<T: 'static> {
    /// index of the partition
    idx: usize,
    /// the parent RDD this partition refers to
    rdd: SerArc<dyn Rdd<Item = T>>,
    /// index of the parent RDD this partition refers to
    parent_rdd_index: usize,
    /// index of the partition within the parent RDD this partition refers to
    parent_rdd_split_index: usize,
}

impl<T: Data> UnionSplit<T> {
    fn parent_partition(&self) -> Box<dyn Split> {
        self.rdd.splits()[self.parent_rdd_split_index].clone()
    }
}

impl<T: Data> Split for UnionSplit<T> {
    fn get_index(&self) -> usize {
        self.idx
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct PartitionerAwareUnionSplit {
    idx: usize,
}

impl PartitionerAwareUnionSplit {
    fn parents<'a, T: Data>(
        &'a self,
        rdds: &'a [SerArc<dyn Rdd<Item = T>>],
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
pub struct UnionRdd<T: 'static>(UnionVariants<T>);

impl<T> UnionRdd<T>
where
    T: Data,
{
    pub(crate) fn new(rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<Self> {
        Ok(UnionRdd(UnionVariants::new(rdds)?))
    }
}

#[derive(Serialize, Deserialize)]
enum UnionVariants<T: 'static> {
    NonUniquePartitioner {
        rdds: Vec<SerArc<dyn Rdd<Item = T>>>,
        vals: Arc<RddVals>,
        ecall_ids: Arc<Mutex<Vec<usize>>>,
    },
    /// An RDD that can take multiple RDDs partitioned by the same partitioner and
    /// unify them into a single RDD while preserving the partitioner. So m RDDs with p partitions each
    /// will be unified to a single RDD with p partitions and the same partitioner.
    PartitionerAware {
        rdds: Vec<SerArc<dyn Rdd<Item = T>>>,
        vals: Arc<RddVals>,
        ecall_ids: Arc<Mutex<Vec<usize>>>,
        #[serde(with = "serde_traitobject")]
        part: Box<dyn Partitioner>,
    },
}

impl<T: Data> Clone for UnionVariants<T> {
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

impl<T: Data> UnionVariants<T> {
    fn new(rdds: &[Arc<dyn Rdd<Item = T>>]) -> Result<Self> {
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

    fn has_unique_partitioner(rdds: &[Arc<dyn Rdd<Item = T>>]) -> bool {
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
}

impl<T: Data> RddBase for UnionRdd<T> {
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

    fn iterator_ser(&self, split: Box<dyn Split>) -> Vec<Vec<u8>> {
        self.secure_compute(split, self.get_rdd_id())
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

impl<T: Data> Rdd for UnionRdd<T> {
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
                    .downcast::<UnionSplit<T>>()
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

    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<Vec<u8>> {
        //TODO
        Vec::new()
    }

}
