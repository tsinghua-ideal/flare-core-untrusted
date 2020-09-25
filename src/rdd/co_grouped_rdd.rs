use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{
    Dependency, NarrowDependencyTrait, OneToOneDependency, ShuffleDependency,
    ShuffleDependencyTrait,
};
use crate::env::Env;
use crate::error::Result;
use crate::partitioner::Partitioner;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data};
use crate::shuffle::ShuffleFetcher;
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use sgx_types::*;

extern "C" {
    fn secure_executing(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        id: usize,
        is_shuffle: u8,
        input: *mut u8,
        captured_vars: *const u8,
    ) -> sgx_status_t;
}

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
pub struct CoGroupedRdd<K: Data> {
    pub(crate) vals: Arc<RddVals>,
    pub(crate) rdds: Vec<SerArc<dyn RddBase>>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) part: Box<dyn Partitioner>,
    _marker: PhantomData<K>,
}

impl<K: Data + Eq + Hash> CoGroupedRdd<K> {
    pub fn new(rdds: Vec<SerArc<dyn RddBase>>, part: Box<dyn Partitioner>) -> Self {
        let context = rdds[0].get_context();
        let secure = rdds[0].get_secure();  //
        let mut vals = RddVals::new(context.clone(), secure);
        let create_combiner = Box::new(Fn!(|v: Box<dyn AnyData>| vec![v]));
        fn merge_value(
            mut buf: Vec<Box<dyn AnyData>>,
            v: Box<dyn AnyData>,
        ) -> Vec<Box<dyn AnyData>> {
            buf.push(v);
            buf
        }
        let merge_value = Box::new(Fn!(|(buf, v)| merge_value(buf, v)));
        fn merge_combiners(
            mut b1: Vec<Box<dyn AnyData>>,
            mut b2: Vec<Box<dyn AnyData>>,
        ) -> Vec<Box<dyn AnyData>> {
            b1.append(&mut b2);
            b1
        }
        let merge_combiners = Box::new(Fn!(|(b1, b2)| merge_combiners(b1, b2)));
        let aggr = Arc::new(
            Aggregator::<K, Box<dyn AnyData>, Vec<Box<dyn AnyData>>>::new(
                create_combiner,
                merge_value,
                merge_combiners,
            ),
        );
        let mut deps = Vec::new();
        for (_index, rdd) in rdds.iter().enumerate() {
            let part = part.clone();
            if rdd
                .partitioner()
                .map_or(false, |p| p.equals(&part as &dyn Any))
            {
                let rdd_base = rdd.clone().into();
                deps.push(Dependency::NarrowDependency(
                    Arc::new(OneToOneDependency::new(rdd_base)) as Arc<dyn NarrowDependencyTrait>,
                ))
            } else {
                let rdd_base = rdd.clone().into();
                log::debug!("creating aggregator inside cogrouprdd");
                deps.push(Dependency::ShuffleDependency(
                    Arc::new(ShuffleDependency::new(
                        context.new_shuffle_id(),
                        true,
                        rdd_base,
                        aggr.clone(),
                        part,
                    )) as Arc<dyn ShuffleDependencyTrait>,
                ))
            }
        }
        vals.dependencies = deps;
        let vals = Arc::new(vals);
        let ecall_ids = rdds[0].get_ecall_ids();
        CoGroupedRdd {
            vals,
            rdds,
            ecall_ids,
            part,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Eq + Hash> RddBase for CoGroupedRdd<K> {
    fn get_rdd_id(&self) -> usize {
        self.vals.id
    }

    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }

    fn get_dependencies(&self) -> Vec<Dependency> {
        self.vals.dependencies.clone()
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
        self.ecall_ids.clone()
    }

    fn insert_ecall_id(&self) {
        if self.vals.secure {
            self.ecall_ids.lock().push(self.vals.id);
        }
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::new();
        for i in 0..self.part.get_num_of_partitions() {
            splits.push(Box::new(CoGroupSplit::new(
                i,
                self.rdds
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

    fn iterator_raw(&self, split: Box<dyn Split>) -> Vec<usize> {
        self.secure_compute(split, self.get_rdd_id())
    }

    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K: Data + Eq + Hash> Rdd for CoGroupedRdd<K> {
    type Item = (K, Vec<Vec<Box<dyn AnyData>>>);
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }

    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }

    #[allow(clippy::type_complexity)]
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut agg: HashMap<K, Vec<Vec<Box<dyn AnyData>>>> = HashMap::new();
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        log::debug!("inside iterator CoGroupedRdd narrow dep");
                        for i in rdd.iterator_any(split)? {
                            log::debug!(
                                "inside iterator CoGroupedRdd narrow dep iterator any: {:?}",
                                i
                            );
                            let b = i
                                .into_any()
                                .downcast::<(Box<dyn AnyData>, Box<dyn AnyData>)>()
                                .unwrap();
                            let (k, v) = *b;
                            let k = *(k.into_any().downcast::<K>().unwrap());
                            agg.entry(k)
                                .or_insert_with(|| vec![Vec::new(); self.rdds.len()])[dep_num]
                                .push(v)
                        }
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        log::debug!("inside iterator CoGroupedRdd shuffle dep, agg: {:?}", agg);
                        let num_rdds = self.rdds.len();
                        let fut = ShuffleFetcher::fetch::<K, Vec<Box<dyn AnyData>>>(
                            shuffle_id,
                            split.get_index(),
                        );
                        for (k, c) in futures::executor::block_on(fut)?.into_iter() {
                            let temp = agg.entry(k).or_insert_with(|| vec![Vec::new(); num_rdds]);
                            for v in c {
                                temp[dep_num].push(v);
                            }
                        }
                    }
                }
            }
            Ok(Box::new(agg.into_iter()))
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
    }
    
    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Vec<usize> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut result_ptr2 = Vec::new();
            let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
            println!("secure_compute in co_group_rdd");
            for (dep_num, dep) in split.clone().deps.into_iter().enumerate() {
                match dep {
                    CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                        let result_ptr = rdd.iterator_raw(split);  //Vec<usize>
                        result_ptr2.push(result_ptr);
                    }
                    CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                        let fut = ShuffleFetcher::fetch::<K, Vec<Box<dyn AnyData>>>(shuffle_id, split.get_index());
                        let buckets = futures::executor::block_on(fut).unwrap(); //may be an erro
                        println!("may be an error");
                        let result_ptr = Box::into_raw(Box::new(buckets));  //memory leak
                        result_ptr2.push(vec![result_ptr as *mut u8 as usize]);
                    }
                }
            };
            
            let data_ptr = Box::into_raw(Box::new(result_ptr2));
            let mut result_ptr: usize = 0;
            let sgx_status = unsafe {
                secure_executing(
                    Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                    &mut result_ptr,
                    id,  
                    0,   //is_shuffle = false
                    data_ptr as *mut u8,
                    &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                )
            };
            let result_ptr2 = unsafe{ Box::from_raw(data_ptr) };
            //TODO the corresponding data needs to be freed
            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };
            vec![result_ptr]
        } else {
            panic!("Got split object from different concrete type other than CoGroupSplit")
        }
    }

}
