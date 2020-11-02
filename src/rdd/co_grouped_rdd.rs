use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
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
pub struct CoGroupedRdd<K, V, W, KE, VE, WE, FE, FD> 
where
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))> + Clone, 
    FD: Func(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone, 
{
    pub(crate) vals: Arc<RddVals>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd0: Arc<dyn RddE<Item = (K, V), ItemE = (KE, VE)>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) rdd1: Arc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
    pub(crate) fe: FE,
    pub(crate) fd: FD,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    #[serde(with = "serde_traitobject")]
    pub(crate) part: Box<dyn Partitioner>,
}

impl<K, V, W, KE, VE, WE, FE, FD> CoGroupedRdd<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: Func(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))> + Clone, 
    FD: Func(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))> + Clone,
{
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
                Arc::new(ShuffleDependency::<_, _, _, KE, Vec<VE>>::new(
                    context.new_shuffle_id(),
                    true,
                    rdd_base,
                    aggr,
                    part.clone(),
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
                Arc::new(ShuffleDependency::<_, _, _, KE, Vec<WE>>::new(
                    context.new_shuffle_id(),
                    true,
                    rdd_base,
                    aggr,
                    part.clone(),
                )) as Arc<dyn ShuffleDependencyTrait>,
            ));
        }

        vals.dependencies = deps;
        let vals = Arc::new(vals);
        let ecall_ids = rdd0.get_ecall_ids();
        CoGroupedRdd {
            vals,
            rdd0,
            rdd1,
            fe,
            fd,
            ecall_ids,
            part,
        }
    }
}

impl<K, V, W, KE, VE, WE, FE, FD> RddBase for CoGroupedRdd<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
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

    fn iterator_raw(&self, split: Box<dyn Split>) -> Result<Vec<usize>> {
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

impl<K, V, W, KE, VE, WE, FE, FD> Rdd for CoGroupedRdd<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>,
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
    
    fn secure_compute(&self, split: Box<dyn Split>, id: usize) -> Result<Vec<usize>> {
        if let Ok(split) = split.downcast::<CoGroupSplit>() {
            let mut result_ptr2 = Vec::new();
            let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
            println!("secure_compute in co_group_rdd");
            let mut deps = split.clone().deps;
            match deps.remove(0) {   //rdd1
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let result_ptr = rdd.iterator_raw(split)?;  //Vec<usize>
                    result_ptr2.push(result_ptr);
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    let fut = ShuffleFetcher::fetch::<KE, Vec<VE>>(shuffle_id, split.get_index());
                    let buckets = futures::executor::block_on(fut)?.into_iter().collect::<Vec<_>>();
                    let result_ptr = Box::into_raw(Box::new(buckets));  //memory leak
                    result_ptr2.push(vec![result_ptr as *mut u8 as usize]);
                }
            };
            
            match deps.remove(0) {    //rdd0
                CoGroupSplitDep::NarrowCoGroupSplitDep { rdd, split } => {
                    let result_ptr = rdd.iterator_raw(split)?;  //Vec<usize>
                    result_ptr2.push(result_ptr);
                }
                CoGroupSplitDep::ShuffleCoGroupSplitDep { shuffle_id } => {
                    let fut = ShuffleFetcher::fetch::<KE, Vec<WE>>(shuffle_id, split.get_index());
                    let buckets = futures::executor::block_on(fut)?.into_iter().collect::<Vec<_>>();
                    let result_ptr = Box::into_raw(Box::new(buckets));  //memory leak
                    result_ptr2.push(vec![result_ptr as *mut u8 as usize]);
                }
            }
            
            let data_ptr = Box::into_raw(Box::new(result_ptr2));
            let mut result_ptr: usize = 0;
            let sgx_status = unsafe {
                secure_executing(
                    Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                    &mut result_ptr,
                    self.get_rdd_id(),  
                    2, //shuffle write 
                    data_ptr as *mut u8,
                    &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                )
            };

            //The corresponding data needs to be freed
            let result_ptr2 = unsafe{ Box::from_raw(data_ptr) };
            let mut deps = split.clone().deps;
            for (i, v) in result_ptr2.into_iter().enumerate() {
                if i == 0 {
                    match deps.remove(0) {
                        CoGroupSplitDep::NarrowCoGroupSplitDep {rdd: _, split: _} => {
                            for ptr in v {
                                let _r = unsafe{ Box::from_raw(ptr as *mut Vec<(KE, VE)>)};
                            }
                        },
                        CoGroupSplitDep::ShuffleCoGroupSplitDep {shuffle_id: _} => {
                            for ptr in v {
                                let _r = unsafe{ Box::from_raw(ptr as *mut Vec<(KE, Vec<VE>)>)};
                            }
                        },
                    };
                } else if i == 1 {
                    match deps.remove(0) {
                        CoGroupSplitDep::NarrowCoGroupSplitDep {rdd: _, split: _} => {
                            for ptr in v {
                                let _r = unsafe{ Box::from_raw(ptr as *mut Vec<(KE, WE)>)};
                            }
                        },
                        CoGroupSplitDep::ShuffleCoGroupSplitDep {shuffle_id: _} => {
                            for ptr in v {
                                let _r = unsafe{ Box::from_raw(ptr as *mut Vec<(KE, Vec<WE>)>)};
                            }
                        },
                    };
                } else {
                    panic!("Vec of result ptrs error!")
                }
            }

            match sgx_status {
                sgx_status_t::SGX_SUCCESS => {},
                _ => {
                    panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                },
            };

            
            let data = unsafe{ Box::from_raw(result_ptr as *mut u8 as *mut Vec<(KE, (Vec<VE>, Vec<WE>))>) };
            let data_size = std::mem::size_of::<(KE, (Vec<VE>, Vec<WE>))>();
            let len = data.len();
    
            //sub-partition
            let block_len = (1 << (10+10)) / data_size;  //each block: 1MB
            let mut cur = 0;
            let mut result_ptr = Vec::new();
            while cur < len {
                let next = match cur + block_len > len {
                    true => len,
                    false => cur + block_len,
                };
                let block = Box::new((&data[cur..next]).to_vec());
                let block_ptr = Box::into_raw(block);
                let mut result_bl_ptr: usize = 0;
                let now = Instant::now();
                let sgx_status = unsafe {
                    secure_executing(
                        Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid(),
                        &mut result_bl_ptr,
                        id,
                        0,   //narrow
                        block_ptr as *mut u8,
                        &captured_vars as *const HashMap<usize, Vec<u8>> as *const u8,
                    )
                };
                let _block = unsafe{ Box::from_raw(block_ptr) };
                let _r = match sgx_status {
                    sgx_status_t::SGX_SUCCESS => {},
                    _ => {
                        panic!("[-] ECALL Enclave Failed {}!", sgx_status.as_str());
                    },
                };
    
                result_ptr.push(result_bl_ptr);
                let dur = now.elapsed().as_nanos() as f64 * 1e-9;
                println!("in ParallelCollectionRdd, compute {:?}", dur);
                cur = next;
            }
            Ok(result_ptr)
        } else {
            Err(Error::DowncastFailure("Got split object from different concrete type other than CoGroupSplit"))
        }
    }

}

impl<K, V, W, KE, VE, WE, FE, FD> RddE for CoGroupedRdd<K, V, W, KE, VE, WE, FE, FD> 
where 
    K: Data + Eq + Hash,
    V: Data,
    W: Data,
    KE: Data,
    VE: Data,
    WE: Data,
    FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> Vec<(KE, (Vec<VE>, Vec<WE>))>, 
    FD: SerFunc(Vec<(KE, (Vec<VE>, Vec<WE>))>) -> Vec<(K, (Vec<V>, Vec<W>))>,
{
    type ItemE = (KE, (Vec<VE>, Vec<WE>));
    fn get_rdde(&self) -> Arc<dyn RddE<Item = Self::Item, ItemE = Self::ItemE>> {
        Arc::new(self.clone())
    }

    fn get_fe(&self) -> Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>> {
        Box::new(self.fe.clone()) as Box<dyn Func(Vec<Self::Item>)->Vec<Self::ItemE>>
    }

    fn get_fd(&self) -> Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>> {
        Box::new(self.fd.clone()) as Box<dyn Func(Vec<Self::ItemE>)->Vec<Self::Item>>
    }
}
