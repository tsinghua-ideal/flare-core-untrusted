use std::hash::Hash;
use std::sync::{Arc, mpsc::Sender};
use std::thread::JoinHandle;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::error::Result;
use crate::partitioner::{HashPartitioner, Partitioner};
use crate::rdd::co_grouped_rdd::CoGroupedRdd;
use crate::rdd::shuffled_rdd::ShuffledRdd;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::split::Split;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use parking_lot::Mutex;

// Trait containing pair rdd methods. No need of implicit conversion like in Spark version.
pub trait PairRdd<K, V, KE, VE>: RddE<Item = (K, V), ItemE = (KE, VE)> + Send + Sync 
where
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data + Eq + Hash, 
    VE: Data,
{
    fn combine_by_key<KE2: Data, C: Data, CE: Data, FE, FD>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn RddE<Item = (K, C), ItemE = (KE2, CE)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
        FE: SerFunc(Vec<(K, C)>) -> Vec<(KE2, CE)>, 
        FD: SerFunc(Vec<(KE2, CE)>) -> Vec<(K, C)>,
    {
        SerArc::new(ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
            fe,
            fd,
        ))
    }

    fn group_by_key(&self, num_splits: usize) -> SerArc<dyn RddE<Item = (K, Vec<V>), ItemE = (KE, Vec<VE>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        self.group_by_key_using_partitioner(
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    fn group_by_key_using_partitioner(
        &self,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn RddE<Item = (K, Vec<V>), ItemE = (KE, Vec<VE>)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
    {
        let fe = self.get_fe();
        let fd = self.get_fd();
        let fe_wrapper = Fn!(move |v: Vec<(K, Vec<V>)>| {
            let mut ct = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                ct.push((ct_x[0].clone(), ct_y));
            } 
            ct
        });
        let fd_wrapper = Fn!(move |v: Vec<(KE, Vec<VE>)>| {
            let mut pt = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                pt.push((pt_x[0].clone(), pt_y));
            }
            pt
        });

        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner, fe_wrapper, fd_wrapper)
    }

    fn reduce_by_key<KE2, VE2, F, FE, FD>(&self, func: F, num_splits: usize, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
        FE: SerFunc(Vec<(K, V)>) -> Vec<(KE2, VE2)>, 
        FD: SerFunc(Vec<(KE2, VE2)>) -> Vec<(K, V)>,        
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
            fe,
            fd
        )
    }

    fn reduce_by_key_using_partitioner<KE2, VE2, F, FE, FD>(
        &self,
        func: F,
        partitioner: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn RddE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
        FE: SerFunc(Vec<(K, V)>) -> Vec<(KE2, VE2)>, 
        FD: SerFunc(Vec<(KE2, VE2)>) -> Vec<(K, V)>,  
    {
        let create_combiner = Box::new(Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner, fe, fd)
    }

    fn map_values<U, UE, F, FE, FD>(
        &self,
        f: F,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn RddE<Item = (K, U), ItemE = (KE, UE)>>
    where
        Self: Sized,
        F: SerFunc(V) -> U + Clone,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>, 
        FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
    {
        SerArc::new(MappedValuesRdd::new(self.get_rdd(), f, fe, fd))
    }

    fn flat_map_values<U, UE, F, FE, FD>(
        &self,
        f: F,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn RddE<Item = (K, U), ItemE = (KE, UE)>>
    where
        Self: Sized,
        F: SerFunc(V) -> Box<dyn Iterator<Item = U>> + Clone,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>, 
        FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>, 
    {
        SerArc::new(FlatMappedValuesRdd::new(self.get_rdd(), f, fe, fd))
    }

    fn join<W: Data, WE: Data>(
        &self,
        other: SerArc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
        num_splits: usize,
    ) -> SerArc<dyn RddE<Item = (K, (V, W)), ItemE = (KE, (VE, WE))>> 
    {
        let f = Fn!(|v: (Vec<V>, Vec<W>)| {
            let (vs, ws) = v;
            let combine = vs
                .into_iter()
                .flat_map(move |v| ws.clone().into_iter().map(move |w| (v.clone(), w)));
            Box::new(combine) as Box<dyn Iterator<Item = (V, W)>>
        });
        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        let fe = Fn!(move |v: Vec<(K, (V, W))>| {
            let (vx, vy): (Vec<K>, Vec<(V, W)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<V>, Vec<W>) = vy.into_iter().unzip();
            let ct_xy: Vec<(KE, VE)> = (fe0)(vx
                .clone()
                .into_iter()
                .zip(vy.into_iter())
                .collect::<Vec<_>>());
            let ct_xz: Vec<(KE, WE)> = (fe1)(vx
                .into_iter()
                .zip(vz.into_iter())
                .collect::<Vec<_>>());
            ct_xy.into_iter()
                .zip(ct_xz.into_iter())
                .map(|((ct_k, ct_v), (_, ct_w))| (ct_k, (ct_v, ct_w)))
                .collect::<Vec<_>>()
        });
        let fd = Fn!(move |v: Vec<(KE, (VE, WE))>| {
            let (vx, vy): (Vec<KE>, Vec<(VE, WE)>) = v.into_iter().unzip();
            let (vy, vz): (Vec<VE>, Vec<WE>) = vy.into_iter().unzip();
            let pt_xy: Vec<(K, V)> = (fd0)(vx
                .clone()
                .into_iter()
                .zip(vy.into_iter())
                .collect::<Vec<_>>());
            let pt_xz: Vec<(K, W)> = (fd1)(vx
                .into_iter()
                .zip(vz.into_iter())
                .collect::<Vec<_>>());
            pt_xy.into_iter()
                .zip(pt_xz.into_iter())
                .map(|((pt_k, pt_v), (_, pt_w))| (pt_k, (pt_v, pt_w)))
                .collect::<Vec<_>>()
        });
        self.cogroup(
            other,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        )
        .flat_map_values(Box::new(f), fe, fd)
    }

    fn cogroup<W: Data, WE: Data>(
        &self,
        other: SerArc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn RddE<Item = (K, (Vec<V>, Vec<W>)), ItemE = (KE, (Vec<VE>, Vec<WE>))>> 
    {
        let fe0 = self.get_fe();
        let fd0 = self.get_fd();
        let fe1 = other.get_fe();
        let fd1 = other.get_fd();
        let fe = Fn!(move |v: Vec<(K, (Vec<V>, Vec<W>))>| {
            let mut ct = Vec::with_capacity(v.len());
            for (x, (vy, vz)) in v {
                let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe0)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                let (_, ct_z): (Vec<KE>, Vec<WE>) = (fe1)(vz
                    .into_iter()
                    .map(|z| (x.clone(), z))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                ct.push((ct_x[0].clone(), (ct_y, ct_z)));
            } 
            ct
        });
        let fd = Fn!(move |v: Vec<(KE, (Vec<VE>, Vec<WE>))>| {
            let mut pt = Vec::with_capacity(v.len());
            for (x, (vy, vz)) in v {
                let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd0)(vy
                    .into_iter()
                    .map(|y| (x.clone(), y))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                let (_, pt_z): (Vec<K>, Vec<W>) = (fd1)(vz
                    .into_iter()
                    .map(|z| (x.clone(), z))
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                pt.push((pt_x[0].clone(), (pt_y, pt_z)));
            }
            pt
        });

        SerArc::new(CoGroupedRdd::new(self.get_rdde(), other.get_rdde(), fe, fd, partitioner))
    }

    fn partition_by_key<FE, FD>(&self, partitioner: Box<dyn Partitioner>, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = V, ItemE = VE>>
    where 
        FE: SerFunc(Vec<V>) -> Vec<VE>, 
        FD: SerFunc(Vec<VE>) -> Vec<V>, 
    {
        // Guarantee the number of partitions by introducing a shuffle phase
        let fe1 = self.get_fe();
        let fd1 = self.get_fd();

        let fe_wrapper_sf = Fn!(move |v: Vec<(K, Vec<V>)>| {
            let mut ct = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let len = vy.len();
                let mut vx = Vec::new();
                vx.resize(len, x);
                let (ct_x, ct_y): (Vec<KE>, Vec<VE>) = (fe1)(vx
                    .into_iter()
                    .zip(vy.into_iter())
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                ct.push((ct_x[0].clone(), ct_y));
            } 
            ct
        });
        let fd_wrapper_sf = Fn!(move |v: Vec<(KE, Vec<VE>)>| {
            let mut pt = Vec::with_capacity(v.len());
            for (x, vy) in v {
                let len = vy.len();
                let mut vx = Vec::new();
                vx.resize(len, x);  //TODO may need revision
                let (pt_x, pt_y): (Vec<K>, Vec<V>) = (fd1)(vx
                    .into_iter()
                    .zip(vy.into_iter())
                    .collect::<Vec<_>>())
                    .into_iter()
                    .unzip();
                pt.push((pt_x[0].clone(), pt_y));
            }
            pt
        });

        let shuffle_steep = ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(Aggregator::<K, V, _>::default()),
            partitioner,
            fe_wrapper_sf,
            fd_wrapper_sf,
        );
        // Flatten the results of the combined partitions
        let flattener = Fn!(|grouped: (K, Vec<V>)| {
            let (_key, values) = grouped;
            let iter: Box<dyn Iterator<Item = _>> = Box::new(values.into_iter());
            iter
        });
        shuffle_steep.flat_map(flattener, fe, fd)
    }
}

// Implementing the PairRdd trait for all types which implements Rdd
impl<K, V, KE, VE, T> PairRdd<K, V, KE, VE> for T
where
    T: RddE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data + Eq + Hash, 
    VE: Data,
{}

impl<K, V, KE, VE, T> PairRdd<K, V, KE, VE> for SerArc<T>
where
    T: RddE<Item = (K, V), ItemE = (KE, VE)>,
    K: Data + Eq + Hash, 
    V: Data, 
    KE: Data + Eq + Hash, 
    VE: Data,
{}


#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    f: F,
    fe: FE,
    fd: FD,
}
 
impl<K, V, U, KE, UE, F, FE, FD> Clone for MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            ecall_ids: self.ecall_ids.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        let ecall_ids = prev.get_ecall_ids();
        MappedValuesRdd {
            prev,
            vals,
            ecall_ids,
            f,
            fe,
            fd,
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> RddBase for MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
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
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_raw(&self, split: Box<dyn Split>, tx: Sender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        self.secure_compute(split, self.get_rdd_id(), tx, is_shuffle)
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside cogroup_iterator_any mapvaluesrdd");
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K, V, U, KE, UE, F, FE, FD> Rdd for MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev.iterator(split)?.map(move |(k, v)| (k, f(v))),
        ))
    }
    fn secure_compute(&self, split: Box<dyn Split>, id: usize, tx: Sender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        self.prev.secure_compute(split, id, tx, is_shuffle)
    }
}

impl<K, V, U, KE, UE, F, FE, FD> RddE for MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> U,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    f: F,
    fe: FE,
    fd: FD,
}

impl<K, V, U, KE, UE, F, FE, FD> Clone for FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            ecall_ids: self.ecall_ids.clone(),
            f: self.f.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> Vec<(KE, UE)> + Clone,
    FD: Func(Vec<(KE, UE)>) -> Vec<(K, U)> + Clone,
{
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        vals.dependencies
            .push(Dependency::NarrowDependency(Arc::new(
                OneToOneDependency::new(prev.get_rdd_base()),
            )));
        let vals = Arc::new(vals);
        let ecall_ids = prev.get_ecall_ids();
        FlatMappedValuesRdd {
            prev,
            vals,
            ecall_ids,
            f,
            fe,
            fd,
        }
    }
}

impl<K, V, U, KE, UE, F, FE, FD> RddBase for FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
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
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }
    
    fn iterator_raw(&self, split: Box<dyn Split>, tx: Sender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        self.secure_compute(split, self.get_rdd_id(), tx, is_shuffle)
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?
                .map(|(k, v)| Box::new((k, v)) as Box<dyn AnyData>),
        ))
    }
    fn cogroup_iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(self.iterator(split)?.map(|(k, v)| {
            Box::new((k, Box::new(v) as Box<dyn AnyData>)) as Box<dyn AnyData>
        })))
    }
}

impl<K, V, U, KE, UE, F, FE, FD> Rdd for FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type Item = (K, U);
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let f = self.f.clone();
        Ok(Box::new(
            self.prev
                .iterator(split)?
                .flat_map(move |(k, v)| f(v).map(move |x| (k.clone(), x))),
        ))
    }
    fn secure_compute(&self, split: Box<dyn Split>, id: usize, tx: Sender<usize>, is_shuffle: u8) -> Result<JoinHandle<()>> {
        self.prev.secure_compute(split, id, tx, is_shuffle)
    }
}

impl<K, V, U, KE, UE, F, FE, FD> RddE for FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: SerFunc(V) -> Box<dyn Iterator<Item = U>>,
    FE: SerFunc(Vec<(K, U)>) -> Vec<(KE, UE)>,
    FD: SerFunc(Vec<(KE, UE)>) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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