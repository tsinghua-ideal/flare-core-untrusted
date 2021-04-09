use std::hash::Hash;
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::JoinHandle;

use crate::aggregator::Aggregator;
use crate::context::Context;
use crate::dependency::{Dependency, OneToOneDependency};
use crate::env::{RDDB_MAP, Env};
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
    #[track_caller]
    fn combine_by_key<KE2: Data, C: Data, CE: Data, FE, FD>(
        &self,
        aggregator: Aggregator<K, V, C>,
        partitioner: Box<dyn Partitioner>,
        fe: FE,
        fd: FD,
    ) -> SerArc<dyn RddE<Item = (K, C), ItemE = (KE2, CE)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
        FE: SerFunc(Vec<(K, C)>) -> (KE2, CE), 
        FD: SerFunc((KE2, CE)) -> Vec<(K, C)>,
    {
        SerArc::new(ShuffledRdd::new(
            self.get_rdd(),
            Arc::new(aggregator),
            partitioner,
            fe,
            fd,
        ))
    }

    #[track_caller]
    fn group_by_key<CE, FE, FD>(&self, fe: FE, fd: FD, num_splits: usize) -> SerArc<dyn RddE<Item = (K, Vec<V>), ItemE = (KE, CE)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
        CE: Data,
        FE: SerFunc(Vec<(K, Vec<V>)>) -> (KE, CE),
        FD: SerFunc((KE, CE)) -> Vec<(K, Vec<V>)>,
    {
        self.group_by_key_using_partitioner(
            fe,
            fd,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>
        )
    }

    #[track_caller]
    fn group_by_key_using_partitioner<CE, FE, FD>(
        &self,
        fe: FE,
        fd: FD,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn RddE<Item = (K, Vec<V>), ItemE = (KE, CE)>>
    where
        Self: Sized + Serialize + Deserialize + 'static,
        CE: Data,
        FE: SerFunc(Vec<(K, Vec<V>)>) -> (KE, CE),
        FD: SerFunc((KE, CE)) -> Vec<(K, Vec<V>)>,
    {
        self.combine_by_key(Aggregator::<K, V, _>::default(), partitioner, fe, fd)
    }

    #[track_caller]
    fn reduce_by_key<KE2, VE2, F, FE, FD>(&self, func: F, num_splits: usize, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = (K, V), ItemE = (KE2, VE2)>>
    where
        KE2: Data,
        VE2: Data,
        F: SerFunc((V, V)) -> V,
        Self: Sized + Serialize + Deserialize + 'static,
        FE: SerFunc(Vec<(K, V)>) -> (KE2, VE2), 
        FD: SerFunc((KE2, VE2)) -> Vec<(K, V)>,        
    {
        self.reduce_by_key_using_partitioner(
            func,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
            fe,
            fd
        )
    }

    #[track_caller]
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
        FE: SerFunc(Vec<(K, V)>) -> (KE2, VE2), 
        FD: SerFunc((KE2, VE2)) -> Vec<(K, V)>,  
    {
        let create_combiner = Box::new(Fn!(|v: V| v));
        let f_clone = func.clone();
        let merge_value = Box::new(Fn!(move |(buf, v)| { (f_clone)((buf, v)) }));
        let merge_combiners = Box::new(Fn!(move |(b1, b2)| { (func)((b1, b2)) }));
        let aggregator = Aggregator::new(create_combiner, merge_value, merge_combiners);
        self.combine_by_key(aggregator, partitioner, fe, fd)
    }

    #[track_caller]
    fn values<FE, FD>(
        &self,
        fe: FE,  //the type corresponding to the encryption form is fixed
        fd: FD,
    ) -> SerArc<dyn RddE<Item = V, ItemE = VE>>
    where
        Self: Sized,
        FE: SerFunc(Vec<V>) -> VE, 
        FD: SerFunc(VE) -> Vec<V>,
    {
        SerArc::new(ValuesRdd::new(self.get_rdd(), fe, fd))
    }

    #[track_caller]
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
        FE: SerFunc(Vec<(K, U)>) -> (KE, UE), 
        FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
    {
        SerArc::new(MappedValuesRdd::new(self.get_rdd(), f, fe, fd))
    }

    #[track_caller]
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
        FE: SerFunc(Vec<(K, U)>) -> (KE, UE), 
        FD: SerFunc((KE, UE)) -> Vec<(K, U)>, 
    {
        SerArc::new(FlatMappedValuesRdd::new(self.get_rdd(), f, fe, fd))
    }

    #[track_caller]
    fn join<W, WE, FE, FD>(
        &self,
        other: SerArc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
        fe: FE,
        fd: FD,
        num_splits: usize,
    ) -> SerArc<dyn RddE<Item = (K, (V, W)), ItemE = (KE, (VE, WE))>> 
    where
        W: Data + Default,
        WE: Data + Default,
        FE: SerFunc(Vec<(K, (V, W))>) -> (KE, (VE, WE)), 
        FD: SerFunc((KE, (VE, WE))) -> Vec<(K, (V, W))>, 
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
        //temporarily built
        let fe_cg = Fn!(move |v: Vec<(K, (Vec<V>, Vec<W>))>| {
            let (k, vw): (Vec<K>, Vec<(Vec<V>, Vec<W>)>) = v.into_iter().unzip();
            let (v, w): (Vec<Vec<V>>, Vec<Vec<W>>) = vw.into_iter().unzip();
            let mut w_padding: Vec<W> = Vec::new();
            w_padding.resize_with(k.len(), Default::default);
            let (ct_x, _) = (fe1)(k.into_iter()
                .zip(w_padding.into_iter())
                .collect::<Vec<_>>()
            );
            (ct_x, (ser_encrypt(v), ser_encrypt(w)))
        });
        let fd_cg = Fn!(move |v: (KE, (Vec<u8>, Vec<u8>))| {
            let (ct_x, (ct_y, ct_z)) = v;
            let w_padding: WE = Default::default();
            let (x, _): (Vec<K>, Vec<W>)= (fd1)((ct_x, w_padding)).into_iter().unzip();
            let y = ser_decrypt(ct_y);
            let z = ser_decrypt(ct_z);
            x.into_iter()
                .zip(y.into_iter()
                    .zip(z.into_iter())
                ).collect::<Vec<_>>()
        });

        let cogrouped = self.cogroup(
            other,
            fe_cg,
            fd_cg,
            Box::new(HashPartitioner::<K>::new(num_splits)) as Box<dyn Partitioner>,
        );
        self.get_context().add_num(1);
        cogrouped.flat_map_values(Box::new(f), fe, fd)
    }

    #[track_caller]
    fn cogroup<W, WE, CE, DE, FE, FD>(
        &self,
        other: SerArc<dyn RddE<Item = (K, W), ItemE = (KE, WE)>>,
        fe: FE,
        fd: FD,
        partitioner: Box<dyn Partitioner>,
    ) -> SerArc<dyn RddE<Item = (K, (Vec<V>, Vec<W>)), ItemE = (KE, (CE, DE))>> 
    where
        W: Data,
        WE: Data,
        CE: Data,
        DE: Data,
        FE: SerFunc(Vec<(K, (Vec<V>, Vec<W>))>) -> (KE, (CE, DE)), 
        FD: SerFunc((KE, (CE, DE))) -> Vec<(K, (Vec<V>, Vec<W>))>, 
    {
        SerArc::new(CoGroupedRdd::new(self.get_rdde(), other.get_rdde(), fe, fd, partitioner))
    }

    #[track_caller]
    fn partition_by_key<FE, FD>(&self, partitioner: Box<dyn Partitioner>, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = V, ItemE = VE>>
    where 
        FE: SerFunc(Vec<V>) -> VE, 
        FD: SerFunc(VE) -> Vec<V>, 
    {
        // Guarantee the number of partitions by introducing a shuffle phase
        let fe_c = fe.clone();
        let fd_c = fd.clone();
        let fe_wrapper_sf = Fn!(move |v: Vec<(K, Vec<V>)>| {
            let (x, vy): (Vec<K>, Vec<Vec<V>>) = v.into_iter().unzip();
            let mut ct_y = Vec::with_capacity(vy.len());
            for y in vy {
                ct_y.push((fe_c)(y));
            }
            (x, ct_y)
        });
        let fd_wrapper_sf = Fn!(move |v: (Vec<K>, Vec<VE>)| {
            let (x, vy) = v;
            let mut pt_y = Vec::with_capacity(vy.len());
            for y in vy {
                pt_y.push((fd_c)(y));
            } 
            x.into_iter().zip(pt_y.into_iter()).collect::<Vec<_>>()
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
        self.get_context().add_num(1);
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
pub struct ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
    fe: FE,
    fd: FD,
}
 
impl<K, V, VE, FE, FD> Clone for ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{
    fn clone(&self) -> Self {
        ValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
            fe: self.fe.clone(),
            fd: self.fd.clone(),
        }
    }
}

impl<K, V, VE, FE, FD> ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: Func(Vec<V>) -> VE + Clone,
    FD: Func(VE) -> Vec<V> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        ValuesRdd {
            prev,
            vals,
            fe,
            fd,
        }
    }
}

impl<K, V, VE, FE, FD> RddBase for ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE + Clone,
    FD: SerFunc(VE) -> Vec<V> + Clone,
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
            Box::from_raw(ptr as *mut Vec<VE>)
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
        if !self.should_cache() {
            self.prev.get_op_ids(op_ids);
        }
    }
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::NarrowDependency(Arc::new(
            OneToOneDependency::new(self.prev.get_rdd_base()),
        ))]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<VE>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    default fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any flatmaprdd",);
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
    }
}

impl<K, V, VE, FE, FD> Rdd for ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE + Clone,
    FD: SerFunc(VE) -> Vec<V> + Clone,
{
    type Item = V;
    fn get_rdd_base(&self) -> Arc<dyn RddBase> {
        Arc::new(self.clone()) as Arc<dyn RddBase>
    }
    fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>> {
        Arc::new(self.clone())
    }
    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        Ok(Box::new(
            self.prev.iterator(split)?.map(|(k, v)| v)),
        )
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
                handles.append(&mut self.prev.secure_compute(split, acc_arg, tx)?);
            }
            Ok(handles)     
        } else {
            self.prev.secure_compute(split, acc_arg, tx)
        }
    }
}

impl<K, V, VE, FE, FD> RddE for ValuesRdd<K, V, VE, FE, FD>
where
    K: Data,
    V: Data,
    VE: Data,
    FE: SerFunc(Vec<V>) -> VE + Clone,
    FD: SerFunc(VE) -> Vec<V> + Clone,
{
    type ItemE = VE;
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


#[derive(Serialize, Deserialize)]
pub struct MappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> U + Clone,
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        MappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        MappedValuesRdd {
            prev,
            vals,
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
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
            Box::from_raw(ptr as *mut Vec<(KE, UE)>)
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
        if !self.should_cache() {
            self.prev.get_op_ids(op_ids);
        }
    }
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::NarrowDependency(Arc::new(
            OneToOneDependency::new(self.prev.get_rdd_base()),
        ))]
    }

    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<(KE, UE)>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }

    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any mapvaluesrdd");
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
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
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_rdd_id(cur_rdd_id);
        acc_arg.insert_op_id(cur_op_id);
        acc_arg.insert_split_num(cur_split_num);
        let captured_vars = self.f.get_ser_captured_var(); 
        if !captured_vars.is_empty() {
            Env::get().captured_vars
                .lock()
                .unwrap()
                .insert(cur_rdd_id, captured_vars);
        }
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
                handles.append(&mut self.prev.secure_compute(split, acc_arg, tx)?);
            }
            Ok(handles)     
        } else {
            self.prev.secure_compute(split, acc_arg, tx)
        }
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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

#[derive(Serialize, Deserialize)]
pub struct FlatMappedValuesRdd<K, V, U, KE, UE, F, FE, FD>
where
    K: Data,
    V: Data,
    U: Data,
    KE: Data,
    UE: Data,
    F: Func(V) -> Box<dyn Iterator<Item = U>> + Clone,
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    #[serde(with = "serde_traitobject")]
    prev: Arc<dyn Rdd<Item = (K, V)>>,
    vals: Arc<RddVals>,
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    fn clone(&self) -> Self {
        FlatMappedValuesRdd {
            prev: self.prev.clone(),
            vals: self.vals.clone(),
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
    FE: Func(Vec<(K, U)>) -> (KE, UE) + Clone,
    FD: Func((KE, UE)) -> Vec<(K, U)> + Clone,
{
    #[track_caller]
    fn new(prev: Arc<dyn Rdd<Item = (K, V)>>, f: F, fe: FE, fd: FD) -> Self {
        let mut vals = RddVals::new(prev.get_context(), prev.get_secure());
        let vals = Arc::new(vals);
        FlatMappedValuesRdd {
            prev,
            vals,
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
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
            Box::from_raw(ptr as *mut Vec<(KE, UE)>)
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
        if !self.should_cache() {
            self.prev.get_op_ids(op_ids);
        }
    }
    fn get_context(&self) -> Arc<Context> {
        self.vals.context.upgrade().unwrap()
    }
    fn get_dependencies(&self) -> Vec<Dependency> {
        vec![Dependency::NarrowDependency(Arc::new(
            OneToOneDependency::new(self.prev.get_rdd_base()),
        ))]
    }
    fn get_secure(&self) -> bool {
        self.vals.secure
    }

    fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
        // rdd_id is actually op_id
        let value = move_data::<(KE, UE)>(self.get_op_id(), value_ptr);
        let size = value.get_size();
        (Box::into_raw(value) as *mut u8, size)
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        self.prev.splits()
    }
    fn number_of_splits(&self) -> usize {
        self.prev.number_of_splits()
    }
    
    fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        self.secure_compute(split, acc_arg, tx)
    }

    // TODO: Analyze the possible error in invariance here
    fn iterator_any(
        &self,
        split: Box<dyn Split>,
    ) -> Result<Box<dyn AnyData>> {
        log::debug!("inside iterator_any flatmapvaluesrdd",);
        Ok(Box::new(
            self.iterator(split)?.collect::<Vec<_>>()
        ))
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
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
    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<(usize, (usize, (f64, f64)))>) -> Result<Vec<JoinHandle<()>>> {
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_rdd_id(cur_rdd_id);
        acc_arg.insert_op_id(cur_op_id);
        acc_arg.insert_split_num(cur_split_num);
        let captured_vars = self.f.get_ser_captured_var(); 
        if !captured_vars.is_empty() {
            Env::get().captured_vars
                .lock()
                .unwrap()
                .insert(cur_rdd_id, captured_vars);
        }
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
                handles.append(&mut self.prev.secure_compute(split, acc_arg, tx)?);
            }
            Ok(handles)     
        } else {
            self.prev.secure_compute(split, acc_arg, tx)
        }
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
    FE: SerFunc(Vec<(K, U)>) -> (KE, UE),
    FD: SerFunc((KE, UE)) -> Vec<(K, U)>,
{
    type ItemE = (KE, UE);
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