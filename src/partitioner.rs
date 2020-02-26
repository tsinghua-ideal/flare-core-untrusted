use crate::rdd::ItemE;
use crate::serializable_traits::Data;

use downcast_rs::Downcast;
use fasthash::MetroHasher;
use serde_derive::{Deserialize, Serialize};
use serde_traitobject::{Deserialize, Serialize};
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

/// Partitioner trait for creating Rdd partitions
pub trait Partitioner:
    Downcast + Send + Sync + dyn_clone::DynClone + Serialize + Deserialize
{
    fn equals(&self, other: &dyn Any) -> bool;
    fn get_num_of_partitions(&self) -> usize;
    fn get_partition(&self, key: &dyn Any) -> usize;
    fn get_range_bound_src(&self) -> Vec<ItemE>;
}

dyn_clone::clone_trait_object!(Partitioner);

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s: MetroHasher = Default::default();
    t.hash(&mut s);
    s.finish()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashPartitioner<K: Data + Hash + Eq> {
    partitions: usize,
    _marker: PhantomData<K>,
}

// Hash partitioner implementing naive hash function.
impl<K: Data + Hash + Eq> HashPartitioner<K> {
    pub fn new(partitions: usize) -> Self {
        HashPartitioner {
            partitions,
            _marker: PhantomData,
        }
    }
}

impl<K: Data + Hash + Eq> Partitioner for HashPartitioner<K> {
    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(hp) = other.downcast_ref::<HashPartitioner<K>>() {
            self.partitions == hp.partitions
        } else {
            false
        }
    }
    fn get_num_of_partitions(&self) -> usize {
        self.partitions
    }
    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();
        hash(key) as usize % self.partitions
    }
    fn get_range_bound_src(&self) -> Vec<ItemE> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangePartitioner<K: Data + Eq + Ord> {
    ascending: bool,
    partitions: usize,
    #[serde(with = "serde_traitobject")]
    range_bounds: Arc<Vec<K>>,
    samples_enc: Vec<ItemE>,
}

impl<K: Data + Eq + Ord> RangePartitioner<K> {
    pub fn new(
        partitions: usize,
        ascending: bool,
        mut samples: Vec<K>,
        samples_enc: Vec<ItemE>,
    ) -> Self {
        let mut range_bounds = Vec::new();

        if partitions > 1 && !samples.is_empty() {
            samples.sort();

            let step: f64 = samples.len() as f64 / (partitions - 1) as f64;
            let mut i: f64 = 0.0;

            for idx in 0..(partitions - 1) {
                range_bounds
                    .push(samples[std::cmp::min((i + step) as usize, samples.len() - 1)].clone());
                i += step;
            }
        }

        RangePartitioner {
            ascending,
            partitions,
            range_bounds: Arc::new(range_bounds),
            samples_enc,
        }
    }
}

impl<K: Data + Eq + Ord> Partitioner for RangePartitioner<K> {
    fn equals(&self, other: &dyn Any) -> bool {
        if let Some(rp) = other.downcast_ref::<RangePartitioner<K>>() {
            return self.partitions == rp.partitions
                && self.ascending == rp.ascending
                && self.range_bounds == rp.range_bounds
                && self.samples_enc == rp.samples_enc;
        } else {
            return false;
        }
    }

    fn get_num_of_partitions(&self) -> usize {
        self.partitions
    }

    fn get_partition(&self, key: &dyn Any) -> usize {
        let key = key.downcast_ref::<K>().unwrap();

        if self.partitions <= 1 {
            return 0;
        }

        return match self.range_bounds.binary_search(key) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
    }
    fn get_range_bound_src(&self) -> Vec<ItemE> {
        self.samples_enc.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hash_partition() {
        let data = vec![1, 2];
        let num_partition = 3;
        let hash_partitioner = HashPartitioner::<i32>::new(num_partition);
        for i in &data {
            println!("value: {:?}-hash: {:?}", i, hash(i));
            println!(
                "value: {:?}-index: {:?}",
                i,
                hash_partitioner.get_partition(i)
            );
        }
        let mut partition = vec![Vec::new(); num_partition];
        for i in &data {
            let index = hash_partitioner.get_partition(i);
            partition[index].push(i)
        }
        assert_eq!(partition.len(), 3)
    }

    #[test]
    fn hash_partitioner_eq() {
        let p1 = HashPartitioner::<i32>::new(1);
        let p2_1 = HashPartitioner::<i32>::new(2);
        let p2_2 = HashPartitioner::<i32>::new(2);
        assert!(p1.equals(&p1));
        assert!(p1.equals(&p1));
        assert!(p2_1.equals(&p2_1));
        assert!(p2_1.equals(&p2_2));
        assert!(p2_2.equals(&p2_1));
        assert!(!p1.equals(&p2_1));
        assert!(!p1.equals(&p2_2));
        let mut p1 = Some(p1);
        assert!(p1.clone().map(|p| (&p).equals(&p1.clone().unwrap())) == Some(true));
        assert!(p1.clone().map(|p| p.equals(&p2_1.clone())) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p2_2.clone())) == Some(false));
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) != None);
        assert!(p1
            .clone()
            .map_or(false, |p| (&p).equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1.clone())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_2.clone())));
        p1 = None;
        assert!(p1.clone().map(|p| p.equals(&p1.clone().unwrap())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_1.clone())) == None);
        assert!(p1.clone().map(|p| p.equals(&p2_2.clone())) == None);
        assert!(!p1
            .clone()
            .map_or(false, |p| (&p).equals(&p1.clone().unwrap())));
        assert!(!p1.clone().map_or(false, |p| p.equals(&p2_1.clone())));
        assert!(!p1.map_or(false, |p| p.equals(&p2_2.clone())));

        let p2_1 = Box::new(p2_1) as Box<dyn Partitioner>;
        let p2_2 = Box::new(p2_2) as Box<dyn Partitioner>;
        assert!(p2_1.equals((&*p2_2).as_any()))
    }
}
