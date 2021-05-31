use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use serde_derive::{Deserialize, Serialize};

use crate::env::RDDB_MAP;
use crate::rdd::CacheMeta;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum CachePutResponse {
    CachePutSuccess(usize),
    CachePutFailure,
}

type CacheMap = Arc<DashMap<((usize, usize), usize), (Vec<u8>, usize)>>;
type SecureCacheMap = Arc<DashMap<((usize, usize), usize), (usize, usize)>>; //(((key_space_id, cached_rdd_id), part_id), (encrypted_data, size))

// Despite the name, it is currently unbounded cache. Once done with LRU iterator, have to make this bounded.
// Since we are storing everything as serialized objects, size estimation is as simple as getting the length of byte vector
#[derive(Debug, Clone)]
pub(crate) struct BoundedMemoryCache {
    max_mbytes: usize,
    next_key_space_id: Arc<AtomicUsize>,
    current_bytes: usize,
    map: CacheMap,
    smap: SecureCacheMap,
}

// TODO: remove all hardcoded values
impl BoundedMemoryCache {
    pub fn new() -> Self {
        BoundedMemoryCache {
            max_mbytes: 2000, // in MB
            next_key_space_id: Arc::new(AtomicUsize::new(0)),
            current_bytes: 0,
            map: Arc::new(DashMap::new()),
            smap: Arc::new(DashMap::new()),
        }
    }

    fn new_key_space_id(&self) -> usize {
        self.next_key_space_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_key_space(&self) -> KeySpace {
        KeySpace::new(self, self.new_key_space_id())
    }

    fn get(&self, dataset_id: (usize, usize), partition: usize) -> Option<Vec<u8>> {
        self.map
            .get(&(dataset_id, partition))
            .map(|entry| entry.0.clone())
    }

    fn put(
        &self,
        dataset_id: (usize, usize),
        partition: usize,
        value: Vec<u8>,
    ) -> CachePutResponse {
        let key = (dataset_id, partition);
        // TODO: logging
        let size = value.len() * 8 + 2 * 8; //this number of MB
        if size as f64 / (1000.0 * 1000.0) > self.max_mbytes as f64 {
            CachePutResponse::CachePutFailure
        } else {
            // TODO: ensure free space needs to be done and this needs to be modified
            self.map.insert(key, (value, size));
            CachePutResponse::CachePutSuccess(size)
        }
    }

    pub fn scontain(&self, dataset_id: (usize, usize), part_id: usize) -> bool {
        self.smap.contains_key(&(dataset_id, part_id))
    }

    pub fn sget(&self, dataset_id: (usize, usize), part_id: usize) -> Option<usize> {
        self.smap
            .get(&(dataset_id, part_id))
            .map(|entry| entry.0.clone())
    }

    pub fn sput(
        &self,
        dataset_id: (usize, usize),
        part_id: usize,
        value_ptr: *mut u8,
        avoid_moving: usize, //0 for need, and >0 for size in case of no need
    ) -> CachePutResponse {
        let rdd_base = match RDDB_MAP.get_rddb(dataset_id.1) {
            Some(rdd_base) => rdd_base,
            None => return CachePutResponse::CachePutFailure,
        };
        let (value, size) = if avoid_moving == 0 {
            rdd_base.move_allocation(value_ptr)
        } else {
            (value_ptr, avoid_moving)
        };

        let key = (dataset_id, part_id);
        if size as f64 / (1000.0 * 1000.0) > self.max_mbytes as f64 {
            self.smap.remove(&key);
            CachePutResponse::CachePutFailure
        } else {
            // TODO: ensure free space needs to be done and this needs to be modified
            self.smap.insert(key, (value as usize, size));
            CachePutResponse::CachePutSuccess(size)
        }
    }

    //should be called in the end of program
    pub fn free_data_enc(&self) {
        for (key, value) in (*self.smap).clone() {
            let rdd_id = key.0.1;
            let rdd_base = match RDDB_MAP.get_rddb(rdd_id) {
                Some(rdd_base) => rdd_base,
                None => panic!("invalid cached rdd id"),
            };
            rdd_base.free_data_enc(value.0 as *mut u8);
        }
    }

    fn ensure_free_space(&self, _dataset_id: u64, _space: u64) -> bool {
        // TODO: logging
        todo!()
    }

    fn report_entry_dropped(_data_set_id: usize, _partition: usize, _entry: (Vec<u8>, usize)) {
        // TODO: loggging
        todo!()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KeySpace<'a> {
    pub cache: &'a BoundedMemoryCache,
    pub key_space_id: usize,
}

impl<'a> KeySpace<'a> {
    fn new(cache: &'a BoundedMemoryCache, key_space_id: usize) -> Self {
        KeySpace {
            cache,
            key_space_id,
        }
    }

    pub fn get(&self, dataset_id: usize, partition: usize) -> Option<Vec<u8>> {
        self.cache.get((self.key_space_id, dataset_id), partition)
    }
    pub fn put(&self, dataset_id: usize, partition: usize, value: Vec<u8>) -> CachePutResponse {
        self.cache
            .put((self.key_space_id, dataset_id), partition, value)
    }
    pub fn scontain(&self, rdd_id: usize, part_id: usize) -> bool {
        self.cache.scontain((self.key_space_id, rdd_id), part_id)
    }
    pub fn sget(&self, dataset_id: usize, part_id: usize) -> Option<usize> {
        self.cache.sget((self.key_space_id, dataset_id), part_id)
    }
    pub fn sput(&self, dataset_id: usize, part_id: usize, value: *mut u8, avoid_moving: usize) -> CachePutResponse {
        self.cache
            .sput((self.key_space_id, dataset_id), part_id, value, avoid_moving)
    }
    pub fn get_capacity(&self) -> usize {
        self.cache.max_mbytes
    }
}
