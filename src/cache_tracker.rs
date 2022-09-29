use std::collections::LinkedList;
use std::collections::{BTreeSet, HashMap};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::cache::{BoundedMemoryCache, CachePutResponse, KeySpace};
use crate::env;
use crate::rdd::Rdd;
use crate::serializable_traits::Data;
use crate::serialized_data_capnp::serialized_data;
use crate::split::Split;
use crate::{Error, NetworkError, Result};
use capnp::message::ReaderOptions;
use capnp_futures::serialize as capnp_serialize;
use dashmap::{DashMap, DashSet};
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: None,
    nesting_limit: 64,
};

/// Cache tracker works by creating a server in master node and slave nodes acting as clients.
#[derive(Serialize, Deserialize)]
pub(crate) enum CacheTrackerMessage {
    AddedToCache {
        rdd_id: usize,
        partition: usize,
        host: Ipv4Addr,
        size: usize,
    },
    DroppedFromCache {
        rdd_id: usize,
        partition: usize,
        host: Ipv4Addr,
        size: usize,
    },
    MemoryCacheLost {
        host: Ipv4Addr,
    },
    RegisterRdd {
        rdd_id: usize,
        num_partitions: usize,
    },
    SlaveCacheStarted {
        host: Ipv4Addr,
        size: usize,
    },
    GetCacheStatus,
    GetCacheLocations,
    StopCacheTracker,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum CacheTrackerMessageReply {
    CacheLocations(HashMap<usize, Vec<LinkedList<Ipv4Addr>>>),
    CacheStatus(Vec<(Ipv4Addr, usize, usize)>),
    Ok,
}

#[derive(Debug)]
pub(crate) struct CacheTracker {
    is_master: bool,
    locs: DashMap<usize, Vec<LinkedList<Ipv4Addr>>>, //(rdd, partition) -> LinkedList
    slave_capacity: DashMap<Ipv4Addr, usize>,
    slave_usage: DashMap<Ipv4Addr, usize>,
    registered_rdd_ids: DashSet<usize>,
    loading: DashSet<(usize, usize)>,  // (rdd, partition)
    sloading: DashSet<(usize, usize)>, // (cached_rdd_id, part_id)
    cache: KeySpace<'static>,
    master_addr: SocketAddr,
}

impl CacheTracker {
    pub fn new(
        is_master: bool,
        master_addr: SocketAddr,
        local_ip: Ipv4Addr,
        the_cache: &'static BoundedMemoryCache,
    ) -> Result<Arc<Self>> {
        let cache = Arc::new(CacheTracker {
            is_master,
            locs: DashMap::new(),
            slave_capacity: DashMap::new(),
            slave_usage: DashMap::new(),
            registered_rdd_ids: DashSet::new(),
            loading: DashSet::new(),
            sloading: DashSet::new(),
            cache: the_cache.new_key_space(),
            master_addr: SocketAddr::new(master_addr.ip(), master_addr.port() + 1),
        });
        cache.clone().server();
        let cachec = cache.clone();
        let fut = async move {
            let size = cachec.cache.get_capacity();
            cachec
                .client(CacheTrackerMessage::SlaveCacheStarted {
                    host: local_ip,
                    size,
                })
                .await
        };
        tokio::task::LocalSet::new().spawn_local(fut);
        Ok(cache)
    }

    // Slave node will ask master node for cache locs
    async fn client(&self, message: CacheTrackerMessage) -> Result<CacheTrackerMessageReply> {
        use futures::io::AllowStdIo;
        use std::net::TcpStream;

        let mut stream = loop {
            match TcpStream::connect(self.master_addr) {
                Ok(stream) => break stream,
                Err(_) => continue,
            }
        };
        let mut stream = AllowStdIo::new(&mut stream);

        {
            // Due to current limitations w/ capnp_futures not being Send we use the normal
            // sync Write version here to avoid spawning a second thread to run the future to completion
            let shuffle_id_bytes = bincode::serialize(&message)?;
            let mut message = capnp::message::Builder::new_default();
            let mut shuffle_data = message.init_root::<serialized_data::Builder>();
            shuffle_data.set_msg(&shuffle_id_bytes);
            capnp::serialize::write_message(&mut stream, &message).map_err(Error::OutputWrite)?;
        }

        let message_reader = capnp_serialize::read_message(stream, CAPNP_BUF_READ_OPTS).await?;
        let shuffle_data = message_reader.get_root::<serialized_data::Reader>()?;
        let reply: CacheTrackerMessageReply = bincode::deserialize(&shuffle_data.get_msg()?)?;
        Ok(reply)
    }

    /// Only will be started in master node and will serve all the slave nodes.
    fn server(self: Arc<Self>) {
        if !self.is_master {
            return;
        }
        log::debug!("cache tracker server starting");
        tokio::spawn(async move {
            let mut listener = TcpListener::bind(self.master_addr)
                .await
                .map_err(NetworkError::TcpListener)?;
            log::debug!("cache tracker server started");
            while let Ok((mut stream, _)) = listener.accept().await {
                let selfc = Arc::clone(&self);
                tokio::spawn(async move {
                    let (reader, writer) = stream.split();
                    let reader = reader.compat();
                    let writer = writer.compat_write();

                    //reading
                    let message = {
                        let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS).await?;
                        let data = message_reader.get_root::<serialized_data::Reader>()?;
                        bincode::deserialize::<CacheTrackerMessage>(data.get_msg()?)?
                    };

                    // send reply
                    let reply = selfc.process_message(message);
                    let result = bincode::serialize(&reply)?;
                    let message = {
                        let mut message = capnp::message::Builder::new_default();
                        let mut locs_data = message.init_root::<serialized_data::Builder>();
                        locs_data.set_msg(&result);
                        message
                    };

                    capnp_serialize::write_message(writer, message).await.unwrap();

                    Ok::<_, Error>(())
                });
            }
            Err::<(), _>(Error::ExecutorShutdown)
        });
    }

    fn process_message(self: Arc<Self>, message: CacheTrackerMessage) -> CacheTrackerMessageReply {
        // TODO: logging
        match message {
            CacheTrackerMessage::SlaveCacheStarted { host, size } => {
                self.slave_capacity.insert(host, size);
                self.slave_usage.insert(host, 0);
                CacheTrackerMessageReply::Ok
            }
            CacheTrackerMessage::RegisterRdd {
                rdd_id,
                num_partitions,
            } => {
                self.locs.insert(
                    rdd_id,
                    (0..num_partitions).map(|_| LinkedList::new()).collect(),
                );
                CacheTrackerMessageReply::Ok
            }
            CacheTrackerMessage::AddedToCache {
                rdd_id,
                partition,
                host,
                size,
            } => {
                if size > 0 {
                    self.slave_usage
                        .insert(host, self.get_cache_usage(host) + size);
                } else {
                    // TODO: logging
                }
                if let Some(mut locs_rdd) = self.locs.get_mut(&rdd_id) {
                    if let Some(locs_rdd_p) = locs_rdd.get_mut(partition) {
                        locs_rdd_p.push_front(host);
                    }
                }
                CacheTrackerMessageReply::Ok
            }
            CacheTrackerMessage::DroppedFromCache {
                rdd_id,
                partition,
                host,
                size,
            } => {
                if size > 0 {
                    let remaining = self.get_cache_usage(host) - size;
                    self.slave_usage.insert(host, remaining);
                }
                let remaining_locs = self
                    .locs
                    .get(&rdd_id)
                    .unwrap()
                    .get(partition)
                    .unwrap()
                    .iter()
                    .filter(|x| *x == &host)
                    .copied()
                    .collect();
                if let Some(mut locs_r) = self.locs.get_mut(&rdd_id) {
                    if let Some(locs_p) = locs_r.get_mut(partition) {
                        *locs_p = remaining_locs;
                    }
                }
                CacheTrackerMessageReply::Ok
            }
            // TODO: memory cache lost needs to be implemented
            CacheTrackerMessage::GetCacheLocations => {
                let locs_clone = self
                    .locs
                    .iter()
                    .map(|kv| {
                        let (k, v) = (kv.key(), kv.value());
                        (*k, v.clone())
                    })
                    .collect();
                CacheTrackerMessageReply::CacheLocations(locs_clone)
            }
            CacheTrackerMessage::GetCacheStatus => {
                let status = self
                    .slave_capacity
                    .iter()
                    .map(|kv| {
                        let (host, capacity) = (kv.key(), kv.value());
                        (*host, *capacity, self.get_cache_usage(*host))
                    })
                    .collect();
                CacheTrackerMessageReply::CacheStatus(status)
            }
            _ => CacheTrackerMessageReply::Ok,
        }
    }

    fn get_cache_usage(self: &Arc<Self>, host: Ipv4Addr) -> usize {
        match self.slave_usage.get(&host) {
            Some(s) => *s,
            None => 0,
        }
    }

    fn get_cache_capacity(slave_capacity: Arc<DashMap<Ipv4Addr, usize>>, host: Ipv4Addr) -> usize {
        match slave_capacity.get(&host) {
            Some(s) => *s,
            None => 0,
        }
    }

    pub async fn register_rdd(&self, rdd_id: usize, num_partitions: usize) -> Result<()> {
        if !self.registered_rdd_ids.contains(&rdd_id) {
            // TODO: logging
            self.registered_rdd_ids.insert(rdd_id);
            self.client(CacheTrackerMessage::RegisterRdd {
                rdd_id,
                num_partitions,
            })
            .await?;
        }
        Ok(())
    }

    pub async fn get_location_snapshot(&self) -> Result<HashMap<usize, Vec<Vec<Ipv4Addr>>>> {
        match self.client(CacheTrackerMessage::GetCacheLocations).await {
            Ok(CacheTrackerMessageReply::CacheLocations(s)) => Ok(s
                .into_iter()
                .map(|(k, v)| {
                    let v = v
                        .into_iter()
                        .map(|x| x.into_iter().map(|x| x).collect())
                        .collect();
                    (k, v)
                })
                .collect()),
            Ok(_) => Err(Error::Other),
            Err(err) => Err(err),
        }
    }

    async fn get_cache_status(&self) -> Result<Vec<(Ipv4Addr, usize, usize)>> {
        match self.client(CacheTrackerMessage::GetCacheStatus).await {
            Ok(CacheTrackerMessageReply::CacheStatus(s)) => Ok(s),
            Ok(_) => Err(Error::Other),
            Err(err) => Err(err),
        }
    }

    pub fn scontain(&self, key: (usize, usize)) -> bool {
        self.cache.scontain(key.0, key.1)
    }

    pub fn get_sdata(&self, key: (usize, usize)) -> Option<(usize, usize)> {
        let (rdd_id, part_id) = key;
        self.cache.sget(rdd_id, part_id)
    }

    pub fn put_sdata(&self, key: (usize, usize), value: (usize, usize), avoid_moving: usize) {
        let (rdd_id, part_id) = key;
        let put_response = self.cache.sput(rdd_id, part_id, value, avoid_moving);
        if let CachePutResponse::CachePutSuccess(size) = put_response {
            futures::executor::block_on(self.client(CacheTrackerMessage::AddedToCache {
                rdd_id: rdd_id,
                partition: part_id,
                host: env::Configuration::get().local_ip,
                size,
            }))
            .unwrap();
        }
    }

    //support local mode only
    pub fn get_or_compute<T: Data>(
        &self,
        rdd: Arc<dyn Rdd<Item = T>>,
        split: Box<dyn Split>,
    ) -> Box<dyn Iterator<Item = T>> {
        if let Some(cached_val) = self.cache.get(rdd.get_rdd_id(), split.get_index()) {
            let res: Vec<T> = bincode::deserialize(&cached_val).unwrap();
            Box::new(res.into_iter())
        } else {
            let key = (rdd.get_rdd_id(), split.get_index());
            while self.loading.contains(&key) {
                let dur = Duration::from_millis(1);
                thread::sleep(dur);
            }
            if let Some(cached_val) = self.cache.get(rdd.get_rdd_id(), split.get_index()) {
                let res: Vec<T> = bincode::deserialize(&cached_val).unwrap();
                return Box::new(res.into_iter());
            }
            self.loading.insert(key);

            let res: Vec<_> = rdd.compute(split.clone()).unwrap().collect();
            let res_bytes = bincode::serialize(&res).unwrap();
            let put_response = self
                .cache
                .put(rdd.get_rdd_id(), split.get_index(), res_bytes);
            self.loading.remove(&key);

            if let CachePutResponse::CachePutSuccess(size) = put_response {
                futures::executor::block_on(self.client(CacheTrackerMessage::AddedToCache {
                    rdd_id: rdd.get_rdd_id(),
                    partition: split.get_index(),
                    host: env::Configuration::get().local_ip,
                    size,
                }))
                .unwrap();
            }
            Box::new(res.into_iter())
        }
    }

    // TODO: drop_entry needs to be implemented
}
