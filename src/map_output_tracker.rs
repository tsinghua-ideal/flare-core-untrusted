use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use crate::serialized_data_capnp::serialized_data;
use crate::{Error, NetworkError, Result};
use capnp::message::{Builder as MsgBuilder, ReaderOptions};
use capnp_futures::serialize as capnp_serialize;
use dashmap::{DashMap, DashSet};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Barrier,
};
use tokio_stream::StreamExt;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

const CAPNP_BUF_READ_OPTS: ReaderOptions = ReaderOptions {
    traversal_limit_in_words: None,
    nesting_limit: 64,
};

#[derive(Clone, Debug)]
pub(crate) enum GetServerUriReq {
    // Contains shuffle_id
    PrevStage(usize),
    // Contains (stage_id, part_id_offset, num_splits)
    CurStage((usize, usize, usize)),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum MapOutputTrackerMessage {
    // Contains shuffle_id
    GetMapOutputLocations(usize),
    // Contains stage_id, not the part group, because the workers will cache the server list
    GetExecutorLocations(usize),
    // Contains part_group, cnt,
    GetMaxCnt((usize, usize, usize), usize),
    // Contains part_group
    CheckStepReady((usize, usize, usize)),
    StopMapOutputTracker,
}

/// The key is the shuffle_id or stage id
pub type ServerUris = Arc<DashMap<usize, Vec<Option<String>>>>;

// Starts the server in master node and client in slave nodes. Similar to cache tracker.
#[derive(Clone, Debug)]
pub(crate) struct MapOutputTracker {
    is_master: bool,
    // shuffle_id -> hosts indexed by partition id
    pub server_uris: ServerUris,
    fetching: Arc<DashSet<usize>>,
    fetching_for_sort: Arc<DashSet<usize>>,
    generation: Arc<Mutex<i64>>,
    master_addr: SocketAddr,
    //(part_group) -> (barrier, num_executors). If the worker is ready for step if the corresponding uri is contained
    step_map: Arc<Mutex<HashMap<(usize, usize, usize), (Arc<Barrier>, usize)>>>,
    //(part_group) -> (cnt, barrier, num_executors)
    cnt_map: Arc<Mutex<HashMap<(usize, usize, usize), (usize, Arc<Barrier>, usize)>>>,
    // stage_id -> hosts indexed by partition id
    executor_map: ServerUris,
}

// Only master_addr doesn't have a default.
impl Default for MapOutputTracker {
    fn default() -> Self {
        MapOutputTracker {
            is_master: Default::default(),
            server_uris: Default::default(),
            fetching: Default::default(),
            fetching_for_sort: Default::default(),
            generation: Default::default(),
            master_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            step_map: Default::default(),
            cnt_map: Default::default(),
            executor_map: Default::default(),
        }
    }
}

impl MapOutputTracker {
    pub fn new(is_master: bool, master_addr: SocketAddr) -> Self {
        let output_tracker = MapOutputTracker {
            is_master,
            server_uris: Arc::new(DashMap::new()),
            fetching: Arc::new(DashSet::new()),
            fetching_for_sort: Arc::new(DashSet::new()),
            generation: Arc::new(Mutex::new(0)),
            master_addr,
            step_map: Arc::new(Mutex::new(HashMap::new())),
            cnt_map: Arc::new(Mutex::new(HashMap::new())),
            executor_map: Arc::new(DashMap::new()),
        };
        output_tracker.server();
        output_tracker
    }

    async fn client_get_uris(&self, req: &GetServerUriReq) -> Result<Vec<String>> {
        let mut stream = loop {
            match TcpStream::connect(self.master_addr).await {
                Ok(stream) => break stream,
                Err(_) => continue,
            }
        };
        let (reader, writer) = stream.split();
        let reader = reader.compat();
        let mut writer = writer.compat_write();

        let bytes = match req {
            GetServerUriReq::CurStage(part_group) => {
                log::debug!(
                    "connected to master to fetch sort task #{:?} data hosts",
                    part_group
                );
                bincode::serialize(&MapOutputTrackerMessage::GetExecutorLocations(part_group.0))?
            }
            GetServerUriReq::PrevStage(shuffle_id) => {
                log::debug!(
                    "connected to master to fetch shuffle task #{} data hosts",
                    shuffle_id
                );
                bincode::serialize(&MapOutputTrackerMessage::GetMapOutputLocations(*shuffle_id))?
            }
        };

        let mut message = MsgBuilder::new_default();
        let mut data = message.init_root::<serialized_data::Builder>();
        data.set_msg(&bytes);
        capnp_serialize::write_message(&mut writer, &message).await?;
        let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS).await?;
        let data = message_reader.get_root::<serialized_data::Reader>()?;
        let locs: Vec<String> = bincode::deserialize(&data.get_msg()?)?;
        Ok(locs)
    }

    async fn client_check_ready(&self, part_group: (usize, usize, usize)) -> Result<()> {
        let mut stream = loop {
            match TcpStream::connect(self.master_addr).await {
                Ok(stream) => break stream,
                Err(_) => continue,
            }
        };
        let (reader, writer) = stream.split();
        let reader = reader.compat();
        let mut writer = writer.compat_write();
        log::debug!("connected to master to sync at part_group {:?}", part_group);
        let bytes = bincode::serialize(&MapOutputTrackerMessage::CheckStepReady(part_group))?;
        let mut message = MsgBuilder::new_default();
        let mut data = message.init_root::<serialized_data::Builder>();
        data.set_msg(&bytes);
        capnp_serialize::write_message(&mut writer, &message).await?;
        let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS).await?;
        let res = message_reader.get_root::<serialized_data::Reader>()?;
        assert!(bincode::deserialize::<bool>(&res.get_msg()?)?);
        Ok(())
    }

    async fn client_get_max_cnt(
        &self,
        part_group: (usize, usize, usize),
        cnt: usize,
    ) -> Result<usize> {
        let mut stream = loop {
            match TcpStream::connect(self.master_addr).await {
                Ok(stream) => break stream,
                Err(_) => continue,
            }
        };
        let (reader, writer) = stream.split();
        let reader = reader.compat();
        let mut writer = writer.compat_write();
        log::debug!(
            "connected to master to fetch cnt at part group #{:?}",
            part_group
        );

        let bytes = bincode::serialize(&MapOutputTrackerMessage::GetMaxCnt(part_group, cnt))?;
        let mut message = MsgBuilder::new_default();
        let mut data = message.init_root::<serialized_data::Builder>();
        data.set_msg(&bytes);
        capnp_serialize::write_message(&mut writer, &message).await?;
        let message_reader = capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS).await?;
        let data = message_reader.get_root::<serialized_data::Reader>()?;
        let cnt: usize = bincode::deserialize(&data.get_msg()?)?;
        Ok(cnt)
    }

    fn server(&self) {
        if !self.is_master {
            return;
        }
        log::debug!("map output tracker server starting");
        let master_addr = self.master_addr;
        let server_uris = self.server_uris.clone();
        let step_map = self.step_map.clone();
        let cnt_map = self.cnt_map.clone();
        let executor_map = self.executor_map.clone();
        tokio::spawn(async move {
            let mut listener = TcpListener::bind(master_addr)
                .await
                .map_err(NetworkError::TcpListener)?;
            log::debug!("map output tracker server started");
            while let Ok((mut stream, _)) = listener.accept().await {
                let server_uris_clone = server_uris.clone();
                let step_map_clone = step_map.clone();
                let cnt_map_clone = cnt_map.clone();
                let executor_map_clone = executor_map.clone();
                tokio::spawn(async move {
                    let (reader, writer) = stream.split();
                    let reader = reader.compat();
                    let writer = writer.compat_write();

                    // reading
                    let message = {
                        let message_reader =
                            capnp_serialize::read_message(reader, CAPNP_BUF_READ_OPTS).await?;
                        let data = message_reader.get_root::<serialized_data::Reader>()?;
                        bincode::deserialize(data.get_msg()?)?
                    };

                    let result = match message {
                        MapOutputTrackerMessage::GetMapOutputLocations(shuffle_id) => {
                            while server_uris_clone
                                .get(&shuffle_id)
                                .ok_or_else(|| MapOutputError::ShuffleIdNotFound(shuffle_id))?
                                .iter()
                                .filter(|x| !x.is_none())
                                .count()
                                == 0
                            {
                                //check whether this will hurt the performance or not
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            let locs = server_uris_clone
                                .get(&shuffle_id)
                                .map(|kv| {
                                    kv.value()
                                        .iter()
                                        .cloned()
                                        .map(|x| x.unwrap())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            log::debug!(
                                "locs inside map output tracker server for shuffle id #{}: {:?}",
                                shuffle_id,
                                locs
                            );

                            // writting response
                            bincode::serialize(&locs)?
                        }
                        MapOutputTrackerMessage::GetExecutorLocations(stage_id) => {
                            while executor_map_clone
                                .get(&stage_id)
                                .ok_or_else(|| MapOutputError::StageIdNotFound(stage_id))?
                                .iter()
                                .filter(|x| !x.is_none())
                                .count()
                                == 0
                            {
                                //check whether this will hurt the performance or not
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                            let locs = executor_map_clone
                                .get(&stage_id)
                                .map(|kv| {
                                    kv.value()
                                        .iter()
                                        .cloned()
                                        .map(|x| x.unwrap())
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default();
                            log::debug!(
                                "locs inside map output tracker server for stage id #{}: {:?}",
                                stage_id,
                                locs
                            );

                            // writting response
                            bincode::serialize(&locs)?
                        }
                        MapOutputTrackerMessage::GetMaxCnt(part_group, cnt_per_partition) => {
                            let num_of_executors = part_group.2;
                            assert!(num_of_executors > 0);

                            let barrier = {
                                let mut unlocked_cnt_map = cnt_map_clone.lock();
                                let (item, barrier, _) =
                                    unlocked_cnt_map.entry(part_group).or_insert((
                                        cnt_per_partition,
                                        Arc::new(Barrier::new(num_of_executors)),
                                        num_of_executors,
                                    ));
                                *item = std::cmp::max(*item, cnt_per_partition);
                                barrier.clone()
                            };

                            //send back
                            log::debug!(
                                "(part_group) wait for the |get max cnt| point ({:?})",
                                part_group,
                            );
                            barrier.wait().await;
                            log::debug!(
                                "(part_group) pass the |get max cnt| point ({:?})",
                                part_group
                            );

                            let (cnt, should_remove) = {
                                let mut unlocked_cnt_map = cnt_map_clone.lock();
                                let (item, _, remain) =
                                    unlocked_cnt_map.get_mut(&part_group).unwrap();
                                *remain -= 1;
                                (*item, *remain == 0)
                            };

                            if should_remove {
                                cnt_map_clone.lock().remove(&part_group).unwrap();
                            }

                            bincode::serialize(&cnt)?
                        }
                        MapOutputTrackerMessage::CheckStepReady(part_group) => {
                            let num_of_executors = part_group.2;
                            assert!(num_of_executors > 0);
                            //insert the value and release the lock immediately

                            let barrier = {
                                let mut unlocked_step_map = step_map_clone.lock();
                                let (barrier, _) = unlocked_step_map.entry(part_group).or_insert((
                                    Arc::new(Barrier::new(num_of_executors)),
                                    num_of_executors,
                                ));
                                barrier.clone()
                            };

                            log::debug!(
                                "(part_group) wait for the |check ready| point {:?}",
                                part_group
                            );
                            barrier.wait().await;
                            log::debug!(
                                "(part_group) pass the |check ready| point {:?}",
                                part_group
                            );

                            let should_remove = {
                                let mut unlocked_step_map = step_map_clone.lock();
                                let (_, remain) = unlocked_step_map.get_mut(&part_group).unwrap();
                                *remain -= 1;
                                *remain == 0
                            };

                            if should_remove {
                                step_map_clone.lock().remove(&part_group).unwrap();
                            }

                            //check
                            bincode::serialize(&true)?
                        }
                        MapOutputTrackerMessage::StopMapOutputTracker => unimplemented!(),
                    };
                    let mut message = MsgBuilder::new_default();
                    let mut data = message.init_root::<serialized_data::Builder>();
                    data.set_msg(&result);
                    // TODO: remove blocking call when possible
                    futures::executor::block_on(async {
                        capnp_futures::serialize::write_message(writer, message)
                            .await
                            .map_err(Error::CapnpDeserialization)?;
                        Ok::<_, Error>(())
                    })?;
                    Ok::<_, Error>(())
                });
            }
            Err::<(), _>(Error::ExecutorShutdown)
        });
    }

    pub fn register_shuffle(&self, shuffle_id: usize, num_maps: usize) {
        log::debug!("inside register shuffle");
        if self.server_uris.get(&shuffle_id).is_some() {
            // TODO: error handling
            log::debug!("map tracker register shuffle none");
            return;
        }
        self.server_uris.insert(shuffle_id, vec![None; num_maps]);
        log::debug!("server_uris after register_shuffle {:?}", self.server_uris);
    }

    pub fn register_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        log::debug!(
            "registering map output from shuffle task #{} with map id #{} at server: {}",
            shuffle_id,
            map_id,
            server_uri
        );
        self.server_uris.get_mut(&shuffle_id).unwrap()[map_id] = Some(server_uri);
    }

    pub fn register_map_outputs(&self, shuffle_id: usize, locs: Vec<Option<String>>) {
        log::debug!(
            "registering map outputs inside map output tracker for shuffle id #{}: {:?}",
            shuffle_id,
            locs
        );
        self.server_uris.insert(shuffle_id, locs);
    }

    pub fn unregister_map_output(&self, shuffle_id: usize, map_id: usize, server_uri: String) {
        let array = self.server_uris.get(&shuffle_id);
        if let Some(arr) = array {
            if arr.get(map_id).unwrap() == &Some(server_uri) {
                self.server_uris
                    .get_mut(&shuffle_id)
                    .unwrap()
                    .insert(map_id, None)
            }
            self.increment_generation();
        } else {
            // TODO: error logging
        }
    }

    pub fn alloc_executor_map_entry(&self, stage_id: usize, num_partitions: usize) {
        self.executor_map
            .entry(stage_id)
            .or_insert(vec![None; num_partitions]);
    }

    pub fn register_executor(&self, stage_id: usize, partition: usize, host: String) {
        let mut hosts = self.executor_map.get_mut(&stage_id).unwrap();
        assert_eq!(hosts[partition], None);
        hosts[partition] = Some(host);
    }

    pub fn unregister_executor(&self, stage_id: usize, partition: usize) -> String {
        let mut hosts = self.executor_map.get_mut(&stage_id).unwrap();
        let host = hosts[partition].take();
        host.unwrap()
    }

    pub async fn get_server_uris(&self, req: &GetServerUriReq) -> Result<Vec<String>> {
        match req {
            GetServerUriReq::CurStage((stage_id, _, _)) => {
                log::debug!(
                    "trying to get uri for stage #{}, current executors: {:?}",
                    stage_id,
                    self.executor_map
                );
                if self
                    .executor_map
                    .get(stage_id)
                    .map(|some| some.iter().filter_map(|x| x.clone()).next())
                    .flatten()
                    .is_none()
                {
                    if self.fetching_for_sort.contains(stage_id) {
                        while self.fetching_for_sort.contains(stage_id) {
                            // TODO: check whether this will hurt the performance or not
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        let servers = self
                            .executor_map
                            .get(stage_id)
                            .ok_or_else(|| MapOutputError::StageIdNotFound(*stage_id))?
                            .iter()
                            .filter(|x| !x.is_none())
                            .map(|x| x.clone().unwrap())
                            .collect::<Vec<_>>();
                        log::debug!("returning after fetching done, return: {:?}", servers);
                        return Ok(servers);
                    } else {
                        log::debug!("adding to fetching queue");
                        self.fetching_for_sort.insert(*stage_id);
                    }
                    let fetched = self.client_get_uris(req).await?;
                    log::debug!("fetched locs from client: {:?}", fetched);
                    self.executor_map
                        .insert(*stage_id, fetched.iter().map(|x| Some(x.clone())).collect());
                    log::debug!("added locs to executor map after fetching");
                    self.fetching_for_sort.remove(stage_id);
                    Ok(fetched)
                } else {
                    Ok(self
                        .executor_map
                        .get(stage_id)
                        .ok_or_else(|| MapOutputError::StageIdNotFound(*stage_id))?
                        .iter()
                        .filter(|x| !x.is_none())
                        .map(|x| x.clone().unwrap())
                        .collect())
                }
            }
            GetServerUriReq::PrevStage(shuffle_id) => {
                log::debug!(
                    "trying to get uri for shuffle task #{}, current server uris: {:?}",
                    shuffle_id,
                    self.server_uris
                );
                if self
                    .server_uris
                    .get(shuffle_id)
                    .map(|some| some.iter().filter_map(|x| x.clone()).next())
                    .flatten()
                    .is_none()
                {
                    if self.fetching.contains(shuffle_id) {
                        while self.fetching.contains(shuffle_id) {
                            // TODO: check whether this will hurt the performance or not
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        let servers = self
                            .server_uris
                            .get(shuffle_id)
                            .ok_or_else(|| MapOutputError::ShuffleIdNotFound(*shuffle_id))?
                            .iter()
                            .filter(|x| !x.is_none())
                            .map(|x| x.clone().unwrap())
                            .collect::<Vec<_>>();
                        log::debug!("returning after fetching done, return: {:?}", servers);
                        return Ok(servers);
                    } else {
                        log::debug!("adding to fetching queue");
                        self.fetching.insert(*shuffle_id);
                    }
                    let fetched = self.client_get_uris(req).await?;
                    log::debug!("fetched locs from client: {:?}", fetched);
                    self.server_uris.insert(
                        *shuffle_id,
                        fetched.iter().map(|x| Some(x.clone())).collect(),
                    );
                    log::debug!("added locs to server uris after fetching");
                    self.fetching.remove(shuffle_id);
                    Ok(fetched)
                } else {
                    Ok(self
                        .server_uris
                        .get(shuffle_id)
                        .ok_or_else(|| MapOutputError::ShuffleIdNotFound(*shuffle_id))?
                        .iter()
                        .filter(|x| !x.is_none())
                        .map(|x| x.clone().unwrap())
                        .collect())
                }
            }
        }
    }

    pub async fn check_ready(&self, part_group: (usize, usize, usize)) -> Result<()> {
        self.client_check_ready(part_group).await?;
        Ok(())
    }

    pub async fn get_max_cnt(
        &self,
        part_group: (usize, usize, usize),
        cnt: usize,
    ) -> Result<usize> {
        let cnt = self.client_get_max_cnt(part_group, cnt).await?;
        Ok(cnt)
    }

    pub fn increment_generation(&self) {
        *self.generation.lock() += 1;
    }

    pub fn get_generation(&self) -> i64 {
        *self.generation.lock()
    }

    pub fn update_generation(&mut self, new_gen: i64) {
        if new_gen > *self.generation.lock() {
            self.server_uris = Arc::new(DashMap::new());
            *self.generation.lock() = new_gen;
        }
    }
}

#[derive(Debug, Error)]
pub enum MapOutputError {
    #[error("Shuffle id output #{0} not found in the map")]
    ShuffleIdNotFound(usize),
    #[error("Stage id output #{0} not found in the map")]
    StageIdNotFound(usize),
}
