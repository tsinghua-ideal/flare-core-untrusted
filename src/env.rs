use std::collections::HashMap;
use std::fs;
use std::mem::forget;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use crate::cache::BoundedMemoryCache;
use crate::cache_tracker::CacheTracker;
use crate::error::Error;
use crate::hosts::Hosts;
use crate::map_output_tracker::MapOutputTracker;
use crate::rdd::RddBase;
use crate::shuffle::{ShuffleFetcher, ShuffleManager};
use dashmap::DashMap;
use log::LevelFilter;
use once_cell::sync::{Lazy, OnceCell};
use serde::{Deserialize, Serialize};
use tokio::runtime::{Handle, Runtime};

use sgx_types::*;
use sgx_urts::SgxEnclave;

/// ShuffleCache
/// The key is: {shuffle_id}/{input_id}/{reduce_id}
type ShuffleCache = Arc<DashMap<(usize, usize, usize), Vec<u8>>>;
/// SortCache
/// The key is: {(stage_id, part_id_offset, num_splits)}/{input_id}/{reduce_id}
type SortCache = Arc<DashMap<((usize, usize, usize), usize, usize), Vec<u8>>>;

const ENV_VAR_PREFIX: &str = "VEGA_";
pub(crate) const THREAD_PREFIX: &str = "_VEGA";
static CONF: OnceCell<Configuration> = OnceCell::new();
static ENV: OnceCell<Env> = OnceCell::new();
static ASYNC_RT: Lazy<Option<Runtime>> = Lazy::new(Env::build_async_executor);

pub(crate) static SHUFFLE_CACHE: Lazy<ShuffleCache> = Lazy::new(|| Arc::new(DashMap::new()));
pub(crate) static SORT_CACHE: Lazy<SortCache> = Lazy::new(|| Arc::new(DashMap::new()));
pub(crate) static BOUNDED_MEM_CACHE: Lazy<BoundedMemoryCache> = Lazy::new(BoundedMemoryCache::new);
pub(crate) static RDDB_MAP: Lazy<RddBMap> = Lazy::new(|| RddBMap::new());

pub(crate) struct RddBMap {
    map: Arc<DashMap<usize, Arc<dyn RddBase>>>, //rdd_id -> rddbase
}

impl RddBMap {
    pub fn new() -> Self {
        let eid = Env::get()
            .enclave
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .geteid();
        RddBMap {
            map: Arc::new(DashMap::new()),
        }
    }

    pub fn get_rddb(&self, rdd_id: usize) -> Option<Arc<dyn RddBase>> {
        self.map.get(&rdd_id).map(|x| x.clone())
    }

    pub fn insert(&self, rdd_id: usize, rdd_base: Arc<dyn RddBase>) {
        self.map.insert(rdd_id, rdd_base);
    }
}

pub(crate) struct Env {
    pub map_output_tracker: MapOutputTracker,
    pub shuffle_manager: ShuffleManager,
    pub shuffle_fetcher: ShuffleFetcher,
    pub cache_tracker: Arc<CacheTracker>,
    pub enclave: Arc<Mutex<Option<SgxEnclave>>>,
    pub enclave_path: PathBuf,
}

impl Env {
    pub fn get() -> &'static Env {
        ENV.get_or_init(Self::new)
    }

    /// Run a function inside the existing Tokio context.
    pub fn run_in_async_rt<F, R>(func: F) -> R
    where
        F: FnOnce() -> R,
    {
        if let Ok(rt) = Handle::try_current() {
            let _guard = rt.enter();
            func()
        } else if let Some(rt) = &*ASYNC_RT {
            let _guard = rt.enter();
            func()
        } else {
            unreachable!()
        }
    }

    /// Builds an async executor for executing DAG tasks according to env,
    /// machine properties and schedulling mode.
    /// Is only built in case there is not an existing one, otherwise the existing one will be used
    /// for running all the async tasks.
    fn build_async_executor() -> Option<Runtime> {
        if Handle::try_current().is_ok() {
            None
        } else {
            Some(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            )
        }
    }

    fn new() -> Self {
        Env::run_in_async_rt(|| -> Self {
            let conf = Configuration::get();
            let hosts = Hosts::get().expect("fatal error: failed loading host file");
            let master_addr = hosts.master;
            let map_output_tracker = MapOutputTracker::new(conf.is_driver, master_addr);
            let shuffle_manager =
                ShuffleManager::new().expect("fatal error: failed creating shuffle manager");

            let binary_path =
                std::env::current_exe().unwrap_or_else(|_| panic!("CurrentBinaryPath error"));
            let mut enclave_path = PathBuf::from("");
            if let Some(dir) = binary_path.parent() {
                enclave_path = dir.join("enclave.signed.so");
            }
            if !enclave_path.exists() {
                log::debug!("enclave.signed.so not found");
                panic!("enclave.signed.so not found!");
            }
            let enclave_path_str = enclave_path
                .to_str()
                .unwrap_or_else(|| panic!("env::Env enclave PathBuf2str error"));
            let enclave = Arc::new(Mutex::new(Some(
                Env::init_enclave(&enclave_path_str)
                    .unwrap_or_else(|x| panic!("[-] Init Enclave Failed {}!", x.as_str())),
            )));
            Env {
                map_output_tracker,
                shuffle_manager,
                shuffle_fetcher: ShuffleFetcher,
                cache_tracker: CacheTracker::new(
                    conf.is_driver,
                    master_addr,
                    conf.local_ip,
                    &BOUNDED_MEM_CACHE,
                )
                .expect("fatal error: failed creating cache tracker"),
                enclave,
                enclave_path,
            }
        })
    }

    //whether secure flag is set or not, the enclave should be inited and destroyed in the end.
    fn init_enclave(enclave_path_str: &str) -> SgxResult<SgxEnclave> {
        let mut launch_token: sgx_launch_token_t = [0; 1024];
        let mut launch_token_updated: i32 = 0;
        // call sgx_create_enclave to initialize an enclave instance
        // Debug Support: set 2nd parameter to 1
        let debug = 1;
        let mut misc_attr = sgx_misc_attribute_t {
            secs_attr: sgx_attributes_t { flags: 0, xfrm: 0 },
            misc_select: 0,
        };
        SgxEnclave::create(
            enclave_path_str,
            debug,
            &mut launch_token,
            &mut launch_token_updated,
            &mut misc_attr,
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogLevel {
    Error,
    Warn,
    Debug,
    Trace,
    Info,
}

impl LogLevel {
    pub fn is_debug_or_lower(self) -> bool {
        use LogLevel::*;
        match self {
            Debug | Trace => true,
            _ => false,
        }
    }
}

impl Into<LevelFilter> for LogLevel {
    fn into(self) -> LevelFilter {
        match self {
            LogLevel::Error => LevelFilter::Error,
            LogLevel::Warn => LevelFilter::Warn,
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Trace => LevelFilter::Trace,
            _ => LevelFilter::Info,
        }
    }
}

/// Struct used for parsing environment vars
#[derive(Deserialize, Debug)]
struct EnvConfig {
    deployment_mode: Option<DeploymentMode>,
    local_ip: Option<String>,
    local_dir: Option<String>,
    log_level: Option<LogLevel>,
    log_cleanup: Option<bool>,
    shuffle_service_port: Option<u16>,
    slave_deployment: Option<bool>,
    slave_port: Option<u16>,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Distributed,
    Local,
}

impl DeploymentMode {
    pub fn is_local(self) -> bool {
        if let DeploymentMode::Local = self {
            true
        } else {
            false
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Configuration {
    pub is_driver: bool,
    pub local_ip: Ipv4Addr,
    pub local_dir: PathBuf,
    pub deployment_mode: DeploymentMode,
    pub shuffle_svc_port: Option<u16>,
    pub slave: Option<SlaveConfig>,
    pub loggin: LogConfig,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct SlaveConfig {
    pub deployment: bool,
    pub port: u16,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct LogConfig {
    pub log_level: LogLevel,
    pub log_cleanup: bool,
}

impl From<(bool, u16)> for SlaveConfig {
    fn from(config: (bool, u16)) -> Self {
        let (deployment, port) = config;
        SlaveConfig { deployment, port }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        use DeploymentMode::*;

        // this may be a worker, try to get conf dynamically from file:
        if let Some(config) = Configuration::get_from_file() {
            return config;
        }

        // get config from env vars:
        let config = envy::prefixed(ENV_VAR_PREFIX)
            .from_env::<EnvConfig>()
            .unwrap();

        let deployment_mode = match config.deployment_mode {
            Some(Distributed) => Distributed,
            _ => Local,
        };

        let local_dir = if let Some(dir) = config.local_dir {
            PathBuf::from(dir)
        } else {
            std::env::temp_dir()
        };

        // loggin config:
        let log_level = match config.log_level {
            Some(val) => val,
            _ => LogLevel::Info,
        };
        let log_cleanup = match config.log_cleanup {
            Some(cond) => cond,
            _ => !cfg!(debug_assertions),
        };
        log::debug!("Setting max log level to: {:?}", log_level);
        log::set_max_level(log_level.into());

        let local_ip: Ipv4Addr = {
            if let Some(ip) = config.local_ip {
                ip.parse().unwrap()
            } else if deployment_mode == Distributed {
                panic!("Local IP required while deploying in distributed mode.")
            } else {
                Ipv4Addr::LOCALHOST
            }
        };

        // master/slave config:
        let is_master;
        let slave: Option<SlaveConfig>;
        match config.slave_deployment {
            Some(true) => {
                if let Some(port) = config.slave_port {
                    is_master = false;
                    slave = Some(SlaveConfig {
                        deployment: true,
                        port,
                    });
                } else {
                    panic!("Port required while deploying a worker.")
                }
            }
            _ => {
                is_master = true;
                slave = None;
            }
        }

        Configuration {
            is_driver: is_master,
            local_ip,
            local_dir,
            deployment_mode,
            loggin: LogConfig {
                log_level,
                log_cleanup,
            },
            shuffle_svc_port: config.shuffle_service_port,
            slave,
        }
    }
}

impl Configuration {
    pub fn get() -> &'static Configuration {
        CONF.get_or_init(Self::default)
    }

    fn get_from_file() -> Option<Configuration> {
        let binary_path = std::env::current_exe()
            .map_err(|_| Error::CurrentBinaryPath)
            .unwrap();
        if let Some(dir) = binary_path.parent() {
            let conf_file = dir.join("config.toml");
            if conf_file.exists() {
                return fs::read_to_string(conf_file)
                    .map(|content| toml::from_str::<Configuration>(&content).ok())
                    .ok()
                    .flatten();
            }
        }
        None
    }
}
