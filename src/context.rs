use std::fmt::Debug;
use std::fs;
use std::io::Write;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::executor::{Executor, Signal};
use crate::io::ReaderConfiguration;
use crate::partial::{ApproximateEvaluator, PartialResult};
use crate::rdd::{ParallelCollection, Rdd, RddE, RddBase, UnionRdd, ser_encrypt, ser_decrypt};
use crate::scheduler::{DistributedScheduler, LocalScheduler, NativeScheduler, TaskContext};
use crate::serializable_traits::{Data, SerFunc};
use crate::serialized_data_capnp::serialized_data;
use crate::{env, hosts, utils, Fn, SerArc};
use log::error;
use once_cell::sync::OnceCell;
use simplelog::*;
use uuid::Uuid;
use Schedulers::*;
use sgx_types::*;

extern "C" {
    fn pre_touching(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        zero: u8,
    ) -> sgx_status_t; 
}
pub const PRI_KEY_LOC: &str =  "/home/lixiang/.ssh/124.9";
// There is a problem with this approach since T needs to satisfy PartialEq, Eq for Range
// No such restrictions are needed for Vec
pub enum Sequence<T> {
    Range(Range<T>),
    Vec(Vec<T>),
}

#[derive(Clone)]
enum Schedulers {
    Local(Arc<LocalScheduler>),
    Distributed(Arc<DistributedScheduler>),
}

impl Default for Schedulers {
    fn default() -> Schedulers {
        Schedulers::Local(Arc::new(LocalScheduler::new(20, true)))
    }
}

impl Schedulers {
    fn run_job<T: Data, TE: Data, U: Data, F>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        partitions: Vec<usize>,
        allow_local: bool,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>))) -> U,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        match self {
            Distributed(distributed) => {
                let res = distributed
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
            Local(local) => {
                let res = local
                    .clone()
                    .run_job(func, final_rdd, partitions, allow_local);
                log::info!(
                    "`{}` job finished, took {}s",
                    op_name,
                    start.elapsed().as_secs()
                );
                res
            }
        }
    }

    fn run_approximate_job<T: Data, TE: Data, U: Data, R, F, E>(
        &self,
        func: Arc<F>,
        final_rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>))) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        let op_name = final_rdd.get_op_name();
        log::info!("starting `{}` job", op_name);
        let start = Instant::now();
        let res = match self {
            Distributed(distributed) => distributed
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
            Local(local) => local
                .clone()
                .run_approximate_job(func, final_rdd, evaluator, timeout),
        };
        log::info!(
            "`{}` job finished, took {}s",
            op_name,
            start.elapsed().as_secs()
        );
        res
    }
}

#[derive(Default)]
pub struct Context {
    next_rdd_id: Arc<AtomicUsize>,
    next_shuffle_id: Arc<AtomicUsize>,
    scheduler: Schedulers,
    pub(crate) address_map: Vec<SocketAddrV4>,
    distributed_driver: bool,
    /// this context/session temp work dir
    work_dir: PathBuf,
}

impl Drop for Context {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let deployment_mode = env::Configuration::get().deployment_mode;
            if self.distributed_driver && deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in master");
            } else if deployment_mode == env::DeploymentMode::Distributed {
                log::info!("inside context drop in executor");
            }
        }
        Context::driver_clean_up_directives(&self.work_dir, &self.address_map);
    }
}

impl Context {
    pub fn new() -> Result<Arc<Self>> {
        Context::with_mode(env::Configuration::get().deployment_mode)
    }

    pub fn with_mode(mode: env::DeploymentMode) -> Result<Arc<Self>> {
        match mode {
            env::DeploymentMode::Distributed => {
                if env::Configuration::get().is_driver {
                    let ctx = Context::init_distributed_driver()?;
                    ctx.set_cleanup_process();
                    Ok(ctx)
                } else {
                    Context::init_distributed_worker()?
                }
            }
            env::DeploymentMode::Local => Context::init_local_scheduler(),
        }
    }

    pub fn launch_pre_touching() {
        let eid = env::Env::get()
            .enclave
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .geteid();
        let child = thread::spawn(move || {
            let mut retval = 1;
            let _sgx_status_t = unsafe {
                pre_touching(eid, &mut retval, 0)
            };
        });
    }
    /// Sets a handler to receives any external signal to stop the process
    /// and shuts down gracefully any ongoing op
    fn set_cleanup_process(&self) {
        let address_map = self.address_map.clone();
        let work_dir = self.work_dir.clone();
        env::Env::run_in_async_rt(|| {
            tokio::spawn(async move {
                // avoid moving a self clone here or drop won't be potentially called
                // before termination and never clean up
                if tokio::signal::ctrl_c().await.is_ok() {
                    log::info!("received termination signal, cleaning up");
                    Context::driver_clean_up_directives(&work_dir, &address_map);
                    std::process::exit(0);
                }
            });
        })
    }

    fn init_local_scheduler() -> Result<Arc<Self>> {
        Context::launch_pre_touching();
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        fs::create_dir_all(&job_work_dir).unwrap();

        initialize_loggers(job_work_dir.join("ns-driver.log"));
        let scheduler = Schedulers::Local(Arc::new(LocalScheduler::new(20, true)));

        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler,
            address_map: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)],
            distributed_driver: false,
            work_dir: job_work_dir,
        }))
    }

    /// Initialization function for the application driver.
    /// * Distributes the configuration setup to the workers.
    /// * Distributes a copy of the application binary to all the active worker host nodes.
    /// * Launches the workers in the remote machine using the same binary (required).
    /// * Creates and returns a working Context.
    fn init_distributed_driver() -> Result<Arc<Self>> {
        let mut port: u16 = 10000;
        let mut address_map = Vec::new();
        let job_id = Uuid::new_v4().to_string();
        let job_work_dir = env::Configuration::get()
            .local_dir
            .join(format!("ns-session-{}", job_id));
        let job_work_dir_str = job_work_dir
            .to_str()
            .ok_or_else(|| Error::PathToString(job_work_dir.clone()))?;

        let binary_path = std::env::current_exe().map_err(|_| Error::CurrentBinaryPath)?;
        let binary_path_str = binary_path
            .to_str()
            .ok_or_else(|| Error::PathToString(binary_path.clone()))?;
        let binary_name = binary_path
            .file_name()
            .ok_or(Error::CurrentBinaryName)?
            .to_os_string()
            .into_string()
            .map_err(Error::OsStringToString)?;

        let mut enclave_path  = PathBuf::from("");
        if let Some(dir) = binary_path.parent() {
            enclave_path = dir.join("enclave.signed.so");
        }
        let enclave_path = enclave_path;
        
        let enclave_path_str = enclave_path
            .to_str()
            .ok_or_else(|| Error::PathToString(enclave_path.clone()))?;
        let enclave_name = enclave_path
            .file_name()
            .ok_or(Error::CurrentEnclaveName)?
            .to_os_string()
            .into_string()
            .map_err(Error::OsStringToString)?;
                    
        fs::create_dir_all(&job_work_dir).unwrap();
        let conf_path = job_work_dir.join("config.toml");
        let conf_path = conf_path.to_str().unwrap();
        initialize_loggers(job_work_dir.join("ns-driver.log"));

        for address in &hosts::Hosts::get()?.slaves {
            log::debug!("deploying executor at address {:?}", address);
            let address_ip: Ipv4Addr = address
                .split('@')
                .nth(1)
                .ok_or_else(|| Error::ParseHostAddress(address.into()))?
                .parse()
                .map_err(|x| Error::ParseHostAddress(format!("{}", x)))?;
            address_map.push(SocketAddrV4::new(address_ip, port));

            log::debug!("creating workdir");
            // Create work dir:
            Command::new("ssh")
                .args(&["-i", PRI_KEY_LOC, address, "mkdir", &job_work_dir_str])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh mkdir".into(),
                })?;

            log::debug!("copy conf file to remote");
            // Copy conf file to remote:
            Context::create_workers_config_file(address_ip, port, conf_path)?;
            let remote_path = format!("{}:{}/config.toml", address, job_work_dir_str);
            Command::new("scp")
                .args(&["-i", PRI_KEY_LOC, conf_path, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp config".into(),
                })?;

            log::debug!("copy binary");
            // Copy binary:
            let remote_path = format!("{}:{}/{}", address, job_work_dir_str, binary_name);
            Command::new("scp")
                .args(&["-i", PRI_KEY_LOC, &binary_path_str, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp executor".into(),
                })?;

            // Copy enclave
            let remote_path = format!("{}:{}/{}", address, job_work_dir_str, enclave_name);
            log::debug!("copy enclave");
            Command::new("scp")
                .args(&["-i", PRI_KEY_LOC, &enclave_path_str, &remote_path])
                .output()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "scp executor enclave".into(),
                })?;
            // Copy pem??

            log::debug!("deploy a remote slave");
            // Deploy a remote slave:
            let path = format!("{}/{}", job_work_dir_str, binary_name);
            log::debug!("remote path {}", path);
            Command::new("ssh")
                .args(&["-i", PRI_KEY_LOC, address, &path])
                .spawn()
                .map_err(|e| Error::CommandOutput {
                    source: e,
                    command: "ssh run".into(),
                })?;
            port += 5000;
        }

        Ok(Arc::new(Context {
            next_rdd_id: Arc::new(AtomicUsize::new(0)),
            next_shuffle_id: Arc::new(AtomicUsize::new(0)),
            scheduler: Schedulers::Distributed(Arc::new(DistributedScheduler::new(
                20,
                true,
                Some(address_map.clone()),
                10000,
            ))),
            address_map,
            distributed_driver: true,
            work_dir: job_work_dir,
        }))
    }

    fn init_distributed_worker() -> Result<!> {
        Context::launch_pre_touching();
        let mut work_dir = PathBuf::from("");
        match std::env::current_exe().map_err(|_| Error::CurrentBinaryPath) {
            Ok(binary_path) => {
                match binary_path.parent().ok_or_else(|| Error::CurrentBinaryPath) {
                    Ok(dir) => work_dir = dir.into(),
                    Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
                };
                initialize_loggers(work_dir.join("ns-executor.log"));
            }
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
        }
        
        if (*env::Env::get().enclave).lock().unwrap().is_some() {
            log::debug!("worker inits enclave successfully");
        }

        log::debug!("starting worker");
        let port = match env::Configuration::get()
            .slave
            .as_ref()
            .map(|c| c.port)
            .ok_or(Error::GetOrCreateConfig("executor port not set"))
        {
            Ok(port) => port,
            Err(err) => Context::worker_clean_up_directives(Err(err), work_dir)?,
        };
        let executor = Arc::new(Executor::new(port));
        Context::worker_clean_up_directives(executor.worker(), work_dir)
    }

    fn worker_clean_up_directives(run_result: Result<Signal>, work_dir: PathBuf) -> Result<!> {
        env::Env::get().shuffle_manager.clean_up_shuffle_data();
        if let Some(enclave) = std::mem::replace(&mut *(*env::Env::get().enclave).lock().unwrap(), None) {
            enclave.destroy();
        }
        env::BOUNDED_MEM_CACHE.free_data_enc();
        utils::clean_up_work_dir(&work_dir);
        match run_result {
            Err(err) => {
                log::error!("executor failed with error: {}", err);
                std::process::exit(1);
            }
            Ok(value) => {
                log::info!("executor closed gracefully with signal: {:?}", value);
                std::process::exit(0);
            }
        }
    }

    fn driver_clean_up_directives(work_dir: &Path, executors: &[SocketAddrV4]) {
        Context::drop_executors(executors);
        // Give some time for the executors to shut down and clean up
        std::thread::sleep(std::time::Duration::from_millis(1_500));
        env::Env::get().shuffle_manager.clean_up_shuffle_data();
        if let Some(enclave) = std::mem::replace(&mut *(*env::Env::get().enclave).lock().unwrap(), None) {
            enclave.destroy();
        }
        env::BOUNDED_MEM_CACHE.free_data_enc();
        utils::clean_up_work_dir(work_dir);
    }

    fn create_workers_config_file(local_ip: Ipv4Addr, port: u16, config_path: &str) -> Result<()> {
        let mut current_config = env::Configuration::get().clone();
        current_config.local_ip = local_ip;
        current_config.slave = Some(std::convert::From::<(bool, u16)>::from((true, port)));
        current_config.is_driver = false;

        let config_string = toml::to_string_pretty(&current_config).unwrap();
        let mut config_file = fs::File::create(config_path).unwrap();
        config_file.write_all(config_string.as_bytes()).unwrap();
        Ok(())
    }

    fn drop_executors(address_map: &[SocketAddrV4]) {
        if env::Configuration::get().deployment_mode.is_local() {
            return;
        }

        for socket_addr in address_map {
            log::debug!(
                "dropping executor in {:?}:{:?}",
                socket_addr.ip(),
                socket_addr.port()
            );
            if let Ok(mut stream) =
                TcpStream::connect(format!("{}:{}", socket_addr.ip(), socket_addr.port() + 10))
            {
                let signal = bincode::serialize(&Signal::ShutDownGracefully).unwrap();
                let mut message = capnp::message::Builder::new_default();
                let mut task_data = message.init_root::<serialized_data::Builder>();
                task_data.set_msg(&signal);
                capnp::serialize::write_message(&mut stream, &message)
                    .map_err(Error::OutputWrite)
                    .unwrap();
            } else {
                error!(
                    "Failed to connect to {}:{} in order to stop its executor",
                    socket_addr.ip(),
                    socket_addr.port()
                );
            }
        }
    }

    pub fn new_rdd_id(self: &Arc<Self>) -> usize {
        self.next_rdd_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn new_shuffle_id(self: &Arc<Self>) -> usize {
        self.next_shuffle_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn make_rdd<T: Data, TE: Data, I, IE, FE, FD>(
        self: &Arc<Self>,
        seq: I,
        seqe: IE,
        fe: FE,
        fd: FD,
        num_slices: usize,
    ) -> SerArc<dyn RddE<Item = T, ItemE = TE>>
    where
        I: IntoIterator<Item = T>,
        IE: IntoIterator<Item = TE>,
        FE: SerFunc(Vec<T>) -> TE,
        FD: SerFunc(TE) -> Vec<T>,
    {
        let rdd = self.parallelize(seq, seqe, fe, fd, num_slices);
        rdd.register_op_name("make_rdd");
        rdd
    }

    pub fn range(
        self: &Arc<Self>,
        start: u64,
        end: u64,
        step: usize,
        num_slices: usize,
    ) -> SerArc<dyn RddE<Item = u64, ItemE = Vec<u8>>> {
        // TODO: input validity check
        let seq = (start..=end).step_by(step);
        let fe = Fn!(|v: Vec<u64>| {
            ser_encrypt::<u64>(v)
        });
        let fd = Fn!(|v: Vec<u8>| {
            ser_decrypt::<u64>(v)
        });

        //no need to ensure privacy
        let rdd = self.parallelize(seq, Vec::new(), fe, fd, num_slices);
        rdd.register_op_name("range");
        rdd
    }

    pub fn parallelize<T: Data, TE: Data, I, IE, FE, FD>(
        self: &Arc<Self>,
        seq: I,
        seqe: IE,
        fe: FE,
        fd: FD,
        num_slices: usize,
    ) -> SerArc<dyn RddE<Item = T, ItemE = TE>>
    where
        I: IntoIterator<Item = T>,
        IE: IntoIterator<Item = TE>,
        FE: SerFunc(Vec<T>) -> TE,
        FD: SerFunc(TE) -> Vec<T>,
    {
        SerArc::new(ParallelCollection::new(self.clone(), seq, seqe, fe, fd, num_slices))
    }

    /// Load from a distributed source and turns it into a parallel collection.
    pub fn read_source<F, C, FE, FD, I: Data, O: Data, OE: Data>(
        self: &Arc<Self>,
        config: C,
        func: F,
        fe: FE,
        fd: FD,
    ) -> impl RddE<Item = O, ItemE = OE>
    where
        F: SerFunc(I) -> O,
        C: ReaderConfiguration<I>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>,
    {
        config.make_reader(self.clone(), func, fe, fd)
    }

    pub fn run_job<T: Data, TE: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>)) -> U,
    {
        let cl = Fn!(move |(_task_context, (iter_p, iter_e))| (func)((iter_p, iter_e)));
        let func = Arc::new(cl);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            true,
        )
    }

    pub fn run_job_with_partitions<T: Data, TE: Data, U: Data, F, P>(
        self: &Arc<Self>,
        rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        func: F,
        partitions: P,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>)) -> U,
        P: IntoIterator<Item = usize>,
    {
        let cl = Fn!(move |(_task_context, iter)| (func)(iter));
        self.scheduler
            .run_job(Arc::new(cl), rdd, partitions.into_iter().collect(), false)
    }

    pub fn run_job_with_context<T: Data, TE: Data, U: Data, F>(
        self: &Arc<Self>,
        rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        func: F,
    ) -> Result<Vec<U>>
    where
        F: SerFunc((TaskContext, (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>))) -> U,
    {
        log::debug!("inside run job in context");
        let func = Arc::new(func);
        self.scheduler.run_job(
            func,
            rdd.clone(),
            (0..rdd.number_of_splits()).collect(),
            false,
        )
    }

    /// Run a job that can return approximate results. Returns a partial result
    /// (how partial depends on whether the job was finished before or after timeout).
    /// TODO need revision
    pub(crate) fn run_approximate_job<T: Data, TE: Data, U: Data, R, F, E>(
        self: &Arc<Self>,
        func: F,
        rdd: Arc<dyn RddE<Item = T, ItemE = TE>>,
        evaluator: E,
        timeout: Duration,
    ) -> Result<PartialResult<R>>
    where
        F: SerFunc((TaskContext, (Box<dyn Iterator<Item = T>>, Box<dyn Iterator<Item = TE>>))) -> U,
        E: ApproximateEvaluator<U, R> + Send + Sync + 'static,
        R: Clone + Debug + Send + Sync + 'static,
    {
        self.scheduler
            .run_approximate_job(Arc::new(func), rdd, evaluator, timeout)
    }

    pub(crate) fn get_preferred_locs(
        &self,
        rdd: Arc<dyn RddBase>,
        partition: usize,
    ) -> Vec<std::net::Ipv4Addr> {
        match &self.scheduler {
            Schedulers::Distributed(scheduler) => scheduler.get_preferred_locs(rdd, partition),
            Schedulers::Local(scheduler) => scheduler.get_preferred_locs(rdd, partition),
        }
    }

    pub fn union<T: Data, TE: Data>(rdds: &[Arc<dyn RddE<Item = T, ItemE = TE>>]) -> Result<impl RddE<Item = T, ItemE = TE>> {
        UnionRdd::new(rdds)
    }
}

static LOGGER: OnceCell<()> = OnceCell::new();

fn initialize_loggers<P: Into<PathBuf>>(file_path: P) {
    fn _initializer(file_path: PathBuf) {
        let log_level = env::Configuration::get().loggin.log_level.into();
        log::info!("path for file logger: {}", file_path.display());
        let file_logger: Box<dyn SharedLogger> = WriteLogger::new(
            log_level,
            Config::default(),
            fs::File::create(file_path).expect("not able to create log file"),
        );
        let mut combined = vec![file_logger];
        if let Some(term_logger) =
            TermLogger::new(log_level, Config::default(), TerminalMode::Mixed)
        {
            let logger: Box<dyn SharedLogger> = term_logger;
            combined.push(logger);
        }
        CombinedLogger::init(combined).unwrap();
    }

    LOGGER.get_or_init(move || _initializer(file_path.into()));
}
