use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc::SyncSender};
use std::thread::{JoinHandle, self};

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{BOUNDED_MEM_CACHE, Env};
use crate::error::{Error, Result};
use crate::io::*;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, SerFunc};
use crate::split::Split;
use crate::Fn;
use log::debug;
use rand::prelude::*;
use serde_derive::{Deserialize, Serialize};
use parking_lot::Mutex;
use sgx_types::*;

pub struct LocalFsReaderConfig {
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    secure: bool,
    dir_path: PathBuf,
    executor_partitions: Option<u64>,
}

impl LocalFsReaderConfig {
    /// Read all the files from a directory or a path.
    pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            filter_ext: None,
            expect_dir: true,
            secure: true,              //default: need security
            dir_path: path.into(),
            executor_partitions: None,
        }
    }

    /// Only will read files with a given extension.
    pub fn filter_extension<T: Into<String>>(&mut self, extension: T) {
        self.filter_ext = Some(extension.into().into());
    }

    /// Default behaviour is to expect the directory to exist in every node,
    /// if it doesn't the executor will panic.
    pub fn expect_directory(mut self, should_exist: bool) -> Self {
        self.expect_dir = should_exist;
        self
    }

    /// Number of partitions to use per executor to perform the load tasks.
    /// One executor must be used per host with as many partitions as CPUs available (ideally).
    pub fn num_partitions_per_executor(mut self, num: u64) -> Self {
        self.executor_partitions = Some(num);
        self
    }
}

impl ReaderConfiguration<Vec<u8>> for LocalFsReaderConfig {
    fn make_reader<F, U, UE, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let reader = LocalFsReader::<BytesReader>::new(self, context);
        let read_files = Fn!(
            |_part: usize, readers: Box<dyn Iterator<Item = BytesReader>>| {
                Box::new(readers.into_iter().map(|file| file.into_iter()).flatten())    //Vec<Vec<Vec<u8>>> -> Vec<Vec<u8>>
                    as Box<dyn Iterator<Item = _>>
            }
        );

        let fe_mpp = Fn!(|v: Vec<Vec<u8>>| {
            let mut ct = Vec::with_capacity(v.len()); 
            for pt in v {
                ct.push(encrypt::<>(pt.as_ref()));
            }
            ct
        });
        let fd_mpp = Fn!(|v: Vec<Vec<u8>>| {
            let mut pt = Vec::with_capacity(v.len());
            for ct in v {
                pt.push(decrypt::<>(ct.as_ref()));
            }
            pt
        });
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files, fe_mpp, fd_mpp).pin(),
        );
        let decoder = MapperRdd::new(files_per_executor, decoder, fe, fd).pin();
        decoder.register_op_name("local_fs_reader<bytes>");
        SerArc::new(decoder)
    }
}

impl ReaderConfiguration<PathBuf> for LocalFsReaderConfig {
    fn make_reader<F, U, UE, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = U, ItemE = UE>>
    where
        F: SerFunc(PathBuf) -> U,
        U: Data,
        UE: Data,
        FE: SerFunc(Vec<U>) -> UE,
        FD: SerFunc(UE) -> Vec<U>,
    {
        let reader = LocalFsReader::<FileReader>::new(self, context);
        let read_files = Fn!(
            |_part: usize, readers: Box<dyn Iterator<Item = FileReader>>| {
                Box::new(readers.map(|reader| reader.into_iter()).flatten())
                    as Box<dyn Iterator<Item = _>>
            }
        );
        
        //No support for LocalFsReader::<FileReader>
        let fe_mpp = Fn!(|v: Vec<PathBuf>| {
            v
        });
        let fd_mpp = Fn!(|v: Vec<PathBuf>| {
            v
        });
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files, fe_mpp, fd_mpp).pin(),
        );
        let decoder = MapperRdd::new(files_per_executor, decoder, fe, fd).pin();
        decoder.register_op_name("local_fs_reader<files>");
        SerArc::new(decoder)
    }
}

/// Reads all files specified in a given directory from the local directory
/// on all executors on every worker node.
#[derive(Clone, Serialize, Deserialize)]
pub struct LocalFsReader<T> {
    id: usize,
    secure: bool,
    ecall_ids: Arc<Mutex<Vec<usize>>>,
    path: PathBuf,
    is_single_file: bool,
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    executor_partitions: Option<u64>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Arc<Context>,
    // explicitly copy the address map as the map under context is not
    // deserialized in tasks and this is required:
    splits: Vec<SocketAddrV4>,
    _marker_reader_data: PhantomData<T>,
}

impl<T: Data> LocalFsReader<T> {
    fn new(config: LocalFsReaderConfig, context: Arc<Context>) -> Self {
        let LocalFsReaderConfig {
            dir_path,
            expect_dir,
            secure,
            filter_ext,
            executor_partitions,
        } = config;

        let is_single_file = {
            let path: &Path = dir_path.as_ref();
            path.is_file()
        };

        LocalFsReader {
            id: context.new_rdd_id(),
            path: dir_path,
            is_single_file,
            filter_ext,
            expect_dir,
            secure,
            ecall_ids: Arc::new(Mutex::new(Vec::new())),
            executor_partitions,
            splits: context.address_map.clone(),
            context,
            _marker_reader_data: PhantomData,
        }
    }

    /// This function should be called once per host to come with the paralel workload.
    /// Is safe to recompute on failure though.
    fn load_local_files(&self) -> Result<Vec<Vec<PathBuf>>> {
        let mut total_size = 0_u64;
        if self.is_single_file {
            let files = vec![vec![self.path.clone()]];
            return Ok(files);
        }

        let mut num_partitions = self.get_executor_partitions();
        let mut files: Vec<(u64, PathBuf)> = vec![];
        // We compute std deviation incrementally to estimate a good breakpoint
        // of size per partition.
        let mut total_files = 0_u64;
        let mut k = 0;
        let mut ex = 0.0;
        let mut ex2 = 0.0;

        for (i, entry) in fs::read_dir(&self.path)
            .map_err(Error::InputRead)?
            .enumerate()
        {
            let path = entry.map_err(Error::InputRead)?.path();
            if path.is_file() {
                let is_proper_file = {
                    self.filter_ext.is_none()
                        || path.extension() == self.filter_ext.as_ref().map(|s| s.as_ref())
                };
                if !is_proper_file {
                    continue;
                }
                let size = fs::metadata(&path).map_err(Error::InputRead)?.len();
                if i == 0 {
                    // assign first file size as reference sample
                    k = size;
                }
                // compute the necessary statistics
                let remain = size as f32 - k as f32;
                ex += remain;
                ex2 += remain.powf(2.0);
                total_size += size;
                total_files += 1;

                files.push((size, path));
            }
        }

        if total_files == 0 {
            return Err(Error::NoFilesFound);
        }

        let file_size_mean = (total_size / total_files) as u64;
        let std_dev = ((ex2 - ex.powf(2.0) / total_files as f32) / total_files as f32).sqrt();

        if total_files < num_partitions {
            // Coerce the number of partitions to the number of files
            num_partitions = total_files;
        }

        let avg_partition_size = (total_size / num_partitions) as u64;

        let partitions = self.assign_files_to_partitions(
            num_partitions,
            files,
            file_size_mean,
            avg_partition_size,
            std_dev,
        );

        Ok(partitions)
    }

    /// Assign files according to total avg partition size and file size.
    /// This should return a fairly balanced total partition size.
    fn assign_files_to_partitions(
        &self,
        num_partitions: u64,
        files: Vec<(u64, PathBuf)>,
        file_size_mean: u64,
        avg_partition_size: u64,
        std_dev: f32,
    ) -> Vec<Vec<PathBuf>> {
        // Accept ~ 0.25 std deviations top from the average partition size
        // when assigning a file to a partition.
        let high_part_size_bound = (avg_partition_size + (std_dev * 0.25) as u64) as u64;

        debug!(
            "the average part size is {} with a high bound of {}",
            avg_partition_size, high_part_size_bound
        );
        debug!(
            "assigning files from local fs to partitions, file size mean: {}; std_dev: {}",
            file_size_mean, std_dev
        );

        let mut partitions = Vec::with_capacity(num_partitions as usize);
        let mut partition = Vec::with_capacity(0);
        let mut curr_part_size = 0_u64;
        let mut rng = rand::thread_rng();

        for (size, file) in files.into_iter() {
            if partitions.len() as u64 == num_partitions - 1 {
                partition.push(file);
                continue;
            }

            let new_part_size = curr_part_size + size;
            let larger_than_mean = rng.gen::<bool>();
            if (larger_than_mean && new_part_size < high_part_size_bound)
                || (!larger_than_mean && new_part_size <= avg_partition_size)
            {
                partition.push(file);
                curr_part_size = new_part_size;
            } else if size > avg_partition_size as u64 {
                if !partition.is_empty() {
                    partitions.push(partition);
                }
                partitions.push(vec![file]);
                partition = vec![];
                curr_part_size = 0;
            } else {
                if !partition.is_empty() {
                    partitions.push(partition);
                }
                partition = vec![file];
                curr_part_size = size;
            }
        }
        if !partition.is_empty() {
            partitions.push(partition);
        }

        let mut current_pos = partitions.len() - 1;
        while (partitions.len() as u64) < num_partitions {
            // If the number of specified partitions is relativelly equal to the number of files
            // or the file size of the last files is low skew can happen and there can be fewer
            // partitions than specified. This the number of partitions is actually the specified.
            if partitions.get(current_pos).unwrap().len() > 1 {
                // Only get elements from part as long as it has more than one element
                let last_part = partitions.get_mut(current_pos).unwrap().pop().unwrap();
                partitions.push(vec![last_part])
            } else if current_pos > 0 {
                current_pos -= 1;
            } else {
                break;
            }
        }
        partitions
    }

    fn get_executor_partitions(&self) -> u64 {
        if let Some(num) = self.executor_partitions {
            num
        } else {
            num_cpus::get() as u64
        }
    }
}

macro_rules! impl_common_lfs_rddb_funcs {
    () => {
        fn cache(&self) {
            panic!("no cache for LocalFsReader");
        }
        
        fn should_cache(&self) -> bool {
            false
        }

        fn free_data_enc(&self, ptr: *mut u8) {
            todo!()
        }

        fn get_rdd_id(&self) -> usize {
            self.id
        }

        fn get_context(&self) -> Arc<Context> {
            self.context.clone()
        }

        fn get_dependencies(&self) -> Vec<Dependency> {
            vec![]
        }

        fn get_secure (&self) -> bool {
            self.secure
        }

        fn get_ecall_ids(&self) -> Arc<Mutex<Vec<usize>>> {
            self.ecall_ids.clone()
        }

        fn insert_ecall_id(&self) {
            if self.secure {
                self.ecall_ids.lock().push(self.id);
            }
        }

        fn move_allocation(&self, value_ptr: *mut u8) -> (*mut u8, usize) {
            todo!()
        }

        fn is_pinned(&self) -> bool {
            true
        }

        fn iterator_raw(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
            self.secure_compute(split, acc_arg, tx)
        }

        default fn iterator_any(
            &self,
            split: Box<dyn Split>,
        ) -> Result<Box<dyn Iterator<Item = Box<dyn AnyData>>>> {
            Ok(Box::new(
                self.iterator(split)?
                    .map(|x| Box::new(x) as Box<dyn AnyData>),
            ))
        }
    };
}

impl RddBase for LocalFsReader<BytesReader> {
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // for a given split there is only one preferred location because this is pinned,
        // the preferred location is the host at which this split will be executed;
        let split = split.downcast_ref::<BytesReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(BytesReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
            }) as Box<dyn Split>)
        }
        splits
    }
}

impl RddBase for LocalFsReader<FileReader> {
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let mut splits = Vec::with_capacity(self.splits.len());
        for (idx, host) in self.splits.iter().enumerate() {
            splits.push(Box::new(FileReader {
                idx,
                host: *host.ip(),
                files: Vec::new(),
            }) as Box<dyn Split>)
        }
        splits
    }
}

macro_rules! impl_common_lfs_rdd_funcs {
    () => {
        fn get_rdd(&self) -> Arc<dyn Rdd<Item = Self::Item>>
        where
            Self: Sized,
        {
            Arc::new(self.clone()) as Arc<dyn Rdd<Item = Self::Item>>
        }

        fn get_rdd_base(&self) -> Arc<dyn RddBase> {
            Arc::new(self.clone()) as Arc<dyn RddBase>
        }

        fn get_or_compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
            let cache_tracker = Env::get().cache_tracker.clone();
            Ok(cache_tracker.get_or_compute(Arc::new(self.clone()), split))
        }
        
    };
}

impl Rdd for LocalFsReader<BytesReader> {
    type Item = BytesReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| BytesReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        // data should be decrypted first and then decoded: Vec<Vec<u8>>
        let data = files_by_part
            .into_iter()
            .map(move |files| BytesReader { files, host, idx })
            .map(|reader| reader.into_iter())
            .flatten()
            .collect::<Vec<_>>();  //Vec<Vec<u8>>
        let len = data.len();
        let captured_vars = std::mem::replace(&mut *Env::get().captured_vars.lock().unwrap(), HashMap::new());
        let cur_rdd_id = self.get_rdd_id();
        acc_arg.insert_rdd_id(cur_rdd_id);
        let eid = Env::get().enclave.lock().unwrap().as_ref().unwrap().geteid();
        let acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let tid: u64 = thread::current().id().as_u64().into();
            let mut sub_part_id = 0;
            let mut cache_meta = acc_arg.to_cache_meta();
            //TODO need to sub-partition
            let block_len = len; //temporary 
            let mut cur = 0;
            while cur < len {
                let next = match cur + block_len > len {
                    true => len,
                    false => cur + block_len,
                };
                if !acc_arg.cached(&sub_part_id) {
                    cache_meta.set_sub_part_id(sub_part_id);
                    BOUNDED_MEM_CACHE.insert_subpid(&cache_meta);
                    let block = Box::new((&data[cur..next]).to_vec());
                    let block_ptr = Box::into_raw(block);
                    let mut result_bl_ptr: usize = 0;
                    let sgx_status = unsafe {
                        secure_executing(
                            eid,
                            &mut result_bl_ptr,
                            tid,
                            &acc_arg.rdd_ids as *const Vec<(usize, usize)> as *const u8,
                            cache_meta,
                            acc_arg.is_shuffle,  
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
                    tx.send(result_bl_ptr).unwrap();
                }
                sub_part_id += 1;
                cur = next; 
            }  
        });
        Ok(vec![handle])
    }
}

impl Rdd for LocalFsReader<FileReader> {
    type Item = FileReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        let files_by_part = self.load_local_files()?;
        let idx = split.idx;
        let host = split.host;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| FileReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }

    fn secure_compute(&self, split: Box<dyn Split>, acc_arg: &mut AccArg, tx: SyncSender<usize>) -> Result<Vec<JoinHandle<()>>> {
        panic!("Don't support secure computing for LocalFsReader<FileReader>")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BytesReader {
    pub files: Vec<PathBuf>,
    idx: usize,
    host: Ipv4Addr,
}

impl Split for BytesReader {
    fn get_index(&self) -> usize {
        self.idx
    }
}

impl Iterator for BytesReader {
    type Item = Vec<u8>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(path) = self.files.pop() {
            let file = fs::File::open(path).unwrap();
            let mut content = vec![];
            let mut reader = BufReader::new(file);
            reader.read_to_end(&mut content).unwrap();
            Some(content)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileReader {
    files: Vec<PathBuf>,
    idx: usize,
    host: Ipv4Addr,
}

impl Split for FileReader {
    fn get_index(&self) -> usize {
        self.idx
    }
}

impl Iterator for FileReader {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        self.files.pop()
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_files() {
        let context = Context::new().unwrap();
        let mut loader: LocalFsReader<Vec<u8>> = LocalFsReader {
            id: 0,
            path: "A".into(),
            is_single_file: false,
            filter_ext: None,
            expect_dir: true,
            executor_partitions: Some(4),
            context,
            splits: Vec::new(),
            _marker_reader_data: PhantomData,
        };

        // Skewed file sizes
        let files = vec![
            (500u64, "A".into()),
            (2000, "B".into()),
            (3900, "C".into()),
            (2000, "D".into()),
            (1000, "E".into()),
            (1500, "F".into()),
            (500, "G".into()),
        ];
        let file_size_mean = 1628;
        let avg_partition_size = 2850;
        let std_dev = 1182f32;
        let files = loader.assign_files_to_partitions(
            4,
            files,
            file_size_mean,
            avg_partition_size,
            std_dev,
        );
        assert_eq!(files.len(), 4);

        // Even size and less files than parts
        loader.executor_partitions = Some(8);
        let files = vec![
            (500u64, "A".into()),
            (500, "B".into()),
            (500, "C".into()),
            (500, "D".into()),
        ];
        let file_size_mean = 500;
        let avg_partition_size = 250;
        let files =
            loader.assign_files_to_partitions(8, files, file_size_mean, avg_partition_size, 0.0);
        assert_eq!(files.len(), 4);

        // Even size and more files than parts
        loader.executor_partitions = Some(2);
        let files = vec![
            (500u64, "A".into()),
            (500, "B".into()),
            (500, "C".into()),
            (500, "D".into()),
            (500, "E".into()),
            (500, "F".into()),
            (500, "G".into()),
            (500, "H".into()),
        ];
        let file_size_mean = 500;
        let avg_partition_size = 2000;
        let files =
            loader.assign_files_to_partitions(2, files, file_size_mean, avg_partition_size, 0.0);
        assert_eq!(files.len(), 2);
        assert!(files[0].len() >= 3 && files[0].len() <= 5);
    }
}

*/
