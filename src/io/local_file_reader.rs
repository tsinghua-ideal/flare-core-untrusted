use core::panic::Location;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::{atomic, mpsc::SyncSender, Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::context::Context;
use crate::dependency::Dependency;
use crate::env::{Env, BOUNDED_MEM_CACHE};
use crate::error::{Error, Result};
use crate::io::*;
use crate::rdd::*;
use crate::serializable_traits::{AnyData, Data, Func, SerFunc};
use crate::serialization_free::Construct;
use crate::split::Split;
use crate::Fn;
use log::debug;
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use sgx_types::*;
pub struct LocalFsReaderConfig {
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    dir_path: PathBuf,
    executor_partitions: Option<u64>,
}

impl LocalFsReaderConfig {
    /// Read all the files from a directory or a path.
    #[track_caller]
    pub fn new<T: Into<PathBuf>>(path: T) -> LocalFsReaderConfig {
        LocalFsReaderConfig {
            filter_ext: None,
            expect_dir: true,
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
    fn make_reader<F, F0, U>(
        self,
        context: Arc<Context>,
        decoder: Option<F>,
        sec_decoder: Option<F0>,
    ) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Vec<u8>) -> U,
        F0: SerFunc(Vec<u8>) -> Vec<ItemE>,
        U: Data,
    {
        let reader = LocalFsReader::<BytesReader, _, _>::new(self, context, sec_decoder.clone());
        let local_num_splits = reader.get_executor_partitions() as usize;
        let read_files = Fn!(
            move |part: usize, readers: Box<dyn Iterator<Item = BytesReader>>| {
                let readers = readers.collect::<Vec<_>>();
                match readers.get(part) {
                    Some(reader) => {
                        Box::new((*reader).clone().into_iter()) as Box<dyn Iterator<Item = _>>
                    }
                    None => Box::new(Vec::new().into_iter()) as Box<dyn Iterator<Item = _>>,
                }
            }
        );

        reader.get_context().add_num(1);
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files).pin(),
        );
        files_per_executor.get_context().add_num(1);
        let decoder = MapperRdd::new(files_per_executor, decoder.unwrap()).pin();
        decoder.register_op_name("local_fs_reader<bytes>");
        SerArc::new(decoder)
    }
}

impl ReaderConfiguration<PathBuf> for LocalFsReaderConfig {
    fn make_reader<F, F0, U>(
        self,
        context: Arc<Context>,
        decoder: Option<F>,
        sec_decoder: Option<F0>,
    ) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(PathBuf) -> U,
        F0: SerFunc(PathBuf) -> Vec<ItemE>,
        U: Data,
    {
        let reader = LocalFsReader::<FileReader, _, _>::new(self, context, sec_decoder.clone());
        let local_num_splits = reader.get_executor_partitions() as usize;
        let read_files = Fn!(
            move |part: usize, readers: Box<dyn Iterator<Item = FileReader>>| {
                let readers = readers.collect::<Vec<_>>();
                match readers.get(part) {
                    Some(reader) => {
                        Box::new((*reader).clone().into_iter()) as Box<dyn Iterator<Item = _>>
                    }
                    None => Box::new(Vec::new().into_iter()) as Box<dyn Iterator<Item = _>>,
                }
            }
        );

        //No support for LocalFsReader::<FileReader>
        reader.get_context().add_num(1);
        let files_per_executor = Arc::new(
            MapPartitionsRdd::new(Arc::new(reader) as Arc<dyn Rdd<Item = _>>, read_files).pin(),
        );
        files_per_executor.get_context().add_num(1);
        let decoder = MapperRdd::new(files_per_executor, decoder.unwrap()).pin();
        decoder.register_op_name("local_fs_reader<files>");
        SerArc::new(decoder)
    }
}

/// Reads all files specified in a given directory from the local directory
/// on all executors on every worker node.
#[derive(Clone, Serialize, Deserialize)]
pub struct LocalFsReader<T, I, F0>
where
    F0: Func(I) -> Vec<ItemE> + Clone,
{
    id: usize,
    op_id: OpId,
    sec_decoder: Option<F0>,
    path: PathBuf,
    is_single_file: bool,
    filter_ext: Option<std::ffi::OsString>,
    expect_dir: bool,
    executor_partitions: Option<u64>,
    #[serde(skip_serializing, skip_deserializing)]
    context: Weak<Context>,
    // explicitly copy the address map as the map under context is not
    // deserialized in tasks and this is required:
    splits: Vec<SocketAddrV4>,
    _marker_reader_data: PhantomData<T>,
    _marker_text_data: PhantomData<I>,
}

impl<T, I, F0> LocalFsReader<T, I, F0>
where
    F0: Func(I) -> Vec<ItemE> + Clone,
{
    #[track_caller]
    fn new(config: LocalFsReaderConfig, context: Arc<Context>, sec_decoder: Option<F0>) -> Self {
        let LocalFsReaderConfig {
            dir_path,
            expect_dir,
            filter_ext,
            executor_partitions,
        } = config;

        let is_single_file = {
            let path: &Path = dir_path.as_ref();
            path.is_file()
        };

        let loc = Location::caller();

        LocalFsReader {
            id: context.new_rdd_id(),
            op_id: context.new_op_id(loc),
            sec_decoder,
            path: dir_path,
            is_single_file,
            filter_ext,
            expect_dir,
            executor_partitions,
            splits: context.address_map.clone(),
            context: Arc::downgrade(&context),
            _marker_reader_data: PhantomData,
            _marker_text_data: PhantomData,
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

        let mut num_partitions = self.splits.len() as u64 * self.get_executor_partitions();

        let mut files: Vec<(u64, PathBuf)> = vec![];
        // We compute std deviation incrementally to estimate a good breakpoint
        // of size per partition.
        let mut total_files = 0_u64;
        let mut k = 0;
        let mut ex = 0.0;
        let mut ex2 = 0.0;

        let mut entries = fs::read_dir(&self.path)
            .map_err(Error::InputRead)?
            .collect::<Vec<_>>();
        entries.sort_by_key(|e| e.as_ref().ok().map(|x| x.file_name()));

        for (i, entry) in entries.into_iter().enumerate() {
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
            // return Err(Error::NoFilesFound);
            return Ok(Vec::new());
        }

        debug!(
            "the number of loaded files {:?}, the files are {:?}",
            total_files, files
        );

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

        for (i, (size, file)) in files.into_iter().enumerate() {
            if partitions.len() as u64 == num_partitions - 1 {
                partition.push(file);
                continue;
            }

            let new_part_size = curr_part_size + size;
            let larger_than_mean = i % 2 == 0;
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

    fn secure_compute_(
        &self,
        stage_id: usize,
        data: Vec<ItemE>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let acc_arg = acc_arg.clone();
        let handle = std::thread::spawn(move || {
            let now = Instant::now();
            let wait = start_execute(stage_id, acc_arg, data, Vec::<ItemE>::new(), tx);
            let dur = now.elapsed().as_nanos() as f64 * 1e-9 - wait;
            println!("***in local file reader, total {:?}***", dur);
        });
        Ok(vec![handle])
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

        fn free_data_enc(&self, ptrs: (usize, usize)) {
            unreachable!()
        }

        fn get_rdd_id(&self) -> usize {
            self.id
        }

        fn get_op_id(&self) -> OpId {
            self.op_id
        }

        fn get_op_ids(&self, op_ids: &mut Vec<OpId>) {
            op_ids.push(self.get_op_id());
        }

        fn get_context(&self) -> Arc<Context> {
            self.context.upgrade().unwrap()
        }

        fn get_dependencies(&self) -> Vec<Dependency> {
            vec![]
        }

        fn get_secure(&self) -> bool {
            self.sec_decoder.is_some()
        }

        fn move_allocation(&self, value_ptr: (usize, usize)) -> ((usize, usize), usize) {
            unreachable!()
        }

        fn is_pinned(&self) -> bool {
            true
        }

        fn iterator_raw(
            &self,
            stage_id: usize,
            split: Box<dyn Split>,
            acc_arg: &mut AccArg,
            tx: SyncSender<(usize, usize)>,
        ) -> Result<Vec<JoinHandle<()>>> {
            self.secure_compute(stage_id, split, acc_arg, tx)
        }

        default fn iterator_any(&self, split: Box<dyn Split>) -> Result<Box<dyn AnyData>> {
            Ok(Box::new(self.iterator(split)?.collect::<Vec<_>>()))
        }
    };
}

impl<F0> RddBase for LocalFsReader<BytesReader, Vec<u8>, F0>
where
    F0: SerFunc(Vec<u8>) -> Vec<ItemE>,
{
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        // for a given split there is only one preferred location because this is pinned,
        // the preferred location is the host at which this split will be executed;
        let split = split.downcast_ref::<BytesReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let local_num_splits = self.get_executor_partitions() as usize;
        let mut splits = Vec::with_capacity(self.splits.len() * local_num_splits);
        for (global_idx, host) in self.splits.iter().enumerate() {
            for local_idx in 0..local_num_splits {
                splits.push(Box::new(BytesReader {
                    idx: global_idx * local_num_splits + local_idx,
                    host: *host.ip(),
                    files: Vec::new(),
                }) as Box<dyn Split>)
            }
        }
        splits
    }
}

impl<F0> RddBase for LocalFsReader<FileReader, PathBuf, F0>
where
    F0: SerFunc(PathBuf) -> Vec<ItemE>,
{
    impl_common_lfs_rddb_funcs!();

    fn preferred_locations(&self, split: Box<dyn Split>) -> Vec<Ipv4Addr> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        vec![split.host]
    }

    fn splits(&self) -> Vec<Box<dyn Split>> {
        let local_num_splits = self.get_executor_partitions() as usize;
        let mut splits = Vec::with_capacity(self.splits.len() * local_num_splits);
        for (global_idx, host) in self.splits.iter().enumerate() {
            for local_idx in 0..local_num_splits {
                splits.push(Box::new(FileReader {
                    idx: global_idx * local_num_splits + local_idx,
                    host: *host.ip(),
                    files: Vec::new(),
                }) as Box<dyn Split>)
            }
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

        fn get_or_compute(
            &self,
            split: Box<dyn Split>,
        ) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
            let cache_tracker = Env::get().cache_tracker.clone();
            Ok(cache_tracker.get_or_compute(Arc::new(self.clone()), split))
        }
    };
}

impl<F0> Rdd for LocalFsReader<BytesReader, Vec<u8>, F0>
where
    F0: SerFunc(Vec<u8>) -> Vec<ItemE>,
{
    type Item = BytesReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
        let idx = split.idx;
        let host = split.host;
        let files_by_part = self.load_local_files()?;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| BytesReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let split = split.downcast_ref::<BytesReader>().unwrap();
        let now = Instant::now();
        let idx = split.idx;
        let host = split.host;
        let files_by_part = self.load_local_files()?;
        let readers = files_by_part
            .into_iter()
            .map(move |files| BytesReader { files, host, idx })
            .collect::<Vec<_>>();
        let data = match readers.get(idx) {
            Some(reader) => (*reader)
                .clone()
                .into_iter() //Vec<Vec<u8>>, ser_enc_data
                .flat_map(|ser| self.sec_decoder.as_ref().unwrap()(ser).into_iter())
                .collect::<Vec<_>>(), //enc_data
            None => Vec::new(),
        };
        let dur = now.elapsed().as_nanos() as f64 * 1e-9;
        println!("io time: {:?} s", dur);
        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_part_id = split.get_index();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_quadruple(cur_rdd_id, cur_op_id, cur_part_id, cur_split_num);
        self.secure_compute_(stage_id, data, acc_arg, tx)
    }
}

impl<F0> Rdd for LocalFsReader<FileReader, PathBuf, F0>
where
    F0: SerFunc(PathBuf) -> Vec<ItemE>,
{
    type Item = FileReader;

    impl_common_lfs_rdd_funcs!();

    fn compute(&self, split: Box<dyn Split>) -> Result<Box<dyn Iterator<Item = Self::Item>>> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        let idx = split.idx;
        let host = split.host;
        let files_by_part = self.load_local_files()?;
        Ok(Box::new(
            files_by_part
                .into_iter()
                .map(move |files| FileReader { files, host, idx }),
        ) as Box<dyn Iterator<Item = Self::Item>>)
    }

    fn secure_compute(
        &self,
        stage_id: usize,
        split: Box<dyn Split>,
        acc_arg: &mut AccArg,
        tx: SyncSender<(usize, usize)>,
    ) -> Result<Vec<JoinHandle<()>>> {
        let split = split.downcast_ref::<FileReader>().unwrap();
        let idx = split.idx;
        let host = split.host;
        let files_by_part = self.load_local_files()?;
        let readers = files_by_part
            .into_iter()
            .map(move |files| FileReader { files, host, idx })
            .collect::<Vec<_>>();
        let data = match readers.get(idx) {
            Some(reader) => (*reader)
                .clone()
                .into_iter() //Vec<Vec<u8>>, ser_enc_data
                .flat_map(|ser| self.sec_decoder.as_ref().unwrap()(ser).into_iter())
                .collect::<Vec<_>>(), //enc_data
            None => Vec::new(),
        };

        let cur_rdd_id = self.get_rdd_id();
        let cur_op_id = self.get_op_id();
        let cur_part_id = split.get_index();
        let cur_split_num = self.number_of_splits();
        acc_arg.insert_quadruple(cur_rdd_id, cur_op_id, cur_part_id, cur_split_num);
        self.secure_compute_(stage_id, data, acc_arg, tx)
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
