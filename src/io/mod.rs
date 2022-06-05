use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, Func, SerFunc};
use crate::SerArc;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    #[track_caller]
    fn make_reader<F, F0, O>(
        self,
        context: Arc<Context>,
        decoder: Option<F>,
        sec_decoder: Option<F0>,
    ) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<Vec<u8>>;
}
