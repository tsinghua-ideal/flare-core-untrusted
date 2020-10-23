use std::sync::Arc;

use crate::context::Context;
use crate::rdd::Rdd;
use crate::serializable_traits::{Data, SerFunc};
use crate::SerArc;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    fn make_reader<F, O, OE, FE, FD>(self, context: Arc<Context>, decoder: F, fe: FE, fd: FD) -> SerArc<dyn Rdd<Item = O>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        FE: SerFunc(Vec<O>) -> Vec<OE>,
        FD: SerFunc(Vec<OE>) -> Vec<O>;
}
