use std::sync::Arc;

use crate::context::Context;
use crate::rdd::RddE;
use crate::serializable_traits::{Data, SerFunc, Func};
use crate::SerArc;

mod local_file_reader;
pub use local_file_reader::{LocalFsReader, LocalFsReaderConfig};

pub trait ReaderConfiguration<I: Data> {
    #[track_caller]
    fn make_reader<F, F0, O, OE, FE, FD>(self, context: Arc<Context>, decoder: Option<F>, sec_decoder: Option<F0>, fe: FE, fd: FD) -> SerArc<dyn RddE<Item = O, ItemE = OE>>
    where
        O: Data,
        OE: Data,
        F: SerFunc(I) -> O,
        F0: SerFunc(I) -> Vec<OE>,
        FE: SerFunc(Vec<O>) -> OE,
        FD: SerFunc(OE) -> Vec<O>;
}
