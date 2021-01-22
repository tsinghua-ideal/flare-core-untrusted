#![feature(
    arbitrary_self_types,
    binary_heap_into_iter_sorted,
    coerce_unsized,
    core_intrinsics,
    fn_traits,
    map_first_last,
    never_type,
    proc_macro_hygiene,
    specialization,
    thread_id_value,
    type_ascription,
    unboxed_closures,
    unsize,
    vec_into_raw_parts,
    vec_resize_default
)]
#![allow(dead_code, where_clauses_object_safety, deprecated)]
#![allow(clippy::single_component_path_imports)]

mod serialized_data_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/serialized_data_capnp.rs"));
}

mod aggregator;
mod cache;
mod cache_tracker;
mod context;
mod dependency;
mod env;
mod executor;
pub mod io;
mod map_output_tracker;
mod partial;
pub mod partitioner;
#[path = "rdd/rdd.rs"]
pub mod rdd;
mod scheduler;
mod serialization_free;
mod serializable_traits;
mod shuffle;
mod split;
pub use env::DeploymentMode;
mod error;
pub mod fs;
mod hosts;
mod utils;

// Import global external types and macros:
pub use serde_closure::Fn;
pub use serde_traitobject::{Arc as SerArc, Box as SerBox};

pub use sgx_types::*;
pub use sgx_urts::SgxEnclave;

// Re-exports:
pub use context::Context;
pub use error::*;
pub use io::LocalFsReaderConfig;
pub use partial::BoundedDouble;
pub use rdd::{PairRdd, Rdd, RddE, encrypt, decrypt, ser_encrypt, ser_decrypt, MAX_ENC_BL};
