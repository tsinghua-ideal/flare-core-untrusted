use std::ffi::OsString;
use std::path::PathBuf;

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;
pub type StdResult<T, E> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    AsyncJoinError(#[from] tokio::task::JoinError),

    #[error("failed to run {command}")]
    CommandOutput {
        source: std::io::Error,
        command: String,
    },

    #[error("failed to create the log file")]
    CreateLogFile(#[source] std::io::Error),

    #[error("failed to create the terminal logger")]
    CreateTerminalLogger,

    #[error("couldn't determine the current binary's name")]
    CurrentBinaryName,

    #[error("couldn't determine the path to the current binary")]
    CurrentBinaryPath,

    #[error("couldn't determine the current enclave's name")]
    CurrentEnclaveName,

    #[error("couldn't determine the path to the current enclave")]
    CurrentEnclavePath,

    #[error("failed trying converting to type {0}")]
    ConversionError(&'static str),

    #[error(transparent)]
    BincodeDeserialization(#[from] bincode::Error),

    #[error(transparent)]
    CapnpDeserialization(#[from] capnp::Error),

    #[error("failure while downcasting an object to a concrete type: {0}")]
    DowncastFailure(&'static str),

    #[error("executor shutdown signal")]
    ExecutorShutdown,

    #[error("configuration failure: {0}")]
    GetOrCreateConfig(&'static str),

    #[error("partitioner not set")]
    LackingPartitioner,

    #[error("failed to load hosts file from {}", path.display())]
    LoadHosts {
        source: std::io::Error,
        path: PathBuf,
    },

    #[error(transparent)]
    MapOutputError(#[from] crate::map_output_tracker::MapOutputError),

    #[error("network error")]
    NetworkError(#[from] NetworkError),

    #[error("unwrapped a non-unique shared reference")]
    NotUniqueSharedRef,

    #[error("failed to determine the home directory")]
    NoHome,

    #[error("failed to convert {:?} to a String", .0)]
    OsStringToString(OsString),

    #[error("failed writing to output destination")]
    OutputWrite(#[source] capnp::Error),

    #[error("failed to parse hosts file at {}", path.display())]
    ParseHosts {
        source: toml::de::Error,
        path: PathBuf,
    },

    #[error(transparent)]
    PartialJobError(#[from] crate::partial::PartialJobError),

    #[error("failed to convert {} to a String", .0.display())]
    PathToString(PathBuf),

    #[error("failed to parse slave address {0}")]
    ParseHostAddress(String),

    #[error("failed reading from input source")]
    InputRead(#[source] std::io::Error),

    #[error(transparent)]
    ShuffleError(#[from] crate::shuffle::ShuffleError),

    #[error("operation not supported: {0}")]
    UnsupportedOperation(&'static str),

    #[error("no files for the given path")]
    NoFilesFound,

    #[error("unrecognized error (todo!)")]
    Other,
}

impl Error {
    pub(crate) fn executor_shutdown(&self) -> bool {
        match self {
            Error::ExecutorShutdown => true,
            _ => false,
        }
    }
}

#[derive(Debug, Error)]
pub enum NetworkError {
    #[error("disconnected from address")]
    ConnectionFailure,

    #[error("failed to find free port {0}, tried {1} times")]
    FreePortNotFound(u16, usize),

    #[error("no message received")]
    NoMessageReceived,

    #[error(transparent)]
    TcpListener(#[from] tokio::io::Error),
}
