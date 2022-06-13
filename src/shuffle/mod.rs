use std::result::Result as StdResult;

use crate::SerBox;
use hyper::{Body, Response, StatusCode};
use thiserror::Error;

pub(self) mod shuffle_fetcher;
pub(self) mod shuffle_manager;
pub(self) mod shuffle_map_task;
// re-exports:
pub(crate) use shuffle_fetcher::ShuffleFetcher;
pub(crate) use shuffle_manager::ShuffleManager;
pub(crate) use shuffle_map_task::ShuffleMapTask;

pub(crate) type Result<T> = StdResult<T, ShuffleError>;

#[derive(Debug, Error)]
pub enum ShuffleError {
    #[error("failed to create local shuffle dir after 10 attempts")]
    CouldNotCreateShuffleDir,

    #[error("deserialization error")]
    DeserializationError(#[from] bincode::Error),

    #[error("incorrect URI sent in the request")]
    IncorrectUri(#[from] http::uri::InvalidUri),

    #[error("internal server error")]
    InternalError,

    #[error("shuffle fetcher failed while fetching chunk")]
    FailedFetchOp,

    #[error("failed to start shuffle server")]
    FailedToStart,

    #[error(transparent)]
    NetworkError(#[from] crate::NetworkError),

    #[error("not valid request")]
    NotValidRequest,

    #[error("cached data not found")]
    RequestedCacheNotFound,

    #[error("unexpected shuffle server problem")]
    UnexpectedServerError(#[from] hyper::Error),

    #[error("unexpected URI sent in the request: {0}")]
    UnexpectedUri(String),

    #[error("failed fetching shuffle data uris")]
    FailFetchingShuffleUris { source: Box<crate::Error> },

    #[error("failed checking other nodes ready")]
    FailCheckingReady,

    #[error("failed getting maximum cnt per partition")]
    FailGettingMaxCnt,

    #[error("unrecognized error (todo!)")]
    Other,
}

impl Into<Response<Body>> for ShuffleError {
    fn into(self) -> Response<Body> {
        match self {
            ShuffleError::UnexpectedUri(uri) => Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("Failed to parse: {}", uri)))
                .unwrap(),
            ShuffleError::RequestedCacheNotFound => Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(&[] as &[u8]))
                .unwrap(),
            _ => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(&[] as &[u8]))
                .unwrap(),
        }
    }
}

impl ShuffleError {
    fn no_port(&self) -> bool {
        match self {
            ShuffleError::NetworkError(crate::NetworkError::FreePortNotFound(_, _)) => true,
            _ => false,
        }
    }

    fn deserialization_err(&self) -> bool {
        match self {
            ShuffleError::DeserializationError(_) => true,
            _ => false,
        }
    }
}
