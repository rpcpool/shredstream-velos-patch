use thiserror::Error;
use tokio::task::JoinError;
use tonic::{transport::Error as TonicError, Status};

#[derive(Error, Debug)]
pub enum ShredstreamError {
    #[error("Transport error: {0}")]
    Transport(#[from] TonicError),

    #[error("gRPC error: {0}")]
    Grpc(#[from] Status),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Runtime error: {0}")]
    Runtime(#[from] JoinError),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Channel error: {0}")]
    Channel(String),
}
