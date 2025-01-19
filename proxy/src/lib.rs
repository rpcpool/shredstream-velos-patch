use std::sync::Arc;

use crossbeam_channel::Receiver;
use solana_perf::packet::PacketBatch;
use tokio::runtime::Runtime;

pub mod client;
pub mod config;
pub mod error;
pub mod heartbeat;
pub mod receiver;
pub mod threads;
pub mod token_auth;
pub mod types;

pub use client::JitoShredsClient;
pub use config::ClientConfig;
pub use error::ShredstreamError;

pub fn subscribe(
    config: ClientConfig,
    runtime: Arc<Runtime>,
) -> Result<Receiver<PacketBatch>, ShredstreamError> {
    let mut client = JitoShredsClient::new(config, runtime)?;
    client.start()
}
