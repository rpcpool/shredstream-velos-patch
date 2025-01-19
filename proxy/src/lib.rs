use crossbeam_channel::Receiver;
use solana_perf::packet::PacketBatch;

pub mod client;
pub mod config;
pub mod error;
pub mod heartbeat;
pub mod receiver;
pub mod token_auth;
pub mod types;

pub use client::JitoShredsClient;
pub use config::ClientConfig;
pub use error::ShredstreamError;

pub fn subscribe(config: ClientConfig) -> Result<Receiver<PacketBatch>, ShredstreamError> {
    let client = JitoShredsClient::new(config)?;
    client.start()
}
