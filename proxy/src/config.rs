use std::{net::IpAddr, sync::Arc};

use solana_sdk::signature::Keypair;

#[derive(Clone)]
pub struct ClientConfig {
    pub block_engine_url: String,
    pub auth_url: Option<String>,
    pub auth_keypair: Arc<Keypair>,
    pub desired_regions: Vec<String>,
    pub bind_addr: IpAddr,
    pub bind_port: u16,
    pub num_threads: Option<usize>,
    pub deduper_size: usize,              // Default to 637_534_199
    pub deduper_false_positive_rate: f64, // Default to 0.01
}
