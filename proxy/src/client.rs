use crossbeam_channel::Receiver;
use solana_perf::packet::PacketBatch;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::runtime::Runtime;
use tracing::info;

use crate::{
    config::ClientConfig,
    error::ShredstreamError,
    heartbeat::{HeartbeatConfig, HeartbeatManager},
    receiver::{ReceiverConfig, ShredReceiver},
    threads::ThreadManager,
    token_auth::TokenAuthenticator,
};

pub struct JitoShredsClient {
    config: ClientConfig,
    runtime: Arc<Runtime>,
    thread_manager: ThreadManager,
    heartbeat_manager: Option<HeartbeatManager>,
    shred_receiver: Option<ShredReceiver>,
    shutdown_initiated: AtomicBool,
}

impl JitoShredsClient {
    pub fn new(config: ClientConfig, runtime: Arc<Runtime>) -> Result<Self, ShredstreamError> {
        Ok(Self {
            config,
            runtime,
            thread_manager: ThreadManager::new(),
            heartbeat_manager: None,
            shred_receiver: None,
            shutdown_initiated: AtomicBool::new(false),
        })
    }

    pub fn start(&mut self) -> Result<Receiver<PacketBatch>, ShredstreamError> {
        info!("Starting JitoShredsClient...");

        info!("Initializing authentication...");
        let auth = TokenAuthenticator::new(
            self.config.block_engine_url.clone(),
            self.config.auth_url.clone(),
            self.config.auth_keypair.clone(),
            Arc::clone(&self.runtime),
        )?;

        let shredstream_client = auth.create_shredstream_client()?;

        info!("Starting heartbeat manager...");
        let heartbeat_config = HeartbeatConfig {
            regions: self.config.desired_regions.clone(),
            bind_addr: self.config.bind_addr,
            bind_port: self.config.bind_port,
        };

        let mut heartbeat = HeartbeatManager::new(shredstream_client, heartbeat_config)?;
        heartbeat.start(&self.runtime)?;
        self.heartbeat_manager = Some(heartbeat);

        info!("Starting shred receiver...");
        let receiver_config = ReceiverConfig {
            bind_addr: self.config.bind_addr,
            bind_port: self.config.bind_port,
            num_threads: self.config.num_threads,
            deduper_size: self.config.deduper_size,
            deduper_false_positive_rate: self.config.deduper_false_positive_rate,
            ring_buffer_size: 10_000,
        };

        let mut shred_receiver = ShredReceiver::new(receiver_config)?;
        let packet_receiver = shred_receiver.start()?;
        self.shred_receiver = Some(shred_receiver);

        info!("JitoShredsClient initialization complete");
        Ok(packet_receiver)
    }

    pub fn shutdown(&mut self) {
        if self
            .shutdown_initiated
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!("Initiating shutdown sequence");

            if let Some(mut heartbeat) = self.heartbeat_manager.take() {
                info!("Shutting down heartbeat manager");
                heartbeat.shutdown();
            }

            if let Some(mut receiver) = self.shred_receiver.take() {
                info!("Shutting down shred receiver");
                receiver.shutdown();
            }

            info!("Shutting down main thread manager");
            self.thread_manager.shutdown();

            info!("Shutdown complete");
        }
    }
}

impl Drop for JitoShredsClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}
