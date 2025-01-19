use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_perf::packet::PacketBatch;
use tokio::runtime::Runtime;

use crate::{
    config::ClientConfig, error::ShredstreamError, heartbeat::HeartbeatManager,
    receiver::ShredReceiver, token_auth::TokenAuthenticator,
};

pub struct JitoShredsClient {
    config: ClientConfig,
    runtime: Arc<Runtime>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Option<Receiver<()>>,
}

impl JitoShredsClient {
    pub fn new(config: ClientConfig) -> Result<Self, ShredstreamError> {
        let exit = Arc::new(AtomicBool::new(false));
        let (_, shutdown_receiver) = Self::setup_shutdown_signal(exit.clone())?;

        Ok(Self {
            config,
            runtime: Arc::new(Runtime::new()?),
            exit,
            shutdown_receiver: Some(shutdown_receiver),
        })
    }

    pub fn start(&self) -> Result<Receiver<PacketBatch>, ShredstreamError> {
        let (sender, receiver) = unbounded();
        let shutdown_receiver = self
            .shutdown_receiver
            .as_ref()
            .expect("Shutdown receiver not initialized");

        let auth = TokenAuthenticator::new(
            self.config.block_engine_url.clone(),
            self.config.auth_url.clone(),
            self.config.auth_keypair.clone(),
            Arc::clone(&self.runtime),
        )?;

        let shredstream_client = auth.create_shredstream_client()?;

        let heartbeat = HeartbeatManager::new(
            shredstream_client,
            self.config.desired_regions.clone(),
            self.config.bind_addr,
            self.config.bind_port,
            self.exit.clone(),
        )?;

        let shred_receiver = ShredReceiver::new(
            self.config.bind_addr,
            self.config.bind_port,
            self.config.num_threads,
            sender,
            self.exit.clone(),
            self.config.deduper_size,
            self.config.deduper_false_positive_rate,
        )?;

        heartbeat.start(&self.runtime, shutdown_receiver)?;
        shred_receiver.start(shutdown_receiver)?;

        Ok(receiver)
    }

    fn setup_shutdown_signal(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
        let (s, r) = crossbeam_channel::bounded(256);
        let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;
        let s_thread = s.clone();

        std::thread::spawn(move || {
            for _ in signals.forever() {
                exit.store(true, Ordering::SeqCst);
                // Send shutdown signal multiple times for all threads
                for _ in 0..256 {
                    if s_thread.send(()).is_err() {
                        break;
                    }
                }
            }
        });

        Ok((s, r))
    }
    pub fn shutdown(&self) {
        self.exit.store(true, Ordering::SeqCst);
        sleep(Duration::from_millis(1500));
    }
}

impl Drop for JitoShredsClient {
    fn drop(&mut self) {
        self.shutdown();
    }
}
