use std::{
    io,
    net::IpAddr,
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, spawn},
    time::Duration,
};

use crossbeam_channel::{Receiver, RecvError, Sender};
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::reqwest;
use solana_perf::deduper::Deduper;
use solana_sdk::signature::Keypair;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;
use tracing::{error, info};

pub use crate::{forwarder::ShredMetrics, token_authenticator::BlockEngineConnectionError};

mod forwarder;
mod heartbeat;
mod token_authenticator;

#[derive(Debug)]
pub struct ShredstreamConfig {
    pub block_engine_url: String,
    pub auth_url: Option<String>,
    pub auth_keypair: Arc<Keypair>,
    pub desired_regions: Vec<String>,
    pub src_bind_addr: IpAddr,
    pub src_bind_port: u16,
    pub num_threads: Option<usize>,
}

#[derive(Debug, Error)]
pub enum ShredstreamProxyError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("ReqwestError {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("RecvError {0}")]
    RecvError(#[from] RecvError),
    #[error("IoError {0}")]
    IoError(#[from] io::Error),
    #[error("Shutdown")]
    Shutdown,
}

fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    std::thread::spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            for _ in 0..256 {
                if s_thread.send(()).is_err() {
                    break;
                }
            }
        }
    });

    Ok((s, r))
}

pub fn run(config: ShredstreamConfig) -> Result<(), ShredstreamProxyError> {
    let exit = Arc::new(AtomicBool::new(false));
    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier(exit.clone()).expect("Failed to set up signal handler");

    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            exit.store(true, Ordering::SeqCst);
            let _ = shutdown_sender.send(());
            error!("exiting process");
            sleep(Duration::from_secs(1));
            panic_hook(panic_info);
        }));
    }

    let metrics = Arc::new(ShredMetrics::new());
    let runtime = Runtime::new()?;
    let mut thread_handles = vec![];

    // Maintain heartbeat connection with Jito
    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        config.block_engine_url.clone(),
        config
            .auth_url
            .unwrap_or_else(|| config.block_engine_url.clone()),
        config.auth_keypair,
        config.desired_regions,
        (config.src_bind_addr, config.src_bind_port).into(),
        runtime,
        "shredstream_receiver".to_string(),
        metrics.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.push(heartbeat_hdl);

    // Initialize deduper
    let deduper = Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
        &mut rand::thread_rng(),
        forwarder::DEDUPER_NUM_BITS,
    )));

    // Start receiver threads
    let receiver_hdls = forwarder::start_receiver_threads(
        config.src_bind_addr,
        config.src_bind_port,
        config.num_threads,
        deduper.clone(),
        metrics.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.extend(receiver_hdls);

    // Start metrics reporting thread
    let metrics_clone = Arc::clone(&metrics);
    let metrics_hdl = spawn(move || {
        while !exit.load(Ordering::Relaxed) {
            sleep(Duration::from_secs(1));
            metrics_clone.report();
        }
    });
    thread_handles.push(metrics_hdl);

    info!(
        "Shredstream receiver started, listening on {}:{}/udp.",
        config.src_bind_addr, config.src_bind_port
    );

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream, received {} shreds, {} duplicates",
        metrics.received.load(Ordering::Relaxed),
        metrics.duplicate.load(Ordering::Relaxed),
    );
    Ok(())
}
