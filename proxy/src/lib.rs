use std::{
    io,
    io::{Error, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    panic,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{sleep, spawn},
    time::Duration,
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvError, Sender};
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::{reqwest, ClientError};
use solana_metrics::set_host_id;
use solana_perf::deduper::Deduper;
use solana_sdk::signature::Keypair;
use solana_streamer::streamer::StreamerReceiveStats;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

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
    pub dest_socketaddrs: Vec<(SocketAddr, String)>,
    pub metrics_report_interval_ms: u64,
    pub debug_trace_shred: bool,
    pub public_ip: Option<IpAddr>,
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
    #[error("SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("RecvError {0}")]
    RecvError(#[from] RecvError),
    #[error("IoError {0}")]
    IoError(#[from] io::Error),
    #[error("Shutdown")]
    Shutdown,
}

/// Returns socket address and original hostname:port string
pub fn resolve_hostname_port(hostname_port: &str) -> io::Result<(SocketAddr, String)> {
    let socketaddr = hostname_port.to_socket_addrs()?.next().ok_or_else(|| {
        Error::new(
            ErrorKind::AddrNotAvailable,
            format!("Could not find destination {hostname_port}"),
        )
    })?;
    Ok((socketaddr, hostname_port.to_string()))
}

/// Returns public-facing IPV4 address
pub fn get_public_ip() -> reqwest::Result<IpAddr> {
    info!("Requesting public ip from ifconfig.me...");
    let client = reqwest::blocking::Client::builder()
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .build()?;
    let response = client.get("https://ifconfig.me").send()?.text()?;
    let public_ip = IpAddr::from_str(&response).unwrap();
    info!("Retrieved public ip: {public_ip:?}");
    Ok(public_ip)
}

fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
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
    set_host_id(hostname::get()?.into_string().unwrap());

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

    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        config.block_engine_url.clone(),
        config
            .auth_url
            .unwrap_or_else(|| config.block_engine_url.clone()),
        config.auth_keypair,
        config.desired_regions,
        SocketAddr::new(
            config.public_ip.unwrap_or_else(|| config.src_bind_addr),
            config.src_bind_port,
        ),
        runtime,
        "shredstream_proxy".to_string(),
        metrics.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.push(heartbeat_hdl);

    let unioned_dest_sockets = Arc::new(ArcSwap::from_pointee(
        config.dest_socketaddrs.iter().map(|x| x.0).collect(),
    ));

    let deduper = Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
        &mut rand::thread_rng(),
        forwarder::DEDUPER_NUM_BITS,
    )));

    let forward_stats = Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread"));

    let forwarder_hdls = forwarder::start_forwarder_threads(
        unioned_dest_sockets.clone(),
        config.src_bind_addr,
        config.src_bind_port,
        config.num_threads,
        deduper.clone(),
        metrics.clone(),
        forward_stats.clone(),
        false,
        config.debug_trace_shred,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.extend(forwarder_hdls);

    let report_metrics_thread = {
        let exit = exit.clone();
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1));
                forward_stats.report();
            }
        })
    };
    thread_handles.push(report_metrics_thread);

    let metrics_hdl = forwarder::start_forwarder_accessory_thread(
        deduper,
        metrics.clone(),
        config.metrics_report_interval_ms,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.push(metrics_hdl);

    info!(
        "Shredstream started, listening on {}:{}/udp.",
        config.src_bind_addr, config.src_bind_port
    );

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream, {} received , {} sent successfully, {} failed, {} duplicate shreds.",
        metrics.agg_received_cumulative.load(Ordering::Relaxed),
        metrics
            .agg_success_forward_cumulative
            .load(Ordering::Relaxed),
        metrics.agg_fail_forward_cumulative.load(Ordering::Relaxed),
        metrics.duplicate_cumulative.load(Ordering::Relaxed),
    );
    Ok(())
}
