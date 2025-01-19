use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use crossbeam_channel::tick;
use jito_protos::{
    shared::Socket,
    shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
};
use tokio::runtime::Runtime;
use tonic::{service::interceptor::InterceptedService, transport::Channel, Code};
use tracing::{error, info, warn};

use crate::{error::ShredstreamError, threads::ThreadManager, token_auth::ClientInterceptor};

pub struct HeartbeatConfig {
    pub regions: Vec<String>,
    pub bind_addr: IpAddr,
    pub bind_port: u16,
}

pub struct HeartbeatManager {
    client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    config: HeartbeatConfig,
    socket: Socket,
    thread_manager: ThreadManager,
    stats: Arc<HeartbeatStats>,
}

struct HeartbeatStats {
    successful_heartbeats: AtomicU64,
    failed_heartbeats: AtomicU64,
    client_restarts: AtomicU64,
}

impl HeartbeatStats {
    fn new() -> Self {
        Self {
            successful_heartbeats: AtomicU64::new(0),
            failed_heartbeats: AtomicU64::new(0),
            client_restarts: AtomicU64::new(0),
        }
    }
}

impl HeartbeatManager {
    pub fn new(
        client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
        config: HeartbeatConfig,
    ) -> Result<Self, ShredstreamError> {
        let ip = if config.bind_addr.is_unspecified() {
            get_public_ip()?.to_string()
        } else {
            info!("Using provided bind address: {}", config.bind_addr);
            config.bind_addr.to_string()
        };

        let socket = Socket {
            ip,
            port: config.bind_port as i64,
        };

        info!(
            "Initializing heartbeat manager with socket {}:{}",
            socket.ip, socket.port
        );

        Ok(Self {
            client,
            config,
            socket,
            thread_manager: ThreadManager::new(),
            stats: Arc::new(HeartbeatStats::new()),
        })
    }

    pub fn start(&mut self, runtime: &Runtime) -> Result<(), ShredstreamError> {
        let client = self.client.clone();
        let regions = self.config.regions.clone();
        let socket = self.socket.clone();
        let runtime = runtime.handle().clone();
        let stats = Arc::clone(&self.stats);

        self.thread_manager
            .spawn("heartbeat", move |exit, shutdown_rx| {
                let mut heartbeat_interval = Duration::from_secs(1);
                let mut heartbeat_tick = tick(heartbeat_interval);
                let metrics_tick = tick(Duration::from_secs(30));

                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        recv(heartbeat_tick) -> _ => {
                            let heartbeat = Heartbeat {
                                socket: Some(socket.clone()),
                                regions: regions.clone(),
                            };

                            match runtime.block_on(client.clone().send_heartbeat(heartbeat)) {
                                Ok(response) => {
                                    stats.successful_heartbeats.fetch_add(1, Ordering::Relaxed);

                                    // Update heartbeat interval based on server response
                                    let new_interval = Duration::from_millis(
                                        (response.get_ref().ttl_ms / 3) as u64
                                    );
                                    if heartbeat_interval != new_interval {
                                        info!("Adjusting heartbeat interval to {:?}", new_interval);
                                        heartbeat_interval = new_interval;
                                        heartbeat_tick = tick(new_interval);
                                    }
                                }
                                Err(status) => {
                                    if status.code() == Code::InvalidArgument {
                                        error!("Invalid heartbeat arguments: {}", status);
                                        break;
                                    }
                                    stats.failed_heartbeats.fetch_add(1, Ordering::Relaxed);
                                    warn!("Heartbeat failed: {}", status);
                                    thread::sleep(Duration::from_secs(5));
                                }
                            }
                        }
                        recv(metrics_tick) -> _ => {
                            info!(
                                "Heartbeat stats: success={}, failures={}, restarts={}",
                                stats.successful_heartbeats.load(Ordering::Relaxed),
                                stats.failed_heartbeats.load(Ordering::Relaxed),
                                stats.client_restarts.load(Ordering::Relaxed)
                            );
                        }
                        recv(shutdown_rx) -> _ => break,
                    }
                }
            });

        Ok(())
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down HeartbeatManager");
        self.thread_manager.shutdown();
    }
}

fn get_public_ip() -> Result<IpAddr, ShredstreamError> {
    info!("Requesting public IP from ifconfig.me...");
    let client = reqwest::blocking::Client::builder()
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .build()
        .map_err(|e| ShredstreamError::Other(e.to_string()))?;

    let response = client
        .get("https://ifconfig.me")
        .send()
        .map_err(|e| ShredstreamError::Other(e.to_string()))?
        .text()
        .map_err(|e| ShredstreamError::Other(e.to_string()))?;

    let public_ip = IpAddr::from_str(&response.trim())
        .map_err(|e| ShredstreamError::Other(format!("Failed to parse IP address: {}", e)))?;

    info!("Retrieved public IP: {}", public_ip);
    Ok(public_ip)
}
