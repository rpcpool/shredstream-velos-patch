use std::{
    net::{IpAddr, Ipv4Addr},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use jito_protos::{
    shared::Socket,
    shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
};
use tokio::runtime::Runtime;
use tonic::{service::interceptor::InterceptedService, transport::Channel};
use tracing::info;

use crate::{error::ShredstreamError, token_auth::ClientInterceptor};

pub struct HeartbeatManager {
    client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    regions: Vec<String>,
    socket: Socket,
    exit: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl HeartbeatManager {
    pub fn new(
        client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
        regions: Vec<String>,
        bind_addr: IpAddr,
        bind_port: u16,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, ShredstreamError> {
        let ip = if bind_addr.is_unspecified() {
            get_public_ip()?.to_string()
        } else {
            info!("Using provided bind address: {}", bind_addr);
            bind_addr.to_string()
        };

        let socket = Socket {
            ip,
            port: bind_port as i64,
        };

        info!(
            "Initializing heartbeat manager with socket {}:{}",
            socket.ip, socket.port
        );

        Ok(Self {
            client,
            regions,
            socket,
            exit,
            thread: None,
        })
    }

    pub fn start(&mut self, runtime: &Runtime) -> Result<(), ShredstreamError> {
        let client = self.client.clone();
        let regions = self.regions.clone();
        let socket = self.socket.clone();
        let exit = self.exit.clone();
        let runtime = runtime.handle().clone();

        info!("Starting heartbeat thread for regions: {:?}", regions);

        let thread = std::thread::Builder::new()
            .name("heartbeat".into())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    let heartbeat = Heartbeat {
                        socket: Some(socket.clone()),
                        regions: regions.clone(),
                    };

                    match runtime.block_on(client.clone().send_heartbeat(heartbeat)) {
                        Ok(_) => {
                            info!("Heartbeat sent successfully");
                        }
                        Err(e) => {
                            tracing::warn!("Heartbeat failed: {}", e);
                        }
                    }

                    std::thread::sleep(Duration::from_secs(1));
                }
                info!("Heartbeat thread exiting");
            })?;

        self.thread = Some(thread);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        self.exit.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for HeartbeatManager {
    fn drop(&mut self) {
        self.shutdown();
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
