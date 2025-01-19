use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    shared::Socket,
    shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
};
use tokio::runtime::Runtime;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::{error::ShredstreamError, token_auth::ClientInterceptor};

pub struct HeartbeatManager {
    client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    regions: Vec<String>,
    socket: Socket,
    exit: Arc<AtomicBool>,
}

impl HeartbeatManager {
    pub fn new(
        client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
        regions: Vec<String>,
        bind_addr: std::net::IpAddr,
        bind_port: u16,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, ShredstreamError> {
        let socket = Socket {
            ip: bind_addr.to_string(),
            port: bind_port as i64,
        };

        Ok(Self {
            client,
            regions,
            socket,
            exit,
        })
    }

    pub fn start(
        &self,
        runtime: &Runtime,
        shutdown_receiver: &Receiver<()>,
    ) -> Result<(), ShredstreamError> {
        let client = self.client.clone();
        let regions = self.regions.clone();
        let socket = self.socket.clone();
        let exit = self.exit.clone();
        let runtime = runtime.handle().clone();
        let shutdown_receiver = shutdown_receiver.clone();

        std::thread::Builder::new()
            .name("heartbeat".into())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                        default => {
                            let heartbeat = Heartbeat {
                                socket: Some(socket.clone()),
                                regions: regions.clone(),
                            };

                            if let Err(e) = runtime.block_on(client.clone().send_heartbeat(heartbeat)) {
                                tracing::warn!("Heartbeat failed: {}", e);
                            }

                            std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                }
            })?;

        Ok(())
    }
}
