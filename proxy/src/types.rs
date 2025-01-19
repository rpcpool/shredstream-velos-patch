use std::{net::IpAddr, sync::atomic::AtomicU64};

use dashmap::DashMap;

#[derive(Debug)]
pub struct Metrics {
    pub received: AtomicU64,
    pub duplicate: AtomicU64,
    pub packets_received: DashMap<IpAddr, u64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            duplicate: AtomicU64::new(0),
            packets_received: DashMap::new(),
        }
    }
}
