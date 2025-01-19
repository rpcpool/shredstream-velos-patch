use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Receiver;
use dashmap::DashMap;
use solana_perf::{
    deduper::Deduper,
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_streamer::{streamer, streamer::StreamerReceiveStats};
use tracing::info;

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB

/// Start receiving shreds
pub fn start_receiver_threads(
    src_addr: IpAddr,
    src_port: u16,
    num_threads: Option<usize>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    metrics: Arc<ShredMetrics>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let num_threads = num_threads
        .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).max(4));

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);
    let stats = Arc::new(StreamerReceiveStats::new("shred_receiver"));

    // Bind sockets and spawn receiver threads
    solana_net_utils::multi_bind_in_range(src_addr, (src_port, src_port + 1), num_threads)
        .unwrap_or_else(|_| {
            panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
        })
        .1
        .into_iter()
        .enumerate()
        .map(|(thread_id, socket)| {
            let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();

            // Start the receiver thread
            let receive_thread = streamer::receiver(
                format!("shredRcv{thread_id}"),
                Arc::new(socket),
                exit.clone(),
                packet_sender,
                recycler.clone(),
                stats.clone(),
                Duration::default(),
                false,
                None,
                false,
            );

            // Process received packets
            let deduper = deduper.clone();
            let metrics = metrics.clone();
            let exit = exit.clone();
            let shutdown_receiver = shutdown_receiver.clone();

            let process_thread = std::thread::Builder::new()
                .name(format!("shredProc{thread_id}"))
                .spawn(move || {
                    while !exit.load(Ordering::Relaxed) {
                        crossbeam_channel::select! {
                            recv(packet_receiver) -> maybe_batch => {
                                if let Ok(batch) = maybe_batch {
                                    process_packet_batch(&batch, &deduper, &metrics);
                                }

                            }
                            recv(shutdown_receiver) -> _ => break,
                        }
                    }
                    info!("Exiting processor thread {thread_id}");
                })
                .unwrap();

            vec![receive_thread, process_thread]
        })
        .flatten()
        .collect()
}

fn process_packet_batch(
    packet_batch: &PacketBatch,
    deduper: &RwLock<Deduper<2, [u8]>>,
    metrics: &ShredMetrics,
) {
    metrics
        .received
        .fetch_add(packet_batch.len() as u64, Ordering::Relaxed);

    let mut packet_batch_vec = vec![packet_batch.clone()];
    let num_deduped = solana_perf::deduper::dedup_packets_and_count_discards(
        &deduper.read().unwrap(),
        &mut packet_batch_vec,
        |_received_packet, _is_already_marked_as_discard, _is_dup| {},
    );

    // Update metrics for received packets
    packet_batch_vec.iter().for_each(|batch| {
        batch.iter().for_each(|packet| {
            metrics
                .packets_received
                .entry(packet.meta().addr)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        });
    });

    metrics.duplicate.fetch_add(num_deduped, Ordering::Relaxed);
}

#[derive(Debug)]
pub struct ShredMetrics {
    pub received: AtomicU64,
    pub duplicate: AtomicU64,
    pub packets_received: DashMap<IpAddr, u64>,
}

impl ShredMetrics {
    pub fn new() -> Self {
        Self {
            received: AtomicU64::new(0),
            duplicate: AtomicU64::new(0),
            packets_received: DashMap::new(),
        }
    }

    pub fn report(&self) {
        info!(
            "Received {} shreds, {} duplicates",
            self.received.load(Ordering::Relaxed),
            self.duplicate.load(Ordering::Relaxed)
        );
    }
}
