use crate::{error::ShredstreamError, threads::ThreadManager, types::Metrics};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use parking_lot::RwLock;
use solana_perf::{deduper::Deduper, packet::PacketBatch, recycler::Recycler};
use solana_streamer::streamer::{self, StreamerReceiveStats};
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct ReceiverConfig {
    pub bind_addr: IpAddr,
    pub bind_port: u16,
    pub num_threads: Option<usize>,
    pub deduper_size: usize,
    pub deduper_false_positive_rate: f64,
    pub ring_buffer_size: usize,
}

struct RingBuffer {
    size: usize,
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    buffer: RwLock<Vec<Option<PacketBatch>>>,
}

impl RingBuffer {
    fn new(size: usize) -> Self {
        let mut buffer = Vec::with_capacity(size);
        buffer.resize_with(size, || None);
        Self {
            size,
            write_pos: AtomicUsize::new(0),
            read_pos: AtomicUsize::new(0),
            buffer: RwLock::new(buffer),
        }
    }

    fn push(&self, batch: PacketBatch) -> bool {
        let current_pos = self.write_pos.fetch_add(1, Ordering::AcqRel) % self.size;
        let mut buffer = self.buffer.write();
        buffer[current_pos] = Some(batch);
        true
    }

    fn try_pop(&self) -> Option<PacketBatch> {
        let current_pos = self.read_pos.fetch_add(1, Ordering::AcqRel) % self.size;
        let mut buffer = self.buffer.write();
        buffer[current_pos].take()
    }

    fn has_pending(&self) -> bool {
        let buffer = self.buffer.read();
        buffer.iter().any(|item| item.is_some())
    }
}

pub struct ShredReceiver {
    thread_manager: ThreadManager,
    config: ReceiverConfig,
    metrics: Arc<Metrics>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
}

impl ShredReceiver {
    pub fn new(config: ReceiverConfig) -> Result<Self, ShredstreamError> {
        let optimal_size =
            (config.deduper_size as f64 / config.deduper_false_positive_rate).ceil() as usize;
        let aligned_size = (optimal_size + 63) & !63;

        info!(
            "Initializing ShredReceiver with deduper size: {} and false positive rate: {}",
            aligned_size, config.deduper_false_positive_rate
        );

        Ok(Self {
            thread_manager: ThreadManager::new(),
            config,
            metrics: Arc::new(Metrics::new()),
            deduper: Arc::new(RwLock::new(Deduper::new(
                &mut rand::thread_rng(),
                aligned_size as u64,
            ))),
        })
    }

    pub fn start(&mut self) -> Result<Receiver<PacketBatch>, ShredstreamError> {
        let (output_sender, output_receiver) = bounded(1024);
        let num_threads = self.config.num_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|x| x.get())
                .unwrap_or(4)
        });

        let recycler = Recycler::warmed(100, 1024);
        let stats = Arc::new(StreamerReceiveStats::new("shred_receiver"));

        let sockets = solana_net_utils::multi_bind_in_range(
            self.config.bind_addr,
            (self.config.bind_port, self.config.bind_port + 1),
            num_threads,
        )?;

        for (i, socket) in sockets.1.into_iter().enumerate() {
            let ring_buffer = Arc::new(RingBuffer::new(self.config.ring_buffer_size));
            let (batch_sender, batch_receiver) = unbounded();

            let socket = Arc::new(socket);
            let stats = stats.clone();
            let recycler = recycler.clone();
            let exit = self.thread_manager.exit_signal();

            streamer::receiver(
                format!("shred_recv_{}", i),
                socket,
                exit,
                batch_sender,
                recycler,
                stats,
                Duration::from_millis(0),
                true,
                None,
                false,
            );

            let ring_buffer_clone = ring_buffer.clone();
            self.thread_manager
                .spawn(&format!("ring_buffer_{}", i), move |exit, shutdown_rx| {
                    Self::run_ring_buffer(exit, shutdown_rx, batch_receiver, ring_buffer_clone);
                });

            let output_sender = output_sender.clone();
            let deduper = self.deduper.clone();
            let metrics = self.metrics.clone();

            self.thread_manager
                .spawn(&format!("processor_{}", i), move |exit, shutdown_rx| {
                    Self::run_processor(
                        exit,
                        shutdown_rx,
                        ring_buffer,
                        deduper,
                        metrics,
                        output_sender,
                    );
                });
        }

        Ok(output_receiver)
    }

    fn run_ring_buffer(
        exit: Arc<AtomicBool>,
        shutdown_rx: Receiver<()>,
        batch_receiver: Receiver<PacketBatch>,
        ring_buffer: Arc<RingBuffer>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                recv(batch_receiver) -> maybe_batch => {
                    if let Ok(batch) = maybe_batch {
                        if !ring_buffer.push(batch) {
                            warn!("Ring buffer full, dropping packet batch");
                        }
                    }
                }
                recv(shutdown_rx) -> _ => break,
                default(Duration::from_micros(100)) => continue,
            }
        }

        while let Ok(batch) = batch_receiver.try_recv() {
            if !ring_buffer.push(batch) {
                break;
            }
        }
    }

    fn run_processor(
        exit: Arc<AtomicBool>,
        shutdown_rx: Receiver<()>,
        ring_buffer: Arc<RingBuffer>,
        deduper: Arc<RwLock<Deduper<2, [u8]>>>,
        metrics: Arc<Metrics>,
        sender: Sender<PacketBatch>,
    ) {
        let mut batch_buffer = Vec::with_capacity(1024);

        while !exit.load(Ordering::Relaxed) || ring_buffer.has_pending() {
            if shutdown_rx.try_recv().is_ok() {
                break;
            }

            while batch_buffer.len() < batch_buffer.capacity() {
                if let Some(batch) = ring_buffer.try_pop() {
                    batch_buffer.push(batch);
                } else {
                    break;
                }
            }

            if !batch_buffer.is_empty() {
                process_batch_buffer(&mut batch_buffer, &deduper, &metrics, &sender);
            } else {
                std::thread::sleep(Duration::from_micros(100));
            }
        }

        if !batch_buffer.is_empty() {
            process_batch_buffer(&mut batch_buffer, &deduper, &metrics, &sender);
        }
    }

    pub fn shutdown(&mut self) {
        info!("Shutting down ShredReceiver");
        self.thread_manager.shutdown();
    }
}

fn process_batch_buffer(
    batch_buffer: &mut Vec<PacketBatch>,
    deduper: &Arc<RwLock<Deduper<2, [u8]>>>,
    metrics: &Arc<Metrics>,
    sender: &Sender<PacketBatch>,
) {
    for batch in batch_buffer.iter_mut() {
        let mut vec_batch = vec![std::mem::take(batch)];
        let num_deduped = solana_perf::deduper::dedup_packets_and_count_discards(
            &deduper.read(),
            &mut vec_batch,
            |_, _, _| {},
        );

        metrics.duplicate.fetch_add(num_deduped, Ordering::Relaxed);
        metrics
            .received
            .fetch_add(vec_batch[0].len() as u64, Ordering::Relaxed);

        if let Err(e) = sender.send(vec_batch.remove(0)) {
            error!("Failed to forward packet batch: {}", e);
            break;
        }
    }
    batch_buffer.clear();
}
