use crate::{error::ShredstreamError, types::Metrics};
use crossbeam_channel::Sender;
use parking_lot::RwLock;
use solana_perf::{deduper::Deduper, packet::PacketBatch, recycler::Recycler};
use solana_streamer::streamer::{self, StreamerReceiveStats};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};
use tracing::info;

struct ThreadHandle {
    buffer: JoinHandle<()>,
    processor: JoinHandle<()>,
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
    bind_addr: std::net::IpAddr,
    bind_port: u16,
    num_threads: Option<usize>,
    sender: Sender<PacketBatch>,
    exit: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    threads: Arc<Mutex<Vec<ThreadHandle>>>,
}

impl ShredReceiver {
    pub fn new(
        bind_addr: std::net::IpAddr,
        bind_port: u16,
        num_threads: Option<usize>,
        sender: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
        deduper_size: usize,
        deduper_false_positive_rate: f64,
    ) -> Result<Self, ShredstreamError> {
        let optimal_size = (deduper_size as f64 / deduper_false_positive_rate).ceil() as usize;
        // Align to cache line size (64 bytes typically)
        let aligned_size = (optimal_size + 63) & !63;

        info!(
            "Initializing ShredReceiver with deduper size: {} and deduper false positive rate: {}",
            aligned_size, deduper_false_positive_rate
        );
        Ok(Self {
            bind_addr,
            bind_port,
            num_threads,
            sender,
            exit,
            metrics: Arc::new(Metrics::new()),
            deduper: Arc::new(RwLock::new(Deduper::new(
                &mut rand::thread_rng(),
                aligned_size as u64,
            ))),
            threads: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn start(
        &self,
        shutdown_receiver: &crossbeam_channel::Receiver<()>,
    ) -> Result<(), ShredstreamError> {
        let num_threads = self.num_threads.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|x| x.get())
                .unwrap_or(4)
        });

        let recycler = Recycler::warmed(100, 1024);
        let stats = Arc::new(StreamerReceiveStats::new("shred_receiver"));

        let sockets = solana_net_utils::multi_bind_in_range(
            self.bind_addr,
            (self.bind_port, self.bind_port + 1),
            num_threads,
        )?;

        for (i, socket) in sockets.1.into_iter().enumerate() {
            let (batch_sender, batch_receiver) = crossbeam_channel::unbounded();
            let ring_buffer = Arc::new(RingBuffer::new(10_000));
            let ring_buffer_clone = ring_buffer.clone();
            let shutdown_receiver = shutdown_receiver.clone();
            let exit_clone = self.exit.clone();
            let socket = Arc::new(socket);
            let stats = stats.clone();

            streamer::receiver(
                format!("shred_recv_{}", i),
                socket,
                exit_clone.clone(),
                batch_sender,
                recycler.clone(),
                stats,
                Duration::from_millis(0),
                true,
                None,
                false,
            );

            let buffer_handle = std::thread::Builder::new()
                .name(format!("ring_buffer_{}", i))
                .spawn(move || {
                    while !exit_clone.load(Ordering::Relaxed) {
                        crossbeam_channel::select! {
                            recv(batch_receiver) -> maybe_batch => {
                                if let Ok(batch) = maybe_batch {
                                    if !ring_buffer.push(batch) {
                                        tracing::warn!("Ring buffer full, dropping packet batch");
                                    }
                                }
                            }
                            recv(shutdown_receiver) -> _ => {
                                break;
                            }
                            default => {
                                std::thread::sleep(Duration::from_micros(100));
                            }
                        }
                    }

                    while let Ok(batch) = batch_receiver.try_recv() {
                        if !ring_buffer.push(batch) {
                            break;
                        }
                    }
                })?;

            let sender = self.sender.clone();
            let deduper = self.deduper.clone();
            let metrics = self.metrics.clone();
            let exit = self.exit.clone();

            let processor_handle = std::thread::Builder::new()
                .name(format!("shred_proc_{}", i))
                .spawn(move || {
                    let mut batch_buffer = Vec::with_capacity(1024);

                    while !exit.load(Ordering::Relaxed) || ring_buffer_clone.has_pending() {
                        while batch_buffer.len() < batch_buffer.capacity() {
                            if let Some(batch) = ring_buffer_clone.try_pop() {
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
                })?;

            self.threads.lock().unwrap().push(ThreadHandle {
                buffer: buffer_handle,
                processor: processor_handle,
            });
        }

        Ok(())
    }

    pub fn shutdown(&self) -> Result<(), ShredstreamError> {
        self.exit.store(true, Ordering::SeqCst);

        let mut threads = self.threads.lock().unwrap();
        while let Some(handle) = threads.pop() {
            if let Err(e) = handle.buffer.join() {
                tracing::error!("Failed to join buffer thread: {:?}", e);
            }
            if let Err(e) = handle.processor.join() {
                tracing::error!("Failed to join processor thread: {:?}", e);
            }
        }

        Ok(())
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
            tracing::error!("Failed to forward packet batch: {}", e);
            break;
        }
    }
    batch_buffer.clear();
}
