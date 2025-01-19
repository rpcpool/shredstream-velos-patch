use crossbeam_channel::{Receiver, Sender};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use tracing::{error, info};

pub struct ThreadHandle<T> {
    handle: JoinHandle<T>,
    name: String,
}

impl<T> ThreadHandle<T> {
    pub fn new(handle: JoinHandle<T>, name: String) -> Self {
        Self { handle, name }
    }

    pub fn join(self) {
        if let Err(e) = self.handle.join() {
            error!("Failed to join thread {}: {:?}", self.name, e);
        }
    }
}

pub struct ThreadManager {
    exit: Arc<AtomicBool>,
    shutdown_sender: Sender<()>,
    shutdown_receiver: Receiver<()>,
    handles: Vec<ThreadHandle<()>>,
    shutdown_initiated: AtomicBool,
}

impl ThreadManager {
    pub fn new() -> Self {
        let exit = Arc::new(AtomicBool::new(false));
        let (shutdown_sender, shutdown_receiver) = crossbeam_channel::bounded(1);

        Self {
            exit,
            shutdown_sender,
            shutdown_receiver,
            handles: Vec::new(),
            shutdown_initiated: AtomicBool::new(false),
        }
    }

    pub fn spawn<F>(&mut self, name: &str, f: F)
    where
        F: FnOnce(Arc<AtomicBool>, Receiver<()>) + Send + 'static,
    {
        let exit = self.exit.clone();
        let shutdown_rx = self.shutdown_receiver.clone();

        let handle = thread::Builder::new()
            .name(name.to_string())
            .spawn(move || f(exit, shutdown_rx))
            .expect("Failed to spawn thread");

        self.handles
            .push(ThreadHandle::new(handle, name.to_string()));
    }

    pub fn shutdown(&mut self) {
        if self
            .shutdown_initiated
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!("Initiating thread manager shutdown");
            self.exit.store(true, Ordering::SeqCst);

            let _ = self.shutdown_sender.send(());

            while let Some(handle) = self.handles.pop() {
                handle.join();
            }

            info!("Thread manager shutdown complete");
        }
    }

    pub fn exit_signal(&self) -> Arc<AtomicBool> {
        self.exit.clone()
    }
}

impl Drop for ThreadManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
