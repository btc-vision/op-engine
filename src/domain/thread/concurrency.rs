use std::sync::{Arc, Mutex};

struct Worker {
    id: usize,
    handle: Option<std::thread::JoinHandle<()>>,
}

/// A closure that takes no arguments, returns nothing, but is `Send + 'static`.
type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<std::sync::mpsc::Sender<Job>>,
}

/// A handle that lets you join on the result of a job submitted to the ThreadPool.
pub struct TaskHandle<T> {
    receiver: std::sync::mpsc::Receiver<T>,
}

impl<T> TaskHandle<T> {
    /// Wait for the task to finish and return its result.
    /// Returns an error if the worker thread panicked or the channel was closed unexpectedly.
    pub fn join(self) -> std::thread::Result<T> {
        match self.receiver.recv() {
            Ok(val) => Ok(val),
            Err(_e) => Err(Box::new("Worker panicked or channel closed.")
                as Box<dyn std::any::Any + Send + 'static>),
        }
    }
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` worker threads.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (tx, rx) = std::sync::mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));

        let mut workers = Vec::with_capacity(size);
        for id in 0..size {
            let rx_clone = Arc::clone(&rx);
            let handle = std::thread::spawn(move || loop {
                let message = {
                    // lock the queue
                    let rx_guard = rx_clone.lock().unwrap();
                    rx_guard.recv()
                };
                match message {
                    Ok(job) => {
                        // run the task
                        job();
                    }
                    Err(_) => {
                        // channel closed => no more tasks
                        break;
                    }
                }
            });

            workers.push(Worker {
                id,
                handle: Some(handle),
            });
        }

        ThreadPool {
            workers,
            sender: Some(tx),
        }
    }

    /// Submit a job that returns a typed result. You get back a TaskHandle<T> that
    /// can be joined to retrieve that result.
    ///
    /// Usage:
    /// ```ignore
    /// let handle = pool.execute(|| {
    ///     // do some work
    ///     123  // typed return value
    /// });
    /// let result = handle.join().expect("Task panicked?");
    /// assert_eq!(result, 123);
    /// ```
    pub fn execute<F, T>(&self, job: F) -> TaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx_res, rx_res) = std::sync::mpsc::channel::<T>();

        // Wrap the user closure in another closure that sends its result.
        let wrapper = move || {
            let result = job();
            let _ = tx_res.send(result);
        };

        if let Some(ref tx) = self.sender {
            // Send the boxed closure to be executed by a worker thread.
            tx.send(Box::new(wrapper))
                .expect("ThreadPool task queue is closed");
        }

        TaskHandle { receiver: rx_res }
    }

    /// Wait for all current tasks to finish by dropping the sender (so workers see an empty queue)
    /// and then joining each worker thread. This is optional if you just drop the entire pool,
    /// but can be convenient when you want to block until tasks finish.
    pub fn join(&mut self) {
        // Drop the sender so all workers receive an empty queue signal
        self.sender.take();

        // Join all threads
        for w in &mut self.workers {
            if let Some(handle) = w.handle.take() {
                let _ = handle.join();
            }
        }
    }
}
