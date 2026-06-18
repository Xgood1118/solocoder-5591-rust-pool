use crate::dead_letter::DeadLetterQueue;
use crate::metrics::{Metrics, MetricsCounters};
use crate::scheduler::Scheduler;
use crate::types::{PoolConfig, SubmitError, Task};
use crate::worker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

pub struct Pool {
    config: PoolConfig,
    handler: worker::TaskHandler,
    task_tx: mpsc::Sender<Task>,
    task_rx: Option<mpsc::Receiver<Task>>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_counters: Arc<MetricsCounters>,
    dead_letter: Arc<DeadLetterQueue>,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    next_worker_id: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
}

impl Pool {
    pub fn new(config: PoolConfig, handler: worker::TaskHandler) -> Self {
        let (task_tx, task_rx) = mpsc::channel::<Task>(config.max_queue_size);
        let scheduler = Arc::new(RwLock::new(Scheduler::new(config.schedule_strategy)));
        let metrics_counters = Arc::new(MetricsCounters::default());
        let dead_letter = Arc::new(DeadLetterQueue::new(config.max_queue_size));
        let metrics = Arc::new(Metrics::new(
            metrics_counters.clone(),
            scheduler.clone(),
            dead_letter.clone(),
        ));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let next_worker_id = Arc::new(AtomicUsize::new(0));
        let active_workers = Arc::new(AtomicUsize::new(0));

        Pool {
            config,
            handler,
            task_tx,
            task_rx: Some(task_rx),
            scheduler,
            metrics_counters,
            dead_letter,
            metrics,
            shutting_down,
            next_worker_id,
            active_workers,
        }
    }

    pub fn submitter(&self) -> PoolSubmitter {
        PoolSubmitter {
            task_tx: self.task_tx.clone(),
            shutting_down: self.shutting_down.clone(),
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn dead_letter(&self) -> Arc<DeadLetterQueue> {
        self.dead_letter.clone()
    }

    pub fn shutting_down(&self) -> Arc<AtomicBool> {
        self.shutting_down.clone()
    }

    fn create_worker_channel(&self) -> (mpsc::Sender<Task>, mpsc::Receiver<Task>, Arc<AtomicUsize>) {
        let (tx, rx) = mpsc::channel::<Task>(256);
        let pending = Arc::new(AtomicUsize::new(0));
        (tx, rx, pending)
    }

    pub async fn start(&mut self) {
        for _ in 0..self.config.min_workers {
            let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx, pending) = self.create_worker_channel();
            self.scheduler.write().await.add_worker(id, tx, pending.clone());
            self.active_workers.fetch_add(1, Ordering::Relaxed);
            worker::spawn_worker(
                id,
                self.handler.clone(),
                rx,
                pending,
                self.metrics_counters.clone(),
                self.dead_letter.clone(),
            );
            info!(id, "spawned initial worker");
        }

        let mut task_rx = self.task_rx.take().expect("task_rx already taken");
        let scheduler = self.scheduler.clone();
        let metrics_counters = self.metrics_counters.clone();
        let dispatch_shutting_down = self.shutting_down.clone();

        tokio::spawn(async move {
            while !dispatch_shutting_down.load(Ordering::Relaxed) {
                match task_rx.recv().await {
                    Some(task) => {
                        metrics_counters.total_submitted.fetch_add(1, Ordering::Relaxed);
                        let mut sched = scheduler.write().await;
                        let _ = sched.dispatch(task).await;
                    }
                    None => {
                        info!("task channel closed, stopping dispatcher");
                        break;
                    }
                }
            }
        });

        let scale_scheduler = self.scheduler.clone();
        let scale_shutting_down = self.shutting_down.clone();
        let scale_handler = self.handler.clone();
        let scale_active_workers = self.active_workers.clone();
        let scale_next_worker_id = self.next_worker_id.clone();
        let scale_metrics = self.metrics_counters.clone();
        let scale_dead_letter = self.dead_letter.clone();
        let scale_config = self.config.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(scale_config.scale_check_interval_secs));
            loop {
                interval.tick().await;
                if scale_shutting_down.load(Ordering::Relaxed) {
                    break;
                }

                let sched = scale_scheduler.read().await;
                let total_pending = sched.total_pending() + sched.queued_count();
                let current_workers = sched.worker_count();
                drop(sched);

                if total_pending > scale_config.scale_up_threshold && current_workers < scale_config.max_workers {
                    let to_spawn = std::cmp::min(
                        (total_pending / scale_config.scale_up_threshold).max(1),
                        scale_config.max_workers - current_workers,
                    );
                    for _ in 0..to_spawn {
                        let id = scale_next_worker_id.fetch_add(1, Ordering::Relaxed);
                        let (tx, rx, pending) = {
                            let (tx, rx) = mpsc::channel::<Task>(256);
                            let pending = Arc::new(AtomicUsize::new(0));
                            (tx, rx, pending)
                        };
                        scale_scheduler.write().await.add_worker(id, tx, pending.clone());
                        scale_active_workers.fetch_add(1, Ordering::Relaxed);
                        worker::spawn_worker(
                            id,
                            scale_handler.clone(),
                            rx,
                            pending,
                            scale_metrics.clone(),
                            scale_dead_letter.clone(),
                        );
                        info!(id, "auto-scaled up worker");
                    }
                } else if total_pending == 0 && current_workers > scale_config.min_workers {
                    let to_remove = std::cmp::min(1, current_workers - scale_config.min_workers);
                    for _ in 0..to_remove {
                        let mut sched = scale_scheduler.write().await;
                        if let Some(id) = sched.last_worker_id() {
                            sched.remove_worker(id);
                            scale_active_workers.fetch_sub(1, Ordering::Relaxed);
                            info!(id, "auto-scaled down worker");
                        }
                    }
                }
            }
        });
    }

    pub async fn shutdown(&self) {
        info!("shutting down pool");
        self.shutting_down.store(true, Ordering::Relaxed);

        let timeout = Duration::from_secs(self.config.shutdown_timeout_secs);
        let start = std::time::Instant::now();

        while self.active_workers.load(Ordering::Relaxed) > 0 && start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if self.active_workers.load(Ordering::Relaxed) > 0 {
            warn!("shutdown timeout, forcing remaining workers to stop");
        }

        info!("pool shutdown complete");
    }
}

#[derive(Clone)]
pub struct PoolSubmitter {
    task_tx: mpsc::Sender<Task>,
    shutting_down: Arc<AtomicBool>,
}

impl PoolSubmitter {
    pub async fn submit(&self, task: Task) -> Result<(), SubmitError> {
        if self.shutting_down.load(Ordering::Relaxed) {
            return Err(SubmitError::ShuttingDown);
        }

        self.task_tx.send(task).await.map_err(|_| SubmitError::ShuttingDown)
    }

    pub fn try_submit(&self, task: Task) -> Result<(), SubmitError> {
        if self.shutting_down.load(Ordering::Relaxed) {
            return Err(SubmitError::ShuttingDown);
        }
        self.task_tx.try_send(task).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => SubmitError::QueueFull,
            mpsc::error::TrySendError::Closed(_) => SubmitError::ShuttingDown,
        })
    }
}
