use crate::dead_letter::DeadLetterQueue;
use crate::metrics::{Metrics, MetricsCounters};
use crate::scheduler::Scheduler;
use crate::types::{PoolConfig, SubmitError, Task};
use crate::worker;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

pub struct Pool {
    config: PoolConfig,
    handler: worker::TaskHandler,
    task_tx: Option<mpsc::Sender<Task>>,
    task_rx: Option<mpsc::Receiver<Task>>,
    scheduler: Arc<RwLock<Scheduler>>,
    metrics_counters: Arc<MetricsCounters>,
    dead_letter: Arc<DeadLetterQueue>,
    metrics: Arc<Metrics>,
    shutting_down: Arc<AtomicBool>,
    next_worker_id: Arc<AtomicUsize>,
    active_workers: Arc<AtomicUsize>,
    global_pending: Arc<AtomicUsize>,
    last_activity: Arc<std::sync::RwLock<Instant>>,
}

impl Pool {
    pub fn new(config: PoolConfig, handler: worker::TaskHandler) -> Self {
        let (task_tx, task_rx) = mpsc::channel::<Task>(config.max_queue_size);
        let global_pending = Arc::new(AtomicUsize::new(0));
        let scheduler = Arc::new(RwLock::new(Scheduler::new(config.schedule_strategy, global_pending.clone())));
        let metrics_counters = Arc::new(MetricsCounters::default());
        let dead_letter = Arc::new(DeadLetterQueue::new(config.max_queue_size));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let next_worker_id = Arc::new(AtomicUsize::new(0));
        let active_workers = Arc::new(AtomicUsize::new(0));
        let last_activity = Arc::new(std::sync::RwLock::new(Instant::now()));
        let metrics = Arc::new(Metrics::new(
            metrics_counters.clone(),
            scheduler.clone(),
            active_workers.clone(),
            dead_letter.clone(),
        ));

        Pool {
            config,
            handler,
            task_tx: Some(task_tx),
            task_rx: Some(task_rx),
            scheduler,
            metrics_counters,
            dead_letter,
            metrics,
            shutting_down,
            next_worker_id,
            active_workers,
            global_pending,
            last_activity,
        }
    }

    pub fn submitter(&self) -> PoolSubmitter {
        PoolSubmitter {
            task_tx: self.task_tx.as_ref().unwrap().clone(),
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

    fn spawn_worker(&self) {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel::<Task>(256);
        let scheduler = self.scheduler.clone();
        let handler = self.handler.clone();
        let active_workers = self.active_workers.clone();
        let global_pending = self.global_pending.clone();
        let last_activity = self.last_activity.clone();
        let metrics = self.metrics_counters.clone();
        let dead_letter = self.dead_letter.clone();

        tokio::spawn(async move {
            scheduler.write().await.add_worker(id, tx);
            worker::spawn_worker(
                id,
                handler,
                rx,
                global_pending,
                active_workers,
                last_activity,
                metrics,
                dead_letter,
            );
            info!(id, "spawned worker");
        });
    }

    pub async fn start(&mut self) {
        for _ in 0..self.config.min_workers {
            self.spawn_worker();
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
        let scale_active_workers = self.active_workers.clone();
        let scale_global_pending = self.global_pending.clone();
        let scale_last_activity = self.last_activity.clone();
        let scale_config = self.config.clone();
        let scale_self_handle = self.handler.clone();
        let scale_next_worker_id = self.next_worker_id.clone();
        let scale_metrics = self.metrics_counters.clone();
        let scale_dead_letter = self.dead_letter.clone();

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

                let idle_for = scale_last_activity.read().map(|t| t.elapsed()).unwrap_or(Duration::ZERO);

                if total_pending > scale_config.scale_up_threshold && current_workers < scale_config.max_workers {
                    let target_workers = std::cmp::min(
                        (total_pending / scale_config.scale_up_threshold).max(current_workers + 1),
                        scale_config.max_workers,
                    );
                    let to_spawn = target_workers - current_workers;
                    info!(to_spawn, total_pending, current_workers, "scaling up workers");
                    for _ in 0..to_spawn {
                        let id = scale_next_worker_id.fetch_add(1, Ordering::Relaxed);
                        let (tx, rx) = mpsc::channel::<Task>(256);
                        let scheduler_clone = scale_scheduler.clone();
                        let handler_clone = scale_self_handle.clone();
                        let active_clone = scale_active_workers.clone();
                        let pending_clone = scale_global_pending.clone();
                        let activity_clone = scale_last_activity.clone();
                        let metrics_clone = scale_metrics.clone();
                        let dl_clone = scale_dead_letter.clone();
                        tokio::spawn(async move {
                            scheduler_clone.write().await.add_worker(id, tx);
                            worker::spawn_worker(
                                id,
                                handler_clone,
                                rx,
                                pending_clone,
                                active_clone,
                                activity_clone,
                                metrics_clone,
                                dl_clone,
                            );
                            info!(id, "auto-scaled up worker");
                        });
                    }
                } else if total_pending == 0
                    && current_workers > scale_config.min_workers
                    && idle_for >= Duration::from_secs(scale_config.idle_timeout_secs)
                {
                    let to_remove = current_workers - scale_config.min_workers;
                    info!(to_remove, current_workers, ?idle_for, "scaling down workers due to idle timeout");

                    let sched = scale_scheduler.write().await;
                    let ids: Vec<usize> = sched.worker_ids().into_iter().rev().take(to_remove).collect();
                    drop(sched);

                    for id in ids {
                        scale_scheduler.write().await.remove_worker(id);
                        info!(id, "auto-scaled down worker");
                    }
                }
            }
        });
    }

    pub async fn shutdown(&mut self) {
        info!("shutting down pool");
        self.shutting_down.store(true, Ordering::Relaxed);

        info!("dropping main task_tx to close dispatcher");
        drop(self.task_tx.take());

        info!("clearing scheduler to drop all worker senders");
        let mut sched = self.scheduler.write().await;
        *sched = Scheduler::new(self.config.schedule_strategy, self.global_pending.clone());
        drop(sched);

        let timeout = Duration::from_secs(self.config.shutdown_timeout_secs);
        let start = Instant::now();

        info!("waiting for in-flight tasks to complete...");
        while self.active_workers.load(Ordering::Relaxed) > 0 && start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if self.active_workers.load(Ordering::Relaxed) > 0 {
            let remaining = self.active_workers.load(Ordering::Relaxed);
            warn!(remaining, "shutdown timeout, forcing remaining workers to stop");
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
