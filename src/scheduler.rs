use crate::types::{ScheduleStrategy, Task};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) struct WorkerSlot {
    pub id: usize,
    pub tx: mpsc::Sender<Task>,
}

pub struct Scheduler {
    strategy: ScheduleStrategy,
    workers: Vec<WorkerSlot>,
    rr_index: AtomicUsize,
    priority_queue: BinaryHeap<Task>,
    delayed_tasks: Vec<Task>,
    global_pending: Arc<AtomicUsize>,
}

impl Scheduler {
    pub fn new(strategy: ScheduleStrategy, global_pending: Arc<AtomicUsize>) -> Self {
        Scheduler {
            strategy,
            workers: Vec::new(),
            rr_index: AtomicUsize::new(0),
            priority_queue: BinaryHeap::new(),
            delayed_tasks: Vec::new(),
            global_pending,
        }
    }

    pub fn add_worker(&mut self, id: usize, tx: mpsc::Sender<Task>) {
        self.workers.push(WorkerSlot { id, tx });
    }

    pub fn remove_worker(&mut self, id: usize) {
        self.workers.retain(|w| w.id != id);
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn has_workers(&self) -> bool {
        !self.workers.is_empty()
    }

    pub fn enqueue(&mut self, task: Task) {
        match self.strategy {
            ScheduleStrategy::Priority => {
                self.priority_queue.push(task);
            }
            ScheduleStrategy::DelayedQueue => {
                if task.is_ready() {
                    self.priority_queue.push(task);
                } else {
                    self.delayed_tasks.push(task);
                }
            }
            _ => {}
        }
    }

    pub async fn dispatch(&mut self, task: Task) -> bool {
        self.global_pending.fetch_add(1, Ordering::Relaxed);
        let ok = match self.strategy {
            ScheduleStrategy::RoundRobin => self.dispatch_round_robin(task).await,
            ScheduleStrategy::LeastLoaded => self.dispatch_least_loaded(task).await,
            ScheduleStrategy::Priority | ScheduleStrategy::DelayedQueue => {
                self.enqueue(task);
                self.flush_queues().await;
                true
            }
        };
        if !ok {
            self.global_pending.fetch_sub(1, Ordering::Relaxed);
        }
        ok
    }

    async fn dispatch_round_robin(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = &self.workers[idx];
        worker.tx.send(task).await.is_ok()
    }

    async fn dispatch_least_loaded(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        let min_len = self.workers.iter()
            .map(|w| w.tx.max_capacity() - w.tx.capacity())
            .min()
            .unwrap_or(usize::MAX);
        if let Some(worker) = self.workers.iter()
            .find(|w| (w.tx.max_capacity() - w.tx.capacity()) == min_len)
        {
            worker.tx.send(task).await.is_ok()
        } else {
            false
        }
    }

    pub async fn flush_queues(&mut self) {
        self.flush_delayed();
        while let Some(task) = self.priority_queue.pop() {
            if !self.dispatch_to_any(task).await {
                break;
            }
        }
    }

    fn flush_delayed(&mut self) {
        let now = chrono::Utc::now();
        let ready: Vec<Task> = self.delayed_tasks.iter()
            .filter(|t| t.is_ready() || t.execute_after.map_or(true, |et| now >= et))
            .cloned()
            .collect();
        self.delayed_tasks.retain(|t| !t.is_ready() && t.execute_after.map_or(false, |et| now < et));
        for task in ready {
            self.priority_queue.push(task);
        }
    }

    async fn dispatch_to_any(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        for worker in &self.workers {
            if worker.tx.capacity() > 0 {
                if worker.tx.send(task).await.is_ok() {
                    return true;
                } else {
                    return false;
                }
            }
        }
        self.workers[0].tx.send(task).await.is_ok()
    }

    pub fn total_pending(&self) -> usize {
        self.global_pending.load(Ordering::Relaxed)
    }

    pub fn queued_count(&self) -> usize {
        self.priority_queue.len() + self.delayed_tasks.len()
    }

    pub fn worker_ids(&self) -> Vec<usize> {
        self.workers.iter().map(|w| w.id).collect()
    }
}
