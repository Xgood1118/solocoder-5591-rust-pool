use crate::types::{ScheduleStrategy, Task};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub(crate) struct WorkerSlot {
    pub id: usize,
    pub tx: mpsc::Sender<Task>,
    pub pending_count: Arc<AtomicUsize>,
}

pub struct Scheduler {
    strategy: ScheduleStrategy,
    workers: Vec<WorkerSlot>,
    rr_index: AtomicUsize,
    priority_queue: BinaryHeap<Task>,
    delayed_tasks: Vec<Task>,
}

impl Scheduler {
    pub fn new(strategy: ScheduleStrategy) -> Self {
        Scheduler {
            strategy,
            workers: Vec::new(),
            rr_index: AtomicUsize::new(0),
            priority_queue: BinaryHeap::new(),
            delayed_tasks: Vec::new(),
        }
    }

    pub fn add_worker(&mut self, id: usize, tx: mpsc::Sender<Task>, pending_count: Arc<AtomicUsize>) {
        self.workers.push(WorkerSlot { id, tx, pending_count });
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
        match self.strategy {
            ScheduleStrategy::RoundRobin => self.dispatch_round_robin(task).await,
            ScheduleStrategy::LeastLoaded => self.dispatch_least_loaded(task).await,
            ScheduleStrategy::Priority | ScheduleStrategy::DelayedQueue => {
                self.enqueue(task);
                self.flush_queues().await;
                true
            }
        }
    }

    async fn dispatch_round_robin(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = &self.workers[idx];
        worker.pending_count.fetch_add(1, Ordering::Relaxed);
        worker.tx.send(task).await.is_ok()
    }

    async fn dispatch_least_loaded(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        let worker = self.workers.iter().min_by_key(|w| w.pending_count.load(Ordering::Relaxed)).unwrap();
        worker.pending_count.fetch_add(1, Ordering::Relaxed);
        worker.tx.send(task).await.is_ok()
    }

    pub async fn flush_queues(&mut self) {
        self.flush_delayed();
        while let Some(task) = self.priority_queue.pop() {
            if !self.dispatch_to_least_loaded(task).await {
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

    async fn dispatch_to_least_loaded(&self, task: Task) -> bool {
        if self.workers.is_empty() {
            return false;
        }
        let worker = self.workers.iter().min_by_key(|w| w.pending_count.load(Ordering::Relaxed)).unwrap();
        worker.pending_count.fetch_add(1, Ordering::Relaxed);
        worker.tx.send(task).await.is_ok()
    }

    pub fn total_pending(&self) -> usize {
        self.workers.iter().map(|w| w.pending_count.load(Ordering::Relaxed)).sum()
    }

    pub fn queued_count(&self) -> usize {
        self.priority_queue.len() + self.delayed_tasks.len()
    }

    pub fn last_worker_id(&self) -> Option<usize> {
        self.workers.last().map(|w| w.id)
    }
}
