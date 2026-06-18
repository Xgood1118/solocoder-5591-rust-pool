use crate::dead_letter::DeadLetterQueue;
use crate::scheduler::Scheduler;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct MetricsCounters {
    pub total_submitted: AtomicU64,
    pub total_completed: AtomicU64,
    pub total_failed: AtomicU64,
    pub total_panicked: AtomicU64,
    pub total_elapsed_us: AtomicU64,
}

#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub total_submitted: u64,
    pub total_completed: u64,
    pub total_failed: u64,
    pub total_panicked: u64,
    pub pending: usize,
    pub active_workers: usize,
    pub dead_letter_count: usize,
    pub avg_elapsed_ms: f64,
}

pub struct Metrics {
    counters: Arc<MetricsCounters>,
    scheduler: Arc<RwLock<Scheduler>>,
    active_workers: Arc<AtomicUsize>,
    dead_letter: Arc<DeadLetterQueue>,
}

impl Metrics {
    pub fn new(
        counters: Arc<MetricsCounters>,
        scheduler: Arc<RwLock<Scheduler>>,
        active_workers: Arc<AtomicUsize>,
        dead_letter: Arc<DeadLetterQueue>,
    ) -> Self {
        Metrics {
            counters,
            scheduler,
            active_workers,
            dead_letter,
        }
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        let total_submitted = self.counters.total_submitted.load(Ordering::Relaxed);
        let total_completed = self.counters.total_completed.load(Ordering::Relaxed);
        let total_failed = self.counters.total_failed.load(Ordering::Relaxed);
        let total_panicked = self.counters.total_panicked.load(Ordering::Relaxed);
        let total_elapsed_us = self.counters.total_elapsed_us.load(Ordering::Relaxed);

        let scheduler = self.scheduler.read().await;
        let pending = scheduler.total_pending() + scheduler.queued_count();
        drop(scheduler);

        let active_workers = self.active_workers.load(Ordering::Relaxed);
        let dead_letter_count = self.dead_letter.len().await;

        let avg_elapsed_ms = if total_completed > 0 {
            (total_elapsed_us as f64 / total_completed as f64) / 1000.0
        } else {
            0.0
        };

        MetricsSnapshot {
            total_submitted,
            total_completed,
            total_failed,
            total_panicked,
            pending,
            active_workers,
            dead_letter_count,
            avg_elapsed_ms,
        }
    }

    pub async fn to_json(&self) -> String {
        let snap = self.snapshot().await;
        serde_json::to_string_pretty(&snap).unwrap_or_else(|_| "{}".to_string())
    }
}
