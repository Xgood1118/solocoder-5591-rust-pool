use crate::types::{DeadLetterEntry, Task};
use chrono::Utc;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DeadLetterQueue {
    entries: Arc<RwLock<VecDeque<DeadLetterEntry>>>,
    max_size: usize,
}

impl DeadLetterQueue {
    pub fn new(max_size: usize) -> Self {
        DeadLetterQueue {
            entries: Arc::new(RwLock::new(VecDeque::new())),
            max_size,
        }
    }

    pub async fn push(&self, task: Task, last_error: String, attempts: u32) {
        let mut entries = self.entries.write().await;
        if entries.len() >= self.max_size {
            entries.pop_front();
        }
        entries.push_back(DeadLetterEntry {
            task,
            last_error,
            attempts,
            failed_at: Utc::now(),
        });
    }

    pub async fn list(&self) -> Vec<DeadLetterEntry> {
        let entries = self.entries.read().await;
        entries.iter().cloned().collect()
    }

    pub async fn len(&self) -> usize {
        let entries = self.entries.read().await;
        entries.len()
    }

    pub async fn is_empty(&self) -> bool {
        let entries = self.entries.read().await;
        entries.is_empty()
    }

    pub async fn requeue_all(&self) -> Vec<Task> {
        let mut entries = self.entries.write().await;
        let tasks: Vec<Task> = entries.drain(..).map(|e| {
            let mut t = e.task;
            t.attempt = 0;
            t
        }).collect();
        tasks
    }

    pub async fn requeue_by_id(&self, task_id: uuid::Uuid) -> Option<Task> {
        let mut entries = self.entries.write().await;
        let idx = entries.iter().position(|e| e.task.id == task_id)?;
        let entry = entries.remove(idx)?;
        let mut task = entry.task;
        task.attempt = 0;
        Some(task)
    }

    pub async fn clear(&self) {
        let mut entries = self.entries.write().await;
        entries.clear();
    }
}
