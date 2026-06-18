use crate::types::{JobResult, Task};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub type TaskHandler = Arc<dyn Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync>;

pub fn spawn_worker(
    worker_id: usize,
    handler: TaskHandler,
    mut task_rx: mpsc::Receiver<Task>,
    _pending_count: Arc<AtomicUsize>,
    metrics_completed: Arc<crate::metrics::MetricsCounters>,
    dead_letter: Arc<crate::dead_letter::DeadLetterQueue>,
) {
    tokio::spawn(async move {
        info!(worker_id, "worker started");
        loop {
            match task_rx.recv().await {
                Some(task) => {
                    let receipt = execute_task(task, &handler, worker_id).await;
                    
                    match &receipt.result {
                        JobResult::Ok(_) => {
                            metrics_completed.total_completed.fetch_add(1, Ordering::Relaxed);
                            metrics_completed.total_elapsed_us.fetch_add(receipt.elapsed_ms * 1000, Ordering::Relaxed);
                            info!(worker_id, task_id = %receipt.task_id, "task completed in {}ms", receipt.elapsed_ms);
                        }
                        JobResult::Err(msg) => {
                            metrics_completed.total_failed.fetch_add(1, Ordering::Relaxed);
                            warn!(worker_id, task_id = %receipt.task_id, "task failed: {}", msg);
                            if receipt.attempts > receipt.max_attempts {
                                dead_letter.push(receipt.original_task, msg.clone(), receipt.attempts).await;
                            }
                        }
                        JobResult::Panic(msg) => {
                            metrics_completed.total_panicked.fetch_add(1, Ordering::Relaxed);
                            error!(worker_id, task_id = %receipt.task_id, "task panicked: {}", msg);
                            dead_letter.push(receipt.original_task, msg.clone(), receipt.attempts).await;
                        }
                    }
                }
                None => {
                    info!(worker_id, "worker channel closed, exiting");
                    break;
                }
            }
        }
        info!(worker_id, "worker stopped");
    });
}

struct TaskResult {
    task_id: uuid::Uuid,
    result: JobResult,
    attempts: u32,
    max_attempts: u32,
    elapsed_ms: u64,
    original_task: Task,
}

async fn execute_task(task: Task, handler: &TaskHandler, worker_id: usize) -> TaskResult {
    let max_retries = task.retry_policy.max_retries();
    let mut attempt = task.attempt;
    let start = Instant::now();
    let original_task = task.clone();
    let mut last_err;
    
    loop {
        attempt += 1;
        let result = run_handler(&task, handler, worker_id);
        
        match &result {
            JobResult::Ok(_) => {
                return TaskResult {
                    task_id: task.id,
                    result,
                    attempts: attempt,
                    max_attempts: max_retries,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    original_task,
                };
            }
            JobResult::Err(e) | JobResult::Panic(e) => {
                last_err = e.clone();
                if attempt > max_retries {
                    break;
                }
                let delay = task.retry_policy.delay_for_attempt(attempt - 1);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }
    
    TaskResult {
        task_id: task.id,
        result: JobResult::Err(last_err),
        attempts: attempt,
        max_attempts: max_retries,
        elapsed_ms: start.elapsed().as_millis() as u64,
        original_task,
    }
}

fn run_handler(task: &Task, handler: &TaskHandler, worker_id: usize) -> JobResult {
    let payload = task.payload.clone();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        handler(payload)
    }));
    
    match result {
        Ok(Ok(data)) => JobResult::Ok(data),
        Ok(Err(msg)) => JobResult::Err(msg),
        Err(panic_val) => {
            let panic_msg = if let Some(s) = panic_val.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = panic_val.downcast_ref::<String>() {
                s.clone()
            } else {
                "unknown panic".to_string()
            };
            error!(worker_id, task_id = %task.id, "caught panic: {}", panic_msg);
            JobResult::Panic(panic_msg)
        }
    }
}
