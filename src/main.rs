use task_pool::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("task_pool=info")
        .init();

    let handler: TaskHandler = Arc::new(|payload| {
        let s = String::from_utf8_lossy(&payload);
        if s.contains("panic") {
            panic!("intentional panic for testing");
        }
        if s.contains("fail") {
            return Err(format!("intentional failure for: {}", s));
        }
        let result = format!("processed: {}", s);
        Ok(result.into_bytes())
    });

    let config = PoolConfig {
        min_workers: 2,
        max_workers: 8,
        max_queue_size: 5000,
        scale_up_threshold: 50,
        idle_timeout_secs: 5,
        scale_check_interval_secs: 2,
        shutdown_timeout_secs: 10,
        schedule_strategy: ScheduleStrategy::Priority,
    };

    let mut pool = Pool::new(config, handler);
    let submitter = pool.submitter();
    let metrics = pool.metrics();
    let dead_letter = pool.dead_letter();
    let shutting_down = pool.shutting_down();

    pool.start().await;

    let submitter_clone = submitter.clone();
    tokio::spawn(async move {
        for i in 0..200u32 {
            let priority = match i % 4 {
                0 => Priority::Critical,
                1 => Priority::High,
                2 => Priority::Normal,
                _ => Priority::Low,
            };
            let task = Task::new(format!("task-{}", i).into_bytes())
                .with_priority(priority)
                .with_retry_policy(RetryPolicy::ExponentialBackoff {
                    max_retries: 3,
                    base_delay_secs: 0,
                    max_delay_secs: 5,
                });
            if let Err(e) = submitter_clone.submit(task).await {
                error!("submit failed: {}", e);
            }
        }
    });

    let fail_submitter = submitter.clone();
    tokio::spawn(async move {
        for i in 0..5u32 {
            let task = Task::new(format!("fail-task-{}", i).into_bytes())
                .with_priority(Priority::Low)
                .with_retry_policy(RetryPolicy::FixedDelay {
                    max_retries: 2,
                    delay_secs: 0,
                });
            if let Err(e) = fail_submitter.submit(task).await {
                error!("submit failed: {}", e);
            }
        }
    });

    let delayed_submitter = submitter.clone();
    tokio::spawn(async move {
        for i in 0..5u32 {
            let task = Task::new(format!("delayed-task-{}", i).into_bytes())
                .with_priority(Priority::Normal)
                .with_delay(Duration::from_secs(3));
            if let Err(e) = delayed_submitter.submit(task).await {
                error!("submit failed: {}", e);
            }
        }
    });

    let metrics_handle = metrics.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let json = metrics_handle.to_json().await;
            info!("metrics:\n{}", json);
        }
    });

    info!("pool running, waiting for SIGTERM or Ctrl+C...");
    let sd = shutting_down.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler")
                .recv()
                .await;
            info!("received SIGTERM");
        }
        #[cfg(not(unix))]
        {
            tokio::signal::ctrl_c().await.expect("failed to listen for ctrl+c");
            info!("received Ctrl+C");
        }
        sd.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    while !shutting_down.load(std::sync::atomic::Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pool.shutdown().await;

    let dl_entries = dead_letter.list().await;
    if !dl_entries.is_empty() {
        warn!("dead letter queue has {} entries:", dl_entries.len());
        for entry in &dl_entries {
            warn!("  task_id={}, error={}, attempts={}", entry.task.id, entry.last_error, entry.attempts);
        }
    }

    let final_metrics = metrics.to_json().await;
    info!("final metrics:\n{}", final_metrics);
}
