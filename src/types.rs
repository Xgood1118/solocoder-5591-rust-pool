use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for Priority {
    fn default() -> Self {
        Priority::Normal
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryPolicy {
    None,
    Immediate { max_retries: u32 },
    FixedDelay { max_retries: u32, delay_secs: u64 },
    ExponentialBackoff { max_retries: u32, base_delay_secs: u64, max_delay_secs: u64 },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy::None
    }
}

impl RetryPolicy {
    pub fn max_retries(&self) -> u32 {
        match self {
            RetryPolicy::None => 0,
            RetryPolicy::Immediate { max_retries } => *max_retries,
            RetryPolicy::FixedDelay { max_retries, .. } => *max_retries,
            RetryPolicy::ExponentialBackoff { max_retries, .. } => *max_retries,
        }
    }

    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self {
            RetryPolicy::None | RetryPolicy::Immediate { .. } => Duration::ZERO,
            RetryPolicy::FixedDelay { delay_secs, .. } => Duration::from_secs(*delay_secs),
            RetryPolicy::ExponentialBackoff { base_delay_secs, max_delay_secs, .. } => {
                let exp = 2u64.saturating_pow(attempt);
                let delay_ms = (*base_delay_secs) * 1000 * exp;
                Duration::from_millis(delay_ms).min(Duration::from_secs(*max_delay_secs))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScheduleStrategy {
    RoundRobin,
    LeastLoaded,
    Priority,
    DelayedQueue,
}

impl Default for ScheduleStrategy {
    fn default() -> Self {
        ScheduleStrategy::RoundRobin
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub id: Uuid,
    pub payload: Vec<u8>,
    pub priority: Priority,
    pub created_at: DateTime<Utc>,
    pub retry_policy: RetryPolicy,
    pub attempt: u32,
    pub execute_after: Option<DateTime<Utc>>,
}

impl Task {
    pub fn new(payload: Vec<u8>) -> Self {
        Task {
            id: Uuid::new_v4(),
            payload,
            priority: Priority::default(),
            created_at: Utc::now(),
            retry_policy: RetryPolicy::default(),
            attempt: 0,
            execute_after: None,
        }
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.execute_after = Some(Utc::now() + chrono::Duration::from_std(delay).unwrap_or(chrono::TimeDelta::MAX));
        self
    }

    pub fn is_ready(&self) -> bool {
        match self.execute_after {
            Some(t) => Utc::now() >= t,
            None => true,
        }
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
            .then_with(|| other.created_at.cmp(&self.created_at))
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobResult {
    Ok(Vec<u8>),
    Err(String),
    Panic(String),
}

impl JobResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, JobResult::Ok(_))
    }

    pub fn is_err(&self) -> bool {
        matches!(self, JobResult::Err(_) | JobResult::Panic(_))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReceipt {
    pub task_id: Uuid,
    pub result: JobResult,
    pub attempts: u32,
    pub completed_at: DateTime<Utc>,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub task: Task,
    pub last_error: String,
    pub attempts: u32,
    pub failed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub min_workers: usize,
    pub max_workers: usize,
    pub max_queue_size: usize,
    pub scale_up_threshold: usize,
    pub idle_timeout_secs: u64,
    pub scale_check_interval_secs: u64,
    pub shutdown_timeout_secs: u64,
    pub schedule_strategy: ScheduleStrategy,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            min_workers: 2,
            max_workers: 16,
            max_queue_size: 10000,
            scale_up_threshold: 100,
            idle_timeout_secs: 60,
            scale_check_interval_secs: 5,
            shutdown_timeout_secs: 30,
            schedule_strategy: ScheduleStrategy::RoundRobin,
        }
    }
}

#[derive(Debug)]
pub enum SubmitError {
    QueueFull,
    ShuttingDown,
}

impl std::fmt::Display for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubmitError::QueueFull => write!(f, "task queue is full"),
            SubmitError::ShuttingDown => write!(f, "pool is shutting down"),
        }
    }
}

impl std::error::Error for SubmitError {}
