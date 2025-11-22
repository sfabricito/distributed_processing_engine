use std::collections::VecDeque;

use crate::common::types::{Task, WorkerId};

use super::registry::Registry;

pub trait SchedulerStrategy: Send {
    fn select_worker(&mut self, registry: &Registry, task: &Task) -> Option<WorkerId>;
}

#[derive(Debug, Default)]
pub struct RoundRobinScheduler {
    cursor: usize,
    queue: VecDeque<WorkerId>,
}

impl SchedulerStrategy for RoundRobinScheduler {
    fn select_worker(&mut self, registry: &Registry, _task: &Task) -> Option<WorkerId> {
        let workers = registry.available_workers();
        if workers.is_empty() {
            return None;
        }

        if self.queue.is_empty() || self.queue.len() != workers.len() {
            self.queue = workers.iter().map(|w| w.id).collect();
        }

        let worker_id = if let Some(id) = self.queue.pop_front() {
            self.queue.push_back(id);
            id
        } else {
            workers[self.cursor % workers.len()].id
        };
        self.cursor = (self.cursor + 1) % workers.len();
        Some(worker_id)
    }
}
