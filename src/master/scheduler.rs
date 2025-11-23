use std::collections::VecDeque;

use crate::common::types::{Task, WorkerId, WorkerInfo};

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

        let min_load = workers
            .iter()
            .map(|w| self.compute_load(w))
            .min()
            .unwrap_or(u32::MAX);
        let least_loaded: Vec<WorkerId> = workers
            .iter()
            .filter(|w| self.compute_load(w) == min_load)
            .map(|w| w.id)
            .collect();

        // Round-robin tie-breaker: walk the queue until we find an eligible worker.
        let mut selected = None;
        for _ in 0..self.queue.len() {
            if let Some(id) = self.queue.pop_front() {
                let eligible = least_loaded.contains(&id);
                self.queue.push_back(id);
                if eligible {
                    selected = Some(id);
                    break;
                }
            }
        }

        // Fallback to simple round-robin
        if selected.is_none() {
            if let Some(id) = self.queue.pop_front() {
                self.queue.push_back(id);
                selected = Some(id);
            }
        }

        self.cursor = (self.cursor + 1) % workers.len();
        selected
    }
}

impl RoundRobinScheduler {
    fn compute_load(&self, worker: &WorkerInfo) -> u32 {
        let tasks = worker.metrics.tasks_in_flight as u32;
        let cpu = worker.metrics.cpu_pct as u32;
        tasks * 2 + cpu
    }
}
