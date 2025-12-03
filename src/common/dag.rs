use serde::{Deserialize, Serialize};

use super::types::{JobId, PartitionId, StageId, Task};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
    Read { uri: String, format: String },
    Map { script: String },
    Filter { predicate: String },
    Reduce { reducer: String },
    Join { on: String },
    Aggregate { aggregation: String },
    Identity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    pub id: String,
    pub operator: OperatorType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagSpecification {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<DagEdge>,
    pub partitions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stage {
    pub stage_id: StageId,
    pub nodes: Vec<DagNode>,
    pub partitions: usize,
}

impl DagSpecification {
    /// Naive stage splitter: each node is treated as an independent stage ordered by insertion.
    pub fn to_stages(&self) -> Vec<Stage> {
        self.ordered_nodes()
            .iter()
            .enumerate()
            .map(|(idx, node)| Stage {
                stage_id: idx as StageId,
                nodes: vec![node.clone()],
                partitions: self.partitions,
            })
            .collect()
    }

    /// Creates a simple list of tasks assuming each stage has the same number of partitions.
    pub fn materialize_tasks(&self, job_id: JobId) -> Vec<Task> {
        let Some((input_uri, input_format)) = self.read_source() else {
            return Vec::new();
        };
        let operators: Vec<OperatorType> = self
            .ordered_nodes()
            .into_iter()
            .map(|node| node.operator)
            .collect();
        if operators.is_empty() {
            return Vec::new();
        }

        let final_stage_id = operators.len().saturating_sub(1) as StageId;
        let mut tasks = Vec::new();
        for partition in 0..self.partitions {
            // Generate deterministic task IDs so they can be reconstructed after restarts.
            let stable_id_input = format!("{}-{}", final_stage_id, partition);
            let task_id = uuid::Uuid::new_v5(&job_id, stable_id_input.as_bytes());

            tasks.push(Task {
                task_id,
                job_id,
                stage_id: final_stage_id,
                attempt: 0,
                operator: operators.last().cloned().unwrap_or(OperatorType::Identity),
                operators: operators.clone(),
                partition: partition as PartitionId,
                input_uri: input_uri.clone(),
                input_format: input_format.clone(),
                total_partitions: self.partitions as PartitionId,
            });
        }
        tasks
    }

    fn ordered_nodes(&self) -> Vec<DagNode> {
        use std::collections::{HashMap, VecDeque};

        if self.edges.is_empty() {
            return self.nodes.clone();
        }

        let mut incoming: HashMap<String, usize> =
            self.nodes.iter().map(|n| (n.id.clone(), 0)).collect();
        let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
        for edge in &self.edges {
            *incoming.entry(edge.to.clone()).or_insert(0) += 1;
            outgoing
                .entry(edge.from.clone())
                .or_default()
                .push(edge.to.clone());
        }

        let mut queue = VecDeque::new();
        for (id, count) in &incoming {
            if *count == 0 {
                queue.push_back(id.clone());
            }
        }

        let mut ordered_ids = Vec::new();
        while let Some(id) = queue.pop_front() {
            ordered_ids.push(id.clone());
            if let Some(children) = outgoing.get(&id) {
                for child in children {
                    if let Some(counter) = incoming.get_mut(child) {
                        *counter = counter.saturating_sub(1);
                        if *counter == 0 {
                            queue.push_back(child.clone());
                        }
                    }
                }
            }
        }

        for node in &self.nodes {
            if !ordered_ids.iter().any(|id| id == &node.id) {
                ordered_ids.push(node.id.clone());
            }
        }

        ordered_ids
            .into_iter()
            .filter_map(|id| self.nodes.iter().find(|n| n.id == id).cloned())
            .collect()
    }

    fn read_source(&self) -> Option<(String, String)> {
        self.nodes.iter().find_map(|node| {
            if let OperatorType::Read { uri, format } = &node.operator {
                Some((uri.clone(), format.clone()))
            } else {
                None
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dag_spec() {
        let json = r#"
        {
            "nodes": [
                {"id": "read", "operator": {"Read": {"uri": "data/input.csv", "format": "csv"}}},
                {"id": "n1", "operator": {"Map": {"script": "x + 1"}}}
            ],
            "edges": [],
            "partitions": 2
        }
        "#;
        let dag: DagSpecification = serde_json::from_str(json).expect("should parse dag json");
        assert_eq!(dag.partitions, 2);
        assert_eq!(dag.nodes.len(), 2);

        let stages = dag.to_stages();
        assert_eq!(stages.len(), 2);
        assert_eq!(stages[0].partitions, 2);
        assert!(matches!(
            stages[0].nodes.first().map(|node| &node.operator),
            Some(OperatorType::Read { .. })
        ));
    }
}
