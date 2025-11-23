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
        self.nodes
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
        let mut tasks = Vec::new();
        for stage in self.to_stages() {
            for partition in 0..stage.partitions {
                if let Some(node) = stage.nodes.first() {
                    // Generate deterministic task IDs so they can be reconstructed after restarts.
                    let stable_id_input = format!("{}-{}", stage.stage_id, partition);
                    let task_id = uuid::Uuid::new_v5(&job_id, stable_id_input.as_bytes());

                    tasks.push(Task {
                        task_id,
                        job_id,
                        stage_id: stage.stage_id,
                        attempt: 0,
                        operator: node.operator.clone(),
                        partition: partition as PartitionId,
                        input_uri: input_uri.clone(),
                        input_format: input_format.clone(),
                        total_partitions: self.partitions as PartitionId,
                    });
                }
            }
        }
        tasks
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
        assert_eq!(dag.nodes.len(), 1);
    }
}
