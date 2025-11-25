use serde::{Deserialize, Serialize};

use super::types::{JobId, PartitionId, StageId, Task};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorType {
    Read(ReadOpConfig),
    Map(MapOpConfig),
    Filter(FilterOpConfig),
    Reduce(ReduceOpConfig),
    Join(JoinOpConfig),
    Aggregate(AggregateOpConfig),
    FlatMap(FlatMapOpConfig),
    Identity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOpConfig {
    pub uri: String,
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapOpConfig {
    pub script: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOpConfig {
    pub predicate: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReduceOpConfig {
    pub key: Option<String>,
    pub func: String,
    pub target_col: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOpConfig {
    pub on: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateOpConfig {
    pub aggregation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatMapOpConfig {
    pub func: String,
    pub input_col: Option<String>,
    pub target_col: Option<String>,
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
if let OperatorType::Read(cfg) = &node.operator {
    Some((cfg.uri.clone(), cfg.format.clone()))
} else {
    None
}
        })
    }
        pub fn set_read_uri(&mut self, uri: String) -> anyhow::Result<()> {
        for node in self.nodes.iter_mut() {
            if let OperatorType::Read(cfg) = &mut node.operator {
                cfg.uri = uri.clone();
                return Ok(());
            }
        }
        anyhow::bail!("DAG does not contain a Read operator to set input");
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
    }

    #[test]
    fn parses_flatmap_and_reduce_config() {
        let json = r#"
        {
            "nodes": [
                {"id": "fm", "operator": {"FlatMap": {"func": "tokenize", "input_col": "text", "target_col": "word"}}},
                {"id": "rd", "operator": {"Reduce": {"key": "word", "func": "sum", "target_col": "count"}}}
            ],
            "edges": [{"from": "fm", "to": "rd"}],
            "partitions": 1
        }
        "#;
        let dag: DagSpecification = serde_json::from_str(json).expect("should parse dag json");
        assert_eq!(dag.nodes.len(), 2);
        assert!(matches!(
            dag.nodes[0].operator,
            OperatorType::FlatMap { .. }
        ));
        assert!(matches!(dag.nodes[1].operator, OperatorType::Reduce { .. }));
    }
}
