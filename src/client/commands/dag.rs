use crate::client::commands::fetch_json;
use crate::client::types::job::{DagEdge, DagNode, DagSpec, JobResponse};
use crate::client::AppContext;
use colored::Colorize;
use std::collections::{HashMap, HashSet, VecDeque};

pub async fn run(ctx: &AppContext, job_id: &str) -> anyhow::Result<()> {
    let url = ctx.url(&format!("/api/v1/jobs/{}", job_id));
    let job: JobResponse = fetch_json(ctx, &url).await?;
    if let Some(dag) = job.dag.clone() {
        render_dag(&dag);
    } else if !job.stages.is_empty() {
        let nodes: Vec<DagNode> = job
            .stages
            .iter()
            .map(|s| DagNode {
                id: s
                    .stage_id
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "stage".into()),
                operator: serde_json::json!({"Stage": s.status}),
            })
            .collect();
        render_dag(&DagSpec {
            nodes,
            edges: Vec::new(),
        });
    } else {
        println!("{}", "No DAG information available".yellow());
    }
    Ok(())
}

fn render_dag(dag: &DagSpec) {
    if dag.edges.is_empty() {
        let line = dag
            .nodes
            .iter()
            .map(|n| format!("[{}]", n.id))
            .collect::<Vec<_>>()
            .join(" → ");
        println!("{}", line);
        return;
    }

    let order = topo_sort(&dag.nodes, &dag.edges);
    let mut lines = Vec::new();
    for (idx, node) in order.iter().enumerate() {
        lines.push(format!("[ {} ]", node));
        if idx + 1 < order.len() {
            lines.push("↓".into());
        }
    }
    for l in lines {
        println!("{}", l);
    }
}

fn topo_sort(nodes: &[DagNode], edges: &[DagEdge]) -> Vec<String> {
    let mut incoming: HashMap<String, usize> = nodes.iter().map(|n| (n.id.clone(), 0)).collect();
    let mut outgoing: HashMap<String, Vec<String>> = HashMap::new();
    for e in edges {
        outgoing
            .entry(e.from.clone())
            .or_default()
            .push(e.to.clone());
        if let Some(c) = incoming.get_mut(&e.to) {
            *c += 1;
        } else {
            incoming.insert(e.to.clone(), 1);
        }
        incoming.entry(e.from.clone()).or_insert(0);
    }

    let mut q: VecDeque<String> = incoming
        .iter()
        .filter(|(_, &v)| v == 0)
        .map(|(k, _)| k.clone())
        .collect();
    let mut seen = HashSet::new();
    let mut order = Vec::new();
    while let Some(n) = q.pop_front() {
        if seen.insert(n.clone()) {
            order.push(n.clone());
            if let Some(children) = outgoing.get(&n) {
                for ch in children {
                    if let Some(entry) = incoming.get_mut(ch) {
                        *entry = entry.saturating_sub(1);
                        if *entry == 0 {
                            q.push_back(ch.clone());
                        }
                    }
                }
            }
        }
    }
    // fallback append unvisited
    for n in nodes {
        if !order.contains(&n.id) {
            order.push(n.id.clone());
        }
    }
    order
}
