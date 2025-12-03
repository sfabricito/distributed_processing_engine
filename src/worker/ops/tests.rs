#[cfg(test)]
mod tests {
    use super::super::flat_map::FlatMapOp;
    use super::super::reduce::{ReduceByKeyOp, ReduceOp};
    use super::super::{ExecutableOp, PartitionData};
    use serde_json::json;
    use uuid::Uuid;

    fn partition(records: Vec<serde_json::Value>) -> PartitionData {
        let spill = std::env::temp_dir().join(format!("dpe-test-{}", Uuid::new_v4()));
        PartitionData::from_records(0, usize::MAX, spill, records).expect("partition")
    }

    #[test]
    fn flat_map_identity() {
        let op = FlatMapOp {
            func: "identity".into(),
        };
        let input = partition(vec![json!({"a": 1})]);
        let out = op.execute(vec![input]).expect("flat_map");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["a"], json!(1));
    }

    #[test]
    fn flat_map_split() {
        let op = FlatMapOp {
            func: "split:tags".into(),
        };
        let input = partition(vec![json!({"tags": "a,b c"})]);
        let out = op.execute(vec![input]).expect("flat_map");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 3);
        assert!(recs.iter().any(|r| r["tags"] == json!("a")));
        assert!(recs.iter().any(|r| r["tags"] == json!("b")));
        assert!(recs.iter().any(|r| r["tags"] == json!("c")));
    }

    #[test]
    fn flat_map_explode_array() {
        let op = FlatMapOp {
            func: "explode_array:vals".into(),
        };
        let input = partition(vec![json!({"vals": [1,2,3]})]);
        let out = op.execute(vec![input]).expect("flat_map");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 3);
        assert!(recs.iter().any(|r| r["vals"] == json!(1)));
    }

    #[test]
    fn flat_map_json_expand() {
        let op = FlatMapOp {
            func: "json_expand".into(),
        };
        let input = partition(vec![json!({"a": 1, "b": 2})]);
        let out = op.execute(vec![input]).expect("flat_map");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 2);
        assert!(recs.iter().any(|r| r.get("a").is_some()));
    }

    #[test]
    fn reduce_sum() {
        let op = ReduceOp {
            reducer: "sum:value".into(),
        };
        let input = partition(vec![json!({"value": 2}), json!({"value": 3})]);
        let out = op.execute(vec![input]).expect("reduce");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["sum_value"], json!(5.0));
    }

    #[test]
    fn reduce_count() {
        let op = ReduceOp {
            reducer: "count".into(),
        };
        let input = partition(vec![json!({"v": 1}), json!({"v": 2})]);
        let out = op.execute(vec![input]).expect("reduce");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs[0]["count"], json!(2));
    }

    #[test]
    fn reduce_by_key_sum() {
        let op = ReduceByKeyOp {
            key: "k".into(),
            reducer: "sum:v".into(),
        };
        let input = partition(vec![
            json!({"k": "a", "v": 1}),
            json!({"k": "a", "v": 2}),
            json!({"k": "b", "v": 3}),
        ]);
        let out = op.execute(vec![input]).expect("reduce_by_key");
        let (_, _, recs) = out.into_iter().next().unwrap().into_parts().unwrap();
        assert_eq!(recs.len(), 2);
        let a = recs.iter().find(|r| r["k"] == json!("a")).unwrap();
        assert_eq!(a["sum_v"], json!(3.0));
        let b = recs.iter().find(|r| r["k"] == json!("b")).unwrap();
        assert_eq!(b["sum_v"], json!(3.0));
    }

    #[test]
    fn reduce_by_key_missing_key_errors() {
        let op = ReduceByKeyOp {
            key: "k".into(),
            reducer: "sum:v".into(),
        };
        let input = partition(vec![json!({"v": 1})]);
        let err = op.execute(vec![input]).unwrap_err();
        assert!(format!("{err}").contains("missing key"));
    }

    // Shuffle tests
    use super::super::shuffle::ShuffleByKeyOp;

    #[test]
    fn shuffle_single_partition_groups_keys() {
        let op = ShuffleByKeyOp {
            key: "k".into(),
            total_partitions: 2,
        };
        let input = partition(vec![
            json!({"k": "a", "v": 1}),
            json!({"k": "b", "v": 2}),
            json!({"k": "a", "v": 3}),
        ]);
        let out = op.execute(vec![input]).expect("shuffle");
        assert_eq!(out.len(), 2);
        let mut counts = vec![0usize; 2];
        for p in out {
            let pid = p.partition_id;
            let (_, _, recs) = p.into_parts().unwrap();
            counts[pid] = recs.len();
        }
        assert_eq!(counts.iter().sum::<usize>(), 3);
    }

    #[test]
    fn shuffle_multiple_partitions_merges_keys() {
        let op = ShuffleByKeyOp {
            key: "k".into(),
            total_partitions: 3,
        };
        let p1 = partition(vec![json!({"k": "x", "v": 1})]);
        let p2 = partition(vec![json!({"k": "x", "v": 2})]);
        let out = op.execute(vec![p1, p2]).expect("shuffle");
        let mut total = 0;
        for p in out {
            let (_, _, recs) = p.into_parts().unwrap();
            total += recs.len();
        }
        assert_eq!(total, 2);
    }

    // Join tests
    use super::super::join::JoinOp;
    use crate::common::dag::JoinType;

    fn extract(out: Vec<PartitionData>) -> Vec<serde_json::Value> {
        let mut recs = Vec::new();
        for p in out {
            let (_, _, r) = p.into_parts().unwrap();
            recs.extend(r);
        }
        recs
    }

    #[test]
    fn inner_join_matches() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Inner,
        };
        let left = partition(vec![json!({"k": "a", "l": 1})]);
        let right = partition(vec![json!({"k": "a", "r": 2})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["left_l"], json!(1));
        assert_eq!(recs[0]["right_r"], json!(2));
    }

    #[test]
    fn inner_join_no_matches() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Inner,
        };
        let left = partition(vec![json!({"k": "a", "l": 1})]);
        let right = partition(vec![json!({"k": "b", "r": 2})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert!(recs.is_empty());
    }

    #[test]
    fn left_join_nulls() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Left,
        };
        let left = partition(vec![json!({"k": "a", "l": 1})]);
        let right = partition(vec![json!({"k": "b", "r": 2})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["left_l"], json!(1));
    }

    #[test]
    fn right_join_nulls() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Right,
        };
        let left = partition(vec![json!({"k": "a", "l": 1})]);
        let right = partition(vec![json!({"k": "b", "r": 2})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["right_r"], json!(2));
    }

    #[test]
    fn full_join_union() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Full,
        };
        let left = partition(vec![json!({"k": "a", "l": 1})]);
        let right = partition(vec![json!({"k": "b", "r": 2})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert_eq!(recs.len(), 2);
    }

    #[test]
    fn join_duplicate_keys() {
        let op = JoinOp {
            key: "k".into(),
            join_type: JoinType::Inner,
        };
        let left = partition(vec![json!({"k": "a", "l": 1}), json!({"k": "a", "l": 2})]);
        let right = partition(vec![json!({"k": "a", "r": 3}), json!({"k": "a", "r": 4})]);
        let out = op.execute(vec![left, right]).expect("join");
        let recs = extract(out);
        assert_eq!(recs.len(), 4);
    }
}
