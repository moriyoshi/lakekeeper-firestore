//! Phase 0 of the normalized-metadata plan (DESIGN.md): how table metadata grows with snapshots.
//! Needs no emulator; run in release for meaningful timings:
//!
//! ```sh
//! cargo test --release metadata_size -- --ignored --nocapture
//! ```

use std::{collections::HashMap, io::Write as _, time::Instant};

use lakekeeper::iceberg::spec::{
    MAIN_BRANCH, Operation, Snapshot, Summary, TableMetadata, TableMetadataBuilder,
};

use super::table::small;

/// A table after `n` appends, each a snapshot with a summary as pyiceberg writes one.
pub(super) fn appended(n: i64) -> TableMetadata {
    let mut builder = TableMetadataBuilder::new_from_metadata(small("gs://bucket/wh/ns/t"), None);
    let start = 1_750_000_000_000;
    for i in 1..=n {
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from(
                [
                    ("added-data-files", "1".to_string()),
                    ("added-records", "5000".to_string()),
                    ("added-files-size", "183456".to_string()),
                    ("changed-partition-count", "1".to_string()),
                    ("total-records", (5000 * i).to_string()),
                    ("total-files-size", (183_456 * i).to_string()),
                    ("total-data-files", i.to_string()),
                    ("total-delete-files", "0".to_string()),
                    ("total-position-deletes", "0".to_string()),
                    ("total-equality-deletes", "0".to_string()),
                ]
                .map(|(k, v)| (k.to_string(), v)),
            ),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(1_000_000_000 + i)
            .with_parent_snapshot_id((i > 1).then_some(1_000_000_000 + i - 1))
            .with_sequence_number(i)
            .with_timestamp_ms(start + i * 60_000)
            .with_manifest_list(format!(
                "gs://bucket/wh/ns/t/metadata/snap-{}-0-{}.avro",
                1_000_000_000 + i,
                uuid::Uuid::now_v7()
            ))
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        builder = builder.set_branch_snapshot(snapshot, MAIN_BRANCH).unwrap();
    }
    builder.build().unwrap().metadata
}

fn gzip(bytes: &[u8]) -> usize {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap().len()
}

#[test]
#[ignore = "a measurement, not a check; see the module docs"]
fn metadata_size() {
    println!(
        "| snapshots | file (JSON) | file (gzip) | parse | head part | per snapshot | per log entry |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- |");
    for n in [100, 1_000, 10_000] {
        let metadata = appended(n);
        let json = serde_json::to_vec(&metadata).unwrap();
        let parse = {
            let runs = 5;
            let started = Instant::now();
            for _ in 0..runs {
                let parsed: TableMetadata = serde_json::from_slice(&json).unwrap();
                assert_eq!(parsed.snapshots().len(), usize::try_from(n).unwrap());
            }
            started.elapsed() / runs
        };
        let mut value: serde_json::Value = serde_json::from_slice(&json).unwrap();
        let object = value.as_object_mut().unwrap();
        let snapshots = object.remove("snapshots").unwrap();
        let log = object.remove("snapshot-log").unwrap();
        let head = serde_json::to_vec(&value).unwrap().len();
        let snapshots = snapshots.as_array().unwrap();
        let per_snapshot = snapshots
            .iter()
            .map(|s| serde_json::to_vec(s).unwrap().len())
            .sum::<usize>()
            / snapshots.len();
        let log = log.as_array().unwrap();
        let per_log = log
            .iter()
            .map(|s| serde_json::to_vec(s).unwrap().len())
            .sum::<usize>()
            / log.len();
        println!(
            "| {n} | {} KB | {} KB | {:.1} ms | {} B | {per_snapshot} B | {per_log} B |",
            json.len() / 1024,
            gzip(&json) / 1024,
            parse.as_secs_f64() * 1000.0,
            head,
        );
    }
}
