//! What the projection costs in the operations Firestore bills: document reads, writes and
//! deletes per catalog operation, next to what the store itself spends, and the size and index
//! entries of the projection's documents. Runs alone, on the emulator, with the production
//! inline limit:
//!
//! ```sh
//! FIRESTORE_EMULATOR_HOST=localhost:28080 cargo test projection_cost -- --ignored --nocapture
//! ```

use std::sync::atomic::Ordering;

use firestore::gcloud_sdk::google::firestore::v1::value::ValueType;
use lakekeeper::{
    iceberg::{
        NamespaceIdent, TableIdent,
        spec::{MAIN_BRANCH, Snapshot, SnapshotReference, TableMetadata},
    },
    service::{TableId, TabularId},
};

use super::{
    db,
    metadata_size::appended,
    projection_store::{commit, run_task},
    table::{create, namespace},
};
use crate::store::{
    docs::{IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, IDX_TABLES},
    fs::{Document, Query, Txn, Value, cost, string_value},
    projection_store::INLINE_WRITES_IN_TESTS,
    search::{Command, Measure, execute},
    tabular,
};

/// Reads, writes and deletes: of the store's own collections, and of the projection's.
#[derive(Debug, Default, Clone, Copy)]
struct Bill {
    store: [u64; 3],
    projection: [u64; 3],
}

fn bill() -> Bill {
    let mut out = Bill::default();
    for ((collection, operation), n) in cost::take() {
        let side = if collection.starts_with("idx_") {
            &mut out.projection
        } else {
            &mut out.store
        };
        side[match operation {
            "read" => 0,
            "write" => 1,
            _ => 2,
        }] += n;
    }
    out
}

fn row(operation: &str, snapshots: usize, b: Bill) {
    let [sr, sw, sd] = b.store;
    let [pr, pw, pd] = b.projection;
    println!("| {operation} | {snapshots} | {sr} / {sw} / {sd} | {pr} / {pw} / {pd} |");
}

/// Firestore's storage size of a value (its documented rules).
fn value_size(v: &Value) -> usize {
    match v.value_type.as_ref() {
        Some(ValueType::StringValue(s)) => s.len() + 1,
        Some(ValueType::BytesValue(b)) => b.len(),
        Some(
            ValueType::IntegerValue(_) | ValueType::DoubleValue(_) | ValueType::TimestampValue(_),
        ) => 8,
        Some(ValueType::BooleanValue(_) | ValueType::NullValue(_)) | None => 1,
        Some(ValueType::ReferenceValue(r)) => r.len() + 1,
        Some(ValueType::GeoPointValue(_)) => 16,
        Some(ValueType::ArrayValue(a)) => a.values.iter().map(value_size).sum(),
        Some(ValueType::MapValue(m)) => m
            .fields
            .iter()
            .map(|(k, v)| k.len() + 1 + value_size(v))
            .sum(),
        // Query-only values; never stored.
        Some(_) => 0,
    }
}

/// The document's storage size: name (collection and ID, plus 16) and fields, plus 32.
fn doc_size(doc: &Document) -> usize {
    let name: usize = doc
        .name
        .split_once("/documents/")
        .map_or(0, |(_, rest)| rest.split('/').map(|s| s.len() + 1).sum());
    name + 16
        + doc
            .fields
            .iter()
            .map(|(k, v)| k.len() + 1 + value_size(v))
            .sum::<usize>()
        + 32
}

/// Single-field index entries: two per indexed scalar (ascending and descending), one per array
/// element (array-contains), and the scalars inside maps; `exempt` fields have none.
fn index_entries(fields: &std::collections::HashMap<String, Value>, exempt: &[&str]) -> usize {
    fn of(v: &Value) -> usize {
        match v.value_type.as_ref() {
            Some(ValueType::ArrayValue(a)) => a.values.len(),
            Some(ValueType::MapValue(m)) => m.fields.values().map(of).sum(),
            _ => 2,
        }
    }
    fields
        .iter()
        .filter(|(k, _)| !exempt.contains(&k.as_str()))
        .map(|(_, v)| of(v))
        .sum()
}

#[tokio::test]
#[ignore = "a measurement, not a check; see the module docs"]
#[allow(clippy::too_many_lines)]
async fn projection_cost() {
    let Some(db) = db().await else { return };
    INLINE_WRITES_IN_TESTS.store(usize::MAX, Ordering::Relaxed);
    println!("Firestore operations, reads / writes / deletes:\n");
    println!("| operation | snapshots | store | projection |");
    println!("| --- | --- | --- | --- |");
    let mut sizes: Vec<String> = Vec::new();
    for n in [10_usize, 100, 1_000] {
        let ns = namespace(&db).await;
        let name = format!("t{n}");
        let location = |v: &str| format!("gs://b/m/{name}-{v}.json");
        let base = appended(i64::try_from(n).unwrap());
        // `appended` builds at a fixed location; each table needs its own.
        let v0: TableMetadata =
            lakekeeper::iceberg::spec::TableMetadataBuilder::new_from_metadata(base, None)
                .set_location(format!("gs://b/wh/ns/{name}"))
                .build()
                .unwrap()
                .metadata;
        let id = v0.uuid();
        let current = v0.current_snapshot_id().unwrap();
        bill();

        create(&db, ns, &name, Some(&location("0")), &v0)
            .await
            .unwrap();
        let tasks = run_task(&db).await;
        row(
            if tasks {
                "create (by a task)"
            } else {
                "create"
            },
            n,
            bill(),
        );

        // One more append: a minute after the last, and not before the metadata log, which
        // uses the wall clock.
        let last = v0.current_snapshot().unwrap();
        let next = Snapshot::builder()
            .with_snapshot_id(current + 1)
            .with_parent_snapshot_id(Some(current))
            .with_sequence_number(last.sequence_number() + 1)
            .with_timestamp_ms(
                (last.timestamp_ms() + 60_000).max(chrono::Utc::now().timestamp_millis()),
            )
            .with_manifest_list(format!("gs://b/m/{name}-snap-next.avro"))
            .with_summary(last.summary().clone())
            .with_schema_id(0)
            .build();
        let v1 = commit(
            &db,
            ns.0,
            (&v0, Some(&location("0"))),
            &location("1"),
            |b| b.set_branch_snapshot(next.clone(), MAIN_BRANCH).unwrap(),
        )
        .await;
        row("append", n, bill());

        let wider = lakekeeper::iceberg::spec::Schema::builder()
            .with_fields(
                v1.current_schema()
                    .as_struct()
                    .fields()
                    .iter()
                    .cloned()
                    .chain([std::sync::Arc::new(
                        lakekeeper::iceberg::spec::NestedField::optional(
                            100,
                            "added",
                            lakekeeper::iceberg::spec::Type::Primitive(
                                lakekeeper::iceberg::spec::PrimitiveType::Long,
                            ),
                        ),
                    )])
                    .collect::<Vec<_>>(),
            )
            .build()
            .unwrap();
        let v2 = commit(
            &db,
            ns.0,
            (&v1, Some(&location("1"))),
            &location("2"),
            |b| b.add_current_schema(wider).unwrap(),
        )
        .await;
        row("add a column", n, bill());

        let retention = v2.refs()[MAIN_BRANCH].retention.clone();
        let v3 = commit(
            &db,
            ns.0,
            (&v2, Some(&location("2"))),
            &location("3"),
            |b| {
                b.set_ref(
                    "audit",
                    SnapshotReference {
                        snapshot_id: current,
                        retention,
                    },
                )
                .unwrap()
            },
        )
        .await;
        row("create a branch", n, bill());

        let oldest: Vec<i64> = {
            let mut ids: Vec<(i64, i64)> = v3
                .snapshots()
                .map(|s| (s.sequence_number(), s.snapshot_id()))
                .collect();
            ids.sort_unstable();
            ids.iter()
                .take((n / 10).max(1))
                .map(|(_, id)| *id)
                .collect()
        };
        commit(
            &db,
            ns.0,
            (&v3, Some(&location("3"))),
            &location("4"),
            |b| b.remove_snapshots(&oldest),
        )
        .await;
        row(&format!("expire {} snapshots", oldest.len()), n, bill());

        // Sizes and index entries of the stored documents, before they go.
        let mut txn = Txn::latest(db.clone());
        for (collection, exempt) in [
            (IDX_TABLES, &["location", "refs"][..]),
            (
                IDX_SNAPSHOTS,
                &[
                    "summary_other",
                    "manifest_list",
                    "statistics",
                    "partition_statistics",
                ][..],
            ),
            (IDX_FIELDS, &["name", "path"][..]),
            (IDX_PARTITION_SPECS, &["names"][..]),
        ] {
            let docs = if collection == IDX_TABLES {
                let name = txn.db().doc_name(IDX_TABLES, &id.to_string());
                txn.get(&name).await.unwrap().into_iter().collect()
            } else {
                txn.query(&Query::new(collection).eq("table_id", string_value(id.to_string())))
                    .await
                    .unwrap()
            };
            if docs.is_empty() {
                continue;
            }
            let count = docs.len();
            let bytes: usize = docs.iter().map(doc_size).sum();
            let entries: usize = docs.iter().map(|d| index_entries(&d.fields, exempt)).sum();
            sizes.push(format!(
                "| {n} | {collection} | {count} | {} | {} |",
                bytes / count,
                entries / count
            ));
        }
        bill();

        for (label, command) in [
            (
                "search history",
                Command::History {
                    warehouse: ns.0.to_string(),
                    table: format!("ns.{name}"),
                },
            ),
            (
                "search largest",
                Command::Largest {
                    warehouse: Some(ns.0.to_string()),
                    by: Measure::Records,
                    limit: 20,
                    include_deleted: false,
                },
            ),
        ] {
            execute(&db, command).await.unwrap();
            row(label, n, bill());
        }

        let tabular_id = TabularId::Table(TableId::from(id));
        let mut txn = Txn::read_write(db.clone());
        tabular::rename(
            ns.0,
            tabular_id,
            ns.1,
            ns.1,
            &TableIdent::new(NamespaceIdent::new("ns".to_string()), name.clone()),
            &TableIdent::new(NamespaceIdent::new("ns".to_string()), format!("{name}r")),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
        row("rename", n, bill());

        let mut txn = Txn::read_write(db.clone());
        tabular::mark_as_deleted(ns.0, tabular_id, false, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        row("soft delete", n, bill());

        let mut txn = Txn::read_write(db.clone());
        tabular::drop(ns.0, tabular_id, true, &mut txn)
            .await
            .unwrap();
        txn.commit().await.unwrap();
        let tasks = run_task(&db).await;
        row(if tasks { "drop (by a task)" } else { "drop" }, n, bill());
    }
    println!("\nStored projection documents, after the operations above:\n");
    println!("| snapshots | collection | documents | bytes each | index entries each |");
    println!("| --- | --- | --- | --- | --- |");
    for line in sizes {
        println!("{line}");
    }
}
