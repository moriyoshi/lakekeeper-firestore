//! The composite indexes and single-field exemptions this catalog needs, as
//! `firestore.indexes.json` for a collection prefix (`lakekeeper-firestore search indexes`).
//! The emulator needs none of them; Firestore rejects a query whose index is missing.
//!
//! Queries with only equality filters need no composite index: Firestore merges single-field
//! indexes for them. The rest are listed here, next to the query that needs them.

use serde_json::{Value, json};

use crate::store::docs::{
    IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, IDX_TABLES, IDX_VIEWS, NAMESPACES, TABULARS,
    TASK_SCHEDULES, TASKS,
};

#[derive(Debug, Clone)]
enum F {
    Asc(&'static str),
    Desc(String),
    Contains(&'static str),
}

/// The measures `search largest` orders by.
pub(super) const MEASURES: [&str; 4] = [
    "total_files_size",
    "total_records",
    "total_data_files",
    "total_delete_files",
];

fn indexes() -> Vec<(&'static str, Vec<F>)> {
    use F::{Asc, Contains};
    let desc = |path: &str| F::Desc(path.to_string());
    let mut out = vec![
        // The store: namespace listing.
        (
            NAMESPACES,
            vec![Asc("warehouse_id"), Asc("parent_key"), Asc("created_at")],
        ),
        // Tabular listings, of all, or tables or views only, in a warehouse or a namespace.
        (TABULARS, vec![Asc("warehouse_id"), Asc("created_at")]),
        (
            TABULARS,
            vec![Asc("warehouse_id"), Asc("namespace_id"), Asc("created_at")],
        ),
        (
            TABULARS,
            vec![Asc("warehouse_id"), Asc("typ"), Asc("created_at")],
        ),
        (
            TABULARS,
            vec![
                Asc("warehouse_id"),
                Asc("namespace_id"),
                Asc("typ"),
                Asc("created_at"),
            ],
        ),
        // Tabulars stored below a location.
        (
            TABULARS,
            vec![Asc("warehouse_id"), Contains("location_prefixes")],
        ),
        // Due tasks of a queue.
        (
            TASKS,
            vec![Asc("queue_name"), Asc("status"), Asc("scheduled_for")],
        ),
        // Due schedules.
        (TASK_SCHEDULES, vec![Asc("enabled"), Asc("next_run_at")]),
        // search snapshots: newest first, in a warehouse or a table, of any or one operation;
        // since and until filter the same field.
        (
            IDX_SNAPSHOTS,
            vec![Asc("warehouse_id"), desc("timestamp_ms")],
        ),
        (
            IDX_SNAPSHOTS,
            vec![Asc("warehouse_id"), Asc("operation"), desc("timestamp_ms")],
        ),
        (IDX_SNAPSHOTS, vec![Asc("table_id"), desc("timestamp_ms")]),
        (
            IDX_SNAPSHOTS,
            vec![Asc("table_id"), Asc("operation"), desc("timestamp_ms")],
        ),
        // search history and schema-changes: a table's snapshots in order.
        (IDX_SNAPSHOTS, vec![Asc("table_id"), Asc("sequence_number")]),
        // search partitioned: by transform, in default specs or all, in a warehouse or all;
        // by column, in one table.
        (
            IDX_PARTITION_SPECS,
            vec![Asc("default"), Contains("transforms")],
        ),
        (
            IDX_PARTITION_SPECS,
            vec![Asc("warehouse_id"), Contains("transforms")],
        ),
        (
            IDX_PARTITION_SPECS,
            vec![Asc("warehouse_id"), Asc("default"), Contains("transforms")],
        ),
        (
            IDX_PARTITION_SPECS,
            vec![Asc("table_id"), Contains("source_field_ids")],
        ),
        (
            IDX_PARTITION_SPECS,
            vec![
                Asc("table_id"),
                Asc("default"),
                Contains("source_field_ids"),
            ],
        ),
    ];
    // search largest: by each measure, live tables or all, in a warehouse or all.
    for measure in MEASURES {
        let path = format!("current_summary.{measure}");
        out.push((IDX_TABLES, vec![Asc("active"), desc(&path)]));
        out.push((IDX_TABLES, vec![Asc("warehouse_id"), desc(&path)]));
        out.push((
            IDX_TABLES,
            vec![Asc("warehouse_id"), Asc("active"), desc(&path)],
        ));
    }
    out
}

/// Fields that no query filters or orders by, kept out of every index to save index writes.
const EXEMPT: [(&str, &str); 10] = [
    (IDX_SNAPSHOTS, "summary_other"),
    (IDX_SNAPSHOTS, "manifest_list"),
    (IDX_SNAPSHOTS, "statistics"),
    (IDX_SNAPSHOTS, "partition_statistics"),
    (IDX_FIELDS, "name"),
    (IDX_FIELDS, "path"),
    (IDX_PARTITION_SPECS, "names"),
    (IDX_TABLES, "location"),
    (IDX_TABLES, "refs"),
    (IDX_VIEWS, "location"),
];

/// `firestore.indexes.json` for collections named with `prefix`.
pub(crate) fn firestore_indexes(prefix: &str) -> Value {
    let indexes: Vec<Value> = indexes()
        .into_iter()
        .map(|(collection, fields)| {
            let fields: Vec<Value> = fields
                .into_iter()
                .map(|f| match f {
                    F::Asc(path) => json!({"fieldPath": path, "order": "ASCENDING"}),
                    F::Desc(path) => json!({"fieldPath": path, "order": "DESCENDING"}),
                    F::Contains(path) => json!({"fieldPath": path, "arrayConfig": "CONTAINS"}),
                })
                .collect();
            json!({
                "collectionGroup": format!("{prefix}{collection}"),
                "queryScope": "COLLECTION",
                "fields": fields,
            })
        })
        .collect();
    let overrides: Vec<Value> = EXEMPT
        .into_iter()
        .map(|(collection, path)| {
            json!({
                "collectionGroup": format!("{prefix}{collection}"),
                "fieldPath": path,
                "indexes": [],
            })
        })
        .collect();
    json!({"indexes": indexes, "fieldOverrides": overrides})
}
