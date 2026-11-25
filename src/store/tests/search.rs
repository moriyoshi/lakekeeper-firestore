//! The search commands on the emulator, over a warehouse built through the store.

use std::{collections::HashMap, sync::Arc};

use lakekeeper::{
    WarehouseId,
    iceberg::spec::{
        MAIN_BRANCH, NestedField, PrimitiveType, Schema, TableMetadata, Transform, Type,
        UnboundPartitionField, UnboundPartitionSpec,
    },
    service::{CreateNamespaceRequest, NamespaceId, NamespaceIdent, TableId, TabularId},
};
use serde_json::{Value, json};

use super::{
    db,
    projection_store::{commit, snapshot},
    table::{create, namespace, small},
};
use crate::store::{
    fs::{Db, Txn},
    namespace as ns_store,
    search::{Command, Measure, Output, execute, firestore_indexes},
    tabular,
};

fn wider() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "n",
                Type::Primitive(PrimitiveType::Long),
            )),
        ])
        .build()
        .unwrap()
}

fn bucket() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .add_partition_field(
            UnboundPartitionField::builder()
                .source_ids(vec![1])
                .name("id_bucket".to_string())
                .transform(Transform::Bucket(4))
                .build()
                .unwrap(),
        )
        .unwrap()
        .build()
}

/// Warehouse `wh` with:
/// - `ns.events`: three appends, the last with a wider schema, partitioned by `bucket[4](id)`;
/// - `other.small`: one append, property `owner=ops`;
/// - `other.gone`: one append, soft-deleted.
async fn fixture(db: &Db) -> (WarehouseId, TableMetadata) {
    let ns = namespace(db).await;
    let other = NamespaceId::new_random();
    let mut txn = Txn::read_write(db.clone());
    ns_store::create(
        ns.0,
        other,
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new("other".to_string()),
            properties: None,
        },
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let e0 = small("gs://b/wh/ns/events");
    create(db, ns, "events", Some("gs://b/m/e0.json"), &e0)
        .await
        .unwrap();
    let e1 = commit(
        db,
        ns.0,
        (&e0, Some("gs://b/m/e0.json")),
        "gs://b/m/e1.json",
        |b| {
            b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
                .unwrap()
                .set_branch_snapshot(snapshot(2, Some(1), 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    let e2 = commit(
        db,
        ns.0,
        (&e1, Some("gs://b/m/e1.json")),
        "gs://b/m/e2.json",
        |b| {
            b.add_current_schema(wider())
                .unwrap()
                .add_default_partition_spec(bucket())
                .unwrap()
                .set_branch_snapshot(snapshot(3, Some(2), 1), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;

    for (name, id) in [("small", 4), ("gone", 5)] {
        let m0 = small(&format!("gs://b/wh/other/{name}"));
        let (first, second) = (
            format!("gs://b/m/{name}0.json"),
            format!("gs://b/m/{name}1.json"),
        );
        create(db, (ns.0, other), name, Some(&first), &m0)
            .await
            .unwrap();
        let properties = HashMap::from([("owner".to_string(), "ops".to_string())]);
        commit(db, ns.0, (&m0, Some(&first)), &second, |b| {
            b.set_properties(properties)
                .unwrap()
                .set_branch_snapshot(snapshot(id, None, 0), MAIN_BRANCH)
                .unwrap()
        })
        .await;
    }
    let gone = tabular::read_by_name(&mut Txn::latest(db.clone()), ns.0, other, "gone")
        .await
        .unwrap()
        .unwrap();
    let mut txn = Txn::read_write(db.clone());
    tabular::mark_as_deleted(
        ns.0,
        TabularId::Table(TableId::from(gone.tabular_id)),
        false,
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    (ns.0, e2)
}

fn strings(values: Vec<Value>) -> Vec<String> {
    values
        .into_iter()
        .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string))
        .collect()
}

async fn run(db: &Db, command: Command) -> Output {
    execute(db, command).await.unwrap()
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn searches_find_what_was_committed() {
    let Some(db) = db().await else { return };
    let (warehouse_id, events) = fixture(&db).await;
    let wh = "wh".to_string();
    let snapshots =
        |table: Option<&str>, conditions: Vec<&str>, include_deleted| Command::Snapshots {
            warehouse: wh.clone(),
            table: table.map(str::to_string),
            since: None,
            until: None,
            operation: None,
            conditions: conditions.into_iter().map(str::to_string).collect(),
            limit: 50,
            include_deleted,
        };

    // Snapshots: newest first, live tables only unless asked; by table; by summary number.
    let out = run(&db, snapshots(None, vec![], false)).await;
    assert_eq!(
        out.column("snapshot_id"),
        [json!(4), json!(3), json!(2), json!(1)]
    );
    let out = run(&db, snapshots(None, vec![], true)).await;
    assert_eq!(
        strings(out.column("table"))[0],
        "other.gone (deleted)",
        "{out:?}"
    );
    let out = run(
        &db,
        snapshots(Some("ns.events"), vec!["total-records>=20"], false),
    )
    .await;
    assert_eq!(out.column("snapshot_id"), [json!(3), json!(2)]);
    let out = run(
        &db,
        snapshots(Some(&events.uuid().to_string()), vec![], false),
    )
    .await;
    assert_eq!(out.rows.len(), 3);
    assert!(
        execute(&db, snapshots(None, vec!["bogus>1"], false))
            .await
            .is_err()
    );

    // History: commit order, with the ref on the newest.
    let out = run(
        &db,
        Command::History {
            warehouse: warehouse_id.to_string(),
            table: "ns.events".to_string(),
        },
    )
    .await;
    assert_eq!(out.column("snapshot_id"), [json!(1), json!(2), json!(3)]);
    assert_eq!(strings(out.column("refs")), ["", "", "main"]);

    // Columns, by name across tables, by type, by current schema.
    let columns = |name: &str, r#type: Option<&str>, include_deleted| Command::Columns {
        name: name.to_string(),
        warehouse: Some(wh.clone()),
        path: false,
        r#type: r#type.map(str::to_string),
        current: false,
        kind: None,
        limit: 100,
        include_deleted,
    };
    let mut tables = strings(run(&db, columns("ID", None, false)).await.column("table"));
    tables.sort();
    assert_eq!(tables, ["ns.events", "other.small"]);
    assert_eq!(
        run(&db, columns("id", None, true)).await.rows.len(),
        3,
        "with the soft-deleted table"
    );
    assert_eq!(
        strings(
            run(&db, columns("n", Some("long"), false))
                .await
                .column("table")
        ),
        ["ns.events"]
    );
    assert_eq!(
        run(&db, columns("n", Some("string"), false))
            .await
            .rows
            .len(),
        0
    );

    // Schema changes: `n` added with schema 1.
    let out = run(
        &db,
        Command::SchemaChanges {
            warehouse: wh.clone(),
            table: "ns.events".to_string(),
        },
    )
    .await;
    assert_eq!(
        strings(out.column("path")),
        ["id", "n"],
        "added in schemas 0 and 1"
    );
    assert_eq!(out.column("schema_id"), [json!(0), json!(1)]);

    // Partitioning, by transform and by column.
    let partitioned = |transform: Option<&str>, column: Option<&str>| Command::Partitioned {
        warehouse: None,
        transform: transform.map(str::to_string),
        column: column.map(str::to_string),
        all_specs: false,
        limit: 100,
        include_deleted: false,
    };
    let out = run(&db, partitioned(Some("bucket[4]"), None)).await;
    assert_eq!(strings(out.column("table")), ["ns.events"]);
    assert_eq!(strings(out.column("fields")), ["id_bucket=bucket[4](id)"]);
    let out = run(&db, partitioned(None, Some("Id"))).await;
    assert_eq!(strings(out.column("table")), ["ns.events"]);
    assert_eq!(
        run(&db, partitioned(Some("identity"), Some("id")))
            .await
            .rows
            .len(),
        0
    );

    // Properties, by value and by key, of live tabulars unless asked.
    let properties = |value: Option<&str>, include_deleted| Command::Properties {
        key: "owner".to_string(),
        value: value.map(str::to_string),
        warehouse: Some(wh.clone()),
        limit: 100,
        include_deleted,
    };
    assert_eq!(
        strings(
            run(&db, properties(Some("ops"), false))
                .await
                .column("name")
        ),
        ["other.small"]
    );
    assert_eq!(run(&db, properties(None, true)).await.rows.len(), 2);
    assert_eq!(
        run(&db, properties(Some("nobody"), true)).await.rows.len(),
        0
    );

    // The largest tables by records: events has 30, small 40.
    let out = run(
        &db,
        Command::Largest {
            warehouse: Some(wh.clone()),
            by: Measure::Records,
            limit: 20,
            include_deleted: false,
        },
    )
    .await;
    assert_eq!(strings(out.column("table")), ["other.small", "ns.events"]);
    assert_eq!(out.column("total_records"), [json!(40), json!(30)]);
}

/// The checked-in `firestore.indexes.json` is the generated one for no prefix.
#[test]
fn checked_in_indexes_are_current() {
    let file = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/firestore.indexes.json"
    ))
    .expect("firestore.indexes.json; regenerate with `lakekeeper-firestore search indexes`");
    let file: Value = serde_json::from_str(&file).unwrap();
    assert_eq!(file, firestore_indexes("", false));
    let prefixed = firestore_indexes("p_", false);
    assert!(
        prefixed["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|i| i["collectionGroup"].as_str().unwrap().starts_with("p_"))
    );
    // The snapshot summary is kept out of indexes unless asked for.
    let summary_exempt = |indexes: &Value| {
        indexes["fieldOverrides"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["collectionGroup"] == "idx_snapshots" && o["fieldPath"] == "summary")
    };
    assert!(summary_exempt(&file));
    assert!(!summary_exempt(&firestore_indexes("", true)));
}
