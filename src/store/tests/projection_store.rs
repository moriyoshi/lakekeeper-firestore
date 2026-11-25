//! The stored projection (phase 2) on the emulator: kept equal to the projection of the current
//! metadata by create and commit, and in line with the table through rename, soft deletion,
//! undrop and drop.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use lakekeeper::{
    WarehouseId,
    api::iceberg::v1::namespace::NamespaceDropFlags,
    iceberg::{
        NamespaceIdent, TableIdent,
        spec::{
            MAIN_BRANCH, NestedField, Operation, PrimitiveType, Schema, Snapshot, Summary,
            TableMetadata, TableMetadataBuilder, Type,
        },
    },
    server::tables::calculate_diffs,
    service::{Location, NamespaceId, TableCommit, TableId, TabularId},
};

use super::{
    db, put_file,
    table::{create, namespace, small},
};
use crate::store::{
    FirestoreBackend,
    docs::{IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, IDX_TABLES, TASK_LOGS, TaskLogDoc},
    fs::{Db, Query, Txn, from_doc, string_value},
    projection::{Projection, TableIdentity, Write, project},
    projection_store,
    projection_task::{self, ProjectionTask},
    table, tabular,
};

/// The stored projection of `table_id`, or `None` if it has no table document.
async fn stored(db: &Db, table_id: uuid::Uuid) -> Option<Projection> {
    let mut txn = Txn::latest(db.clone());
    let name = txn.db().doc_name(IDX_TABLES, &table_id.to_string());
    let table = from_doc(&txn.get(&name).await.unwrap()?).unwrap();
    let mut parts = async |collection| {
        txn.query(&Query::new(collection).eq("table_id", string_value(table_id.to_string())))
            .await
            .unwrap()
    };
    let snapshots = parts(IDX_SNAPSHOTS).await;
    let fields = parts(IDX_FIELDS).await;
    let specs = parts(IDX_PARTITION_SPECS).await;
    Some(Projection {
        table,
        snapshots: snapshots
            .iter()
            .map(|d| from_doc::<crate::store::projection::IdxSnapshot>(d).unwrap())
            .map(|s| (s.snapshot_id, s))
            .collect(),
        fields: fields
            .iter()
            .map(|d| from_doc::<crate::store::projection::IdxField>(d).unwrap())
            .map(|f| (f.field_id, f))
            .collect(),
        specs: specs
            .iter()
            .map(|d| from_doc::<crate::store::projection::IdxPartitionSpec>(d).unwrap())
            .map(|s| (s.spec_id, s))
            .collect(),
    })
}

/// Whether any projection document of `table_id` is left.
pub(super) async fn any_stored(db: &Db, table_id: uuid::Uuid) -> bool {
    let mut txn = Txn::latest(db.clone());
    let name = txn.db().doc_name(IDX_TABLES, &table_id.to_string());
    if txn.get(&name).await.unwrap().is_some() {
        return true;
    }
    for collection in [IDX_SNAPSHOTS, IDX_FIELDS, IDX_PARTITION_SPECS] {
        let query = Query::new(collection).eq("table_id", string_value(table_id.to_string()));
        if !txn.query(&query).await.unwrap().is_empty() {
            return true;
        }
    }
    false
}

fn identity(
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    metadata: &TableMetadata,
    name: &str,
    active: bool,
) -> TableIdentity {
    TableIdentity {
        warehouse_id,
        namespace_id,
        table_id: metadata.uuid(),
        name: name.to_string(),
        active,
    }
}

/// Commit what `f` builds on `previous`, as the core does: with the diffs and the updates.
pub(super) async fn commit(
    db: &Db,
    warehouse_id: WarehouseId,
    previous: (&TableMetadata, Option<&str>),
    new_location: &str,
    f: impl FnOnce(TableMetadataBuilder) -> TableMetadataBuilder,
) -> TableMetadata {
    let built = f(TableMetadataBuilder::new_from_metadata(
        previous.0.clone(),
        previous.1.map(ToString::to_string),
    ))
    .build()
    .unwrap();
    put_file(warehouse_id, new_location, &built.metadata);
    let commit = TableCommit {
        diffs: calculate_diffs(
            &built.metadata,
            previous.0,
            1,
            built.expired_metadata_logs.len(),
        ),
        new_metadata: Arc::new(built.metadata.clone()),
        new_metadata_location: Location::from_str(new_location).unwrap(),
        previous_metadata_location: previous.1.map(|l| Location::from_str(l).unwrap()),
        updates: Arc::new(built.changes),
    };
    let mut txn = Txn::read_write(db.clone());
    table::commit(warehouse_id, [commit], &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    built.metadata
}

pub(super) fn snapshot(id: i64, parent: Option<i64>, schema_id: i32) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis() + id * 1000)
        .with_manifest_list(format!("gs://b/m/snap-{id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([(
                "total-records".to_string(),
                (10 * id).to_string(),
            )]),
        })
        .with_schema_id(schema_id)
        .build()
}

fn wider_schema() -> Schema {
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

#[tokio::test]
async fn commits_keep_the_stored_projection() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/p");
    let id = v0.uuid();
    let me = |m: &TableMetadata| project(&identity(ns, m, "p", true), m);
    create(&db, ns, "p", Some("gs://b/m/p0.json"), &v0)
        .await
        .unwrap();
    assert_eq!(stored(&db, id).await, Some(me(&v0)));

    let v1 = commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/p0.json")),
        "gs://b/m/p1.json",
        |b| {
            b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    let v2 = commit(
        &db,
        ns.0,
        (&v1, Some("gs://b/m/p1.json")),
        "gs://b/m/p2.json",
        |b| {
            b.add_current_schema(wider_schema())
                .unwrap()
                .set_branch_snapshot(snapshot(2, Some(1), 1), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    assert_eq!(stored(&db, id).await, Some(me(&v2)));
    assert_eq!(stored(&db, id).await.unwrap().fields[&2].r#type, "long");

    // Removing a schema reads the stored fields.
    let v3 = commit(
        &db,
        ns.0,
        (&v2, Some("gs://b/m/p2.json")),
        "gs://b/m/p3.json",
        |b| b.remove_snapshots(&[1]).remove_schemas(&[0]).unwrap(),
    )
    .await;
    assert_eq!(stored(&db, id).await, Some(me(&v3)));
}

/// A staged table has no projection until its first commit stores the whole of it.
#[tokio::test]
async fn staged_tables_are_projected_from_their_first_commit() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/s");
    create(&db, ns, "s", None, &v0).await.unwrap();
    assert!(!any_stored(&db, v0.uuid()).await);

    let v1 = commit(&db, ns.0, (&v0, None), "gs://b/m/s1.json", |b| {
        b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
            .unwrap()
    })
    .await;
    assert_eq!(
        stored(&db, v0.uuid()).await,
        Some(project(&identity(ns, &v1, "s", true), &v1))
    );
}

#[tokio::test]
async fn renames_soft_deletion_and_drops_reach_the_projection() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/r");
    let id = TabularId::Table(TableId::from(v0.uuid()));
    create(&db, ns, "r", Some("gs://b/m/r0.json"), &v0)
        .await
        .unwrap();
    let v1 = commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/r0.json")),
        "gs://b/m/r1.json",
        |b| {
            b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;

    let source = TableIdent::new(NamespaceIdent::new("ns".to_string()), "r".to_string());
    let destination = TableIdent::new(NamespaceIdent::new("ns".to_string()), "R2".to_string());
    let mut txn = Txn::read_write(db.clone());
    tabular::rename(ns.0, id, ns.1, ns.1, &source, &destination, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        stored(&db, v0.uuid()).await,
        Some(project(&identity(ns, &v1, "R2", true), &v1))
    );

    let mut txn = Txn::read_write(db.clone());
    tabular::mark_as_deleted(ns.0, id, false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(!stored(&db, v0.uuid()).await.unwrap().table.active);

    let mut txn = Txn::read_write(db.clone());
    tabular::clear_deleted_at(ns.0, &[id], None, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        stored(&db, v0.uuid()).await,
        Some(project(&identity(ns, &v1, "R2", true), &v1))
    );

    let mut txn = Txn::read_write(db.clone());
    tabular::drop(ns.0, id, false, &mut txn).await.unwrap();
    txn.commit().await.unwrap();
    assert!(!any_stored(&db, v0.uuid()).await);
}

#[tokio::test]
async fn recursive_namespace_drops_delete_the_projection() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/d");
    create(&db, ns, "d", Some("gs://b/m/d0.json"), &v0)
        .await
        .unwrap();
    commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/d0.json")),
        "gs://b/m/d1.json",
        |b| {
            b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    assert!(any_stored(&db, v0.uuid()).await);

    let flags = NamespaceDropFlags {
        force: false,
        purge: false,
        recursive: true,
    };
    let mut txn = Txn::read_write(db.clone());
    crate::store::namespace::drop(ns.0, ns.1, flags, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(!any_stored(&db, v0.uuid()).await);
}

/// `n` appends on `metadata`, as one change.
fn appended(metadata: &TableMetadata, n: i64) -> TableMetadata {
    let mut builder = TableMetadataBuilder::new_from_metadata(metadata.clone(), None);
    let first = metadata.current_snapshot_id().unwrap_or(0) + 1;
    for id in first..first + n {
        builder = builder
            .set_branch_snapshot(snapshot(id, (id > 1).then_some(id - 1), 0), MAIN_BRANCH)
            .unwrap();
    }
    builder.build().unwrap().metadata
}

/// Pick the next projection task and run it; `false` if there is none.
pub(super) async fn run_task(db: &Db) -> bool {
    let state = super::tasks::state(db);
    let Some(task) = ProjectionTask::pick_new_task::<FirestoreBackend>(state.clone())
        .await
        .unwrap()
    else {
        return false;
    };
    projection_task::run(&state, &task).await;
    true
}

#[tokio::test]
async fn large_creates_are_projected_by_a_task() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = appended(&small("gs://b/wh/ns/big"), 20);
    let id = v0.uuid();
    create(&db, ns, "big", Some("gs://b/m/big0.json"), &v0)
        .await
        .unwrap();
    let pending = stored(&db, id).await.unwrap();
    assert!(pending.table.pending);
    assert_eq!(pending.snapshots.len(), 0);

    // A commit while pending only rewrites the table document; the task finds it.
    let v1 = commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/big0.json")),
        "gs://b/m/big1.json",
        |b| {
            b.set_branch_snapshot(snapshot(21, Some(20), 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    let still = stored(&db, id).await.unwrap();
    assert!(still.table.pending);
    assert_eq!(still.table.current_snapshot_id, Some(21));

    assert!(run_task(&db).await);
    assert_eq!(
        stored(&db, id).await,
        Some(project(&identity(ns, &v1, "big", true), &v1))
    );
    assert!(!run_task(&db).await, "one task for both changes");

    // Back to inline commits.
    let v2 = commit(
        &db,
        ns.0,
        (&v1, Some("gs://b/m/big1.json")),
        "gs://b/m/big2.json",
        |b| {
            b.set_branch_snapshot(snapshot(22, Some(21), 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    assert_eq!(
        stored(&db, id).await,
        Some(project(&identity(ns, &v2, "big", true), &v2))
    );
}

#[tokio::test]
async fn large_commits_and_drops_are_left_to_a_task() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/grow");
    let id = v0.uuid();
    create(&db, ns, "grow", Some("gs://b/m/grow0.json"), &v0)
        .await
        .unwrap();
    let v1 = commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/grow0.json")),
        "gs://b/m/grow1.json",
        |b| {
            let mut b = b;
            for id in 1..=20 {
                b = b
                    .set_branch_snapshot(snapshot(id, (id > 1).then_some(id - 1), 0), MAIN_BRANCH)
                    .unwrap();
            }
            b
        },
    )
    .await;
    assert!(stored(&db, id).await.unwrap().table.pending);
    assert!(run_task(&db).await);
    assert_eq!(
        stored(&db, id).await,
        Some(project(&identity(ns, &v1, "grow", true), &v1))
    );

    let mut txn = Txn::read_write(db.clone());
    tabular::drop(ns.0, TabularId::Table(TableId::from(id)), false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(any_stored(&db, id).await, "left to the task");
    assert!(run_task(&db).await);
    assert!(!any_stored(&db, id).await);
}

/// A task scheduled for a table, as through the management API, repairs its projection,
/// writing only the documents that differ.
#[tokio::test]
async fn tasks_repair_a_projection() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small("gs://b/wh/ns/fix");
    let id = v0.uuid();
    create(&db, ns, "fix", Some("gs://b/m/fix0.json"), &v0)
        .await
        .unwrap();
    let v1 = commit(
        &db,
        ns.0,
        (&v0, Some("gs://b/m/fix0.json")),
        "gs://b/m/fix1.json",
        |b| {
            b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH)
                .unwrap()
                .set_branch_snapshot(snapshot(2, Some(1), 0), MAIN_BRANCH)
                .unwrap()
        },
    )
    .await;
    let expected = project(&identity(ns, &v1, "fix", true), &v1);

    // One snapshot lost, one stale, and one that never existed.
    let mut txn = Txn::read_write(db.clone());
    let mut stale = expected.snapshots[&2].clone();
    stale.refs.clear();
    let mut stray = stale.clone();
    stray.snapshot_id = 99;
    for w in [
        Write::DeleteSnapshot(1),
        Write::Snapshot(stale),
        Write::Snapshot(stray),
    ] {
        projection_store::write(&mut txn, id, &w).unwrap();
    }
    let t = tabular::read(&mut txn, id).await.unwrap().unwrap();
    projection_task::enqueue(&mut txn, &t).await.unwrap();
    txn.commit().await.unwrap();
    assert_ne!(stored(&db, id).await, Some(expected.clone()));

    assert!(run_task(&db).await);
    assert_eq!(stored(&db, id).await, Some(expected));
    let logs: Vec<(String, Option<String>)> = Txn::latest(db.clone())
        .query(&Query::new(TASK_LOGS).eq(
            "queue_name",
            string_value(projection_task::QUEUE_NAME.as_str()),
        ))
        .await
        .unwrap()
        .iter()
        .map(|d| from_doc::<TaskLogDoc>(d).unwrap())
        .map(|l| (l.status, l.message))
        .collect();
    assert_eq!(
        logs,
        vec![(
            "success".to_string(),
            Some("Projected; 3 documents written".to_string())
        )]
    );
}

/// Metadata files that run a change, once, when the first file is read: a commit landing
/// while a task works.
struct ChangingFiles(
    tokio::sync::Mutex<Option<std::pin::Pin<Box<dyn Future<Output = ()> + Send>>>>,
);

impl std::fmt::Debug for ChangingFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChangingFiles")
    }
}

#[async_trait::async_trait]
impl crate::store::MetadataFiles for ChangingFiles {
    async fn read(
        &self,
        warehouse: &crate::store::docs::WarehouseDoc,
        location: &Location,
    ) -> Result<Vec<u8>, lakekeeper::service::CatalogBackendError> {
        let change = self.0.lock().await.take();
        if let Some(change) = change {
            change.await;
        }
        super::files().read(warehouse, location).await
    }
}

/// A commit between a task's read of the metadata and its writes makes it start over from the
/// new metadata.
#[tokio::test]
async fn tasks_start_over_when_the_table_changes_under_them() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = appended(&small("gs://b/wh/ns/busy"), 20);
    let id = v0.uuid();
    create(&db, ns, "busy", Some("gs://b/m/busy0.json"), &v0)
        .await
        .unwrap();
    let committed = Arc::new(std::sync::Mutex::new(None));
    let change = {
        let (db, v0, committed) = (db.clone(), v0.clone(), committed.clone());
        Box::pin(async move {
            let v1 = commit(
                &db,
                ns.0,
                (&v0, Some("gs://b/m/busy0.json")),
                "gs://b/m/busy1.json",
                |b| {
                    b.set_branch_snapshot(snapshot(21, Some(20), 0), MAIN_BRANCH)
                        .unwrap()
                },
            )
            .await;
            *committed.lock().unwrap() = Some(v1);
        })
    };

    let mut state = super::tasks::state(&db);
    state.files = Arc::new(ChangingFiles(tokio::sync::Mutex::new(Some(change))));
    let task = ProjectionTask::pick_new_task::<FirestoreBackend>(state.clone())
        .await
        .unwrap()
        .unwrap();
    projection_task::run(&state, &task).await;
    let v1 = committed.lock().unwrap().take().expect("the change ran");
    assert_eq!(
        stored(&db, id).await,
        Some(project(&identity(ns, &v1, "busy", true), &v1))
    );
}
