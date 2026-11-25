//! Table create, load and commit on the emulator.

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use lakekeeper::{
    WarehouseId,
    api::{ErrorModel, iceberg::v1::tables::LoadTableFilters},
    iceberg::{
        TableIdent,
        spec::{
            FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadata,
            TableMetadataBuilder, Type, UnboundPartitionSpec,
        },
    },
    server::tables::calculate_diffs,
    service::{
        CreateNamespaceRequest, CreateTableError, Location, NamespaceId, NamespaceIdent,
        TableCommit, TableCreation, TableId,
    },
    tokio::time::timeout,
};

use super::{db, files, futures_join, new_project, put_file, warehouse_request};
use crate::store::{
    docs::TABLE_METADATA,
    fs::{Db, Txn},
    namespace, table, tabular, warehouse,
};

/// A warehouse with namespace `ns`.
pub(super) async fn namespace(db: &Db) -> (WarehouseId, NamespaceId) {
    let project_id = new_project(db).await;
    let mut txn = Txn::read_write(db.clone());
    let wh = warehouse::create_warehouse(&project_id, warehouse_request("wh"), &mut txn)
        .await
        .unwrap();
    let namespace_id = NamespaceId::new_random();
    let request = CreateNamespaceRequest {
        namespace: NamespaceIdent::new("ns".to_string()),
        properties: None,
    };
    namespace::create(wh.warehouse_id, namespace_id, request, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    (wh.warehouse_id, namespace_id)
}

fn schema(fields: impl IntoIterator<Item = String>) -> Schema {
    let fields = fields
        .into_iter()
        .zip(1..)
        .map(|(name, id)| NestedField::optional(id, name, Type::Primitive(PrimitiveType::String)))
        .map(Arc::new)
        .collect::<Vec<_>>();
    Schema::builder().with_fields(fields).build().unwrap()
}

fn metadata(location: &str, schema: Schema) -> TableMetadata {
    TableMetadataBuilder::new(
        schema,
        UnboundPartitionSpec::builder().build(),
        SortOrder::unsorted_order(),
        location.to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

pub(super) fn small(location: &str) -> TableMetadata {
    metadata(location, schema(["id".to_string()]))
}

fn loc(s: &str) -> Location {
    Location::from_str(s).unwrap()
}

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("ns".to_string()), name.to_string())
}

pub(super) async fn create(
    db: &Db,
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    name: &str,
    metadata_location: Option<&str>,
    metadata: &TableMetadata,
) -> Result<Option<TableId>, CreateTableError> {
    let table_ident = ident(name);
    if let Some(location) = metadata_location {
        put_file(warehouse_id, location, metadata);
    }
    let metadata_location = metadata_location.map(loc);
    let mut txn = Txn::read_write(db.clone());
    let created = table::create(
        TableCreation {
            warehouse_id,
            namespace_id,
            table_ident: &table_ident,
            metadata_location: metadata_location.as_ref(),
            table_metadata: metadata,
        },
        &mut txn,
    )
    .await;
    let (_, staged) = match created {
        Ok(created) => created,
        Err(e) => {
            // A dropped transaction rolls back in the background, which a test runtime may
            // not live to run.
            txn.rollback().await.unwrap();
            return Err(e);
        }
    };
    txn.commit().await.map_err(|e| {
        CreateTableError::from(lakekeeper::service::CatalogBackendError::new_unexpected(
            std::io::Error::other(format!("{e:?}")),
        ))
    })?;
    Ok(staged.map(|s| s.0))
}

/// Commit `new` over `previous_location` in one transaction.
async fn commit(
    db: Db,
    warehouse_id: WarehouseId,
    previous: &TableMetadata,
    new: TableMetadata,
    previous_location: Option<&str>,
    new_location: &str,
) -> Result<(), ErrorModel> {
    put_file(warehouse_id, new_location, &new);
    let commit = TableCommit {
        diffs: calculate_diffs(&new, previous, 0, 0),
        new_metadata: Arc::new(new),
        new_metadata_location: loc(new_location),
        previous_metadata_location: previous_location.map(loc),
        updates: Arc::new(Vec::new()),
    };
    let mut txn = Txn::read_write(db);
    if let Err(e) = table::commit(warehouse_id, [commit], &mut txn).await {
        txn.rollback().await.unwrap();
        return Err(ErrorModel::from(e));
    }
    txn.commit().await
}

async fn load_any(
    db: &Db,
    warehouse_id: WarehouseId,
    id: uuid::Uuid,
) -> (TableMetadata, Option<String>) {
    let mut loaded = table::load(
        warehouse_id,
        [TableId::from(id)],
        false,
        &LoadTableFilters::default(),
        &mut Txn::latest(db.clone()),
        &*files(),
    )
    .await
    .unwrap();
    assert_eq!(loaded.len(), 1);
    let loaded = loaded.pop().unwrap();
    (
        loaded.table_metadata,
        loaded.metadata_location.map(|l| l.to_string()),
    )
}

async fn load(db: &Db, warehouse_id: WarehouseId, id: uuid::Uuid) -> (TableMetadata, String) {
    let (metadata, location) = load_any(db, warehouse_id, id).await;
    (metadata, location.unwrap())
}

fn with_property(metadata: &TableMetadata, value: &str) -> TableMetadata {
    TableMetadataBuilder::new_from_metadata(metadata.clone(), None)
        .set_properties(HashMap::from([("k".to_string(), value.to_string())]))
        .unwrap()
        .build()
        .unwrap()
        .metadata
}

const T1: &str = "gs://b/wh/ns/t1";

#[tokio::test]
async fn tables_round_trip_through_commits() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small(T1);
    create(&db, ns, "t1", Some("gs://b/wh/ns/t1/metadata/v0.json"), &v0)
        .await
        .unwrap();
    assert_eq!(
        load(&db, ns.0, v0.uuid()).await,
        (v0.clone(), "gs://b/wh/ns/t1/metadata/v0.json".to_string())
    );

    let v1 = with_property(&v0, "one");
    commit(
        db.clone(),
        ns.0,
        &v0,
        v1.clone(),
        Some("gs://b/wh/ns/t1/metadata/v0.json"),
        "gs://b/wh/ns/t1/metadata/v1.json",
    )
    .await
    .unwrap();
    let (loaded, location) = load(&db, ns.0, v0.uuid()).await;
    assert_eq!(loaded, v1);
    assert_eq!(location, "gs://b/wh/ns/t1/metadata/v1.json");
}

#[tokio::test]
async fn stale_commits_conflict() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small(T1);
    create(&db, ns, "t1", Some("gs://b/m/v0.json"), &v0)
        .await
        .unwrap();
    let v1 = with_property(&v0, "one");
    commit(
        db.clone(),
        ns.0,
        &v0,
        v1.clone(),
        Some("gs://b/m/v0.json"),
        "gs://b/m/v1.json",
    )
    .await
    .unwrap();

    // Based on v0, which is no longer current.
    let err = commit(
        db.clone(),
        ns.0,
        &v0,
        with_property(&v0, "stale"),
        Some("gs://b/m/v0.json"),
        "gs://b/m/v2.json",
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, 409, "{err:?}");
    assert_eq!(
        load(&db, ns.0, v0.uuid()).await,
        (v1, "gs://b/m/v1.json".to_string())
    );
}

/// Two commits based on the same version, run concurrently. At most one wins, and the stored
/// metadata is the winner's, or unchanged if Firestore aborts both.
#[tokio::test]
async fn racing_commits_keep_one_version() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small(T1);
    create(&db, ns, "t1", Some("gs://b/m/v0.json"), &v0)
        .await
        .unwrap();

    let (a, b) = timeout(
        Duration::from_secs(60),
        futures_join(
            Box::pin(commit(
                db.clone(),
                ns.0,
                &v0,
                with_property(&v0, "a"),
                Some("gs://b/m/v0.json"),
                "gs://b/m/a.json",
            )),
            Box::pin(commit(
                db.clone(),
                ns.0,
                &v0,
                with_property(&v0, "b"),
                Some("gs://b/m/v0.json"),
                "gs://b/m/b.json",
            )),
        ),
    )
    .await
    .expect("the commits finish");
    assert!(a.is_err() || b.is_err(), "both committed");
    for err in [&a, &b].into_iter().filter_map(|r| r.as_ref().err()) {
        assert_eq!(err.code, 409, "{err:?}");
    }

    let (loaded, location) = load(&db, ns.0, v0.uuid()).await;
    let expected = match (a.is_ok(), b.is_ok()) {
        (true, _) => ("a", "gs://b/m/a.json"),
        (_, true) => ("b", "gs://b/m/b.json"),
        _ => ("", "gs://b/m/v0.json"),
    };
    assert_eq!(location, expected.1);
    assert_eq!(
        loaded.properties().get("k").map_or("", String::as_str),
        expected.0
    );
}

/// Whether the initial metadata of staged table `id` is stored.
async fn staged_metadata_stored(db: &Db, id: uuid::Uuid) -> bool {
    let mut txn = Txn::latest(db.clone());
    let name = txn.db().doc_name(TABLE_METADATA, &id.to_string());
    txn.get(&name).await.unwrap().is_some()
}

/// A staged table has no metadata file, so its metadata is stored until the first commit, which
/// points to a file instead.
#[tokio::test]
async fn staged_tables_keep_metadata_until_committed() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small(T1);
    let id = v0.uuid();
    create(&db, ns, "t1", None, &v0).await.unwrap();
    assert!(staged_metadata_stored(&db, id).await);
    assert_eq!(load_any(&db, ns.0, id).await, (v0.clone(), None));

    let v1 = with_property(&v0, "one");
    commit(db.clone(), ns.0, &v0, v1.clone(), None, "gs://b/m/v1.json")
        .await
        .unwrap();
    assert!(!staged_metadata_stored(&db, id).await);
    assert_eq!(
        load(&db, ns.0, id).await,
        (v1, "gs://b/m/v1.json".to_string())
    );
}

/// Loading a table whose metadata file is gone fails, rather than returning stale metadata.
#[tokio::test]
async fn tables_load_from_their_metadata_file() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let v0 = small(T1);
    create(&db, ns, "t1", Some("gs://b/m/v0.json"), &v0)
        .await
        .unwrap();
    let mut txn = Txn::read_write(db.clone());
    let mut t = tabular::read(&mut txn, v0.uuid()).await.unwrap().unwrap();
    t.metadata_location = Some("gs://b/m/missing.json".to_string());
    tabular::write_docs(&mut txn, &t, false).unwrap();
    txn.commit().await.unwrap();
    let err = table::load(
        ns.0,
        [TableId::from(v0.uuid())],
        false,
        &LoadTableFilters::default(),
        &mut Txn::latest(db.clone()),
        &*files(),
    )
    .await
    .unwrap_err();
    assert!(
        format!("{err:?}").contains("gs://b/m/missing.json"),
        "{err:?}"
    );
}

#[tokio::test]
async fn table_locations_do_not_overlap() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    create(&db, ns, "t1", Some("gs://b/m/t1.json"), &small(T1))
        .await
        .unwrap();

    for (name, location) in [
        ("inside", "gs://b/wh/ns/t1/sub"),
        ("same", "gs://b/wh/ns/t1/"),
        ("around", "gs://b/wh/ns"),
    ] {
        let err = create(&db, ns, name, Some("gs://b/m/x.json"), &small(location))
            .await
            .unwrap_err();
        assert!(
            matches!(err, CreateTableError::LocationAlreadyTaken(_)),
            "{name}: {err:?}"
        );
    }
    // A sibling whose name starts with the same characters is not inside t1.
    create(
        &db,
        ns,
        "t10",
        Some("gs://b/m/t10.json"),
        &small("gs://b/wh/ns/t10"),
    )
    .await
    .unwrap();
}

/// A new table must not land where a pending purge will delete files.
#[tokio::test]
async fn pending_purges_block_their_location() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let project_id = warehouse::read(&mut Txn::latest(db.clone()), ns.0)
        .await
        .unwrap()
        .unwrap()
        .project_id;
    let purge = lakekeeper::service::tasks::TaskInput {
        task_metadata: lakekeeper::service::tasks::ScheduleTaskMetadata {
            project_id: Arc::new(project_id),
            parent_task_id: None,
            scheduled_for: None,
            entity: lakekeeper::service::tasks::TaskEntity::EntityInWarehouse {
                warehouse_id: ns.0,
                entity_id: lakekeeper::service::tasks::WarehouseTaskEntityId::Table {
                    table_id: TableId::from(uuid::Uuid::now_v7()),
                },
                entity_name: vec!["ns".to_string(), "gone".to_string()],
            },
            resource_demand: None,
        },
        payload: serde_json::json!({"tabular_location": "gs://b/wh/ns/gone/"}),
    };
    let mut txn = Txn::read_write(db.clone());
    crate::store::tasks::enqueue_task(
        &lakekeeper::service::tasks::tabular_purge_queue::QUEUE_NAME,
        purge,
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    for location in ["gs://b/wh/ns/gone", "gs://b/wh/ns/gone/sub", "gs://b/wh/ns"] {
        let err = create(&db, ns, "t", Some("gs://b/m/t.json"), &small(location))
            .await
            .unwrap_err();
        assert!(
            matches!(err, CreateTableError::LocationAlreadyTaken(_)),
            "{location}: {err:?}"
        );
    }
    create(
        &db,
        ns,
        "t",
        Some("gs://b/m/t.json"),
        &small("gs://b/wh/ns/gone2"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn table_names_are_unique_and_staged_tables_are_replaced() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let staged = small("gs://b/wh/ns/staged-1");
    assert_eq!(create(&db, ns, "t", None, &staged).await.unwrap(), None);

    let created = small("gs://b/wh/ns/t");
    let replaced = create(&db, ns, "T", Some("gs://b/m/t.json"), &created)
        .await
        .unwrap();
    assert_eq!(replaced, Some(TableId::from(staged.uuid())));

    let err = create(
        &db,
        ns,
        "t",
        Some("gs://b/m/u.json"),
        &small("gs://b/wh/ns/u"),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, CreateTableError::TabularAlreadyExists(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_refs_load_returns_the_referenced_snapshots_only() {
    use lakekeeper::{
        api::iceberg::v1::tables::SnapshotsQuery,
        iceberg::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention},
    };

    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let appended = super::metadata_size::appended(5);
    let mut ids: Vec<i64> = appended.snapshots().map(|s| s.snapshot_id()).collect();
    ids.sort_unstable();
    // A tag on the oldest snapshot, besides `main` on the newest.
    let metadata = TableMetadataBuilder::new_from_metadata(appended, None)
        .set_ref(
            "first",
            SnapshotReference {
                snapshot_id: ids[0],
                retention: SnapshotRetention::Tag {
                    max_ref_age_ms: None,
                },
            },
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let id = metadata.uuid();
    create(&db, ns, "refs", Some("gs://bucket/m/refs.json"), &metadata)
        .await
        .unwrap();

    let load = |snapshots| {
        let db = db.clone();
        async move {
            table::load(
                ns.0,
                [TableId::from(id)],
                false,
                &LoadTableFilters { snapshots },
                &mut Txn::latest(db),
                &*files(),
            )
            .await
            .unwrap()
            .pop()
            .unwrap()
            .table_metadata
        }
    };
    assert_eq!(load(SnapshotsQuery::All).await.snapshots().count(), 5);
    let refs = load(SnapshotsQuery::Refs).await;
    let mut kept: Vec<i64> = refs.snapshots().map(|s| s.snapshot_id()).collect();
    kept.sort_unstable();
    assert_eq!(kept, [ids[0], ids[4]]);
    assert_eq!(refs.refs()[MAIN_BRANCH].snapshot_id, ids[4]);
    assert_eq!(refs.current_snapshot_id(), Some(ids[4]));
    assert_eq!(refs.history().len(), metadata.history().len());
}
