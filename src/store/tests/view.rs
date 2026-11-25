//! Views on the emulator.

use std::{collections::HashMap, str::FromStr, sync::Arc};

use lakekeeper::{
    WarehouseId,
    iceberg::{
        NamespaceIdent, TableIdent,
        spec::{
            NestedField, PrimitiveType, Schema, SqlViewRepresentation, Type, ViewFormatVersion,
            ViewMetadata, ViewMetadataBuilder, ViewRepresentation, ViewRepresentations,
            ViewVersion,
        },
    },
    service::{
        CatalogView, CommitViewError, CreateViewError, Location, NamespaceId, TabularId,
        TabularListFlags, ViewCommit, ViewId,
    },
};

use super::{
    db, files, put_file,
    table::{create as create_table, namespace, small},
};
use crate::store::{
    fs::{Db, Txn},
    tabular, view,
};

pub(super) fn version(sql: &str) -> ViewVersion {
    ViewVersion::builder()
        .with_schema_id(0)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_representations(
            ViewRepresentations::builder()
                .add_all_representations(vec![ViewRepresentation::Sql(SqlViewRepresentation {
                    sql: sql.to_string(),
                    dialect: "spark".to_string(),
                })])
                .build()
                .unwrap(),
        )
        .with_default_namespace(NamespaceIdent::new("ns".to_string()))
        .build()
}

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap()
}

pub(super) fn metadata(location: &str) -> ViewMetadata {
    ViewMetadataBuilder::new(
        location.to_string(),
        schema(),
        version("select 1 as id"),
        ViewFormatVersion::V1,
        HashMap::from([("owner".to_string(), "me".to_string())]),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

pub(super) fn loc(s: &str) -> Location {
    Location::from_str(s).unwrap()
}

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("ns".to_string()), name.to_string())
}

/// Run `f` in a transaction, committing on success and rolling back on error.
async fn in_txn<T, E: std::fmt::Debug>(
    db: &Db,
    f: impl AsyncFnOnce(&mut Txn) -> Result<T, E>,
) -> Result<T, E> {
    let mut txn = Txn::read_write(db.clone());
    match f(&mut txn).await {
        Ok(out) => {
            txn.commit().await.unwrap();
            Ok(out)
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(e)
        }
    }
}

pub(super) async fn create(
    db: &Db,
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    name: &str,
    metadata: &ViewMetadata,
    metadata_location: &str,
) -> Result<(), CreateViewError> {
    put_file(warehouse_id, metadata_location, metadata);
    in_txn(db, async |txn| {
        view::create(
            warehouse_id,
            namespace_id,
            &ident(name),
            metadata,
            &loc(metadata_location),
            txn,
        )
        .await
    })
    .await
    .map(|_| ())
}

pub(super) async fn load(db: &Db, warehouse_id: WarehouseId, id: uuid::Uuid) -> CatalogView {
    view::load(
        warehouse_id,
        ViewId::from(id),
        false,
        &mut Txn::latest(db.clone()),
        &*files(),
    )
    .await
    .unwrap()
}

pub(super) async fn commit(
    db: &Db,
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    previous: &CatalogView,
    new: &CatalogView,
) -> Result<(), CommitViewError> {
    put_file(
        warehouse_id,
        new.metadata_location.as_str(),
        new.metadata.as_ref(),
    );
    in_txn(db, async |txn| {
        view::commit(
            ViewCommit {
                namespace_id,
                warehouse_id,
                previous_view: previous,
                new_view: new,
            },
            txn,
        )
        .await
    })
    .await
    .map(|_| ())
}

/// `previous` with a new version, location and metadata location.
fn next(previous: &CatalogView, location: &str, metadata_location: &str) -> CatalogView {
    let metadata = ViewMetadataBuilder::new_from_metadata((*previous.metadata).clone())
        .set_current_version(version("select 2 as id"), schema())
        .unwrap()
        .set_location(location.to_string())
        .build()
        .unwrap()
        .metadata;
    CatalogView {
        metadata_location: loc(metadata_location),
        location: loc(location),
        metadata: Arc::new(metadata),
        warehouse_version: previous.warehouse_version,
    }
}

#[tokio::test]
async fn views_round_trip_and_commit() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = metadata("gs://b/wh/ns/v");
    let id = created.uuid();
    create(&db, ns, "v", &created, "gs://b/wh/ns/v/metadata/1.json")
        .await
        .unwrap();

    let first = load(&db, ns.0, id).await;
    assert_eq!(*first.metadata, created);
    assert_eq!(
        first.metadata_location.as_str(),
        "gs://b/wh/ns/v/metadata/1.json"
    );
    assert_eq!(first.location.as_str(), "gs://b/wh/ns/v");

    let second = next(&first, "gs://b/wh/ns/v2", "gs://b/wh/ns/v2/metadata/2.json");
    commit(&db, ns, &first, &second).await.unwrap();
    let loaded = load(&db, ns.0, id).await;
    assert_eq!(*loaded.metadata, *second.metadata);
    assert_eq!(loaded.metadata_location, second.metadata_location);

    // A commit against the first version lost the race.
    let stale = next(&first, "gs://b/wh/ns/v", "gs://b/wh/ns/v/metadata/3.json");
    let err = commit(&db, ns, &first, &stale).await.unwrap_err();
    assert!(
        matches!(err, CommitViewError::ConcurrentUpdateError(_)),
        "{err:?}"
    );
    // A commit naming another namespace finds no view.
    let elsewhere = (ns.0, NamespaceId::new_random());
    let third = next(
        &loaded,
        "gs://b/wh/ns/v2",
        "gs://b/wh/ns/v2/metadata/3.json",
    );
    let err = commit(&db, elsewhere, &loaded, &third).await.unwrap_err();
    assert!(
        matches!(err, CommitViewError::TabularNotFound(_)),
        "{err:?}"
    );

    // The view's old location is free again, its new one is not.
    create_table(
        &db,
        ns,
        "t",
        Some("gs://b/m/t.json"),
        &small("gs://b/wh/ns/v"),
    )
    .await
    .unwrap();
    let err = create_table(
        &db,
        ns,
        "t2",
        Some("gs://b/m/t2.json"),
        &small("gs://b/wh/ns/v2"),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(
            err,
            lakekeeper::service::CreateTableError::LocationAlreadyTaken(_)
        ),
        "{err:?}"
    );

    let listed = tabular::list(
        ns.0,
        Some(ns.1),
        TabularListFlags::active(),
        &mut Txn::latest(db.clone()),
        Some(lakekeeper::api::management::v1::TabularType::View),
        lakekeeper::api::iceberg::v1::PaginationQuery::new_with_page_size(10),
    )
    .await
    .unwrap();
    let views: Vec<_> = listed.into_iter().map(|(id, _)| id).collect();
    assert_eq!(views, vec![TabularId::View(ViewId::from(id))]);
}

#[tokio::test]
async fn view_names_are_shared_with_tables() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    create_table(
        &db,
        ns,
        "t",
        Some("gs://b/m/t.json"),
        &small("gs://b/wh/ns/t"),
    )
    .await
    .unwrap();
    let err = create(
        &db,
        ns,
        "t",
        &metadata("gs://b/wh/ns/vt"),
        "gs://b/m/vt.json",
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, CreateViewError::TabularAlreadyExists(_)),
        "{err:?}"
    );
    let err = create(
        &db,
        ns,
        "v",
        &metadata("gs://b/wh/ns/t/v"),
        "gs://b/m/v.json",
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, CreateViewError::LocationAlreadyTaken(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn dropped_views_are_gone() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = metadata("gs://b/wh/ns/d");
    let id = created.uuid();
    create(&db, ns, "d", &created, "gs://b/m/d.json")
        .await
        .unwrap();
    in_txn(&db, async |txn| {
        tabular::drop(ns.0, TabularId::View(ViewId::from(id)), false, txn).await
    })
    .await
    .unwrap();
    let mut read = Txn::latest(db.clone());
    assert!(tabular::read(&mut read, id).await.unwrap().is_none());
}
