//! The projection of views on the emulator: kept by create and replace, in line through
//! rename, soft deletion, undrop and drop, left to a task when large, and searched by the CLI.

use std::sync::Arc;

use lakekeeper::{
    iceberg::{
        NamespaceIdent, TableIdent,
        spec::{NestedField, PrimitiveType, Schema, ViewMetadataBuilder},
    },
    service::{CatalogView, TabularId, ViewId},
};
use serde_json::json;

use super::{
    db,
    projection_store::{any_stored, run_task},
    table::{create as create_table, namespace, small},
    view::{commit, create, load, loc, metadata, version},
};
use crate::store::{
    docs::{IDX_FIELDS, IDX_VIEWS},
    fs::{Db, Query, Txn, from_doc, string_value},
    projection::{IdxField, TableIdentity, ViewProjection, project_view},
    search::{Command, Kind, execute},
    tabular,
};

/// The stored projection of view `id`, or `None` without a view document.
async fn stored(db: &Db, id: uuid::Uuid) -> Option<ViewProjection> {
    let mut txn = Txn::latest(db.clone());
    let name = txn.db().doc_name(IDX_VIEWS, &id.to_string());
    let view = from_doc(&txn.get(&name).await.unwrap()?).unwrap();
    let fields = txn
        .query(&Query::new(IDX_FIELDS).eq("table_id", string_value(id.to_string())))
        .await
        .unwrap()
        .iter()
        .map(|d| from_doc::<IdxField>(d).unwrap())
        .map(|f| (f.field_id, f))
        .collect();
    Some(ViewProjection { view, fields })
}

/// A schema of `n` long columns `c1` to `cn`.
fn wide(n: i32) -> Schema {
    Schema::builder()
        .with_fields(
            (1..=n)
                .map(|i| {
                    Arc::new(NestedField::optional(
                        i,
                        format!("c{i}"),
                        lakekeeper::iceberg::spec::Type::Primitive(PrimitiveType::Long),
                    ))
                })
                .collect::<Vec<_>>(),
        )
        .build()
        .unwrap()
}

/// `previous` replaced by a version with `schema`, at `metadata_location`.
fn replaced(previous: &CatalogView, schema: Schema, metadata_location: &str) -> CatalogView {
    let metadata = ViewMetadataBuilder::new_from_metadata((*previous.metadata).clone())
        .set_current_version(version("select *"), schema)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    CatalogView {
        metadata_location: loc(metadata_location),
        location: previous.location.clone(),
        metadata: Arc::new(metadata),
        warehouse_version: previous.warehouse_version,
    }
}

fn expected(
    ns: (lakekeeper::WarehouseId, lakekeeper::service::NamespaceId),
    view: &CatalogView,
    name: &str,
) -> ViewProjection {
    let identity = TableIdentity {
        warehouse_id: ns.0,
        namespace_id: ns.1,
        table_id: view.metadata.uuid(),
        name: name.to_string(),
        active: true,
    };
    project_view(&identity, &view.metadata)
}

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("ns".to_string()), name.to_string())
}

#[tokio::test]
async fn views_keep_their_projection() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = metadata("gs://b/wh/ns/v");
    let id = created.uuid();
    let tabular_id = TabularId::View(ViewId::from(id));
    create(&db, ns, "v", &created, "gs://b/m/v1.json")
        .await
        .unwrap();
    let first = load(&db, ns.0, id).await;
    assert_eq!(stored(&db, id).await, Some(expected(ns, &first, "v")));

    let second = replaced(&first, wide(3), "gs://b/m/v2.json");
    commit(&db, ns, &first, &second).await.unwrap();
    let projection = stored(&db, id).await.unwrap();
    assert_eq!(projection, expected(ns, &second, "v"));
    assert_eq!(projection.fields[&3].kind, "view");

    let mut txn = Txn::read_write(db.clone());
    tabular::rename(
        ns.0,
        tabular_id,
        ns.1,
        ns.1,
        &ident("v"),
        &ident("w"),
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(stored(&db, id).await, Some(expected(ns, &second, "w")));

    let mut txn = Txn::read_write(db.clone());
    tabular::mark_as_deleted(ns.0, tabular_id, false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(!stored(&db, id).await.unwrap().view.active);
    let mut txn = Txn::read_write(db.clone());
    tabular::clear_deleted_at(ns.0, &[tabular_id], None, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(stored(&db, id).await.unwrap().view.active);

    let mut txn = Txn::read_write(db.clone());
    tabular::drop(ns.0, tabular_id, false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(stored(&db, id).await, None);
    assert!(!any_stored(&db, id).await);
}

/// A view with more columns than a transaction writes inline is projected, and cleaned up, by
/// a task.
#[tokio::test]
async fn wide_views_are_left_to_a_task() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = ViewMetadataBuilder::new_from_metadata(metadata("gs://b/wh/ns/wide"))
        .set_current_version(version("select *"), wide(20))
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    let id = created.uuid();
    create(&db, ns, "wide", &created, "gs://b/m/wide1.json")
        .await
        .unwrap();
    let pending = stored(&db, id).await.unwrap();
    assert!(pending.view.pending);
    assert_eq!(pending.fields.len(), 0);

    assert!(run_task(&db).await);
    let view = load(&db, ns.0, id).await;
    assert_eq!(stored(&db, id).await, Some(expected(ns, &view, "wide")));

    let mut txn = Txn::read_write(db.clone());
    tabular::drop(ns.0, TabularId::View(ViewId::from(id)), false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert!(any_stored(&db, id).await, "left to the task");
    assert!(run_task(&db).await);
    assert!(!any_stored(&db, id).await);
}

#[tokio::test]
async fn columns_of_views_are_searchable() {
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
    create(&db, ns, "v", &metadata("gs://b/wh/ns/v"), "gs://b/m/v.json")
        .await
        .unwrap();
    let columns = |kind| Command::Columns {
        name: "id".to_string(),
        warehouse: Some("wh".to_string()),
        path: false,
        r#type: None,
        current: false,
        kind,
        limit: 100,
        include_deleted: false,
    };
    let out = execute(&db, columns(None)).await.unwrap();
    let mut found: Vec<(serde_json::Value, serde_json::Value)> = out
        .column("table")
        .into_iter()
        .zip(out.column("kind"))
        .collect();
    found.sort_by_key(|(t, _)| t.to_string());
    assert_eq!(
        found,
        [
            (json!("ns.t"), json!("table")),
            (json!("ns.v"), json!("view"))
        ]
    );
    let out = execute(&db, columns(Some(Kind::View))).await.unwrap();
    assert_eq!(out.column("table"), [json!("ns.v")]);
    assert_eq!(out.column("type"), [json!("long")]);
}
