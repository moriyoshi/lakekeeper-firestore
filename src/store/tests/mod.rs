//! Tests on the Firestore emulator. They are skipped unless `FIRESTORE_EMULATOR_HOST` is set,
//! for example by `tests/e2e/run.sh` or by hand:
//!
//! ```sh
//! FIRESTORE_EMULATOR_HOST=localhost:28080 cargo test
//! ```
//!
//! Each test writes under its own collection prefix, so tests run in parallel and need no
//! cleanup.

use std::{sync::Arc, time::Duration};

use lakekeeper::{
    ProjectId,
    api::ErrorModel,
    api::iceberg::v1::PageToken,
    service::{
        CatalogCreateNamespaceError, CatalogCreateWarehouseError, CatalogCreateWarehouseRequest,
        CreateNamespaceRequest, ListNamespacesQuery, NamespaceId, NamespaceIdent,
    },
    tokio::{sync::Barrier, time::timeout},
};

use super::{
    fs::{Db, Document, Query, Txn, int_value, string_value},
    namespace, project, warehouse,
};

/// Metadata files in memory, by warehouse and location, standing in for object storage. Each
/// test has warehouses of its own, so tests sharing locations do not see each other's files.
#[derive(Debug, Default)]
pub(super) struct MemoryFiles(
    std::sync::Mutex<std::collections::HashMap<(lakekeeper::WarehouseId, String), Vec<u8>>>,
);

static FILES: std::sync::LazyLock<Arc<MemoryFiles>> = std::sync::LazyLock::new(Arc::default);

pub(super) fn files() -> Arc<MemoryFiles> {
    FILES.clone()
}

/// Write `metadata` as JSON to `location` of `warehouse_id`, as the core writes a metadata file.
pub(super) fn put_file(
    warehouse_id: lakekeeper::WarehouseId,
    location: &str,
    metadata: &impl serde::Serialize,
) {
    FILES.0.lock().unwrap().insert(
        (warehouse_id, location.to_string()),
        serde_json::to_vec(metadata).unwrap(),
    );
}

#[async_trait::async_trait]
impl super::MetadataFiles for MemoryFiles {
    async fn read(
        &self,
        warehouse: &super::docs::WarehouseDoc,
        location: &lakekeeper::service::Location,
    ) -> Result<Vec<u8>, lakekeeper::service::CatalogBackendError> {
        self.0
            .lock()
            .unwrap()
            .get(&(warehouse.warehouse_id, location.to_string()))
            .cloned()
            .ok_or_else(|| {
                super::fs::backend_error(std::io::Error::other(format!(
                    "No metadata file at {location}"
                )))
            })
    }
}

const PROJECT: &str = "demo-lakekeeper";
const COLLECTION: &str = "things";

/// A database with a fresh collection prefix, or `None` (and the test is skipped) without an
/// emulator.
async fn db() -> Option<Db> {
    if std::env::var_os("FIRESTORE_EMULATOR_HOST").is_none() {
        eprintln!("skipped: FIRESTORE_EMULATOR_HOST is not set");
        return None;
    }
    let prefix = format!("t{}_", uuid::Uuid::now_v7().simple());
    Some(
        Db::connect(PROJECT.to_string(), None, prefix)
            .await
            .expect("connect to the Firestore emulator"),
    )
}

fn doc(
    db: &Db,
    id: &str,
    fields: &[(&str, firestore::gcloud_sdk::google::firestore::v1::Value)],
) -> Document {
    Document {
        name: db.doc_name(COLLECTION, id),
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        create_time: None,
        update_time: None,
    }
}

fn ids(docs: &[Document]) -> Vec<&str> {
    docs.iter()
        .map(|d| d.name.rsplit('/').next().unwrap())
        .collect()
}

async fn commit(db: &Db, docs: Vec<Document>) {
    let mut txn = Txn::read_write(db.clone());
    for d in docs {
        txn.set(d);
    }
    txn.commit().await.expect("commit");
}

#[tokio::test]
async fn reads_see_pending_writes() {
    let Some(db) = db().await else { return };
    commit(&db, vec![doc(&db, "kept", &[]), doc(&db, "gone", &[])]).await;

    let mut txn = Txn::read_write(db.clone());
    txn.set(doc(&db, "new", &[("n", int_value(1))]));
    txn.delete(db.doc_name(COLLECTION, "gone"));
    let names: Vec<String> = ["kept", "gone", "new", "missing"]
        .iter()
        .map(|id| db.doc_name(COLLECTION, id))
        .collect();
    let found: Vec<bool> = txn
        .get_many(&names)
        .await
        .unwrap()
        .iter()
        .map(Option::is_some)
        .collect();
    assert_eq!(found, [true, false, true, false]);
    txn.commit().await.unwrap();

    let mut after = Txn::latest(db.clone());
    let found: Vec<bool> = after
        .get_many(&names)
        .await
        .unwrap()
        .iter()
        .map(Option::is_some)
        .collect();
    assert_eq!(found, [true, false, true, false]);
}

#[tokio::test]
async fn queries_merge_pending_writes() {
    let Some(db) = db().await else { return };
    let x = || string_value("x");
    commit(
        &db,
        vec![
            doc(&db, "a1", &[("g", x()), ("k", int_value(1))]),
            doc(&db, "a2", &[("g", x()), ("k", int_value(2))]),
            doc(&db, "a3", &[("g", x()), ("k", int_value(3))]),
            doc(&db, "a5", &[("g", x()), ("k", int_value(5))]),
        ],
    )
    .await;

    let mut txn = Txn::read_write(db.clone());
    // New and matching, deleted, and changed so that it no longer matches.
    txn.set(doc(&db, "a0", &[("g", x()), ("k", int_value(0))]));
    txn.delete(db.doc_name(COLLECTION, "a2"));
    txn.set(doc(
        &db,
        "a3",
        &[("g", string_value("y")), ("k", int_value(3))],
    ));
    // Changed and still matching, moved to the end of the order.
    txn.set(doc(&db, "a1", &[("g", x()), ("k", int_value(9))]));

    let query = Query::new(COLLECTION).eq("g", x()).order_by("k");
    let all = txn.query(&query).await.unwrap();
    assert_eq!(ids(&all), ["a0", "a5", "a1"]);

    // The limit applies after the merge: the deleted and changed documents do not use it up.
    let first = txn.query(&query.clone().limit(Some(2))).await.unwrap();
    assert_eq!(ids(&first), ["a0", "a5"]);

    let cursor = Some((int_value(5), db.doc_name(COLLECTION, "a5")));
    let rest = txn
        .query(&query.clone().start_after(cursor).limit(Some(2)))
        .await
        .unwrap();
    assert_eq!(ids(&rest), ["a1"]);
    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn array_contains_queries_merge_pending_writes() {
    let Some(db) = db().await else { return };
    let arr = |vals: &[&str]| firestore::gcloud_sdk::google::firestore::v1::Value {
        value_type: Some(
            firestore::gcloud_sdk::google::firestore::v1::value::ValueType::ArrayValue(
                firestore::gcloud_sdk::google::firestore::v1::ArrayValue {
                    values: vals.iter().map(|v| string_value(*v)).collect(),
                },
            ),
        ),
    };
    commit(&db, vec![doc(&db, "p1", &[("tags", arr(&["a", "b"]))])]).await;

    let mut txn = Txn::read_write(db.clone());
    txn.set(doc(&db, "p2", &[("tags", arr(&["b"]))]));
    txn.set(doc(&db, "p3", &[("tags", arr(&["c"]))]));
    let found = txn
        .query(&Query::new(COLLECTION).array_contains("tags", string_value("b")))
        .await
        .unwrap();
    let mut found = ids(&found);
    found.sort_unstable();
    assert_eq!(found, ["p1", "p2"]);
    txn.rollback().await.unwrap();
}

#[tokio::test]
async fn concurrent_creates_conflict() {
    let Some(db) = db().await else { return };
    let mut first = Txn::read_write(db.clone());
    let mut second = Txn::read_write(db.clone());
    first.create(doc(&db, "once", &[("by", string_value("first"))]));
    second.create(doc(&db, "once", &[("by", string_value("second"))]));
    first.commit().await.unwrap();
    let err = second.commit().await.unwrap_err();
    assert_eq!(err.code, 409, "{err:?}");

    let stored = Txn::latest(db.clone())
        .get(&db.doc_name(COLLECTION, "once"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.fields["by"], string_value("first"));
}

#[tokio::test]
async fn create_then_delete_writes_nothing() {
    let Some(db) = db().await else { return };
    commit(&db, vec![doc(&db, "exists", &[])]).await;

    let mut txn = Txn::read_write(db.clone());
    // A create of an existing document that is deleted again sends no write, so no
    // precondition can fail.
    txn.create(doc(&db, "exists", &[]));
    txn.delete(db.doc_name(COLLECTION, "exists"));
    txn.create(doc(&db, "fresh", &[]));
    txn.delete(db.doc_name(COLLECTION, "fresh"));
    txn.commit().await.unwrap();

    let mut after = Txn::latest(db.clone());
    let found: Vec<bool> = after
        .get_many(&[
            db.doc_name(COLLECTION, "exists"),
            db.doc_name(COLLECTION, "fresh"),
        ])
        .await
        .unwrap()
        .iter()
        .map(Option::is_some)
        .collect();
    assert_eq!(found, [true, false]);
}

/// A transaction dropped after a read, as on an error path, does not hold its lock: a later
/// writer of the document commits.
#[tokio::test]
async fn dropped_transactions_release_their_locks() {
    let Some(db) = db().await else { return };
    commit(&db, vec![doc(&db, "locked", &[("n", int_value(0))])]).await;
    let name = db.doc_name(COLLECTION, "locked");

    let mut abandoned = Txn::read_write(db.clone());
    abandoned.get(&name).await.unwrap();
    drop(abandoned);

    let mut writer = Txn::read_write(db.clone());
    writer.get(&name).await.unwrap();
    writer.set(doc(&db, "locked", &[("n", int_value(1))]));
    writer.commit().await.unwrap();
}

/// Two read-modify-write transactions on one document. At most one commits, and the other
/// fails as a conflict; when both wait on each other's read lock, Firestore aborts both.
#[tokio::test]
async fn concurrent_read_modify_write_conflicts() {
    let Some(db) = db().await else { return };
    commit(&db, vec![doc(&db, "counter", &[("n", int_value(0))])]).await;
    let barrier = Arc::new(Barrier::new(2));

    let increment = |db: Db, barrier: Arc<Barrier>| async move {
        let mut txn = Txn::read_write(db.clone());
        let read = txn.get(&db.doc_name(COLLECTION, "counter")).await;
        // Both transactions have read before either writes.
        barrier.wait().await;
        read.map_err(ErrorModel::from)?.expect("counter exists");
        txn.set(doc(&db, "counter", &[("n", int_value(1))]));
        txn.commit().await
    };
    let (a, b) = timeout(
        Duration::from_secs(60),
        futures_join(
            increment(db.clone(), barrier.clone()),
            increment(db.clone(), barrier),
        ),
    )
    .await
    .expect("the transactions finish");
    assert!(a.is_err() || b.is_err(), "both committed");
    for err in [a.err(), b.err()].into_iter().flatten() {
        assert_eq!(err.code, 409, "{err:?}");
    }
}

async fn futures_join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    lakekeeper::tokio::join!(a, b)
}

fn warehouse_request(name: &str) -> CatalogCreateWarehouseRequest {
    CatalogCreateWarehouseRequest::builder()
        .warehouse_name(name.to_string())
        .storage_profile(
            serde_json::from_value(serde_json::json!({
                "type": "gcs", "bucket": "b", "sts-enabled": false
            }))
            .unwrap(),
        )
        .delete_profile(serde_json::from_value(serde_json::json!({"type": "hard"})).unwrap())
        .build()
}

async fn new_project(db: &Db) -> ProjectId {
    let project_id = ProjectId::new_random();
    let mut txn = Txn::read_write(db.clone());
    project::create_project(&project_id, "p".to_string(), &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    project_id
}

#[tokio::test]
async fn warehouse_names_are_unique_ignoring_case() {
    let Some(db) = db().await else { return };
    let project_id = new_project(&db).await;

    let mut txn = Txn::read_write(db.clone());
    warehouse::create_warehouse(&project_id, warehouse_request("Errtraq"), &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let mut txn = Txn::read_write(db.clone());
    let err = warehouse::create_warehouse(&project_id, warehouse_request("ERRTRAQ"), &mut txn)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogCreateWarehouseError::WarehouseAlreadyExists(_)),
        "{err:?}"
    );
    txn.rollback().await.unwrap();
}

/// Both transactions pass the name check before either commits. At most one may commit, and
/// the other fails as a conflict: the marker's `create` precondition stops it, or, when both
/// wait on each other's read lock, Firestore aborts both.
#[tokio::test]
async fn warehouse_name_race_conflicts() {
    let Some(db) = db().await else { return };
    let project_id = new_project(&db).await;

    let mut first = Txn::read_write(db.clone());
    let mut second = Txn::read_write(db.clone());
    warehouse::create_warehouse(&project_id, warehouse_request("race"), &mut first)
        .await
        .unwrap();
    warehouse::create_warehouse(&project_id, warehouse_request("Race"), &mut second)
        .await
        .unwrap();
    let (a, b) = timeout(
        Duration::from_secs(60),
        futures_join(first.commit(), second.commit()),
    )
    .await
    .expect("the transactions finish");
    assert!(a.is_err() || b.is_err(), "both committed");
    for err in [a.err(), b.err()].into_iter().flatten() {
        assert_eq!(err.code, 409, "{err:?}");
    }

    let listed = warehouse::list_warehouses(&project_id, None, &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    assert!(listed.len() <= 1, "{listed:?}");
}

/// The same race, committed one after the other: the marker's `create` precondition stops the
/// second transaction.
#[tokio::test]
async fn warehouse_name_race_loser_conflicts() {
    let Some(db) = db().await else { return };
    let project_id = new_project(&db).await;

    let mut first = Txn::read_write(db.clone());
    warehouse::create_warehouse(&project_id, warehouse_request("race"), &mut first)
        .await
        .unwrap();
    // No read in this transaction, so it holds no lock that the first commit waits for.
    let mut second = Txn::read_write(db.clone());
    first.commit().await.unwrap();
    second.create(
        super::fs::to_doc(
            db.doc_name(
                super::docs::WAREHOUSE_NAMES,
                &super::docs::warehouse_name_id(&project_id, "RACE"),
            ),
            &super::docs::NameMarker {
                id: "other".to_string(),
            },
        )
        .unwrap(),
    );
    let err = second.commit().await.unwrap_err();
    assert_eq!(err.code, 409, "{err:?}");
}

#[tokio::test]
async fn namespaces_list_in_pages() {
    let Some(db) = db().await else { return };
    let project_id = new_project(&db).await;
    let mut txn = Txn::read_write(db.clone());
    let wh = warehouse::create_warehouse(&project_id, warehouse_request("wh"), &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let create = |name: &str| CreateNamespaceRequest {
        namespace: NamespaceIdent::new(name.to_string()),
        properties: None,
    };
    for name in ["n1", "n2", "n3"] {
        let mut txn = Txn::read_write(db.clone());
        namespace::create(
            wh.warehouse_id,
            NamespaceId::new_random(),
            create(name),
            &mut txn,
        )
        .await
        .unwrap();
        txn.commit().await.unwrap();
    }

    let mut txn = Txn::read_write(db.clone());
    let err = namespace::create(
        wh.warehouse_id,
        NamespaceId::new_random(),
        create("N2"),
        &mut txn,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, CatalogCreateNamespaceError::NamespaceAlreadyExists(_)),
        "{err:?}"
    );
    txn.rollback().await.unwrap();

    let mut names = Vec::new();
    let mut page_token = PageToken::Empty;
    loop {
        let query = ListNamespacesQuery {
            page_token: page_token.clone(),
            page_size: Some(2),
            parent: None,
            return_uuids: false,
            return_protection_status: false,
        };
        let page = namespace::list(wh.warehouse_id, &query, &mut Txn::latest(db.clone()))
            .await
            .unwrap()
            .namespaces;
        assert!(page.len() <= 2);
        let mut next = None;
        for (_, ns, token) in page.into_iter_with_page_tokens() {
            names.push(ns.namespace_ident().clone().inner().join("."));
            next = Some(token);
        }
        match next {
            Some(token) if names.len() < 4 => page_token = PageToken::Present(token),
            _ => break,
        }
    }
    assert_eq!(names, ["n1", "n2", "n3"]);
}

mod cache_change_log;
mod generic_table;
mod idempotency;
mod metadata_size;
mod namespace_move;
mod projection;
mod projection_cost;
mod projection_store;
mod rename;
mod search;
mod search_tabular;
mod secrets;
mod soft_delete;
mod table;
mod tag;
mod task_admin;
mod tasks;
mod user;
mod view;
mod view_projection;
