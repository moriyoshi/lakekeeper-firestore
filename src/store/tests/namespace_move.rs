//! Moving and renaming namespaces on the emulator.

use lakekeeper::{
    WarehouseId,
    api::ErrorModel,
    service::{CreateNamespaceRequest, MovedNamespace, NamespaceId, NamespaceIdent},
};

use super::{db, table::namespace};
use crate::store::{
    fs::{Db, Txn},
    namespace as ns_store,
};

fn path(p: &[&str]) -> NamespaceIdent {
    NamespaceIdent::from_vec(p.iter().map(ToString::to_string).collect()).unwrap()
}

async fn create(db: &Db, warehouse_id: WarehouseId, p: &[&str]) -> NamespaceId {
    let id = NamespaceId::new_random();
    let mut txn = Txn::read_write(db.clone());
    ns_store::create(
        warehouse_id,
        id,
        CreateNamespaceRequest {
            namespace: path(p),
            properties: None,
        },
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    id
}

async fn move_to(
    db: &Db,
    warehouse_id: WarehouseId,
    id: NamespaceId,
    destination: &[&str],
    force: bool,
) -> Result<MovedNamespace, ErrorModel> {
    let mut txn = Txn::read_write(db.clone());
    match ns_store::move_to(warehouse_id, id, &path(destination), force, &mut txn).await {
        Ok(moved) => {
            txn.commit().await?;
            Ok(moved)
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(ErrorModel::from(e))
        }
    }
}

/// The stored path of the namespace at `p` (looked up ignoring case), if any.
async fn at(db: &Db, warehouse_id: WarehouseId, p: &[&str]) -> Option<(NamespaceId, Vec<String>)> {
    ns_store::get_by_ident(warehouse_id, &[&path(p)], &mut Txn::latest(db.clone()))
        .await
        .unwrap()
        .into_iter()
        .find(|n| n.canonical_ident().len() == p.len())
        .map(|n| (n.namespace_id(), n.canonical_ident().clone().inner()))
}

#[tokio::test]
async fn rename_and_reparent() {
    let Some(db) = db().await else { return };
    let (warehouse_id, ns) = namespace(&db).await;
    let parent = create(&db, warehouse_id, &["Parent"]).await;

    // Rename at the root.
    let moved = move_to(&db, warehouse_id, ns, &["renamed"], false)
        .await
        .unwrap();
    assert!(!moved.is_noop());
    assert_eq!(moved.previous_ident, path(&["ns"]));
    assert_eq!(moved.previous_parent, None);
    assert!(at(&db, warehouse_id, &["ns"]).await.is_none());
    assert_eq!(
        at(&db, warehouse_id, &["RENAMED"]).await,
        Some((ns, vec!["renamed".to_string()]))
    );

    // Into another namespace: the ancestors keep their stored spelling.
    let moved = move_to(&db, warehouse_id, ns, &["parent", "Child"], false)
        .await
        .unwrap();
    assert!(moved.changed_parent());
    assert_eq!(moved.namespace.parent_namespaces_id(), Some(parent));
    assert_eq!(
        at(&db, warehouse_id, &["parent", "child"]).await,
        Some((ns, vec!["Parent".to_string(), "Child".to_string()]))
    );
    // Retrying succeeds and changes nothing, as does another spelling of the ancestors.
    let version = moved.namespace.version();
    for destination in [&["Parent", "Child"], &["PARENT", "Child"]] {
        let again = move_to(&db, warehouse_id, ns, destination, false)
            .await
            .unwrap();
        assert!(again.is_noop());
        assert_eq!(again.namespace.version(), version);
    }

    // A change of case alone.
    move_to(&db, warehouse_id, ns, &["Parent", "child"], false)
        .await
        .unwrap();
    assert_eq!(
        at(&db, warehouse_id, &["parent", "CHILD"]).await,
        Some((ns, vec!["Parent".to_string(), "child".to_string()]))
    );

    // Back to the root.
    let moved = move_to(&db, warehouse_id, ns, &["top"], false)
        .await
        .unwrap();
    assert_eq!(moved.previous_parent, Some(parent));
    assert_eq!(moved.namespace.parent_namespaces_id(), None);
    assert!(at(&db, warehouse_id, &["parent", "child"]).await.is_none());
}

#[tokio::test]
async fn refused_moves() {
    let Some(db) = db().await else { return };
    let (warehouse_id, ns) = namespace(&db).await;
    let parent = create(&db, warehouse_id, &["parent"]).await;
    create(&db, warehouse_id, &["parent", "child"]).await;
    let leaf = create(&db, warehouse_id, &["leaf"]).await;

    let error = |r: Result<MovedNamespace, ErrorModel>| {
        let e = r.unwrap_err();
        (e.code, e.r#type)
    };
    assert_eq!(
        error(move_to(&db, warehouse_id, parent, &["elsewhere"], false).await),
        (409, "NamespaceHasChildren".to_string())
    );
    assert_eq!(
        error(move_to(&db, warehouse_id, leaf, &["leaf", "x"], false).await),
        (400, "NamespaceCannotMoveIntoSelf".to_string())
    );
    assert_eq!(
        error(move_to(&db, warehouse_id, leaf, &["NS"], false).await).0,
        409
    );
    assert_eq!(
        error(move_to(&db, warehouse_id, leaf, &["parent", "CHILD"], false).await).0,
        409
    );
    assert_eq!(
        error(move_to(&db, warehouse_id, leaf, &["missing", "leaf"], false).await).0,
        404
    );

    let mut txn = Txn::read_write(db.clone());
    ns_store::set_protected(warehouse_id, ns, true, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        error(move_to(&db, warehouse_id, ns, &["other"], false).await).0,
        409
    );
    move_to(&db, warehouse_id, ns, &["other"], true)
        .await
        .unwrap();
    assert!(at(&db, warehouse_id, &["other"]).await.is_some());
}
