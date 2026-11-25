//! Renaming tables on the emulator.

use lakekeeper::{
    WarehouseId,
    iceberg::{NamespaceIdent, TableIdent},
    service::{CreateNamespaceRequest, NamespaceId, RenameTabularError, TableId, TabularId},
};

use super::{
    db,
    table::{create, namespace, small},
};
use crate::store::{
    fs::{Db, Txn},
    namespace as ns_store, tabular,
};

fn ident(ns: &str, name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(ns.to_string()), name.to_string())
}

async fn table_id(
    db: &Db,
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    name: &str,
) -> Option<TableId> {
    tabular::read_by_name(
        &mut Txn::latest(db.clone()),
        warehouse_id,
        namespace_id,
        name,
    )
    .await
    .unwrap()
    .map(|t| t.tabular_id.into())
}

async fn new_namespace(db: &Db, warehouse_id: WarehouseId, name: &str) -> NamespaceId {
    let id = NamespaceId::new_random();
    let mut txn = Txn::read_write(db.clone());
    ns_store::create(
        warehouse_id,
        id,
        CreateNamespaceRequest {
            namespace: NamespaceIdent::new(name.to_string()),
            properties: None,
        },
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    id
}

async fn rename(
    db: &Db,
    warehouse_id: WarehouseId,
    id: TableId,
    (source_ns, source): (NamespaceId, TableIdent),
    (destination_ns, destination): (NamespaceId, TableIdent),
) -> Result<(), RenameTabularError> {
    let mut txn = Txn::read_write(db.clone());
    let result = tabular::rename(
        warehouse_id,
        TabularId::Table(id),
        source_ns,
        destination_ns,
        &source,
        &destination,
        &mut txn,
    )
    .await;
    match result {
        Ok(info) => {
            assert_eq!(info.tabular_ident(), &destination);
            txn.commit().await.unwrap();
            Ok(())
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(e)
        }
    }
}

#[tokio::test]
async fn renames_within_and_across_namespaces() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (wh, ns_id) = ns;
    create(
        &db,
        ns,
        "t",
        Some("gs://b/m/t.json"),
        &small("gs://b/wh/ns/t"),
    )
    .await
    .unwrap();
    let id = table_id(&db, ns, "t").await.unwrap();

    rename(
        &db,
        wh,
        id,
        (ns_id, ident("ns", "t")),
        (ns_id, ident("ns", "u")),
    )
    .await
    .unwrap();
    assert_eq!(table_id(&db, ns, "u").await, Some(id));
    assert_eq!(table_id(&db, ns, "t").await, None);
    // The old name is free again.
    create(
        &db,
        ns,
        "t",
        Some("gs://b/m/t2.json"),
        &small("gs://b/wh/ns/t2"),
    )
    .await
    .unwrap();

    // Only the case changes: the name is still the tabular's own.
    rename(
        &db,
        wh,
        id,
        (ns_id, ident("ns", "u")),
        (ns_id, ident("ns", "U")),
    )
    .await
    .unwrap();
    let stored = tabular::read(&mut Txn::latest(db.clone()), *id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.name, "U");

    let other = new_namespace(&db, wh, "other").await;
    rename(
        &db,
        wh,
        id,
        (ns_id, ident("ns", "U")),
        (other, ident("other", "v")),
    )
    .await
    .unwrap();
    assert_eq!(table_id(&db, (wh, other), "v").await, Some(id));
    assert_eq!(table_id(&db, ns, "U").await, None);
}

#[tokio::test]
async fn renames_that_lost_a_race_find_nothing() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (wh, ns_id) = ns;
    create(
        &db,
        ns,
        "a",
        Some("gs://b/m/a.json"),
        &small("gs://b/wh/ns/a"),
    )
    .await
    .unwrap();
    create(
        &db,
        ns,
        "b",
        Some("gs://b/m/b.json"),
        &small("gs://b/wh/ns/b"),
    )
    .await
    .unwrap();
    create(&db, ns, "staged", None, &small("gs://b/wh/ns/staged"))
        .await
        .unwrap();
    let a = table_id(&db, ns, "a").await.unwrap();
    let staged = table_id(&db, ns, "staged").await.unwrap();
    let other = new_namespace(&db, wh, "other").await;

    let not_found = [
        // The source is under another name, or in another namespace, by now.
        rename(
            &db,
            wh,
            a,
            (ns_id, ident("ns", "x")),
            (ns_id, ident("ns", "y")),
        )
        .await,
        rename(
            &db,
            wh,
            a,
            (other, ident("other", "a")),
            (ns_id, ident("ns", "y")),
        )
        .await,
        // The destination namespace no longer bears the name the request resolved.
        rename(
            &db,
            wh,
            a,
            (ns_id, ident("ns", "a")),
            (other, ident("renamed", "y")),
        )
        .await,
        // Staged tables cannot be renamed.
        rename(
            &db,
            wh,
            staged,
            (ns_id, ident("ns", "staged")),
            (ns_id, ident("ns", "y")),
        )
        .await,
    ];
    for result in not_found {
        let err = result.unwrap_err();
        assert!(
            matches!(err, RenameTabularError::TabularNotFound(_)),
            "{err:?}"
        );
    }

    let err = rename(
        &db,
        wh,
        a,
        (ns_id, ident("ns", "a")),
        (ns_id, ident("ns", "B")),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, RenameTabularError::TabularAlreadyExists(_)),
        "{err:?}"
    );
    assert_eq!(table_id(&db, ns, "a").await, Some(a));
}
