//! Generic tables on the emulator.

use std::{collections::HashMap, str::FromStr};

use lakekeeper::{
    WarehouseId,
    api::{ErrorModel, iceberg::v1::PaginationQuery, management::v1::TabularType},
    iceberg::{NamespaceIdent, TableIdent},
    service::{
        GenericTableCreation, GenericTableId, GenericTableInfo, Location, NamespaceId, TabularId,
        TabularListFlags,
    },
};

use super::{
    db,
    table::{create as create_table, namespace, small},
};
use crate::store::{
    fs::{Db, Txn},
    generic_table, tabular,
};

fn creation(
    (warehouse_id, namespace_id): (WarehouseId, NamespaceId),
    name: &str,
    location: &str,
) -> GenericTableCreation {
    GenericTableCreation {
        generic_table_id: GenericTableId::new_random(),
        namespace_id,
        warehouse_id,
        name: name.to_string(),
        format: "lance".into(),
        location: Location::from_str(location).unwrap(),
        doc: Some("A test table".to_string()),
        schema: Some(serde_json::json!({"fields": [{"name": "id", "type": "int64"}]})),
        statistics: None,
        properties: HashMap::from([("owner".to_string(), "team-a".to_string())]),
    }
}

async fn create(db: &Db, c: GenericTableCreation) -> Result<GenericTableInfo, ErrorModel> {
    let mut txn = Txn::read_write(db.clone());
    match generic_table::create(c, &mut txn).await {
        Ok(info) => {
            txn.commit().await?;
            Ok(info)
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(e.into())
        }
    }
}

async fn load(db: &Db, ns: (WarehouseId, NamespaceId), name: &str) -> Option<GenericTableInfo> {
    generic_table::load(ns.0, ns.1, name, &mut Txn::latest(db.clone()))
        .await
        .ok()
}

fn ident(name: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new("ns".to_string()), name.to_string())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn create_load_list_and_drop() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = create(&db, creation(ns, "lance1", "s3://b/gt/lance1"))
        .await
        .unwrap();
    assert_eq!(created.tabular_ident, ident("lance1"));

    let loaded = load(&db, ns, "lance1").await.unwrap();
    assert_eq!(loaded.generic_table_id, created.generic_table_id);
    assert_eq!(loaded.format.as_str(), "lance");
    assert_eq!(loaded.doc.as_deref(), Some("A test table"));
    assert_eq!(
        loaded.schema,
        Some(serde_json::json!({"fields": [{"name": "id", "type": "int64"}]}))
    );
    assert_eq!(loaded.statistics, None);
    assert_eq!(loaded.properties["owner"], "team-a");
    assert_eq!(loaded.location.as_str(), "s3://b/gt/lance1");
    let by_id =
        generic_table::load_by_id(ns.0, created.generic_table_id, &mut Txn::latest(db.clone()))
            .await
            .unwrap();
    assert_eq!(by_id.name, "lance1");
    // Not found as a table.
    assert!(
        tabular::get_by_ident(
            ns.0,
            &[lakekeeper::service::TabularIdentBorrowed::Table(&ident(
                "lance1"
            ))],
            TabularListFlags::active(),
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap()
        .is_empty()
    );

    // Names and locations are shared with tables and views.
    create_table(&db, ns, "iceberg1", None, &small("s3://b/gt/iceberg1"))
        .await
        .unwrap();
    let taken = create(&db, creation(ns, "ICEBERG1", "s3://b/gt/other"))
        .await
        .unwrap_err();
    assert_eq!(taken.r#type, "GenericTableAlreadyExists");
    let taken = create(&db, creation(ns, "lance2", "s3://b/gt/lance1/inner"))
        .await
        .unwrap_err();
    assert_eq!(taken.code, 409);

    // Listing pages through generic tables only.
    create(&db, creation(ns, "lance2", "s3://b/gt/lance2"))
        .await
        .unwrap();
    create(&db, creation(ns, "lance3", "s3://b/gt/lance3"))
        .await
        .unwrap();
    let nsi = NamespaceIdent::new("ns".to_string());
    let mut names = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = generic_table::list(
            ns.0,
            ns.1,
            &nsi,
            Some(2),
            token.as_deref(),
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap();
        let rows: Vec<_> = page.into_iter_with_page_tokens().collect();
        names.extend(rows.iter().map(|(_, e, _)| e.name.clone()));
        match rows.last() {
            Some((_, _, t)) if rows.len() == 2 => token = Some(t.clone()),
            _ => break,
        }
    }
    assert_eq!(names, ["lance1", "lance2", "lance3"]);
    // The shared listing has them too.
    let listed = tabular::list(
        ns.0,
        Some(ns.1),
        TabularListFlags::active(),
        &mut Txn::latest(db.clone()),
        Some(TabularType::GenericTable),
        PaginationQuery::new_with_page_size(100),
    )
    .await
    .unwrap();
    assert_eq!(listed.len(), 3);

    // Rename through the shared tabular code.
    let id = TabularId::GenericTable(created.generic_table_id);
    let mut txn = Txn::read_write(db.clone());
    tabular::rename(
        ns.0,
        id,
        ns.1,
        ns.1,
        &ident("lance1"),
        &ident("renamed"),
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert!(load(&db, ns, "lance1").await.is_none());
    assert!(load(&db, ns, "renamed").await.is_some());

    // Protected: drop refuses.
    let mut txn = Txn::read_write(db.clone());
    tabular::set_protected(ns.0, id, true, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let mut txn = Txn::read_write(db.clone());
    let refused = generic_table::drop(ns.0, ns.1, "renamed", &mut txn)
        .await
        .unwrap_err();
    txn.rollback().await.unwrap();
    assert_eq!(ErrorModel::from(refused).code, 409);
    let mut txn = Txn::read_write(db.clone());
    tabular::set_protected(ns.0, id, false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let mut txn = Txn::read_write(db.clone());
    let dropped = generic_table::drop(ns.0, ns.1, "renamed", &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(dropped, created.generic_table_id);
    assert!(load(&db, ns, "renamed").await.is_none());
    // Its name and location are free again.
    create(&db, creation(ns, "renamed", "s3://b/gt/lance1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn soft_deleted_generic_tables_are_hidden() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let created = create(&db, creation(ns, "g", "s3://b/gt/soft/g"))
        .await
        .unwrap();
    let mut txn = Txn::read_write(db.clone());
    tabular::mark_as_deleted(
        ns.0,
        TabularId::GenericTable(created.generic_table_id),
        false,
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert!(load(&db, ns, "g").await.is_none());
    assert!(
        generic_table::load_by_id(ns.0, created.generic_table_id, &mut Txn::latest(db.clone()))
            .await
            .is_err()
    );
    let listed = generic_table::list(
        ns.0,
        ns.1,
        &NamespaceIdent::new("ns".to_string()),
        None,
        None,
        &mut Txn::latest(db.clone()),
    )
    .await
    .unwrap();
    assert_eq!(listed.len(), 0);
}
