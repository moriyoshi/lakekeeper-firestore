//! Idempotency keys on the emulator.

use lakekeeper::axum::http::StatusCode;
use lakekeeper::{
    WarehouseId,
    api::endpoints::EndpointFlat,
    service::idempotency::{IdempotencyInfo, IdempotencyKey},
};

use super::db;
use crate::store::{
    docs::{IDEMPOTENCY_RECORDS, IdempotencyDoc},
    fs::{Db, Txn, to_doc},
    idempotency::{check, try_insert},
};

fn info(key: IdempotencyKey, endpoint: EndpointFlat) -> IdempotencyInfo {
    IdempotencyInfo::builder()
        .key(key)
        .endpoint(endpoint)
        .http_status(StatusCode::OK)
        .build()
}

async fn insert(db: &Db, warehouse_id: WarehouseId, info: &IdempotencyInfo) -> bool {
    let mut txn = Txn::read_write(db.clone());
    let inserted = try_insert(&mut txn, warehouse_id, info).await.unwrap();
    txn.commit().await.unwrap();
    inserted
}

async fn is_replay(db: &Db, warehouse_id: WarehouseId, key: &IdempotencyKey) -> bool {
    check(
        &mut Txn::latest(db.clone()),
        warehouse_id,
        key,
        EndpointFlat::CatalogV1CreateTable,
    )
    .await
    .unwrap()
    .is_replay()
}

#[tokio::test]
async fn a_committed_key_replays_for_its_endpoint_only() {
    let Some(db) = db().await else { return };
    let warehouse_id = WarehouseId::new_random();
    let key = IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    assert!(!is_replay(&db, warehouse_id, &key).await);

    let create = info(key, EndpointFlat::CatalogV1CreateTable);
    assert!(insert(&db, warehouse_id, &create).await);
    assert!(is_replay(&db, warehouse_id, &key).await);
    // A second insert of the same key loses.
    assert!(!insert(&db, warehouse_id, &create).await);
    // Keys are per warehouse.
    assert!(!is_replay(&db, WarehouseId::new_random(), &key).await);

    let reused = check(
        &mut Txn::latest(db.clone()),
        warehouse_id,
        &key,
        EndpointFlat::CatalogV1DropTable,
    )
    .await
    .unwrap_err();
    assert_eq!(reused.error.code, 400);
    assert_eq!(reused.error.r#type, "IdempotencyKeyReused");
}

#[tokio::test]
async fn an_expired_key_is_new_again() {
    let Some(db) = db().await else { return };
    let warehouse_id = WarehouseId::new_random();
    let key = IdempotencyKey::parse(&uuid::Uuid::now_v7().to_string()).unwrap();
    // A record the TTL policy has not deleted yet.
    let mut txn = Txn::read_write(db.clone());
    let name = db.doc_name(
        IDEMPOTENCY_RECORDS,
        &format!("{warehouse_id}:{}", key.as_uuid()),
    );
    let expired = IdempotencyDoc {
        warehouse_id,
        idempotency_key: key.as_uuid(),
        operation: EndpointFlat::CatalogV1CreateTable.name().to_string(),
        http_status: 200,
        created_at: 0,
        expire_at: firestore::jiff::Timestamp::now()
            - firestore::jiff::SignedDuration::from_secs(1),
    };
    txn.set(to_doc(name, &expired).unwrap());
    txn.commit().await.unwrap();

    assert!(!is_replay(&db, warehouse_id, &key).await);
    assert!(
        insert(
            &db,
            warehouse_id,
            &info(key, EndpointFlat::CatalogV1CreateTable)
        )
        .await
    );
    assert!(is_replay(&db, warehouse_id, &key).await);
}
