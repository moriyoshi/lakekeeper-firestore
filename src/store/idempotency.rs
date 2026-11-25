//! Idempotency keys (the `Idempotency-Key` header), following upstream's Postgres store: one
//! record per committed success, written in the mutation's transaction.
//!
//! Records expire `lifetime + grace_period` after they are written. Firestore's TTL policy on
//! `expire_at` (declared in `firestore.indexes.json`) deletes them, up to a day late, so a
//! record past `expire_at` counts as absent here.

use firestore::jiff::Timestamp;
use lakekeeper::{
    CONFIG, WarehouseId,
    api::{ErrorModel, Result, endpoints::EndpointFlat},
    service::{
        CatalogBackendError,
        idempotency::{IdempotencyCheck, IdempotencyInfo, IdempotencyKey},
    },
};

use super::{
    docs::{IDEMPOTENCY_RECORDS, IdempotencyDoc, now_micros},
    fs::{Txn, from_doc, to_doc},
};

fn name(txn: &Txn, warehouse_id: WarehouseId, key: &IdempotencyKey) -> String {
    txn.db().doc_name(
        IDEMPOTENCY_RECORDS,
        &format!("{warehouse_id}:{}", key.as_uuid()),
    )
}

/// The unexpired record of `key`.
async fn read(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    key: &IdempotencyKey,
) -> Result<Option<IdempotencyDoc>, CatalogBackendError> {
    let name = name(txn, warehouse_id, key);
    let Some(doc) = txn.get(&name).await? else {
        return Ok(None);
    };
    let record: IdempotencyDoc = from_doc(&doc)?;
    Ok((record.expire_at > Timestamp::now()).then_some(record))
}

/// Called outside a transaction. Firestore reads are strongly consistent, so a record committed
/// just before is seen, as upstream requires of this read.
pub(super) async fn check(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    key: &IdempotencyKey,
    endpoint: EndpointFlat,
) -> Result<IdempotencyCheck> {
    let Some(record) = read(txn, warehouse_id, key)
        .await
        .map_err(ErrorModel::from)?
    else {
        return Ok(IdempotencyCheck::NewRequest);
    };
    // Keys are globally unique: a key of another operation must not replay its response.
    if record.operation != endpoint.name() {
        return Err(ErrorModel::bad_request(
            "Idempotency-Key was already used for a different operation. \
             Keys must be globally unique and must not be reused across operations.",
            "IdempotencyKeyReused",
            None,
        )
        .into());
    }
    // Only successes are recorded; anything else is refused, as upstream does.
    if (200..300).contains(&record.http_status) {
        Ok(IdempotencyCheck::Replay)
    } else {
        lakekeeper::tracing::error!(
            warehouse_id = %warehouse_id,
            idempotency_key = %key.as_uuid(),
            status = record.http_status,
            "Idempotency record holds a non-success status, which the write path cannot produce"
        );
        Err(ErrorModel::internal(
            "Idempotency record holds an unexpected status and cannot be replayed.",
            "CorruptIdempotencyRecord",
            None,
        )
        .into())
    }
}

/// `false` if an unexpired record of the key exists. The read locks the record, so of two
/// transactions inserting the same key, one commits and the other fails with 409, where
/// upstream's returns `false` and replays; the client's retry then replays.
pub(super) async fn try_insert(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    info: &IdempotencyInfo,
) -> Result<bool> {
    if read(txn, warehouse_id, &info.key)
        .await
        .map_err(ErrorModel::from)?
        .is_some()
    {
        return Ok(false);
    }
    // Timestamps end at year 9999, as Firestore's do; the configured lifetime cannot reach it.
    let expire_at = firestore::jiff::SignedDuration::try_from(CONFIG.idempotency.total_retention())
        .ok()
        .and_then(|retention| Timestamp::now().checked_add(retention).ok())
        .ok_or_else(|| {
            ErrorModel::internal(
                "Idempotency retention is out of range",
                "IdempotencyRetentionOutOfRange",
                None,
            )
        })?;
    let doc = IdempotencyDoc {
        warehouse_id,
        idempotency_key: info.key.as_uuid(),
        operation: info.endpoint.name().to_string(),
        http_status: info.http_status.as_u16().into(),
        created_at: now_micros(),
        expire_at,
    };
    // `set`, not `create`: an expired record may still be there.
    let name = name(txn, warehouse_id, &info.key);
    txn.set(to_doc(name, &doc).map_err(ErrorModel::from)?);
    Ok(true)
}
