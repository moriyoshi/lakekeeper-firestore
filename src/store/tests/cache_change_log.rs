//! The cache change log on the emulator.

use std::time::Duration;

use lakekeeper::{
    WarehouseId,
    service::cache_invalidation::{
        CacheChangeLog, CacheDomain, CacheKey, ChangeSet, LoggedChange, PollOutcome,
    },
    tokio::time::{Instant, sleep, timeout},
};

use super::db;
use crate::store::{
    cache_change_log::{FirestoreChangeLog, prune, record},
    docs::{CACHE_CHANGES, CacheChangeDoc},
    fs::{Db, Query, Txn, to_doc},
};

async fn changes_within(log: &mut FirestoreChangeLog, wait: Duration) -> Vec<LoggedChange> {
    let deadline = Instant::now() + wait;
    let mut out = Vec::new();
    while Instant::now() < deadline {
        match log.poll().await.unwrap() {
            PollOutcome::Changes(changes) => out.extend(changes),
            PollOutcome::Reset(reason) => panic!("reset: {reason:?}"),
        }
        if !out.is_empty() {
            // Give a second batch of the same write a moment to arrive.
            sleep(Duration::from_millis(200)).await;
            if let PollOutcome::Changes(more) = log.poll().await.unwrap() {
                out.extend(more);
            }
            return out;
        }
        sleep(Duration::from_millis(50)).await;
    }
    out
}

async fn commit(db: &Db, set: &ChangeSet) {
    let mut txn = Txn::read_write(db.clone());
    record(&mut txn, set).unwrap();
    txn.commit().await.unwrap();
}

#[tokio::test]
async fn another_instance_reads_committed_changes() {
    let Some(db) = db().await else { return };
    // A change recorded before the log starts is not reported.
    let mut before = ChangeSet::new();
    before.insert(CacheKey::Warehouse(WarehouseId::new_random()));
    commit(&db, &before).await;

    let mut log = FirestoreChangeLog::new(db.clone());
    let first = timeout(Duration::from_secs(10), log.poll())
        .await
        .expect("the first poll waits for the listener")
        .unwrap();
    assert_eq!(first, PollOutcome::Changes(Vec::new()));
    assert_eq!(
        changes_within(&mut log, Duration::from_millis(500)).await,
        []
    );

    let warehouse = WarehouseId::new_random();
    let mut set = ChangeSet::new();
    set.insert(CacheKey::Warehouse(warehouse))
        .insert_whole_domain(CacheDomain::RoleAncestors);
    commit(&db, &set).await;
    let mut changes = changes_within(&mut log, Duration::from_secs(5)).await;
    changes.sort_by(|a, b| a.domain.cmp(&b.domain));
    assert_eq!(
        changes,
        [
            LoggedChange {
                domain: CacheDomain::RoleAncestors.as_str().to_string(),
                keys: Vec::new(),
            },
            LoggedChange {
                domain: CacheDomain::Warehouse.as_str().to_string(),
                keys: vec![CacheKey::Warehouse(warehouse).encode()],
            },
        ]
    );

    // A rolled-back write records nothing.
    let mut txn = Txn::read_write(db.clone());
    record(&mut txn, &before).unwrap();
    txn.rollback().await.unwrap();
    assert_eq!(
        changes_within(&mut log, Duration::from_millis(500)).await,
        []
    );

    // Many keys of one domain span several documents.
    let mut many = ChangeSet::new();
    for _ in 0..1500 {
        many.insert(CacheKey::Warehouse(WarehouseId::new_random()));
    }
    commit(&db, &many).await;
    let changes = changes_within(&mut log, Duration::from_secs(5)).await;
    assert_eq!(changes.iter().map(|c| c.keys.len()).sum::<usize>(), 1500);
    assert_eq!(changes.len(), 2);
}

#[tokio::test]
async fn pruning_deletes_old_changes() {
    let Some(db) = db().await else { return };
    let old = CacheChangeDoc {
        domain: "warehouse".to_string(),
        keys: Vec::new(),
        recorded_at: 0,
    };
    let mut txn = Txn::read_write(db.clone());
    txn.set(to_doc(db.doc_name(CACHE_CHANGES, "old"), &old).unwrap());
    txn.commit().await.unwrap();
    let mut recent = ChangeSet::new();
    recent.insert(CacheKey::Warehouse(WarehouseId::new_random()));
    commit(&db, &recent).await;

    assert_eq!(prune(&db, Duration::from_hours(2)).await.unwrap(), 1);
    let left = Txn::latest(db.clone())
        .query(&Query::new(CACHE_CHANGES))
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(prune(&db, Duration::from_hours(2)).await.unwrap(), 0);
}
