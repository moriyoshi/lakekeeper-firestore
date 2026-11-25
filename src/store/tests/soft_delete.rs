//! Soft deletion, its expiration task, and undrop on the emulator.

use std::sync::Arc;

use lakekeeper::{
    WarehouseId,
    api::{
        ErrorModel,
        iceberg::v1::{PaginationQuery, namespace::NamespaceDropFlags},
    },
    service::{
        CatalogNamespaceDropError, ClearTabularDeletedAtError, NamespaceId, TableId, TabularId,
        TabularListFlags, ViewOrTableDeletionInfo,
        tasks::{
            EnqueueOutcome, ScheduleTaskMetadata, TaskEntity, TaskId, TaskInput,
            WarehouseTaskEntityId, tabular_expiration_queue::QUEUE_NAME,
        },
    },
};

use super::{
    db,
    table::{create, namespace, small},
    tasks::{in_txn, pick},
};
use crate::store::{
    fs::{Db, Txn},
    namespace as ns_store, tabular, tasks, warehouse,
};

/// Table `name` in `ns`, soft-deleted with a soft-deletion task due in `due_in`, as the core
/// does it. Returns the table and its task.
async fn soft_delete(
    db: &Db,
    ns: (WarehouseId, NamespaceId),
    name: &str,
    due_in: chrono::Duration,
) -> (TableId, TaskId) {
    create(
        db,
        ns,
        name,
        Some(&format!("gs://b/m/{name}.json")),
        &small(&format!("gs://b/wh/ns/{}", uuid::Uuid::now_v7())),
    )
    .await
    .unwrap();
    let table_id = tabular::read_by_name(&mut Txn::latest(db.clone()), ns.0, ns.1, name)
        .await
        .unwrap()
        .unwrap()
        .tabular_id
        .into();
    let project_id = warehouse::read(&mut Txn::latest(db.clone()), ns.0)
        .await
        .unwrap()
        .unwrap()
        .project_id;
    let task = TaskInput {
        task_metadata: ScheduleTaskMetadata {
            project_id: Arc::new(project_id),
            parent_task_id: None,
            scheduled_for: Some(chrono::Utc::now() + due_in),
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id: ns.0,
                entity_id: WarehouseTaskEntityId::Table { table_id },
                entity_name: vec!["ns".to_string(), name.to_string()],
            },
            resource_demand: None,
        },
        payload: serde_json::json!({}),
    };
    let outcome = in_txn(db, async |txn| {
        tabular::mark_as_deleted(ns.0, TabularId::Table(table_id), false, txn)
            .await
            .map_err(ErrorModel::from)?;
        tasks::enqueue_task(&QUEUE_NAME, task, txn).await
    })
    .await
    .unwrap();
    let EnqueueOutcome::Inserted(task_id) = outcome else {
        panic!("{outcome:?}");
    };
    (table_id, task_id)
}

async fn deleted_listing(db: &Db, warehouse_id: WarehouseId) -> Vec<ViewOrTableDeletionInfo> {
    let flags = TabularListFlags {
        include_active: false,
        include_staged: false,
        include_deleted: true,
    };
    tabular::list(
        warehouse_id,
        None,
        flags,
        &mut Txn::latest(db.clone()),
        None,
        PaginationQuery::new_with_page_size(100),
    )
    .await
    .unwrap()
    .into_iter()
    .map(|(_, info)| info)
    .collect()
}

async fn undrop(
    db: &Db,
    warehouse_id: WarehouseId,
    ids: &[TabularId],
    task_ids: Option<&[TaskId]>,
) -> Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
    let mut txn = Txn::read_write(db.clone());
    match tabular::clear_deleted_at(warehouse_id, ids, task_ids, &mut txn).await {
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

async fn is_named(db: &Db, warehouse_id: WarehouseId, ns: NamespaceId, name: &str) -> bool {
    tabular::read_by_name(&mut Txn::latest(db.clone()), warehouse_id, ns, name)
        .await
        .unwrap()
        .is_some()
}

#[tokio::test]
async fn undrop_restores_the_name_and_returns_the_task() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (table_id, task_id) = soft_delete(&db, ns, "t", chrono::Duration::days(7)).await;
    assert!(!is_named(&db, ns.0, ns.1, "t").await);

    let listed = deleted_listing(&db, ns.0).await;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        listed[0].expiration_task().map(|e| e.task_id),
        Some(task_id)
    );

    let unknown = TabularId::Table(TableId::new_random());
    let err = undrop(&db, ns.0, &[TabularId::Table(table_id), unknown], None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClearTabularDeletedAtError::TabularNotFound(_)),
        "{err:?}"
    );

    let undropped = undrop(&db, ns.0, &[TabularId::Table(table_id)], None)
        .await
        .unwrap();
    assert_eq!(undropped.len(), 1);
    assert_eq!(undropped[0].deleted_at(), None);
    assert_eq!(
        undropped[0].expiration_task().map(|e| e.task_id),
        Some(task_id)
    );
    assert!(is_named(&db, ns.0, ns.1, "t").await);
    assert_eq!(deleted_listing(&db, ns.0).await.len(), 0);
}

#[tokio::test]
async fn undrop_fails_when_the_name_is_taken() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (table_id, _) = soft_delete(&db, ns, "t", chrono::Duration::days(7)).await;
    create(
        &db,
        ns,
        "t",
        Some("gs://b/m/t2.json"),
        &small("gs://b/wh/ns/t2"),
    )
    .await
    .unwrap();
    let err = undrop(&db, ns.0, &[TabularId::Table(table_id)], None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, ClearTabularDeletedAtError::TabularAlreadyExists(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn undrop_for_tasks_takes_only_tabulars_of_those_tasks() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (a, a_task) = soft_delete(&db, ns, "a", chrono::Duration::days(7)).await;
    let (b, _) = soft_delete(&db, ns, "b", chrono::Duration::days(7)).await;
    let ids = [TabularId::Table(a), TabularId::Table(b)];

    let other = TaskId::from(uuid::Uuid::now_v7());
    let undropped = undrop(&db, ns.0, &ids, Some(&[other])).await.unwrap();
    assert_eq!(undropped.len(), 0);
    assert_eq!(deleted_listing(&db, ns.0).await.len(), 2);

    let undropped = undrop(&db, ns.0, &ids, Some(&[a_task])).await.unwrap();
    assert_eq!(undropped.len(), 1);
    assert_eq!(undropped[0].tabular_id(), TabularId::Table(a));
    assert!(is_named(&db, ns.0, ns.1, "a").await);
    assert!(!is_named(&db, ns.0, ns.1, "b").await);
}

#[tokio::test]
async fn namespace_drop_waits_for_running_soft_deletions() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let (_, task_id) = soft_delete(&db, ns, "t", chrono::Duration::zero()).await;
    let flags = NamespaceDropFlags {
        force: false,
        purge: false,
        recursive: true,
    };
    let drop = async || {
        let mut txn = Txn::read_write(db.clone());
        let result = ns_store::drop(ns.0, ns.1, flags, &mut txn).await;
        txn.rollback().await.unwrap();
        result
    };
    // Scheduled, the task is reported for the core to cancel.
    assert_eq!(drop().await.unwrap().open_tasks, vec![task_id]);

    // Other tests' soft-deletion tasks are due in days, so only this one can be picked.
    let picked = pick(&db, &QUEUE_NAME).await.unwrap();
    assert_eq!(picked.id.task_id, task_id);
    let err = drop().await.unwrap_err();
    assert!(
        matches!(
            err,
            CatalogNamespaceDropError::NamespaceHasRunningTabularExpirations(_)
        ),
        "{err:?}"
    );
}
