//! The task management API on the emulator.

use std::{collections::HashSet, sync::Arc};

use lakekeeper::{
    ProjectId, WarehouseId,
    api::management::v1::tasks::{ListTasksRequest, TaskStatus, WarehouseTaskEntityFilter},
    service::{
        TaskList,
        tasks::{
            EnqueueOutcome, ResolvedTaskEntity, RetryDecision, ScheduleTaskMetadata,
            TaskCheckState, TaskDetailsScope, TaskEntity, TaskFilter, TaskId, TaskInput,
            TaskQueueName, TaskResolveScope,
        },
    },
};

use super::{
    db,
    tasks::{enqueue, failure, in_txn, pick, queue, scope, success, table_task},
};
use crate::store::{
    fs::{Db, Txn},
    task_admin, tasks,
};

fn inserted(outcome: EnqueueOutcome) -> TaskId {
    let EnqueueOutcome::Inserted(id) = outcome else {
        panic!("{outcome:?}");
    };
    id
}

async fn list(db: &Db, filter: &TaskFilter, query: &ListTasksRequest) -> TaskList {
    task_admin::list_tasks(filter, query, &mut Txn::latest(db.clone()))
        .await
        .unwrap()
}

/// `(task, attempt, status)` of the listed rows, in order.
fn rows(list: &TaskList) -> Vec<(TaskId, i32, TaskStatus)> {
    list.tasks
        .iter()
        .map(|t| (t.task_id(), t.id.attempt, t.status))
        .collect()
}

fn in_queue(q: &TaskQueueName) -> ListTasksRequest {
    ListTasksRequest {
        queue_name: Some(vec![q.clone()]),
        ..Default::default()
    }
}

/// In queue `q` of a new warehouse: `retried`, whose first attempt failed and which waits for
/// its second, then `done`, which succeeded.
async fn retried_and_done(db: &Db, q: &TaskQueueName) -> (ProjectId, WarehouseId, TaskId, TaskId) {
    let (project_id, warehouse_id) = scope(db).await;
    let retried = inserted(
        enqueue(
            db,
            q,
            table_task(&project_id, warehouse_id, uuid::Uuid::now_v7(), None),
        )
        .await,
    );
    let task = pick(db, q).await.unwrap();
    failure(
        db,
        task.id,
        5,
        RetryDecision::retry(chrono::Duration::hours(1)),
    )
    .await
    .unwrap();
    let done = inserted(
        enqueue(
            db,
            q,
            table_task(&project_id, warehouse_id, uuid::Uuid::now_v7(), None),
        )
        .await,
    );
    let task = pick(db, q).await.unwrap();
    assert_eq!(task.id.task_id, done);
    success(db, task.id).await.unwrap();
    (project_id, warehouse_id, retried, done)
}

#[tokio::test]
async fn list_shows_active_tasks_and_their_newest_logs() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id, retried, done) = retried_and_done(&db, &q).await;
    let filter = TaskFilter::WarehouseId {
        warehouse_id,
        project_id: Arc::new(project_id.clone()),
    };

    // A retried task is listed twice, as upstream does: active, and its failed attempt.
    let all = vec![
        (done, 1, TaskStatus::Success),
        (retried, 1, TaskStatus::Scheduled),
        (retried, 1, TaskStatus::Failed),
    ];
    assert_eq!(rows(&list(&db, &filter, &in_queue(&q)).await), all);

    // Pages of one row walk the same rows.
    let mut paged = Vec::new();
    let mut query = ListTasksRequest {
        page_size: Some(1),
        ..in_queue(&q)
    };
    loop {
        let page = list(&db, &filter, &query).await;
        if page.tasks.is_empty() {
            break;
        }
        paged.extend(rows(&page));
        query.page_token = page.next_page_token;
    }
    assert_eq!(paged, all);

    let failed = ListTasksRequest {
        status: Some(vec![TaskStatus::Failed]),
        ..in_queue(&q)
    };
    assert_eq!(
        rows(&list(&db, &filter, &failed).await),
        vec![(retried, 1, TaskStatus::Failed)]
    );

    let details = task_admin::get_task_details(
        done,
        TaskDetailsScope::Warehouse {
            project_id: Arc::new(project_id.clone()),
            warehouse_id,
        },
        10,
        &mut Txn::latest(db.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    let table_id = match details.task.task_metadata.entity {
        TaskEntity::EntityInWarehouse { entity_id, .. } => entity_id.as_uuid(),
        other => panic!("{other:?}"),
    };
    let by_table = ListTasksRequest {
        entities: Some(vec![WarehouseTaskEntityFilter::Table {
            table_id: table_id.into(),
        }]),
        ..in_queue(&q)
    };
    assert_eq!(
        rows(&list(&db, &filter, &by_table).await),
        vec![(done, 1, TaskStatus::Success)]
    );

    // Another warehouse of the project sees none of them.
    let (_, other) = scope(&db).await;
    let other = TaskFilter::WarehouseId {
        warehouse_id: other,
        project_id: Arc::new(project_id),
    };
    assert_eq!(rows(&list(&db, &other, &in_queue(&q)).await), vec![]);
}

#[tokio::test]
async fn project_listings_include_warehouse_tasks_on_request() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id) = scope(&db).await;
    let in_warehouse = inserted(
        enqueue(
            &db,
            &q,
            table_task(&project_id, warehouse_id, uuid::Uuid::now_v7(), None),
        )
        .await,
    );
    let project_task = TaskInput {
        task_metadata: ScheduleTaskMetadata {
            project_id: Arc::new(project_id.clone()),
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::Project,
            resource_demand: None,
        },
        payload: serde_json::json!({}),
    };
    let in_project = inserted(enqueue(&db, &q, project_task).await);

    let listed = async |include_sub_tasks| {
        let filter = TaskFilter::ProjectId {
            project_id: Arc::new(project_id.clone()),
            include_sub_tasks,
        };
        list(&db, &filter, &in_queue(&q))
            .await
            .tasks
            .iter()
            .map(lakekeeper::service::tasks::TaskInfo::task_id)
            .collect::<HashSet<_>>()
    };
    assert_eq!(listed(false).await, HashSet::from([in_project]));
    assert_eq!(
        listed(true).await,
        HashSet::from([in_project, in_warehouse])
    );
}

#[tokio::test]
async fn details_have_the_latest_attempt_and_earlier_ones() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id, retried, _) = retried_and_done(&db, &q).await;
    in_txn(&db, async |txn| {
        task_admin::run_tasks_at(&[retried], None, txn).await
    })
    .await
    .unwrap();
    let second = pick(&db, &q).await.unwrap();
    assert_eq!((second.id.task_id, second.id.attempt), (retried, 2));

    let details = async |scope, num_attempts| {
        task_admin::get_task_details(retried, scope, num_attempts, &mut Txn::latest(db.clone()))
            .await
            .unwrap()
    };
    let in_warehouse = TaskDetailsScope::Warehouse {
        project_id: Arc::new(project_id.clone()),
        warehouse_id,
    };
    let d = details(in_warehouse.clone(), 10).await.unwrap();
    assert_eq!((d.task.id.attempt, d.task.status), (2, TaskStatus::Running));
    assert_eq!(d.data, serde_json::json!({"n": 1}));
    assert_eq!(d.message, None);
    let earlier: Vec<_> = d
        .attempts
        .iter()
        .map(|a| (a.attempt, a.status, a.message.clone()))
        .collect();
    assert_eq!(
        earlier,
        vec![(1, TaskStatus::Failed, Some("boom".to_string()))]
    );

    let latest_only = details(in_warehouse, 0).await.unwrap();
    assert_eq!(latest_only.attempts.len(), 0);
    let in_project = TaskDetailsScope::Project {
        project_id: Arc::new(project_id),
    };
    assert!(details(in_project, 10).await.is_none());
}

#[tokio::test]
async fn resolve_finds_active_and_finished_tasks() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id, retried, done) = retried_and_done(&db, &q).await;
    let unknown = TaskId::from(uuid::Uuid::now_v7());
    let resolve = async |scope| {
        task_admin::resolve_tasks(
            scope,
            &[retried, done, unknown],
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap()
    };

    let resolved = resolve(TaskResolveScope::Warehouse {
        project_id: Arc::new(project_id.clone()),
        warehouse_id: None,
    })
    .await;
    let ids: HashSet<TaskId> = resolved.iter().map(|r| r.task_id).collect();
    assert_eq!(ids, HashSet::from([retried, done]));
    for r in &resolved {
        assert_eq!(r.queue_name, q);
        let ResolvedTaskEntity::Table(table) = &r.entity else {
            panic!("{:?}", r.entity);
        };
        assert_eq!(table.warehouse_id, warehouse_id);
        assert_eq!(table.table_ident.to_string(), "ns.t");
    }

    let project_only = resolve(TaskResolveScope::Project {
        project_id: Arc::new(project_id),
    })
    .await;
    assert_eq!(project_only.len(), 0);
}

#[tokio::test]
async fn stopped_tasks_can_be_rescheduled() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id) = scope(&db).await;
    let id = inserted(
        enqueue(
            &db,
            &q,
            table_task(&project_id, warehouse_id, uuid::Uuid::now_v7(), None),
        )
        .await,
    );
    let first = pick(&db, &q).await.unwrap();
    in_txn(&db, async |txn| task_admin::stop_tasks(&[id], txn).await)
        .await
        .unwrap();
    let state = in_txn(&db, async |txn| {
        tasks::check_and_heartbeat(first.id, 0.5, None, txn).await
    })
    .await
    .unwrap();
    assert_eq!(state, TaskCheckState::Stop);

    // Rescheduled for later, the stopping attempt is failed and the task is not due.
    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    in_txn(&db, async |txn| {
        task_admin::run_tasks_at(&[id], Some(later), txn).await
    })
    .await
    .unwrap();
    assert!(pick(&db, &q).await.is_none());
    let filter = TaskFilter::WarehouseId {
        warehouse_id,
        project_id: Arc::new(project_id),
    };
    assert_eq!(
        rows(&list(&db, &filter, &in_queue(&q)).await),
        vec![(id, 1, TaskStatus::Scheduled), (id, 1, TaskStatus::Failed)]
    );
    // The stopped worker can no longer record its attempt.
    assert!(success(&db, first.id).await.is_err());

    in_txn(&db, async |txn| {
        task_admin::run_tasks_at(&[id], None, txn).await
    })
    .await
    .unwrap();
    let second = pick(&db, &q).await.unwrap();
    assert_eq!((second.id.task_id, second.id.attempt), (id, 2));
}

#[tokio::test]
async fn only_scheduled_tasks_of_the_queue_are_reparented() {
    let Some(db) = db().await else { return };
    let q = queue();
    let (project_id, warehouse_id) = scope(&db).await;
    let id = inserted(
        enqueue(
            &db,
            &q,
            table_task(&project_id, warehouse_id, uuid::Uuid::now_v7(), None),
        )
        .await,
    );
    let parent = TaskId::from(uuid::Uuid::now_v7());
    let data = serde_json::json!({"n": 2});
    let reparent = async |queue_name: &TaskQueueName| {
        in_txn(&db, async |txn| {
            task_admin::reparent_pending_task(queue_name, &[], id, parent, &data, txn).await
        })
        .await
        .unwrap()
    };

    assert!(!reparent(&queue()).await);
    assert!(reparent(&q).await);
    let task = pick(&db, &q).await.unwrap();
    assert_eq!(task.task_metadata.parent_task_id, Some(parent));
    assert_eq!(task.data, data);
    // Running now, so no longer pending.
    assert!(!reparent(&q).await);
}
