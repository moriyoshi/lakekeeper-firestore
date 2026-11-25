//! The task queue on the emulator.

use std::{sync::Arc, time::Duration};

use lakekeeper::{
    ProjectId, WarehouseId,
    api::{ErrorModel, management::v1::DeleteWarehouseQuery},
    service::{
        CatalogDeleteWarehouseError, TableId,
        tasks::{
            CancelTasksFilter, EnqueueOutcome, FailedAttempt, PickTaskRequest, RetryDecision,
            RetryPolicy, ScheduleTaskMetadata, TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE, Task,
            TaskAttemptId, TaskCheckState, TaskEntity, TaskInput, TaskQueueDemandRequest,
            TaskQueueName, TaskResourceDemandRange, TaskScheduleCondition, TaskScheduleUpsert,
            UpsertTaskScheduleError, WarehouseTaskEntityId,
        },
    },
    tokio::{sync::RwLock, time::timeout},
};

use super::{db, futures_join, new_project, warehouse_request};
use crate::store::{
    FirestoreState,
    docs::TASK_LOGS,
    fs::{Db, Query, Txn, string_value},
    tasks, warehouse,
};

pub(super) fn state(db: &Db) -> FirestoreState {
    FirestoreState {
        db: db.clone(),
        files: super::files(),
        health: Arc::new(RwLock::new(Vec::new())),
    }
}

fn retry_now(_: &FailedAttempt<'_>) -> RetryDecision {
    RetryDecision::retry(chrono::Duration::zero())
}

fn retry_later(_: &FailedAttempt<'_>) -> RetryDecision {
    RetryDecision::retry(chrono::Duration::hours(1))
}

fn request(
    queue: &TaskQueueName,
    heartbeat_timeout: chrono::Duration,
    demand: TaskResourceDemandRange,
) -> PickTaskRequest<'_> {
    PickTaskRequest::builder()
        .queue_name(queue)
        .default_max_time_since_last_heartbeat(heartbeat_timeout)
        .retry_policy(RetryPolicy {
            max_retries: 3,
            retry_decision: retry_now,
        })
        .resource_demand(demand)
        .build()
}

pub(super) async fn pick(db: &Db, queue: &TaskQueueName) -> Option<Task> {
    tasks::pick_new_task(
        request(
            queue,
            chrono::Duration::hours(1),
            TaskResourceDemandRange::default(),
        ),
        &state(db),
    )
    .await
    .unwrap()
}

/// A queue name of its own for each test; queues are shared within the database.
pub(super) fn queue() -> TaskQueueName {
    TaskQueueName::from(format!("q{}", uuid::Uuid::now_v7().simple()))
}

pub(super) fn table_task(
    project_id: &ProjectId,
    warehouse_id: WarehouseId,
    table: uuid::Uuid,
    demand: Option<i64>,
) -> TaskInput {
    TaskInput {
        task_metadata: ScheduleTaskMetadata {
            project_id: Arc::new(project_id.clone()),
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id,
                entity_id: WarehouseTaskEntityId::Table {
                    table_id: TableId::from(table),
                },
                entity_name: vec!["ns".to_string(), "t".to_string()],
            },
            resource_demand: demand,
        },
        payload: serde_json::json!({"n": 1}),
    }
}

pub(super) async fn enqueue(db: &Db, queue: &TaskQueueName, task: TaskInput) -> EnqueueOutcome {
    // The store wants a `'static` queue name, as the core's are.
    let queue: &'static TaskQueueName = Box::leak(Box::new(queue.clone()));
    let mut txn = Txn::read_write(db.clone());
    let outcome = tasks::enqueue_task(queue, task, &mut txn).await.unwrap();
    txn.commit().await.unwrap();
    outcome
}

pub(super) async fn in_txn<T>(
    db: &Db,
    f: impl AsyncFnOnce(&mut Txn) -> Result<T, ErrorModel>,
) -> Result<T, ErrorModel> {
    let mut txn = Txn::read_write(db.clone());
    match f(&mut txn).await {
        Ok(out) => {
            txn.commit().await?;
            Ok(out)
        }
        Err(e) => {
            // A dropped transaction rolls back in the background, which a test runtime may
            // not live to run.
            txn.rollback().await.unwrap();
            Err(e)
        }
    }
}

pub(super) async fn success(db: &Db, id: TaskAttemptId) -> Result<(), ErrorModel> {
    in_txn(db, async |txn| {
        tasks::record_success(id, Some("done"), txn).await
    })
    .await
}

pub(super) async fn failure(
    db: &Db,
    id: TaskAttemptId,
    max_retries: i32,
    decision: RetryDecision,
) -> Result<(), ErrorModel> {
    in_txn(db, async |txn| {
        tasks::record_failure(id, "boom", max_retries, &decision, txn).await
    })
    .await
}

/// `(attempt, status)` of the logged attempts of `task_id`, in attempt order.
async fn logs(db: &Db, task_id: uuid::Uuid) -> Vec<(i64, String)> {
    let docs = Txn::latest(db.clone())
        .query(&Query::new(TASK_LOGS).eq("task_id", string_value(task_id.to_string())))
        .await
        .unwrap();
    let mut out: Vec<(i64, String)> = docs
        .iter()
        .map(|d| {
            let log: crate::store::docs::TaskLogDoc = crate::store::fs::from_doc(d).unwrap();
            (i64::from(log.attempt), log.status)
        })
        .collect();
    out.sort();
    out
}

/// A project with one warehouse.
pub(super) async fn scope(db: &Db) -> (ProjectId, WarehouseId) {
    let project_id = new_project(db).await;
    let mut txn = Txn::read_write(db.clone());
    let wh = warehouse::create_warehouse(&project_id, warehouse_request("wh"), &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    (project_id, wh.warehouse_id)
}

#[tokio::test]
async fn one_active_task_per_entity_and_queue() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let (queue_name1, queue_name2) = (queue(), queue());
    let table = uuid::Uuid::now_v7();

    let EnqueueOutcome::Inserted(first) =
        enqueue(&db, &queue_name1, table_task(&project, wh, table, None)).await
    else {
        panic!("not inserted")
    };
    match enqueue(&db, &queue_name1, table_task(&project, wh, table, None)).await {
        EnqueueOutcome::Existing { task_id, .. } => assert_eq!(task_id, first),
        other @ EnqueueOutcome::Inserted(_) => panic!("{other:?}"),
    }
    assert!(matches!(
        enqueue(&db, &queue_name2, table_task(&project, wh, table, None)).await,
        EnqueueOutcome::Inserted(_)
    ));

    // Once the task is done, the entity can have a new one.
    let task = pick(&db, &queue_name1).await.unwrap();
    success(&db, task.id).await.unwrap();
    assert!(matches!(
        enqueue(&db, &queue_name1, table_task(&project, wh, table, None)).await,
        EnqueueOutcome::Inserted(_)
    ));
}

#[tokio::test]
async fn picked_tasks_complete_once() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let task = pick(&db, &queue_name).await.unwrap();
    assert_eq!(task.id.attempt, 1);
    assert_eq!(task.data, serde_json::json!({"n": 1}));
    assert!(
        pick(&db, &queue_name).await.is_none(),
        "a running task is not picked again"
    );

    success(&db, task.id).await.unwrap();
    let again = success(&db, task.id).await.unwrap_err();
    assert_eq!(
        (again.code, again.r#type.as_str()),
        (409, TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE)
    );
    let unknown = TaskAttemptId {
        task_id: uuid::Uuid::now_v7().into(),
        attempt: 1,
    };
    assert_eq!(success(&db, unknown).await.unwrap_err().code, 404);
    assert_eq!(
        logs(&db, *task.id.task_id).await,
        [(1, "success".to_string())]
    );
}

#[tokio::test]
async fn failures_retry_until_max_retries() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let first = pick(&db, &queue_name).await.unwrap();
    failure(&db, first.id, 2, retry_now(&dummy_attempt()))
        .await
        .unwrap();
    // A zero backoff makes it due again at once.
    let second = pick(&db, &queue_name).await.unwrap();
    assert_eq!(second.id.attempt, 2);
    // The second attempt is the last one allowed.
    failure(&db, second.id, 2, retry_now(&dummy_attempt()))
        .await
        .unwrap();
    assert!(pick(&db, &queue_name).await.is_none());
    assert_eq!(
        logs(&db, *first.id.task_id).await,
        [(1, "failed".to_string()), (2, "failed".to_string())]
    );
    assert_eq!(
        failure(&db, second.id, 2, retry_now(&dummy_attempt()))
            .await
            .unwrap_err()
            .code,
        409
    );
}

fn dummy_attempt() -> FailedAttempt<'static> {
    FailedAttempt {
        task_id: uuid::Uuid::nil().into(),
        attempt: 1,
        schedule_id: None,
        kind: lakekeeper::service::tasks::FailureKind::Error,
        error: "",
        resource_demand: None,
        execution_details: None,
    }
}

#[tokio::test]
async fn backoff_and_give_up() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let first = pick(&db, &queue_name).await.unwrap();
    failure(&db, first.id, 10, retry_later(&dummy_attempt()))
        .await
        .unwrap();
    assert!(
        pick(&db, &queue_name).await.is_none(),
        "backed off for an hour"
    );

    // A task given up ends at once, whatever its retries.
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;
    let other = pick(&db, &queue_name).await.unwrap();
    failure(&db, other.id, 10, RetryDecision::give_up("no point"))
        .await
        .unwrap();
    assert!(pick(&db, &queue_name).await.is_none());
    assert_eq!(
        logs(&db, *other.id.task_id).await,
        [(1, "failed".to_string())]
    );
}

#[tokio::test]
async fn expired_heartbeats_are_retried() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;
    let short = chrono::Duration::milliseconds(200);

    let first = tasks::pick_new_task(
        request(&queue_name, short, TaskResourceDemandRange::default()),
        &state(&db),
    )
    .await
    .unwrap()
    .unwrap();
    // Not expired yet: nothing to pick.
    assert!(
        tasks::pick_new_task(
            request(&queue_name, short, TaskResourceDemandRange::default()),
            &state(&db)
        )
        .await
        .unwrap()
        .is_none()
    );
    lakekeeper::tokio::time::sleep(Duration::from_millis(400)).await;
    // The next pick fails the expired attempt, and the retry is due at once.
    let second = tasks::pick_new_task(
        request(&queue_name, short, TaskResourceDemandRange::default()),
        &state(&db),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(second.id.task_id, first.id.task_id);
    assert_eq!(second.id.attempt, 2);
    // The worker of the first attempt finds it closed.
    let late = success(&db, first.id).await.unwrap_err();
    assert_eq!(late.code, 409, "{late:?}");
}

#[tokio::test]
async fn heartbeats_report_cancellation() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;
    let task = pick(&db, &queue_name).await.unwrap();

    let beat = |db: Db, id| async move {
        in_txn(&db, async |txn| {
            tasks::check_and_heartbeat(id, 0.5, Some(serde_json::json!({"step": 1})), txn).await
        })
        .await
        .unwrap()
    };
    assert_eq!(beat(db.clone(), task.id).await, TaskCheckState::Continue);
    in_txn(&db, async |txn| {
        tasks::cancel_scheduled_tasks(
            None,
            &[],
            CancelTasksFilter::TaskIds(vec![task.id.task_id]),
            true,
            txn,
        )
        .await
    })
    .await
    .unwrap();
    assert_eq!(beat(db.clone(), task.id).await, TaskCheckState::NotActive);
    assert_eq!(
        logs(&db, *task.id.task_id).await,
        [(1, "cancelled".to_string())]
    );
}

#[tokio::test]
async fn cancel_leaves_running_tasks_unless_asked() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;
    let running = pick(&db, &queue_name).await.unwrap();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let cancel = |db: Db, all: bool| {
        let queue_name = queue_name.clone();
        async move {
            in_txn(&db, async |txn| {
                tasks::cancel_scheduled_tasks(
                    Some(&queue_name),
                    &[],
                    CancelTasksFilter::WarehouseId { warehouse_id: wh },
                    all,
                    txn,
                )
                .await
            })
            .await
            .unwrap();
        }
    };
    cancel(db.clone(), false).await;
    assert!(
        pick(&db, &queue_name).await.is_none(),
        "the scheduled task is cancelled"
    );
    success(&db, running.id).await.unwrap();
}

#[tokio::test]
async fn racing_workers_pick_a_task_once() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    // A worker whose claim aborts in lock contention (409) picks again, as the core's workers
    // do on their next poll; on a busy emulator both claims can abort.
    let pick_retrying = || async {
        for _ in 0..5 {
            match tasks::pick_new_task(
                request(
                    &queue_name,
                    chrono::Duration::hours(1),
                    TaskResourceDemandRange::default(),
                ),
                &state(&db),
            )
            .await
            {
                Err(e) if e.code == 409 => {}
                other => return other.unwrap(),
            }
        }
        panic!("every claim aborted");
    };
    let (a, b) = timeout(
        Duration::from_secs(60),
        futures_join(Box::pin(pick_retrying()), Box::pin(pick_retrying())),
    )
    .await
    .expect("the picks finish");
    assert_eq!(
        [a.is_some(), b.is_some()].iter().filter(|x| **x).count(),
        1,
        "{a:?} {b:?}"
    );
}

#[tokio::test]
async fn picks_respect_the_demand_range() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), Some(10)),
    )
    .await;
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let pick_range = |range| {
        let (db, queue_name) = (db.clone(), queue_name.clone());
        async move {
            tasks::pick_new_task(
                request(&queue_name, chrono::Duration::hours(1), range),
                &state(&db),
            )
            .await
            .unwrap()
        }
    };
    let small = TaskResourceDemandRange::builder().at_most(5).build();
    let big = TaskResourceDemandRange::builder().above(5).build();
    let empty = TaskResourceDemandRange::builder()
        .above(5)
        .at_most(5)
        .build();
    assert!(pick_range(empty).await.is_none());
    let picked = pick_range(big).await.unwrap();
    assert_eq!(picked.task_metadata.resource_demand, Some(10));
    let picked = pick_range(small).await.unwrap();
    assert_eq!(picked.task_metadata.resource_demand, None);
    assert!(pick_range(small).await.is_none());
}

#[tokio::test]
async fn demand_counts_what_a_pick_can_take() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let (queue_name, legacy) = (queue(), queue());
    let mut later = table_task(&project, wh, uuid::Uuid::now_v7(), None);
    later.task_metadata.scheduled_for = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    for (q, task) in [
        (
            &queue_name,
            table_task(&project, wh, uuid::Uuid::now_v7(), Some(10)),
        ),
        (
            &queue_name,
            table_task(&project, wh, uuid::Uuid::now_v7(), None),
        ),
        (
            &legacy,
            table_task(&project, wh, uuid::Uuid::now_v7(), None),
        ),
        (&queue_name, later),
    ] {
        enqueue(&db, q, task).await;
    }
    let legacy_names = [&legacy];
    let demand = async |range, heartbeat_timeout| {
        let request = || {
            TaskQueueDemandRequest::builder()
                .queue_name(&queue_name)
                .legacy_queue_names(&legacy_names)
                .default_max_time_since_last_heartbeat(heartbeat_timeout)
                .resource_demand(range)
                .build()
        };
        let demand = tasks::task_queue_demand(request(), &state(&db))
            .await
            .unwrap();
        let oldest = tasks::task_queue_oldest_due(request(), &state(&db))
            .await
            .unwrap();
        assert_eq!(oldest, demand.oldest_due_since);
        (demand.due, demand.heartbeat_expired, demand.running, oldest)
    };
    let all = TaskResourceDemandRange::default();
    let big = TaskResourceDemandRange::builder().above(5).build();
    let empty = TaskResourceDemandRange::builder()
        .above(5)
        .at_most(5)
        .build();
    let hour = chrono::Duration::hours(1);

    let (due, expired, running, oldest) = demand(all, hour).await;
    assert_eq!((due, expired, running), (3, 0, 0));
    assert!(oldest.unwrap() <= chrono::Utc::now());
    assert_eq!(demand(big, hour).await.0, 1);
    assert_eq!(demand(empty, hour).await, (0, 0, 0, None));

    let picked = pick(&db, &queue_name).await.unwrap();
    let (due, expired, running, _) = demand(all, hour).await;
    assert_eq!((due, expired, running), (2, 0, 1));
    // With no time allowed between heartbeats, the picked task's heartbeat has expired.
    lakekeeper::tokio::time::sleep(Duration::from_millis(10)).await;
    let (due, expired, running, oldest) = demand(all, chrono::Duration::zero()).await;
    assert_eq!((due, expired, running), (2, 1, 0));
    assert!(oldest.is_some());

    success(&db, picked.id).await.unwrap();
    while let Some(t) = pick(&db, &queue_name).await {
        success(&db, t.id).await.unwrap();
    }
    while let Some(t) = pick(&db, &legacy).await {
        success(&db, t.id).await.unwrap();
    }
    assert_eq!(demand(all, hour).await, (0, 0, 0, None));
}

fn schedule(project_id: &ProjectId, queue: &TaskQueueName) -> TaskScheduleUpsert {
    TaskScheduleUpsert::builder()
        .project_id(Arc::new(project_id.clone()))
        .queue_name(queue.clone())
        .schedule_key("default")
        .interval(chrono::Duration::hours(1))
        .anchor(Some(chrono::Utc::now() - chrono::Duration::minutes(90)))
        .payload(Some(serde_json::json!({"keep": "days"})))
        .build()
}

#[tokio::test]
async fn schedules_upsert_with_conditions() {
    let Some(db) = db().await else { return };
    let (project, _) = scope(&db).await;
    let queue_name = queue();

    let mut absent = schedule(&project, &queue_name);
    absent.condition = Some(TaskScheduleCondition::Absent);
    let created = in_txn(&db, async |txn| {
        tasks::upsert_task_schedule(absent.clone(), txn)
            .await
            .map_err(ErrorModel::from)
    })
    .await
    .unwrap();
    assert!(created.next_run_at > chrono::Utc::now());
    assert_eq!(created.payload, serde_json::json!({"keep": "days"}));

    let err = in_txn(&db, async |txn| {
        tasks::upsert_task_schedule(absent.clone(), txn)
            .await
            .map_err(ErrorModel::from)
    })
    .await
    .unwrap_err();
    assert_eq!(err.r#type, "TaskScheduleConditionFailed");

    let mut txn = Txn::read_write(db.clone());
    let missing =
        tasks::upsert_task_schedule(schedule(&ProjectId::new_random(), &queue_name), &mut txn)
            .await
            .unwrap_err();
    txn.rollback().await.unwrap();
    assert!(
        matches!(
            missing,
            UpsertTaskScheduleError::TaskScheduleScopeNotFoundError(_)
        ),
        "{missing:?}"
    );
}

#[tokio::test]
async fn due_schedules_materialize_one_instance() {
    let Some(db) = db().await else { return };
    let (project, _) = scope(&db).await;
    let queue_name = queue();
    let created = in_txn(&db, async |txn| {
        tasks::upsert_task_schedule(schedule(&project, &queue_name), txn)
            .await
            .map_err(ErrorModel::from)
    })
    .await
    .unwrap();

    // Make it due: the anchor was 90 minutes ago, so a tick 30 minutes ago was missed.
    let mut make_due = schedule(&project, &queue_name);
    make_due.anchor = Some(created.anchor - chrono::Duration::minutes(1));
    in_txn(&db, async |txn| {
        tasks::upsert_task_schedule(make_due.clone(), txn)
            .await
            .map_err(ErrorModel::from)
    })
    .await
    .unwrap();
    let doc = Txn::latest(db.clone())
        .get(&db.doc_name(
            crate::store::docs::TASK_SCHEDULES,
            &created.schedule_id.to_string(),
        ))
        .await
        .unwrap()
        .unwrap();
    let mut stored: crate::store::docs::TaskScheduleDoc = crate::store::fs::from_doc(&doc).unwrap();
    stored.next_run_at = (chrono::Utc::now() - chrono::Duration::minutes(5)).timestamp_micros();
    super::commit(
        &db,
        vec![crate::store::fs::to_doc(doc.name.clone(), &stored).unwrap()],
    )
    .await;

    let materialize = |db: Db| async move {
        in_txn(&db, async |txn| {
            tasks::materialize_due_task_schedules(1000, txn)
                .await
                .map_err(ErrorModel::from)
        })
        .await
        .unwrap()
    };
    let ticks: Vec<_> = materialize(db.clone())
        .await
        .into_iter()
        .filter(|t| t.schedule_id == created.schedule_id)
        .collect();
    assert_eq!(ticks.len(), 1);
    assert!(ticks[0].next_run_at > chrono::Utc::now());

    let task = pick(&db, &queue_name).await.unwrap();
    assert_eq!(task.task_metadata.schedule_id, Some(created.schedule_id));
    assert_eq!(task.data, serde_json::json!({"keep": "days"}));
    // Not due again until the next tick.
    assert!(
        !materialize(db.clone())
            .await
            .iter()
            .any(|t| t.schedule_id == created.schedule_id)
    );
}

#[tokio::test]
async fn warehouses_with_tasks_cannot_be_deleted() {
    let Some(db) = db().await else { return };
    let (project, wh) = scope(&db).await;
    let queue_name = queue();
    enqueue(
        &db,
        &queue_name,
        table_task(&project, wh, uuid::Uuid::now_v7(), None),
    )
    .await;

    let mut txn = Txn::read_write(db.clone());
    let err = warehouse::delete_warehouse(wh, DeleteWarehouseQuery { force: true }, &mut txn)
        .await
        .unwrap_err();
    txn.rollback().await.unwrap();
    assert!(
        matches!(
            err,
            CatalogDeleteWarehouseError::WarehouseHasUnfinishedTasks(_)
        ),
        "{err:?}"
    );
}
