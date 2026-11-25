//! The task queue: active tasks, attempt logs, queue configs and schedules.
//!
//! An active task is a [`TaskDoc`]; its [`task_key_id`] marker keeps one active task per
//! entity and queue. Each finished attempt is a [`TaskLogDoc`] keyed by task and attempt, created
//! with an `exists = false` precondition: whoever records an attempt first wins, and a later
//! recorder gets 409 (the log exists) or 404 (neither the task nor the log exists), which the
//! core treats as "this attempt is closed".
//!
//! Picking has no `SKIP LOCKED`: candidates come from a query, and each is claimed in its own
//! transaction that re-reads it and checks it is still scheduled at the same attempt. Times come
//! from this server's clock.

use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, SubsecRound, Utc};
use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ErrorModel,
        management::v1::task_queue::{
            GetTaskQueueConfigResponse, QueueConfigResponse, SetTaskQueueConfigRequest,
        },
    },
    service::{
        ArcProjectId, CatalogBackendError, ExpirationTaskInfo, GenericTableId, TableId, ViewId,
        task_configs::TaskQueueConfigFilter,
        tasks::{
            CancelTasksFilter, EnqueueOutcome, FailedAttempt, FailureKind, MaterializedTick,
            PickTaskRequest, ProjectTaskScheduleState, RetryDecision,
            TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE, TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE,
            Task, TaskAttemptId, TaskCheckState, TaskEntity, TaskId, TaskInput,
            TaskIntermediateStatus, TaskMetadata, TaskQueueDemand, TaskQueueDemandRequest,
            TaskQueueName, TaskResourceDemandRange, TaskSchedule, TaskScheduleCondition,
            TaskScheduleConditionFailedError, TaskScheduleFilter, TaskScheduleId,
            TaskScheduleNotFoundError, TaskScheduleScopeNotFoundError,
            TaskScheduleSpecVersionUnsupportedError, TaskScheduleStoreError, TaskScheduleUpsert,
            TriggerTaskScheduleError, UpsertTaskScheduleError, WarehouseTaskEntityId, due_tick,
            first_tick_at_or_after, given_up_message, record_timed_out_attempt,
            tabular_expiration_queue as soft_deletion_queue,
        },
    },
    tracing,
};

use super::{
    FirestoreState,
    docs::{
        NameMarker, PROJECTS, ProjectDoc, TASK_CONFIGS, TASK_KEYS, TASK_LOGS, TASK_SCHEDULES,
        TASKS, TabularDoc, TaskConfigDoc, TaskDoc, TaskLogDoc, TaskScheduleDoc, from_micros,
        now_micros, task_config_id, task_key_id, task_log_id, task_schedule_id,
    },
    fs::{Query, Txn, backend_error, from_doc, int_value, string_value, to_doc},
};

pub(super) const SCHEDULED: &str = "scheduled";
pub(super) const RUNNING: &str = "running";
pub(super) const SHOULD_STOP: &str = "should-stop";
/// The schedule `spec_version` this server writes, lists and materializes.
const SPEC_VERSION: i64 = 1;
/// Longest retry backoff, as upstream clamps it.
const MAX_BACKOFF: chrono::Duration = chrono::Duration::days(36_500);
/// Due tasks read per query page while picking.
const PICK_PAGE: usize = 50;
/// Task log documents deleted per cleanup run; the rest go in later runs.
const MAX_LOG_DELETES: usize = 400;

type Result<T, E = ErrorModel> = std::result::Result<T, E>;

pub(super) fn json_text(value: &serde_json::Value) -> String {
    value.to_string()
}

pub(super) fn parse_json(text: &str) -> Result<serde_json::Value, CatalogBackendError> {
    serde_json::from_str(text).map_err(backend_error)
}

pub(super) fn task_name(txn: &Txn, task_id: &uuid::Uuid) -> String {
    txn.db().doc_name(TASKS, &task_id.to_string())
}

pub(super) fn log_name(txn: &Txn, task_id: &uuid::Uuid, attempt: i32) -> String {
    txn.db().doc_name(TASK_LOGS, &task_log_id(task_id, attempt))
}

fn key_name(txn: &Txn, t: &TaskDoc) -> String {
    let warehouse_id = t.warehouse_id.map(|w| w.to_string());
    txn.db().doc_name(
        TASK_KEYS,
        &task_key_id(
            t.project_id.as_str(),
            warehouse_id.as_deref(),
            &t.entity_type,
            t.entity_id.as_ref(),
            &t.queue_name,
        ),
    )
}

fn config_name(
    txn: &Txn,
    project_id: &ProjectId,
    warehouse_id: Option<WarehouseId>,
    queue_name: &str,
) -> String {
    let warehouse_id = warehouse_id.map(|w| w.to_string());
    txn.db().doc_name(
        TASK_CONFIGS,
        &task_config_id(project_id.as_str(), warehouse_id.as_deref(), queue_name),
    )
}

fn schedule_name(txn: &Txn, schedule_id: &uuid::Uuid) -> String {
    txn.db().doc_name(TASK_SCHEDULES, &schedule_id.to_string())
}

/// The status a task document holds.
fn intermediate_status(status: &str) -> Result<TaskIntermediateStatus, CatalogBackendError> {
    match status {
        SCHEDULED => Ok(TaskIntermediateStatus::Scheduled),
        RUNNING => Ok(TaskIntermediateStatus::Running),
        SHOULD_STOP => Ok(TaskIntermediateStatus::ShouldStop),
        other => Err(backend_error(std::io::Error::other(format!(
            "Unknown task status `{other}`"
        )))),
    }
}

/// `(warehouse, entity type, entity ID, entity name)` of a task entity.
fn entity_fields(
    entity: &TaskEntity,
) -> (
    Option<WarehouseId>,
    &'static str,
    Option<uuid::Uuid>,
    Option<Vec<String>>,
) {
    match entity {
        TaskEntity::Project => (None, "project", None, None),
        TaskEntity::Warehouse { warehouse_id } => (Some(*warehouse_id), "warehouse", None, None),
        TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            entity_name,
        } => {
            let (typ, id) = match entity_id {
                WarehouseTaskEntityId::Table { table_id } => ("table", **table_id),
                WarehouseTaskEntityId::View { view_id } => ("view", **view_id),
                WarehouseTaskEntityId::GenericTable { generic_table_id } => {
                    ("generic-table", **generic_table_id)
                }
            };
            (
                Some(*warehouse_id),
                typ,
                Some(id),
                Some(entity_name.clone()),
            )
        }
    }
}

pub(super) fn entity_of(t: &TaskDoc) -> Result<TaskEntity, CatalogBackendError> {
    let integrity = |what: &str| {
        backend_error(std::io::Error::other(format!(
            "Task {} has no {what}",
            t.task_id
        )))
    };
    if t.entity_type == "project" {
        return Ok(TaskEntity::Project);
    }
    let warehouse_id = t.warehouse_id.ok_or_else(|| integrity("warehouse"))?;
    if t.entity_type == "warehouse" {
        return Ok(TaskEntity::Warehouse { warehouse_id });
    }
    let id = t.entity_id.ok_or_else(|| integrity("entity ID"))?;
    let entity_id = match t.entity_type.as_str() {
        "table" => WarehouseTaskEntityId::Table {
            table_id: TableId::from(id),
        },
        "view" => WarehouseTaskEntityId::View {
            view_id: ViewId::from(id),
        },
        "generic-table" => WarehouseTaskEntityId::GenericTable {
            generic_table_id: GenericTableId::from(id),
        },
        other => return Err(integrity(&format!("known entity type (`{other}`)"))),
    };
    Ok(TaskEntity::EntityInWarehouse {
        warehouse_id,
        entity_id,
        entity_name: t
            .entity_name
            .clone()
            .ok_or_else(|| integrity("entity name"))?,
    })
}

/// The schedule that produced `t`, if any.
fn schedule_of(t: &TaskDoc) -> Option<TaskScheduleId> {
    matches!(t.entity_type.as_str(), "project" | "warehouse")
        .then_some(t.entity_id)
        .flatten()
        .map(TaskScheduleId::from)
}

pub(super) fn metadata_of(t: &TaskDoc) -> Result<TaskMetadata, CatalogBackendError> {
    Ok(TaskMetadata {
        project_id: Arc::new(t.project_id.clone()),
        parent_task_id: t.parent_task_id.map(TaskId::from),
        scheduled_for: from_micros(t.scheduled_for),
        entity: entity_of(t)?,
        schedule_id: schedule_of(t),
        resource_demand: t.resource_demand,
    })
}

fn new_task_doc(
    queue_name: &str,
    task_metadata: &lakekeeper::service::tasks::ScheduleTaskMetadata,
    entity_id_override: Option<uuid::Uuid>,
    payload: &serde_json::Value,
    now: i64,
) -> TaskDoc {
    let (warehouse_id, entity_type, entity_id, entity_name) = entity_fields(&task_metadata.entity);
    TaskDoc {
        task_id: uuid::Uuid::now_v7(),
        project_id: (*task_metadata.project_id).clone(),
        warehouse_id,
        queue_name: queue_name.to_string(),
        status: SCHEDULED.to_string(),
        attempt: 0,
        parent_task_id: task_metadata.parent_task_id.map(Into::into),
        scheduled_for: task_metadata
            .scheduled_for
            .map_or(now, |t| t.timestamp_micros()),
        picked_up_at: None,
        last_heartbeat_at: None,
        progress: 0.0,
        execution_details: None,
        task_data: json_text(payload),
        entity_type: entity_type.to_string(),
        entity_id: entity_id_override.or(entity_id),
        entity_name,
        resource_demand: task_metadata.resource_demand.map(|d| d.max(0)),
        created_at: now,
        updated_at: None,
    }
}

/// Insert `doc` unless its entity and queue already have an active task.
async fn insert_task(txn: &mut Txn, doc: TaskDoc) -> Result<EnqueueOutcome, CatalogBackendError> {
    let key = key_name(txn, &doc);
    if let Some(marker) = txn.get(&key).await? {
        let marker: NameMarker = from_doc(&marker)?;
        let task_id: uuid::Uuid = marker.id.parse().map_err(backend_error)?;
        let existing = txn.get(&task_name(txn, &task_id)).await?.ok_or_else(|| {
            backend_error(std::io::Error::other(format!(
                "Task key {key} names task {task_id}, which does not exist"
            )))
        })?;
        let existing: TaskDoc = from_doc(&existing)?;
        return Ok(EnqueueOutcome::Existing {
            task_id: TaskId::from(task_id),
            status: intermediate_status(&existing.status)?,
        });
    }
    let task_id = doc.task_id;
    txn.create(to_doc(
        key,
        &NameMarker {
            id: task_id.to_string(),
        },
    )?);
    txn.create(to_doc(task_name(txn, &task_id), &doc)?);
    Ok(EnqueueOutcome::Inserted(TaskId::from(task_id)))
}

pub(super) async fn enqueue_task(
    queue_name: &TaskQueueName,
    task: TaskInput,
    txn: &mut Txn,
) -> Result<EnqueueOutcome> {
    Ok(enqueue(queue_name, task, txn).await?)
}

/// [`enqueue_task`], for the store's own tasks.
pub(super) async fn enqueue(
    queue_name: &TaskQueueName,
    task: TaskInput,
    txn: &mut Txn,
) -> Result<EnqueueOutcome, CatalogBackendError> {
    let doc = new_task_doc(
        queue_name,
        &task.task_metadata,
        None,
        &task.payload,
        now_micros(),
    );
    insert_task(txn, doc).await
}

pub(super) async fn enqueue_tasks(
    queue_name: &TaskQueueName,
    tasks: Vec<TaskInput>,
    txn: &mut Txn,
) -> Result<Vec<TaskId>> {
    let now = now_micros();
    let mut inserted = Vec::new();
    for task in tasks {
        let doc = new_task_doc(queue_name, &task.task_metadata, None, &task.payload, now);
        if let EnqueueOutcome::Inserted(id) = insert_task(txn, doc).await? {
            inserted.push(id);
        }
    }
    Ok(inserted)
}

/// Remove the active task `t`.
fn delete_task(txn: &mut Txn, t: &TaskDoc) {
    let key = key_name(txn, t);
    txn.delete(key);
    txn.delete(task_name(txn, &t.task_id));
}

pub(super) fn log_doc(
    t: &TaskDoc,
    attempt: i32,
    status: &str,
    message: Option<String>,
    now: i64,
) -> TaskLogDoc {
    TaskLogDoc {
        task_id: t.task_id,
        attempt,
        project_id: t.project_id.clone(),
        warehouse_id: t.warehouse_id,
        queue_name: t.queue_name.clone(),
        status: status.to_string(),
        message,
        task_data: t.task_data.clone(),
        entity_type: t.entity_type.clone(),
        entity_id: t.entity_id,
        entity_name: t.entity_name.clone(),
        started_at: t.picked_up_at,
        duration_micros: t.picked_up_at.map(|p| now - p),
        progress: if status == "success" { 1.0 } else { t.progress },
        execution_details: t.execution_details.clone(),
        attempt_scheduled_for: t.scheduled_for,
        last_heartbeat_at: t.last_heartbeat_at,
        parent_task_id: t.parent_task_id,
        task_created_at: t.created_at,
        resource_demand: t.resource_demand,
        created_at: now,
    }
}

pub(super) fn write_log(txn: &mut Txn, log: &TaskLogDoc) -> Result<(), CatalogBackendError> {
    let name = log_name(txn, &log.task_id, log.attempt);
    txn.create(to_doc(name, log)?);
    Ok(())
}

/// The active task of `id`, if it is the running (or stopping) attempt `id`; otherwise the
/// error for an attempt that was already recorded or does not exist.
async fn open_attempt(
    txn: &mut Txn,
    id: TaskAttemptId,
    conflict_type: &str,
    action: &str,
) -> Result<TaskDoc> {
    let task_id: uuid::Uuid = *id.task_id;
    let names = [
        task_name(txn, &task_id),
        log_name(txn, &task_id, id.attempt),
    ];
    let [task, log]: [_; 2] = txn
        .get_many(&names)
        .await?
        .try_into()
        .map_err(|_| backend_error(std::io::Error::other("short batch read")))?;
    if let Some(task) = task {
        let task: TaskDoc = from_doc(&task)?;
        if task.attempt == id.attempt && task.status != SCHEDULED {
            return Ok(task);
        }
    }
    if let Some(log) = log {
        let log: TaskLogDoc = from_doc(&log)?;
        let verb = if conflict_type == TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE {
            "completed"
        } else {
            "failed"
        };
        return Err(ErrorModel::conflict(
            format!(
                "Task {} with attempt {} has already been recorded as {verb}.",
                id.task_id, id.attempt
            ),
            conflict_type,
            None,
        )
        .append_detail(action)
        .append_detail(format!(
            "Original attempt recorded at {} with message: '{}'",
            from_micros(log.created_at),
            log.message.unwrap_or_default()
        )));
    }
    Err(ErrorModel::not_found(
        format!(
            "Task {} with attempt {} not found in active tasks.",
            id.task_id, id.attempt
        ),
        "TaskNotFound",
        None,
    )
    .append_detail(action))
}

pub(super) async fn record_success(
    id: TaskAttemptId,
    message: Option<&str>,
    txn: &mut Txn,
) -> Result<()> {
    let t = open_attempt(
        txn,
        id,
        TASK_ATTEMPT_ALREADY_COMPLETED_ERROR_TYPE,
        "Error recording task success.",
    )
    .await?;
    let now = now_micros();
    write_log(
        txn,
        &log_doc(&t, t.attempt, "success", message.map(str::to_string), now),
    )?;
    delete_task(txn, &t);
    Ok(())
}

/// Record a failure of the running attempt of `t`: retry it, or end the task.
fn fail_attempt(
    txn: &mut Txn,
    mut t: TaskDoc,
    error_details: &str,
    max_retries: i32,
    retry_decision: &RetryDecision,
    now: i64,
) -> Result<(), CatalogBackendError> {
    let retry = match retry_decision {
        RetryDecision::GiveUp { reason, .. } => Err(Some(given_up_message(error_details, reason))),
        RetryDecision::Retry { .. } if t.attempt >= max_retries => Err(None),
        RetryDecision::Retry {
            backoff,
            resource_demand,
            ..
        } => Ok((*backoff, *resource_demand)),
    };
    match retry {
        Err(message) => {
            let message = message.unwrap_or_else(|| error_details.to_string());
            write_log(txn, &log_doc(&t, t.attempt, "failed", Some(message), now))?;
            delete_task(txn, &t);
        }
        Ok((backoff, resource_demand)) => {
            write_log(
                txn,
                &log_doc(
                    &t,
                    t.attempt,
                    "failed",
                    Some(error_details.to_string()),
                    now,
                ),
            )?;
            let backoff = backoff.clamp(chrono::Duration::zero(), MAX_BACKOFF);
            // A zero backoff keeps the task's place in the queue.
            if backoff > chrono::Duration::zero() {
                t.scheduled_for = now + backoff.num_microseconds().unwrap_or(i64::MAX - now);
            }
            if let Some(demand) = resource_demand {
                t.resource_demand = Some(demand.max(0));
            }
            t.status = SCHEDULED.to_string();
            t.progress = 0.0;
            t.picked_up_at = None;
            t.last_heartbeat_at = None;
            t.execution_details = None;
            t.updated_at = Some(now);
            let name = task_name(txn, &t.task_id);
            txn.set(to_doc(name, &t)?);
        }
    }
    Ok(())
}

pub(super) async fn record_failure(
    id: TaskAttemptId,
    error_details: &str,
    max_retries: i32,
    retry_decision: &RetryDecision,
    txn: &mut Txn,
) -> Result<()> {
    let t = open_attempt(
        txn,
        id,
        TASK_ATTEMPT_ALREADY_FAILED_ERROR_TYPE,
        "Error recording task attempt failure.",
    )
    .await?;
    fail_attempt(
        txn,
        t,
        error_details,
        max_retries,
        retry_decision,
        now_micros(),
    )?;
    Ok(())
}

pub(super) async fn check_and_heartbeat(
    id: TaskAttemptId,
    progress: f32,
    execution_details: Option<serde_json::Value>,
    txn: &mut Txn,
) -> Result<TaskCheckState> {
    let name = task_name(txn, &id.task_id);
    let Some(doc) = txn.get(&name).await? else {
        return Ok(TaskCheckState::NotActive);
    };
    let mut t: TaskDoc = from_doc(&doc)?;
    if t.attempt != id.attempt {
        return Ok(TaskCheckState::NotActive);
    }
    let now = now_micros();
    t.last_heartbeat_at = Some(now);
    t.progress = f64::from(progress);
    t.execution_details = execution_details.as_ref().map(json_text);
    t.updated_at = Some(now);
    let state = if t.status == RUNNING {
        TaskCheckState::Continue
    } else {
        TaskCheckState::Stop
    };
    txn.set(to_doc(name, &t)?);
    Ok(state)
}

/// The names a request picks from: the queue name first, then its legacy names.
pub(super) fn queue_names(queue_name: &TaskQueueName, legacy: &[&TaskQueueName]) -> Vec<String> {
    std::iter::once(queue_name)
        .chain(legacy.iter().copied())
        .map(ToString::to_string)
        .collect()
}

/// The stored config of a queue for exactly this project and warehouse, under the first of
/// `names` that has one.
async fn queue_config(
    txn: &mut Txn,
    project_id: &ProjectId,
    warehouse_id: Option<WarehouseId>,
    names: &[String],
) -> Result<Option<TaskConfigDoc>, CatalogBackendError> {
    let docs: Vec<String> = names
        .iter()
        .map(|n| config_name(txn, project_id, warehouse_id, n))
        .collect();
    txn.get_many(&docs)
        .await?
        .into_iter()
        .flatten()
        .next()
        .map(|doc| from_doc(&doc))
        .transpose()
}

async fn active_tasks_of_queue(
    txn: &mut Txn,
    queue_name: &str,
    status: &str,
) -> Result<Vec<TaskDoc>, CatalogBackendError> {
    txn.query(
        &Query::new(TASKS)
            .eq("queue_name", string_value(queue_name))
            .eq("status", string_value(status)),
    )
    .await?
    .iter()
    .map(from_doc)
    .collect()
}

/// When the heartbeat of the running (or stopping) task `t` of a queue named `names` expires:
/// its queue config's timeout, or `default_timeout`, after its last heartbeat.
async fn heartbeat_deadline(
    read: &mut Txn,
    t: &TaskDoc,
    names: &[String],
    default_timeout: chrono::Duration,
) -> Result<i64, CatalogBackendError> {
    let timeout = queue_config(read, &t.project_id, t.warehouse_id, names)
        .await?
        .and_then(|c| c.max_seconds_since_last_heartbeat)
        .map_or(default_timeout, chrono::Duration::seconds);
    Ok(t.last_heartbeat_at
        .unwrap_or(t.scheduled_for)
        .saturating_add(timeout.num_microseconds().unwrap_or(i64::MAX)))
}

fn in_demand_range(range: TaskResourceDemandRange, t: &TaskDoc) -> bool {
    let demand = t.resource_demand.unwrap_or(0);
    range.above.is_none_or(|above| demand > above)
        && range.at_most.is_none_or(|at_most| demand <= at_most)
}

/// Fail one attempt of the queue whose heartbeat expired, so that it is retried or ends.
async fn fail_timed_out_attempt(
    state: &FirestoreState,
    request: &PickTaskRequest<'_>,
    names: &[String],
) -> Result<(), CatalogBackendError> {
    let now = now_micros();
    let mut read = state.latest();
    let mut expired = Vec::new();
    for name in names {
        for status in [RUNNING, SHOULD_STOP] {
            for t in active_tasks_of_queue(&mut read, name, status).await? {
                let deadline = heartbeat_deadline(
                    &mut read,
                    &t,
                    names,
                    request.default_max_time_since_last_heartbeat,
                )
                .await?;
                if deadline < now {
                    expired.push(t);
                }
            }
        }
    }
    let Some(oldest) = expired
        .into_iter()
        .min_by_key(|t| (t.scheduled_for, t.task_id))
    else {
        return Ok(());
    };

    let mut txn = Txn::read_write(state.db.clone());
    let name = task_name(&txn, &oldest.task_id);
    let Some(current) = txn.get(&name).await? else {
        return Ok(());
    };
    let current: TaskDoc = from_doc(&current)?;
    if current.attempt != oldest.attempt || current.status == SCHEDULED {
        return Ok(());
    }
    let execution_details = current
        .execution_details
        .as_deref()
        .map(parse_json)
        .transpose()?;
    let decision = request.retry_policy.decide(&FailedAttempt {
        task_id: TaskId::from(current.task_id),
        attempt: current.attempt,
        schedule_id: schedule_of(&current),
        kind: FailureKind::TimedOut,
        error: "Attempt timed out.",
        resource_demand: current.resource_demand,
        execution_details: execution_details.as_ref(),
    });
    let queue_name = TaskQueueName::from(&current.queue_name);
    fail_attempt(
        &mut txn,
        current,
        "Attempt timed out.",
        request.retry_policy.max_retries,
        &decision,
        now,
    )?;
    txn.commit()
        .await
        .map_err(|e| CatalogBackendError::new_unexpected(std::io::Error::other(e.to_string())))?;
    record_timed_out_attempt(&queue_name);
    Ok(())
}

/// Due tasks of one queue name in pick order, filtered to the demand range.
async fn due_candidates(
    state: &FirestoreState,
    range: TaskResourceDemandRange,
    queue_name: &str,
    now: i64,
) -> Result<Option<TaskDoc>, CatalogBackendError> {
    let in_range = |t: &TaskDoc| in_demand_range(range, t);
    let mut read = state.latest();
    let mut cursor = None;
    loop {
        let page = read
            .query(
                &Query::new(TASKS)
                    .eq("queue_name", string_value(queue_name))
                    .eq("status", string_value(SCHEDULED))
                    .at_most("scheduled_for", int_value(now))
                    .order_by("scheduled_for")
                    .start_after(cursor.take())
                    .limit(Some(PICK_PAGE)),
            )
            .await?;
        let Some(last) = page.last() else {
            return Ok(None);
        };
        cursor = Some((
            last.fields
                .get("scheduled_for")
                .cloned()
                .unwrap_or_else(|| int_value(0)),
            last.name.clone(),
        ));
        let full = page.len() == PICK_PAGE;
        for doc in &page {
            let t: TaskDoc = from_doc(doc)?;
            if in_range(&t) {
                return Ok(Some(t));
            }
        }
        if !full {
            return Ok(None);
        }
    }
}

/// Claim `candidate` if it is still due at the same attempt.
async fn claim(
    state: &FirestoreState,
    candidate: &TaskDoc,
    now: i64,
) -> Result<Option<TaskDoc>, CatalogBackendError> {
    let mut txn = Txn::read_write(state.db.clone());
    let name = task_name(&txn, &candidate.task_id);
    let Some(current) = txn.get(&name).await? else {
        return Ok(None);
    };
    let mut t: TaskDoc = from_doc(&current)?;
    if t.status != SCHEDULED || t.attempt != candidate.attempt || t.scheduled_for > now {
        return Ok(None);
    }
    t.status = RUNNING.to_string();
    t.attempt += 1;
    t.picked_up_at = Some(now);
    t.last_heartbeat_at = Some(now);
    t.progress = 0.0;
    t.execution_details = None;
    t.updated_at = Some(now);
    txn.set(to_doc(name, &t)?);
    match txn.commit().await {
        Ok(()) => Ok(Some(t)),
        // Another worker claimed it first, or its transaction held the lock.
        Err(e) if e.code == 409 => Ok(None),
        Err(e) => Err(CatalogBackendError::new_unexpected(std::io::Error::other(
            e.to_string(),
        ))),
    }
}

pub(super) async fn pick_new_task(
    request: PickTaskRequest<'_>,
    state: &FirestoreState,
) -> Result<Option<Task>> {
    let names = queue_names(request.queue_name, request.legacy_queue_names);
    if let Err(error) = fail_timed_out_attempt(state, &request, &names).await {
        tracing::warn!(%error, queue = %request.queue_name, "Failed to fail a timed-out task attempt");
    }
    if request.resource_demand.is_empty() {
        return Ok(None);
    }
    let now = now_micros();
    // A claim lost to another worker moves on to the next candidate.
    for _ in 0..3 {
        let mut candidates = Vec::new();
        for name in &names {
            if let Some(t) = due_candidates(state, request.resource_demand, name, now).await? {
                candidates.push(t);
            }
        }
        candidates.sort_by_key(|t| (t.scheduled_for, t.task_id));
        let Some(candidate) = candidates.into_iter().next() else {
            return Ok(None);
        };
        let Some(t) = claim(state, &candidate, now).await? else {
            continue;
        };
        let mut read = state.latest();
        let config = queue_config(&mut read, &t.project_id, t.warehouse_id, &names)
            .await?
            .map(|c| parse_json(&c.config))
            .transpose()?;
        return Ok(Some(Task {
            task_metadata: metadata_of(&t)?,
            queue_name: TaskQueueName::from(&t.queue_name),
            id: TaskAttemptId {
                task_id: TaskId::from(t.task_id),
                attempt: t.attempt,
            },
            status: TaskIntermediateStatus::Running,
            picked_up_at: t.picked_up_at.map(from_micros),
            config,
            data: parse_json(&t.task_data)?,
        }));
    }
    Ok(None)
}

/// Active tasks matching `filter`, any queue and status.
async fn tasks_matching(
    txn: &mut Txn,
    filter: &CancelTasksFilter,
) -> Result<Vec<TaskDoc>, CatalogBackendError> {
    let docs = match filter {
        CancelTasksFilter::WarehouseId { warehouse_id } => {
            txn.query(&Query::new(TASKS).eq("warehouse_id", string_value(warehouse_id.to_string())))
                .await?
        }
        CancelTasksFilter::TaskIds(ids) => {
            let names: Vec<String> = ids.iter().map(|id| task_name(txn, id)).collect();
            txn.get_many(&names).await?.into_iter().flatten().collect()
        }
        CancelTasksFilter::ProjectId {
            project_id,
            include_sub_tasks,
        } => {
            let docs = txn
                .query(&Query::new(TASKS).eq("project_id", string_value(project_id.as_str())))
                .await?;
            let mut out = Vec::new();
            for d in docs {
                let t: TaskDoc = from_doc(&d)?;
                if *include_sub_tasks || t.warehouse_id.is_none() {
                    out.push(d);
                }
            }
            out
        }
    };
    docs.iter().map(from_doc).collect()
}

pub(super) async fn cancel_scheduled_tasks(
    queue_name: Option<&TaskQueueName>,
    legacy_queue_names: &[&TaskQueueName],
    filter: CancelTasksFilter,
    cancel_running_and_should_stop: bool,
    txn: &mut Txn,
) -> Result<()> {
    let names = queue_name.map(|q| queue_names(q, legacy_queue_names));
    let tasks: Vec<TaskDoc> = tasks_matching(txn, &filter)
        .await?
        .into_iter()
        .filter(|t| names.as_ref().is_none_or(|n| n.contains(&t.queue_name)))
        .filter(|t| cancel_running_and_should_stop || t.status == SCHEDULED)
        .collect();
    // A scheduled task after a failed attempt N already has a log for N.
    let attempts: Vec<i32> = tasks
        .iter()
        .map(|t| {
            if t.status == SCHEDULED && t.attempt > 0 {
                t.attempt + 1
            } else {
                t.attempt
            }
        })
        .collect();
    let logs: Vec<String> = tasks
        .iter()
        .zip(&attempts)
        .map(|(t, a)| log_name(txn, &t.task_id, *a))
        .collect();
    let existing = txn.get_many(&logs).await?;
    let now = now_micros();
    for ((t, attempt), existing) in tasks.iter().zip(attempts).zip(existing) {
        if existing.is_none() {
            write_log(txn, &log_doc(t, attempt, "cancelled", None, now))?;
        }
        delete_task(txn, t);
    }
    Ok(())
}

/// IDs of the active tasks whose entity is one of `entity_ids`, in warehouse `warehouse_id`.
/// The active tasks of `entity_ids` in `warehouse_id`, in any queue.
pub(super) async fn active_tasks_of_entities(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    entity_ids: &[uuid::Uuid],
) -> Result<Vec<TaskDoc>, CatalogBackendError> {
    if entity_ids.is_empty() {
        return Ok(Vec::new());
    }
    let filter = CancelTasksFilter::WarehouseId { warehouse_id };
    Ok(tasks_matching(txn, &filter)
        .await?
        .into_iter()
        .filter(|t| t.entity_id.is_some_and(|e| entity_ids.contains(&e)))
        .collect())
}

/// Whether `t` is a soft-deletion task, under the current or the legacy queue name.
pub(super) fn is_soft_deletion(t: &TaskDoc) -> bool {
    t.queue_name == soft_deletion_queue::QUEUE_NAME.as_str()
        || t.queue_name == soft_deletion_queue::LEGACY_QUEUE_NAME.as_str()
}

pub(super) fn is_running(t: &TaskDoc) -> bool {
    t.status == RUNNING
}

/// The active soft-deletion task of each of `tabulars` that has one, by tabular ID. Reads the
/// task key markers, so it needs no query.
pub(super) async fn expiration_tasks(
    txn: &mut Txn,
    project_id: &ProjectId,
    tabulars: &[&TabularDoc],
) -> Result<HashMap<uuid::Uuid, ExpirationTaskInfo>, CatalogBackendError> {
    let queues = [
        soft_deletion_queue::QUEUE_NAME.as_str(),
        soft_deletion_queue::LEGACY_QUEUE_NAME.as_str(),
    ];
    let mut keys = Vec::new();
    for t in tabulars {
        let warehouse_id = t.warehouse_id.to_string();
        for queue in queues {
            let id = task_key_id(
                project_id.as_str(),
                Some(&warehouse_id),
                &t.typ,
                Some(&t.tabular_id),
                queue,
            );
            keys.push(txn.db().doc_name(TASK_KEYS, &id));
        }
    }
    let mut names = Vec::new();
    for marker in txn.get_many(&keys).await?.into_iter().flatten() {
        let marker: NameMarker = from_doc(&marker)?;
        names.push(task_name(txn, &marker.id.parse().map_err(backend_error)?));
    }
    let mut out = HashMap::new();
    for d in txn.get_many(&names).await?.into_iter().flatten() {
        let t: TaskDoc = from_doc(&d)?;
        if let Some(entity_id) = t.entity_id {
            out.insert(
                entity_id,
                ExpirationTaskInfo {
                    task_id: t.task_id.into(),
                    expiration_date: from_micros(t.scheduled_for),
                },
            );
        }
    }
    Ok(out)
}

/// Active tasks per queue in `warehouse_id`, for the "unfinished tasks" check of a delete.
pub(super) async fn active_task_counts(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
) -> Result<Vec<(String, usize)>, CatalogBackendError> {
    let filter = CancelTasksFilter::WarehouseId { warehouse_id };
    let mut counts: HashMap<String, usize> = HashMap::new();
    for t in tasks_matching(txn, &filter).await? {
        *counts.entry(t.queue_name).or_default() += 1;
    }
    let mut counts: Vec<_> = counts.into_iter().collect();
    counts.sort();
    Ok(counts)
}

/// Locations of the active purge tasks in `warehouse_id`.
pub(super) async fn purge_locations(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
) -> Result<Vec<String>, CatalogBackendError> {
    let docs = txn
        .query(
            &Query::new(TASKS)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .eq(
                    "queue_name",
                    string_value(
                        lakekeeper::service::tasks::tabular_purge_queue::QUEUE_NAME.to_string(),
                    ),
                ),
        )
        .await?;
    let mut locations = Vec::new();
    for d in docs {
        let t: TaskDoc = from_doc(&d)?;
        if let Some(location) = parse_json(&t.task_data)?
            .get("tabular_location")
            .and_then(serde_json::Value::as_str)
        {
            locations.push(location.trim_end_matches('/').to_string());
        }
    }
    Ok(locations)
}

/// Delete the active tasks, schedules and queue configs of `project_id`. Attempt logs stay
/// until log cleanup, which runs per project and so does not reach them; they are small.
pub(super) async fn delete_project_tasks(
    txn: &mut Txn,
    project_id: &ProjectId,
) -> Result<(), CatalogBackendError> {
    let filter = CancelTasksFilter::ProjectId {
        project_id: project_id.clone(),
        include_sub_tasks: true,
    };
    for t in tasks_matching(txn, &filter).await? {
        delete_task(txn, &t);
    }
    for collection in [TASK_SCHEDULES, TASK_CONFIGS] {
        let docs = txn
            .query(&Query::new(collection).eq("project_id", string_value(project_id.as_str())))
            .await?;
        for d in docs {
            txn.delete(d.name);
        }
    }
    Ok(())
}

pub(super) async fn set_task_queue_config(
    project_id: &ArcProjectId,
    warehouse_id: Option<WarehouseId>,
    queue_name: &TaskQueueName,
    config: &SetTaskQueueConfigRequest,
    txn: &mut Txn,
) -> Result<()> {
    let name = config_name(txn, project_id, warehouse_id, queue_name);
    let previous: Option<TaskConfigDoc> =
        txn.get(&name).await?.as_ref().map(from_doc).transpose()?;
    let doc = TaskConfigDoc {
        project_id: (**project_id).clone(),
        warehouse_id,
        queue_name: queue_name.to_string(),
        config: json_text(config.queue_config.as_json()),
        max_seconds_since_last_heartbeat: config
            .max_seconds_since_last_heartbeat
            .or(previous.and_then(|p| p.max_seconds_since_last_heartbeat)),
    };
    txn.set(to_doc(name, &doc)?);
    Ok(())
}

pub(super) async fn get_task_queue_config(
    filter: &TaskQueueConfigFilter,
    queue_name: &TaskQueueName,
    txn: &mut Txn,
) -> Result<Option<GetTaskQueueConfigResponse>> {
    let name = match filter {
        TaskQueueConfigFilter::WarehouseId { warehouse_id } => {
            let Some(warehouse) = super::warehouse::read(txn, *warehouse_id).await? else {
                return Ok(None);
            };
            config_name(txn, &warehouse.project_id, Some(*warehouse_id), queue_name)
        }
        TaskQueueConfigFilter::ProjectId { project_id } => {
            config_name(txn, project_id, None, queue_name)
        }
    };
    let Some(doc) = txn.get(&name).await? else {
        return Ok(None);
    };
    let doc: TaskConfigDoc = from_doc(&doc)?;
    Ok(Some(GetTaskQueueConfigResponse {
        queue_config: QueueConfigResponse {
            config: parse_json(&doc.config)?,
            queue_name: queue_name.clone(),
        },
        max_seconds_since_last_heartbeat: doc.max_seconds_since_last_heartbeat,
    }))
}

/// Delete all attempt logs of the tasks of `project_id` that are no longer active and whose
/// last attempt ended before `now - retention_period`.
pub(super) async fn cleanup_task_logs_older_than(
    txn: &mut Txn,
    retention_period: chrono::Duration,
    project_id: &ProjectId,
) -> Result<()> {
    let cutoff = (Utc::now() - retention_period).timestamp_micros();
    let docs = txn
        .query(&Query::new(TASK_LOGS).eq("project_id", string_value(project_id.as_str())))
        .await?;
    let mut by_task: HashMap<uuid::Uuid, (i64, Vec<String>)> = HashMap::new();
    for d in docs {
        let log: TaskLogDoc = from_doc(&d)?;
        let entry = by_task.entry(log.task_id).or_insert((i64::MIN, Vec::new()));
        entry.0 = entry.0.max(log.created_at);
        entry.1.push(d.name);
    }
    let old: Vec<(uuid::Uuid, Vec<String>)> = by_task
        .into_iter()
        .filter(|(_, (last, _))| *last < cutoff)
        .map(|(id, (_, names))| (id, names))
        .collect();
    let active_names: Vec<String> = old.iter().map(|(id, _)| task_name(txn, id)).collect();
    let active = txn.get_many(&active_names).await?;
    let mut deleted = 0;
    for ((_, names), active) in old.into_iter().zip(active) {
        if active.is_some() || deleted + names.len() > MAX_LOG_DELETES {
            continue;
        }
        deleted += names.len();
        for name in names {
            txn.delete(name);
        }
    }
    Ok(())
}

fn schedule_from_doc(doc: &TaskScheduleDoc) -> Result<TaskSchedule, CatalogBackendError> {
    Ok(TaskSchedule {
        schedule_id: TaskScheduleId::from(doc.schedule_id),
        project_id: Arc::new(doc.project_id.clone()),
        warehouse_id: doc.warehouse_id,
        queue_name: TaskQueueName::from(&doc.queue_name),
        schedule_key: doc.schedule_key.clone(),
        interval: chrono::Duration::seconds(doc.interval_seconds),
        anchor: from_micros(doc.anchor),
        payload: parse_json(&doc.payload)?,
        enabled: doc.enabled,
        next_run_at: from_micros(doc.next_run_at),
        missed_runs: doc.missed_runs,
        created_at: from_micros(doc.created_at),
        updated_at: doc.updated_at.map(from_micros),
    })
}

fn tick_error(e: impl std::fmt::Debug) -> CatalogBackendError {
    backend_error(std::io::Error::other(format!(
        "Task schedule tick out of range: {e:?}"
    )))
}

/// The scope, spec version and write condition of an upsert; `stored` is the current schedule.
async fn check_upsert(
    upsert: &TaskScheduleUpsert,
    stored: Option<&TaskScheduleDoc>,
    txn: &mut Txn,
) -> Result<(), UpsertTaskScheduleError> {
    if stored.is_none() {
        let scope_exists = match upsert.warehouse_id {
            None => super::project::exists(txn, &upsert.project_id).await?,
            Some(w) => super::warehouse::read(txn, w)
                .await?
                .is_some_and(|w| w.project_id == *upsert.project_id),
        };
        if !scope_exists {
            return Err(TaskScheduleScopeNotFoundError {
                project_id: upsert.project_id.clone(),
                warehouse_id: upsert.warehouse_id,
                stack: Vec::new(),
            }
            .into());
        }
    }
    if stored
        .as_ref()
        .is_some_and(|s| s.spec_version > SPEC_VERSION)
    {
        return Err(TaskScheduleSpecVersionUnsupportedError {
            queue_name: upsert.queue_name.clone(),
            schedule_key: upsert.schedule_key.clone(),
            stack: Vec::new(),
        }
        .into());
    }
    let condition_met = match upsert.condition {
        None => true,
        Some(TaskScheduleCondition::Absent) => stored.is_none(),
        Some(TaskScheduleCondition::Interval(d)) => {
            stored.is_some_and(|s| s.interval_seconds == d.num_seconds())
        }
    };
    if !condition_met {
        return Err(TaskScheduleConditionFailedError {
            queue_name: upsert.queue_name.clone(),
            schedule_key: upsert.schedule_key.clone(),
            stack: Vec::new(),
        }
        .into());
    }

    Ok(())
}

pub(super) async fn upsert_task_schedule(
    upsert: TaskScheduleUpsert,
    txn: &mut Txn,
) -> Result<TaskSchedule, UpsertTaskScheduleError> {
    let warehouse_str = upsert.warehouse_id.map(|w| w.to_string());
    let schedule_id = task_schedule_id(
        upsert.project_id.as_str(),
        warehouse_str.as_deref(),
        &upsert.queue_name,
        &upsert.schedule_key,
    );
    let name = schedule_name(txn, &schedule_id);
    let stored: Option<TaskScheduleDoc> =
        txn.get(&name).await?.as_ref().map(from_doc).transpose()?;

    check_upsert(&upsert, stored.as_ref(), txn).await?;
    let interval_seconds = upsert.interval.num_seconds();
    let now = Utc::now();
    let anchor: DateTime<Utc> = upsert
        .anchor
        .map(|a| a.trunc_subsecs(6))
        .or(stored.as_ref().map(|s| from_micros(s.anchor)))
        .unwrap_or_else(|| now.trunc_subsecs(6));
    let keep_next_run = stored.as_ref().is_some_and(|s| {
        s.interval_seconds == interval_seconds
            && s.anchor == anchor.timestamp_micros()
            && (s.enabled || upsert.enabled != Some(true))
    });
    let next_run_at = match &stored {
        Some(s) if keep_next_run => s.next_run_at,
        _ => first_tick_at_or_after(anchor, interval_seconds, now)
            .map_err(tick_error)?
            .timestamp_micros(),
    };
    let now_micros = now.timestamp_micros();
    let doc = TaskScheduleDoc {
        schedule_id,
        project_id: (*upsert.project_id).clone(),
        warehouse_id: upsert.warehouse_id,
        queue_name: upsert.queue_name.to_string(),
        schedule_key: upsert.schedule_key.clone(),
        interval_seconds,
        anchor: anchor.timestamp_micros(),
        payload: upsert
            .payload
            .as_ref()
            .map(json_text)
            .or(stored.as_ref().map(|s| s.payload.clone()))
            .unwrap_or_else(|| "{}".to_string()),
        enabled: upsert
            .enabled
            .or(stored.as_ref().map(|s| s.enabled))
            .unwrap_or(true),
        next_run_at,
        missed_runs: stored.as_ref().map_or(0, |s| s.missed_runs),
        spec_version: SPEC_VERSION,
        created_at: stored.as_ref().map_or(now_micros, |s| s.created_at),
        updated_at: stored.as_ref().map(|_| now_micros),
    };
    let document = to_doc(name, &doc)?;
    if stored.is_some() {
        txn.set(document);
    } else {
        txn.create(document);
    }
    Ok(schedule_from_doc(&doc)?)
}

pub(super) async fn list_task_schedules(
    filter: &TaskScheduleFilter,
    txn: &mut Txn,
) -> Result<Vec<TaskSchedule>, TaskScheduleStoreError> {
    let mut query =
        Query::new(TASK_SCHEDULES).eq("project_id", string_value(filter.project_id.as_str()));
    if let Some(w) = filter.warehouse_id {
        query = query.eq("warehouse_id", string_value(w.to_string()));
    }
    if let Some(q) = &filter.queue_name {
        query = query.eq("queue_name", string_value(q.to_string()));
    }
    if let Some(k) = &filter.schedule_key {
        query = query.eq("schedule_key", string_value(k.clone()));
    }
    let mut docs: Vec<TaskScheduleDoc> = txn
        .query(&query)
        .await?
        .iter()
        .map(from_doc)
        .collect::<Result<_, _>>()?;
    docs.retain(|d| d.spec_version <= SPEC_VERSION);
    docs.sort_by_key(|d| d.schedule_id);
    Ok(docs
        .iter()
        .map(schedule_from_doc)
        .collect::<Result<_, _>>()?)
}

pub(super) async fn delete_task_schedule(
    schedule_id: TaskScheduleId,
    txn: &mut Txn,
) -> Result<bool, TaskScheduleStoreError> {
    let name = schedule_name(txn, &schedule_id);
    if txn.get(&name).await?.is_none() {
        return Ok(false);
    }
    txn.delete(name);
    Ok(true)
}

pub(super) async fn delete_warehouse_task_schedules(
    warehouse_id: WarehouseId,
    txn: &mut Txn,
) -> Result<(), TaskScheduleStoreError> {
    let docs = txn
        .query(
            &Query::new(TASK_SCHEDULES).eq("warehouse_id", string_value(warehouse_id.to_string())),
        )
        .await?;
    for d in docs {
        txn.delete(d.name);
    }
    Ok(())
}

pub(super) async fn list_project_task_schedule_states(
    queue_name: &TaskQueueName,
    schedule_key: &str,
    txn: &mut Txn,
) -> Result<Vec<ProjectTaskScheduleState>, TaskScheduleStoreError> {
    let mut projects: Vec<ProjectDoc> = txn
        .query(&Query::new(PROJECTS))
        .await?
        .iter()
        .map(from_doc)
        .collect::<Result<_, _>>()?;
    projects.sort_by(|a, b| a.project_id.as_str().cmp(b.project_id.as_str()));
    let mut states = Vec::with_capacity(projects.len());
    for p in projects {
        let id = task_schedule_id(p.project_id.as_str(), None, queue_name, schedule_key);
        let names = [
            schedule_name(txn, &id),
            config_name(txn, &p.project_id, None, queue_name),
        ];
        let found = txn.get_many(&names).await?;
        let interval = found[0]
            .as_ref()
            .map(from_doc::<TaskScheduleDoc>)
            .transpose()?
            .map(|s| chrono::Duration::seconds(s.interval_seconds));
        let queue_config = found[1]
            .as_ref()
            .map(from_doc::<TaskConfigDoc>)
            .transpose()?
            .map(|c| parse_json(&c.config))
            .transpose()?;
        states.push(ProjectTaskScheduleState {
            project_id: Arc::new(p.project_id),
            interval,
            queue_config,
        });
    }
    Ok(states)
}

/// A task for one tick of schedule `s`, scheduled for `scheduled_for`.
fn schedule_task(
    s: &TaskScheduleDoc,
    scheduled_for: i64,
    now: i64,
) -> Result<TaskDoc, CatalogBackendError> {
    let project_id = s.project_id.clone();
    let entity = match s.warehouse_id {
        Some(warehouse_id) => TaskEntity::Warehouse { warehouse_id },
        None => TaskEntity::Project,
    };
    let metadata = lakekeeper::service::tasks::ScheduleTaskMetadata {
        project_id: Arc::new(project_id),
        parent_task_id: None,
        scheduled_for: Some(from_micros(scheduled_for)),
        entity,
        resource_demand: None,
    };
    Ok(new_task_doc(
        &s.queue_name,
        &metadata,
        Some(s.schedule_id),
        &parse_json(&s.payload)?,
        now,
    ))
}

pub(super) async fn trigger_task_schedule(
    schedule_id: TaskScheduleId,
    txn: &mut Txn,
) -> Result<EnqueueOutcome, TriggerTaskScheduleError> {
    let name = schedule_name(txn, &schedule_id);
    let Some(doc) = txn.get(&name).await? else {
        return Err(TaskScheduleNotFoundError {
            schedule_id,
            stack: Vec::new(),
        }
        .into());
    };
    let s: TaskScheduleDoc = from_doc(&doc)?;
    let now = now_micros();
    Ok(insert_task(txn, schedule_task(&s, now, now)?).await?)
}

pub(super) async fn materialize_due_task_schedules(
    limit: i64,
    txn: &mut Txn,
) -> Result<Vec<MaterializedTick>, TaskScheduleStoreError> {
    let limit = usize::try_from(limit).unwrap_or(0);
    let now = Utc::now();
    let now_micros = now.timestamp_micros();
    let mut ticks = Vec::new();
    let mut cursor = None;
    while ticks.len() < limit {
        let page = txn
            .query(
                &Query::new(TASK_SCHEDULES)
                    .eq("enabled", firestore::gcloud_sdk::google::firestore::v1::Value {
                        value_type: Some(
                            firestore::gcloud_sdk::google::firestore::v1::value::ValueType::BooleanValue(true),
                        ),
                    })
                    .at_most("next_run_at", int_value(now_micros))
                    .order_by("next_run_at")
                    .start_after(cursor.take())
                    .limit(Some(limit.min(500))),
            )
            .await?;
        let Some(last) = page.last() else { break };
        cursor = Some((
            last.fields
                .get("next_run_at")
                .cloned()
                .unwrap_or_else(|| int_value(0)),
            last.name.clone(),
        ));
        let full = page.len() == limit.min(500);
        for d in &page {
            if ticks.len() >= limit {
                break;
            }
            let mut s: TaskScheduleDoc = from_doc(d)?;
            if s.spec_version > SPEC_VERSION {
                continue;
            }
            let due = due_tick(
                from_micros(s.anchor),
                s.interval_seconds,
                from_micros(s.next_run_at),
                now,
            )
            .map_err(tick_error)?;
            let task = schedule_task(&s, due.tick.timestamp_micros(), now_micros)?;
            // A schedule whose last instance is still active stays due.
            let EnqueueOutcome::Inserted(task_id) = insert_task(txn, task).await? else {
                continue;
            };
            s.next_run_at = due.following.timestamp_micros();
            s.missed_runs += due.missed_runs;
            s.updated_at = Some(now_micros);
            txn.set(to_doc(d.name.clone(), &s)?);
            ticks.push(MaterializedTick {
                schedule_id: TaskScheduleId::from(s.schedule_id),
                task_id,
                tick: due.tick,
                missed_runs: s.missed_runs,
                next_run_at: due.following,
            });
        }
        if !full {
            break;
        }
    }
    Ok(ticks)
}

// ---------------------------------------------------------------------------------------------
// Demand
// ---------------------------------------------------------------------------------------------

/// The deadlines of the running (or stopping) tasks of the queue in the demand range of
/// `request`, and the queue's names.
async fn heartbeat_deadlines(
    read: &mut Txn,
    request: &TaskQueueDemandRequest<'_>,
) -> Result<Vec<i64>, CatalogBackendError> {
    let names = queue_names(request.queue_name, request.legacy_queue_names);
    let mut deadlines = Vec::new();
    for name in &names {
        for status in [RUNNING, SHOULD_STOP] {
            for t in active_tasks_of_queue(read, name, status).await? {
                if in_demand_range(request.resource_demand, &t) {
                    deadlines.push(
                        heartbeat_deadline(
                            read,
                            &t,
                            &names,
                            request.default_max_time_since_last_heartbeat,
                        )
                        .await?,
                    );
                }
            }
        }
    }
    Ok(deadlines)
}

/// Due, heartbeat-expired and running tasks of a queue in a demand range, as a pick sees them:
/// a heartbeat expires as the pick's sweep decides it.
pub(super) async fn task_queue_demand(
    request: TaskQueueDemandRequest<'_>,
    state: &FirestoreState,
) -> Result<TaskQueueDemand, CatalogBackendError> {
    if request.resource_demand.is_empty() {
        return Ok(TaskQueueDemand::default());
    }
    let now = now_micros();
    let mut read = state.latest();
    let mut demand = TaskQueueDemand::default();
    let mut oldest: Option<i64> = None;
    for name in queue_names(request.queue_name, request.legacy_queue_names) {
        let due = read
            .query(
                &Query::new(TASKS)
                    .eq("queue_name", string_value(name))
                    .eq("status", string_value(SCHEDULED))
                    .at_most("scheduled_for", int_value(now)),
            )
            .await?;
        for d in due {
            let t: TaskDoc = from_doc(&d)?;
            if in_demand_range(request.resource_demand, &t) {
                demand.due += 1;
                oldest = Some(oldest.map_or(t.scheduled_for, |o| o.min(t.scheduled_for)));
            }
        }
    }
    for deadline in heartbeat_deadlines(&mut read, &request).await? {
        if deadline < now {
            demand.heartbeat_expired += 1;
            oldest = Some(oldest.map_or(deadline, |o| o.min(deadline)));
        } else {
            demand.running += 1;
        }
    }
    demand.oldest_due_since = oldest.map(from_micros);
    Ok(demand)
}

/// Since when the longest waiting due or heartbeat-expired task of the queue can be taken,
/// reading the due tasks only up to the oldest one in the range.
pub(super) async fn task_queue_oldest_due(
    request: TaskQueueDemandRequest<'_>,
    state: &FirestoreState,
) -> Result<Option<DateTime<Utc>>, CatalogBackendError> {
    if request.resource_demand.is_empty() {
        return Ok(None);
    }
    let now = now_micros();
    let mut oldest: Option<i64> = None;
    for name in queue_names(request.queue_name, request.legacy_queue_names) {
        if let Some(t) = due_candidates(state, request.resource_demand, &name, now).await? {
            oldest = Some(oldest.map_or(t.scheduled_for, |o| o.min(t.scheduled_for)));
        }
    }
    let expired = heartbeat_deadlines(&mut state.latest(), &request)
        .await?
        .into_iter()
        .filter(|deadline| *deadline < now)
        .min();
    Ok(oldest.into_iter().chain(expired).min().map(from_micros))
}
