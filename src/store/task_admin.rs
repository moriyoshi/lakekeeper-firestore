//! The task management API: listing, details, resolving, stopping, rescheduling and
//! re-parenting tasks.
//!
//! A task shows up as its active [`TaskDoc`], its attempt logs, or both, as upstream lists
//! them: a listing has the active task and the newest matching log of each task, so a task
//! being retried appears twice. Listings read every active task and log of the warehouse (or
//! project) and filter, sort and page in memory: queries by one field each, which need no
//! composite index, at the cost of reading what the filters drop. Task log cleanup bounds it.

use std::{collections::HashMap, sync::Arc};

use lakekeeper::{
    api::{
        ErrorModel,
        management::v1::tasks::{
            ListTasksRequest, TaskAttempt, TaskStatus, WarehouseTaskEntityFilter,
        },
    },
    service::{
        CatalogBackendError, DatabaseIntegrityError, GenericTableNamed, GetTaskDetailsError,
        ResolveTasksError, ResolvedTask, TableNamed, TaskDetails, TaskList, ViewNamed,
        tasks::{
            ResolvedTaskEntity, TaskAttemptId, TaskDetailsScope, TaskEntity, TaskFilter, TaskId,
            TaskInfo, TaskQueueName, TaskResolveScope, WarehouseTaskEntityId,
        },
    },
};

use super::{
    docs::{TASK_LOGS, TASKS, TaskDoc, TaskLogDoc, from_micros, now_micros},
    fs::{Query, Txn, backend_error, from_doc, string_value, to_doc},
    tasks::{
        RUNNING, SCHEDULED, SHOULD_STOP, entity_of, log_doc, log_name, metadata_of, parse_json,
        queue_names, task_name, write_log,
    },
};

type Result<T, E = ErrorModel> = std::result::Result<T, E>;

/// A task log as the task it recorded, at the logged attempt; `status` is the outcome.
fn logged_task(l: TaskLogDoc) -> TaskDoc {
    TaskDoc {
        task_id: l.task_id,
        project_id: l.project_id,
        warehouse_id: l.warehouse_id,
        queue_name: l.queue_name,
        status: l.status,
        attempt: l.attempt,
        parent_task_id: l.parent_task_id,
        scheduled_for: l.attempt_scheduled_for,
        picked_up_at: l.started_at,
        last_heartbeat_at: l.last_heartbeat_at,
        progress: l.progress,
        execution_details: l.execution_details,
        task_data: l.task_data,
        entity_type: l.entity_type,
        entity_id: l.entity_id,
        entity_name: l.entity_name,
        resource_demand: l.resource_demand,
        created_at: l.task_created_at,
        updated_at: None,
    }
}

fn status_of(t: &TaskDoc) -> Result<TaskStatus, CatalogBackendError> {
    Ok(match t.status.as_str() {
        SCHEDULED => TaskStatus::Scheduled,
        RUNNING => TaskStatus::Running,
        SHOULD_STOP => TaskStatus::Stopping,
        "success" => TaskStatus::Success,
        "failed" => TaskStatus::Failed,
        "cancelled" => TaskStatus::Cancelled,
        other => {
            return Err(backend_error(std::io::Error::other(format!(
                "Unknown task status `{other}`"
            ))));
        }
    })
}

#[allow(clippy::cast_possible_truncation)] // Progress is stored as f64 but was an f32.
fn progress(t: &TaskDoc) -> f32 {
    t.progress as f32
}

fn info_of(t: &TaskDoc) -> Result<TaskInfo, CatalogBackendError> {
    Ok(TaskInfo {
        task_metadata: metadata_of(t)?,
        queue_name: TaskQueueName::from(t.queue_name.clone()),
        id: TaskAttemptId {
            task_id: t.task_id.into(),
            attempt: t.attempt,
        },
        status: status_of(t)?,
        picked_up_at: t.picked_up_at.map(from_micros),
        last_heartbeat_at: t.last_heartbeat_at.map(from_micros),
        progress: progress(t),
        created_at: from_micros(t.created_at),
        updated_at: t.updated_at.map(from_micros),
    })
}

fn optional_json(text: Option<&str>) -> Result<Option<serde_json::Value>, CatalogBackendError> {
    text.map(parse_json).transpose()
}

async fn logs_of(
    txn: &mut Txn,
    task_id: uuid::Uuid,
) -> Result<Vec<TaskLogDoc>, CatalogBackendError> {
    txn.query(&Query::new(TASK_LOGS).eq("task_id", string_value(task_id.to_string())))
        .await?
        .iter()
        .map(from_doc)
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------------------------

/// Sort key of a listed row, newest first: task creation, task ID, attempt, and 1 for the
/// active task, which can share the attempt of its newest log.
type RowKey = (i64, uuid::Uuid, i32, u8);

fn row_key(t: &TaskDoc) -> RowKey {
    let active = matches!(t.status.as_str(), SCHEDULED | RUNNING | SHOULD_STOP);
    (t.created_at, t.task_id, t.attempt, u8::from(active))
}

fn page_token((created_at, task_id, attempt, active): RowKey) -> String {
    format!("{created_at}|{task_id}|{attempt}|{active}")
}

fn parse_page_token(token: &str) -> Result<RowKey> {
    let invalid = || ErrorModel::bad_request("Invalid page token", "InvalidPageToken", None);
    let mut parts = token.split('|');
    let (Some(created_at), Some(task_id), Some(attempt), Some(active), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(invalid());
    };
    Ok((
        created_at.parse().map_err(|_| invalid())?,
        task_id.parse().map_err(|_| invalid())?,
        attempt.parse().map_err(|_| invalid())?,
        active.parse().map_err(|_| invalid())?,
    ))
}

fn entity_matches(filters: &[WarehouseTaskEntityFilter], t: &TaskDoc) -> bool {
    let is = |typ: &str, id: uuid::Uuid| t.entity_type == typ && t.entity_id == Some(id);
    filters.iter().any(|f| match f {
        WarehouseTaskEntityFilter::Table { table_id } => is("table", **table_id),
        WarehouseTaskEntityFilter::View { view_id } => is("view", **view_id),
        WarehouseTaskEntityFilter::GenericTable { generic_table_id } => {
            is("generic-table", **generic_table_id)
        }
        // A warehouse task may carry the ID of its schedule.
        WarehouseTaskEntityFilter::Warehouse => t.entity_type == "warehouse",
    })
}

/// Whether `t` passes the filters of `query` other than paging.
fn query_matches(query: &ListTasksRequest, t: &TaskDoc) -> Result<bool, CatalogBackendError> {
    let status = status_of(t)?;
    Ok(query
        .status
        .as_ref()
        .is_none_or(|statuses| statuses.contains(&status))
        && query
            .queue_name
            .as_ref()
            .is_none_or(|names| names.iter().any(|n| n.as_str() == t.queue_name))
        && query
            .entities
            .as_ref()
            .is_none_or(|filters| entity_matches(filters, t))
        && query
            .created_after
            .is_none_or(|after| t.created_at >= after.timestamp_micros())
        && query
            .created_before
            .is_none_or(|before| t.created_at <= before.timestamp_micros())
        && query
            .parent_task_id
            .is_none_or(|parent| t.parent_task_id == Some(*parent)))
}

pub(super) async fn list_tasks(
    filter: &TaskFilter,
    query: &ListTasksRequest,
    txn: &mut Txn,
) -> Result<TaskList> {
    let (project_id, warehouse_id, include_sub_tasks) = match filter {
        TaskFilter::WarehouseId {
            warehouse_id,
            project_id,
        } => (project_id, Some(*warehouse_id), false),
        TaskFilter::ProjectId {
            project_id,
            include_sub_tasks,
        } => (project_id, None, *include_sub_tasks),
        TaskFilter::TaskIds(_) => {
            return Err(ErrorModel::internal(
                "TaskFilter for TaskIds not implemented for list_tasks.",
                "InternalError",
                None,
            ));
        }
    };
    let in_scope = |t: &TaskDoc| {
        t.project_id == **project_id
            && match warehouse_id {
                Some(w) => t.warehouse_id == Some(w),
                None => include_sub_tasks || t.warehouse_id.is_none(),
            }
    };
    let scoped = |collection| match warehouse_id {
        Some(w) => Query::new(collection).eq("warehouse_id", string_value(w.to_string())),
        None => Query::new(collection).eq("project_id", string_value(project_id.as_str())),
    };
    let page_size =
        usize::try_from(lakekeeper::CONFIG.page_size_or_pagination_default(query.page_size))
            .unwrap_or(usize::MAX);
    let cursor = query
        .page_token
        .as_deref()
        .map(parse_page_token)
        .transpose()?;

    let mut rows = Vec::new();
    for d in txn.query(&scoped(TASKS)).await? {
        let t: TaskDoc = from_doc(&d)?;
        if in_scope(&t) && query_matches(query, &t)? {
            rows.push(t);
        }
    }
    // The newest matching attempt of each task.
    let mut newest_logs: HashMap<uuid::Uuid, TaskDoc> = HashMap::new();
    for d in txn.query(&scoped(TASK_LOGS)).await? {
        let t = logged_task(from_doc(&d)?);
        if in_scope(&t)
            && query_matches(query, &t)?
            && newest_logs
                .get(&t.task_id)
                .is_none_or(|n| n.attempt < t.attempt)
        {
            newest_logs.insert(t.task_id, t);
        }
    }
    rows.extend(newest_logs.into_values());
    rows.retain(|t| cursor.is_none_or(|c| row_key(t) < c));
    rows.sort_by_key(|t| std::cmp::Reverse(row_key(t)));
    rows.truncate(page_size);

    let tasks = rows.iter().map(info_of).collect::<Result<Vec<_>, _>>()?;
    // As upstream: the last row's token, or the given one for an empty page.
    let next_page_token = rows
        .last()
        .map(|t| page_token(row_key(t)))
        .or_else(|| query.page_token.clone());
    Ok(TaskList {
        tasks,
        next_page_token,
    })
}

// ---------------------------------------------------------------------------------------------
// Details and resolving
// ---------------------------------------------------------------------------------------------

/// One attempt of a task's details: the active task, or a log.
struct Attempt {
    task: TaskDoc,
    /// When the log was written; `None` for the active task.
    created_at: Option<i64>,
    duration_micros: Option<i64>,
    message: Option<String>,
}

pub(super) async fn get_task_details(
    task_id: TaskId,
    scope: TaskDetailsScope,
    num_attempts: u16,
    txn: &mut Txn,
) -> Result<Option<TaskDetails>, GetTaskDetailsError> {
    let project_id = scope.project_id();
    let warehouse_id = scope.warehouse_id();
    let in_scope = |t: &TaskDoc| t.project_id == *project_id && t.warehouse_id == warehouse_id;
    let now = now_micros();

    let mut attempts: Vec<Attempt> = txn
        .get(&task_name(txn, &task_id))
        .await?
        .map(|d| from_doc::<TaskDoc>(&d))
        .transpose()?
        .filter(in_scope)
        .map(|t| Attempt {
            duration_micros: t.picked_up_at.map(|p| now - p),
            task: t,
            created_at: None,
            message: None,
        })
        .into_iter()
        .collect();
    for l in logs_of(txn, *task_id).await? {
        let (created_at, duration_micros, message) =
            (l.created_at, l.duration_micros, l.message.clone());
        let task = logged_task(l);
        if in_scope(&task) {
            attempts.push(Attempt {
                task,
                created_at: Some(created_at),
                duration_micros,
                message,
            });
        }
    }
    // Newest first. A retried task keeps the number of its failed attempt until picked again;
    // the active task, pushed first, stays first among equals, as upstream orders them.
    attempts.sort_by_key(|a| std::cmp::Reverse(a.task.attempt));
    let mut attempts = attempts.into_iter();
    let Some(latest) = attempts.next() else {
        return Ok(None);
    };
    let earlier = attempts
        .take(usize::from(num_attempts))
        .map(|a| {
            Ok(TaskAttempt {
                attempt: a.task.attempt,
                status: status_of(&a.task)?,
                scheduled_for: from_micros(a.task.scheduled_for),
                started_at: a.task.picked_up_at.map(from_micros),
                duration: a.duration_micros.map(chrono::Duration::microseconds),
                message: a.message,
                created_at: from_micros(a.created_at.ok_or_else(|| {
                    DatabaseIntegrityError::new("Task attempt is missing created_at timestamp.")
                })?),
                progress: progress(&a.task),
                resource_demand: a.task.resource_demand,
                execution_details: optional_json(a.task.execution_details.as_deref())?,
            })
        })
        .collect::<Result<Vec<_>, GetTaskDetailsError>>()?;
    Ok(Some(TaskDetails {
        task: info_of(&latest.task)?,
        execution_details: optional_json(latest.task.execution_details.as_deref())?,
        data: parse_json(&latest.task.task_data)?,
        attempts: earlier,
        message: latest.message,
    }))
}

fn ident(t: &TaskDoc) -> Result<lakekeeper::service::TableIdent, DatabaseIntegrityError> {
    let parts = t.entity_name.clone().unwrap_or_default();
    lakekeeper::service::TableIdent::from_strs(&parts).map_err(|e| {
        DatabaseIntegrityError::new(format!(
            "Task {} has an invalid entity name: {e}",
            t.task_id
        ))
    })
}

fn resolved_entity(t: &TaskDoc) -> Result<ResolvedTaskEntity, ResolveTasksError> {
    Ok(match entity_of(t)? {
        TaskEntity::Project => ResolvedTaskEntity::Project,
        TaskEntity::Warehouse { warehouse_id } => warehouse_id.into(),
        TaskEntity::EntityInWarehouse {
            warehouse_id,
            entity_id,
            ..
        } => match entity_id {
            WarehouseTaskEntityId::Table { table_id } => TableNamed {
                warehouse_id,
                table_ident: ident(t)?,
                table_id,
            }
            .into(),
            WarehouseTaskEntityId::View { view_id } => ViewNamed {
                warehouse_id,
                view_ident: ident(t)?,
                view_id,
            }
            .into(),
            WarehouseTaskEntityId::GenericTable { generic_table_id } => GenericTableNamed {
                warehouse_id,
                generic_table_ident: ident(t)?,
                generic_table_id,
            }
            .into(),
        },
    })
}

/// The active task, or else the newest attempt log, of each of `task_ids` in `scope`.
pub(super) async fn resolve_tasks(
    scope: TaskResolveScope,
    task_ids: &[TaskId],
    txn: &mut Txn,
) -> Result<Vec<ResolvedTask>, ResolveTasksError> {
    let project_id = scope.project_id();
    let in_scope = |t: &TaskDoc| {
        t.project_id == *project_id
            && match &scope {
                TaskResolveScope::Project { .. } => t.warehouse_id.is_none(),
                TaskResolveScope::Warehouse { warehouse_id, .. } => {
                    warehouse_id.is_none_or(|w| t.warehouse_id == Some(w))
                }
            }
    };
    let names: Vec<String> = task_ids.iter().map(|id| task_name(txn, id)).collect();
    let mut found: HashMap<uuid::Uuid, TaskDoc> = HashMap::new();
    for d in txn.get_many(&names).await?.into_iter().flatten() {
        let t: TaskDoc = from_doc(&d)?;
        if in_scope(&t) {
            found.insert(t.task_id, t);
        }
    }
    for id in task_ids {
        if found.contains_key(&**id) {
            continue;
        }
        let newest = logs_of(txn, **id)
            .await?
            .into_iter()
            .map(logged_task)
            .filter(in_scope)
            .max_by_key(|t| t.attempt);
        if let Some(t) = newest {
            found.insert(t.task_id, t);
        }
    }
    found
        .into_values()
        .map(|t| {
            Ok(ResolvedTask {
                task_id: t.task_id.into(),
                project_id: Arc::new(t.project_id.clone()),
                entity: resolved_entity(&t)?,
                queue_name: TaskQueueName::from(t.queue_name.clone()),
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Control
// ---------------------------------------------------------------------------------------------

async fn read_tasks(txn: &mut Txn, task_ids: &[TaskId]) -> Result<Vec<TaskDoc>> {
    let names: Vec<String> = task_ids.iter().map(|id| task_name(txn, id)).collect();
    Ok(txn
        .get_many(&names)
        .await?
        .into_iter()
        .flatten()
        .map(|d| from_doc(&d))
        .collect::<Result<_, _>>()?)
}

/// Ask the running tasks among `task_ids` to stop; others are left alone.
pub(super) async fn stop_tasks(task_ids: &[TaskId], txn: &mut Txn) -> Result<()> {
    for mut t in read_tasks(txn, task_ids).await? {
        if t.status == RUNNING {
            t.status = SHOULD_STOP.to_string();
            txn.set(to_doc(task_name(txn, &t.task_id), &t)?);
        }
    }
    Ok(())
}

/// Schedule the scheduled or stopping tasks among `task_ids` for `scheduled_for`, or now. A
/// stopping task's attempt is logged as failed.
pub(super) async fn run_tasks_at(
    task_ids: &[TaskId],
    scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    txn: &mut Txn,
) -> Result<()> {
    let now = now_micros();
    let tasks: Vec<TaskDoc> = read_tasks(txn, task_ids)
        .await?
        .into_iter()
        .filter(|t| t.status == SCHEDULED || t.status == SHOULD_STOP)
        .collect();
    let logs: Vec<String> = tasks
        .iter()
        .map(|t| log_name(txn, &t.task_id, t.attempt))
        .collect();
    let existing = txn.get_many(&logs).await?;
    for (mut t, existing) in tasks.into_iter().zip(existing) {
        if t.status == SHOULD_STOP && existing.is_none() {
            let message = "Task did not stop in time before being rescheduled.".to_string();
            write_log(txn, &log_doc(&t, t.attempt, "failed", Some(message), now))?;
        }
        t.scheduled_for = scheduled_for.map_or(now, |at| at.timestamp_micros());
        t.status = SCHEDULED.to_string();
        t.progress = 0.0;
        t.execution_details = None;
        t.last_heartbeat_at = None;
        t.picked_up_at = None;
        txn.set(to_doc(task_name(txn, &t.task_id), &t)?);
    }
    Ok(())
}

/// Move a scheduled task of the queue (under its name or a legacy one) to `new_parent` with
/// new data. Returns `false`, changing nothing, for any other task.
pub(super) async fn reparent_pending_task(
    queue_name: &TaskQueueName,
    legacy_queue_names: &[&TaskQueueName],
    task_id: TaskId,
    new_parent: TaskId,
    task_data: &serde_json::Value,
    txn: &mut Txn,
) -> Result<bool> {
    let names = queue_names(queue_name, legacy_queue_names);
    let Some(mut t) = read_tasks(txn, &[task_id]).await?.pop() else {
        return Ok(false);
    };
    if t.status != SCHEDULED || !names.contains(&t.queue_name) {
        return Ok(false);
    }
    t.parent_task_id = Some(*new_parent);
    t.task_data = super::tasks::json_text(task_data);
    txn.set(to_doc(task_name(txn, &t.task_id), &t)?);
    Ok(true)
}
