//! The `metadata_projection` task queue: brings the stored projection of a table or view in
//! line with it, for changes too large for the transaction that made them (DESIGN.md, "Large
//! histories"), and on request through the management API, to repair a projection.
//!
//! A task reconciles: if the table or view exists with a metadata file, the stored projection
//! becomes the projection of that file; otherwise every document of the projection is deleted.
//! It compares the stored documents with the target and writes only those that differ, in
//! batches, each in a transaction that first checks that the table or view is still where the
//! pass started. The last transaction writes the head document, clearing `pending`, and records
//! the task's success, after the same check; a commit, drop or rename in between makes the pass
//! start over.

use std::{collections::BTreeMap, str::FromStr, sync::LazyLock, time::Duration};

use lakekeeper::{
    CancellationToken,
    api::ErrorModel,
    iceberg::spec::{TableMetadata, ViewMetadata},
    service::{
        CatalogBackendError, InternalParseLocationError, Location,
        tasks::{
            QueueRegistration, QueueScope, RetryDecision, ScheduleTaskMetadata, SpecializedTask,
            TaskConfig, TaskData, TaskEntity, TaskExecutionDetails, TaskInput, TaskQueueName,
            TaskQueueRegistry, UserScheduling, WarehouseTaskEntityId,
        },
    },
    tracing,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    FirestoreBackend, FirestoreState,
    docs::{IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, TabularDoc},
    files,
    fs::{Document, Txn, backend_error, from_doc},
    namespace,
    projection::{
        IdxField, IdxPartitionSpec, IdxSnapshot, Write, project, project_view, table_of, view_of,
    },
    projection_store::{self, identity},
    tabular::{self, TABLE, VIEW},
    tasks, warehouse,
};

const QN_STR: &str = "metadata_projection";
pub(crate) static QUEUE_NAME: LazyLock<TaskQueueName> = LazyLock::new(|| QN_STR.into());

/// Documents written per transaction.
const BATCH: usize = 400;
/// Passes a task makes before it fails, if the table keeps changing under it. A failed task is
/// retried.
const MAX_PASSES: usize = 5;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ProjectionQueueConfig {}

impl TaskConfig for ProjectionQueueConfig {
    fn queue_name() -> &'static TaskQueueName {
        &QUEUE_NAME
    }

    fn max_time_since_last_heartbeat() -> chrono::Duration {
        chrono::Duration::seconds(300)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ProjectionPayload {}

impl TaskData for ProjectionPayload {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectionExecutionDetails {}

impl TaskExecutionDetails for ProjectionExecutionDetails {}

pub(crate) type ProjectionTask =
    SpecializedTask<ProjectionQueueConfig, ProjectionPayload, ProjectionExecutionDetails>;

/// Enqueue a reconciliation of the projection of `t`, unless one is active already: an active
/// one checks the table again before it finishes.
pub(super) async fn enqueue(txn: &mut Txn, t: &TabularDoc) -> Result<(), CatalogBackendError> {
    let warehouse = warehouse::read(txn, t.warehouse_id).await?.ok_or_else(|| {
        backend_error(std::io::Error::other(format!(
            "Warehouse {} is missing",
            t.warehouse_id
        )))
    })?;
    let mut entity_name = namespace::read(txn, t.namespace_id)
        .await?
        .map(|n| n.name)
        .unwrap_or_default();
    entity_name.push(t.name.clone());
    let task = TaskInput {
        task_metadata: ScheduleTaskMetadata {
            project_id: std::sync::Arc::new(warehouse.project_id),
            parent_task_id: None,
            scheduled_for: None,
            entity: TaskEntity::EntityInWarehouse {
                warehouse_id: t.warehouse_id,
                entity_id: if t.typ == TABLE {
                    WarehouseTaskEntityId::Table {
                        table_id: t.tabular_id.into(),
                    }
                } else {
                    WarehouseTaskEntityId::View {
                        view_id: t.tabular_id.into(),
                    }
                },
                entity_name,
            },
            resource_demand: None,
        },
        payload: serde_json::to_value(ProjectionPayload::default()).map_err(backend_error)?,
    };
    tasks::enqueue(&QUEUE_NAME, task, txn).await?;
    Ok(())
}

/// Register the queue with `registry`, with its workers unless the server is in read-only
/// maintenance mode. Registered even without workers, so that the management API knows it.
pub(crate) async fn register(registry: &TaskQueueRegistry, state: FirestoreState) {
    let num_workers = if lakekeeper::CONFIG.maintenance_mode.is_read_only() {
        0
    } else {
        crate::CONFIG_BIN.firestore.projection_workers.unwrap_or(2)
    };
    let poll_interval = lakekeeper::CONFIG.task_poll_interval;
    registry
        .register_queue::<ProjectionQueueConfig, ProjectionPayload>(QueueRegistration {
            queue_name: &QUEUE_NAME,
            worker_fn: std::sync::Arc::new(move |cancellation_token| {
                Box::pin(worker(state.clone(), poll_interval, cancellation_token))
            }),
            num_workers,
            scope: QueueScope::Warehouse,
            // Scheduling a task for a table through the management API repairs its projection.
            user_scheduling: UserScheduling::Enabled,
        })
        .await;
}

async fn worker(
    state: FirestoreState,
    poll_interval: Duration,
    cancellation_token: CancellationToken,
) {
    loop {
        let Some(task) = ProjectionTask::poll_for_new_task::<FirestoreBackend>(
            state.clone(),
            &poll_interval,
            cancellation_token.clone(),
        )
        .await
        else {
            tracing::info!("Graceful shutdown: exiting `{QN_STR}` worker");
            return;
        };
        run(&state, &task).await;
    }
}

enum Outcome {
    Finished,
    Stopped,
}

/// Run `task` and record how it ended.
pub(crate) async fn run(state: &FirestoreState, task: &ProjectionTask) {
    match reconcile(state, task).await {
        Ok(Outcome::Finished) => {}
        Ok(Outcome::Stopped) => {
            task.record_failure_with_decision::<FirestoreBackend>(
                state.clone(),
                "Stopped on request; the projection stays pending until it is reconciled.",
                RetryDecision::give_up("stopped on request"),
            )
            .await;
        }
        Err(e) => {
            tracing::error!(task_id = %task.task_id(), "`{QN_STR}` task failed: {e:?}");
            task.record_failure::<FirestoreBackend>(
                state.clone(),
                &format!("Reconciling the metadata projection failed: {}", e.message),
            )
            .await;
        }
    }
}

/// What a pass reconciles the projection with: the table at a metadata location, or nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    At(String),
    Gone,
}

async fn read_source(
    txn: &mut Txn,
    warehouse_id: lakekeeper::WarehouseId,
    (typ, id): (&str, uuid::Uuid),
) -> Result<(Source, Option<TabularDoc>), CatalogBackendError> {
    let t = tabular::read(txn, id)
        .await?
        .filter(|t| t.warehouse_id == warehouse_id && t.typ == typ);
    let source = match t.as_ref().and_then(|t| t.metadata_location.clone()) {
        Some(location) => Source::At(location),
        None => Source::Gone,
    };
    Ok((source, t))
}

/// The metadata a pass projects.
enum Metadata {
    Table(Box<TableMetadata>),
    View(Box<ViewMetadata>),
}

#[allow(clippy::too_many_lines)]
async fn reconcile(state: &FirestoreState, task: &ProjectionTask) -> Result<Outcome, ErrorModel> {
    let entity = match task.task_metadata.warehouse_task_sub_entity() {
        Some((warehouse_id, WarehouseTaskEntityId::Table { table_id }, _)) => {
            (warehouse_id, (TABLE, **table_id))
        }
        Some((warehouse_id, WarehouseTaskEntityId::View { view_id }, _)) => {
            (warehouse_id, (VIEW, **view_id))
        }
        _ => {
            return Err(ErrorModel::internal(
                format!("A `{QN_STR}` task must have a table or view as its entity"),
                "UnexpectedTaskEntity",
                None,
            ));
        }
    };
    let (warehouse_id, (typ, id)) = entity;

    'passes: for _ in 0..MAX_PASSES {
        let mut txn = Txn::latest(state.db.clone());
        let (source, t) = read_source(&mut txn, warehouse_id, (typ, id)).await?;
        let metadata = match &source {
            Source::At(location) => {
                let warehouse =
                    warehouse::read(&mut txn, warehouse_id)
                        .await?
                        .ok_or_else(|| {
                            backend_error(std::io::Error::other(format!(
                                "Warehouse {warehouse_id} is missing"
                            )))
                        })?;
                let location =
                    Location::from_str(location).map_err(InternalParseLocationError::from)?;
                let files = &*state.files;
                Some(if typ == TABLE {
                    Metadata::Table(Box::new(
                        files::read_metadata(files, &warehouse, &location).await?,
                    ))
                } else {
                    Metadata::View(Box::new(
                        files::read_metadata(files, &warehouse, &location).await?,
                    ))
                })
            }
            Source::Gone => None,
        };
        let writes = match (t.as_ref(), metadata.as_ref()) {
            (Some(t), Some(Metadata::Table(m))) => {
                let p = project(&identity(t), m);
                differences(
                    &mut txn,
                    id,
                    Some(&p.snapshots),
                    Some(&p.fields),
                    Some(&p.specs),
                )
                .await?
            }
            (Some(t), Some(Metadata::View(m))) => {
                let p = project_view(&identity(t), m);
                differences(&mut txn, id, None, Some(&p.fields), None).await?
            }
            _ => differences(&mut txn, id, None, None, None).await?,
        };

        let batches = writes.len().div_ceil(BATCH);
        for (i, batch) in writes.chunks(BATCH).enumerate() {
            let mut txn = Txn::read_write(state.db.clone());
            if read_source(&mut txn, warehouse_id, (typ, id)).await?.0 != source {
                txn.rollback().await?;
                continue 'passes;
            }
            for w in batch {
                projection_store::write(&mut txn, id, w)?;
            }
            txn.commit().await?;
            #[allow(clippy::cast_precision_loss)]
            let progress = (i + 1) as f32 / batches as f32;
            let check = task
                .heartbeat::<FirestoreBackend>(state.clone(), progress, None)
                .await?;
            if check.should_terminate() {
                return Ok(Outcome::Stopped);
            }
        }

        let mut txn = Txn::read_write(state.db.clone());
        let (now, t) = read_source(&mut txn, warehouse_id, (typ, id)).await?;
        if now != source {
            txn.rollback().await?;
            continue;
        }
        // From the table or view as it is now: a rename since the pass started is kept.
        let head = match (t, metadata) {
            (Some(t), Some(Metadata::Table(m))) => Some(Write::Table(table_of(&identity(&t), &m))),
            (Some(t), Some(Metadata::View(m))) => Some(Write::View(view_of(&identity(&t), &m))),
            _ => None,
        };
        let message = if let Some(head) = head {
            projection_store::write(&mut txn, id, &head)?;
            format!("Projected; {} documents written", writes.len())
        } else {
            projection_store::delete_head(&mut txn, typ, id);
            format!("Cleaned up; {} documents deleted", writes.len())
        };
        if let Err(e) = tasks::record_success(task.id(), Some(&message), &mut txn).await {
            txn.rollback().await?;
            return Err(e);
        }
        txn.commit().await?;
        return Ok(Outcome::Finished);
    }
    Err(ErrorModel::conflict(
        format!("The {typ} kept changing over {MAX_PASSES} passes"),
        "ConcurrentUpdate",
        None,
    ))
}

/// The writes that turn the stored part documents of `id` into the given target parts. Without
/// a target, every stored document of the table's (or, for views, field) collections is
/// deleted. The head document is not among them.
async fn differences(
    txn: &mut Txn,
    id: uuid::Uuid,
    snapshots: Option<&BTreeMap<i64, IdxSnapshot>>,
    fields: Option<&BTreeMap<i32, IdxField>>,
    specs: Option<&BTreeMap<i32, IdxPartitionSpec>>,
) -> Result<Vec<Write>, CatalogBackendError> {
    let mut writes = Vec::new();
    let stored = projection_store::parts(txn, IDX_SNAPSHOTS, id).await?;
    diff(
        &stored,
        snapshots,
        Write::Snapshot,
        Write::DeleteSnapshot,
        &mut writes,
    );
    let stored = projection_store::parts(txn, IDX_FIELDS, id).await?;
    diff(
        &stored,
        fields,
        Write::Field,
        Write::DeleteField,
        &mut writes,
    );
    let stored = projection_store::parts(txn, IDX_PARTITION_SPECS, id).await?;
    diff(&stored, specs, Write::Spec, Write::DeleteSpec, &mut writes);
    Ok(writes)
}

/// Compare `stored` documents, keyed by the part of their name after the last `:`, with
/// `target`: write what is missing or differs (including documents that no longer decode),
/// delete what the target lacks.
fn diff<K, T>(
    stored: &[Document],
    target: Option<&BTreeMap<K, T>>,
    set: fn(T) -> Write,
    delete: fn(K) -> Write,
    writes: &mut Vec<Write>,
) where
    K: Ord + Copy + FromStr,
    T: Clone + PartialEq + DeserializeOwned,
{
    let empty = BTreeMap::new();
    let target = target.unwrap_or(&empty);
    let mut current: BTreeMap<K, Option<T>> = BTreeMap::new();
    for doc in stored {
        let Some(key) = doc
            .name
            .rsplit(':')
            .next()
            .and_then(|k| k.parse::<K>().ok())
        else {
            continue;
        };
        current.insert(key, from_doc(doc).ok());
    }
    for key in current.keys() {
        if !target.contains_key(key) {
            writes.push(delete(*key));
        }
    }
    for (key, value) in target {
        if current.get(key).and_then(Option::as_ref) != Some(value) {
            writes.push(set(value.clone()));
        }
    }
}
