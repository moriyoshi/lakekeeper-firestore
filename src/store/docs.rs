//! Document layout. Every collection is top-level and carries the configured prefix.
//!
//! | Collection | Document ID | Contents |
//! | --- | --- | --- |
//! | `server` | `singleton` | [`ServerDoc`] |
//! | `projects` | project ID | [`ProjectDoc`] |
//! | `warehouses` | warehouse ID | [`WarehouseDoc`] |
//! | `warehouse_names` | `{project}:{folded name}` | [`NameMarker`] |
//! | `namespaces` | namespace ID | [`NamespaceDoc`] |
//! | `namespace_names` | `{warehouse}:{folded path}` | [`NameMarker`] |
//! | `tabulars` | tabular ID | [`TabularDoc`]: a table, view or generic table |
//! | `tabular_names` | `{warehouse}:{namespace}:{folded name}` | [`NameMarker`] (active tabulars only) |
//! | `tabular_locations` | `{warehouse}:{location}` | [`NameMarker`] |
//! | `table_metadata` | table ID | [`StagedMetadataDoc`]: gzipped metadata JSON of a staged table |
//! | `tasks` | task ID | [`TaskDoc`]: an active (scheduled, running or stopping) task |
//! | `task_keys` | [`task_key_id`] | [`NameMarker`]: the active task of an entity and queue |
//! | `task_logs` | `{task ID}.{attempt, 10 digits}` | [`TaskLogDoc`]: one finished attempt |
//! | `task_configs` | [`task_config_id`] | [`TaskConfigDoc`] |
//! | `task_schedules` | schedule ID ([`task_schedule_id`]) | [`TaskScheduleDoc`] |
//! | `users` | user ID | [`UserDoc`] |
//! | `tag_definitions` | tag definition ID | [`TagDefinitionDoc`] |
//! | `tag_definition_names` | `{project}:{folded name}` | [`NameMarker`] |
//! | `tags` | `{target}:{definition}:{source}` | [`TagDoc`]: a definition attached to a target |
//! | `secrets` | secret ID | a storage credential without secret material ([`super::secrets`]) |
//! | `cache_changes` | random (`UUIDv7`) | [`CacheChangeDoc`]: changed cache keys of one write |
//! | `idempotency_records` | `{warehouse}:{key}` | [`IdempotencyDoc`] |
//! | `idx_tables` | table ID | [`IdxTable`](super::projection::IdxTable) |
//! | `idx_views` | view ID | [`IdxView`](super::projection::IdxView) |
//! | `idx_snapshots` | `{table}:{snapshot ID}` | [`IdxSnapshot`](super::projection::IdxSnapshot) |
//! | `idx_fields` | `{table or view}:{field ID}` | [`IdxField`](super::projection::IdxField) |
//! | `idx_partition_specs` | `{table}:{spec ID}` | [`IdxPartitionSpec`](super::projection::IdxPartitionSpec) |
//!
//! The `idx_` collections are the searchable projection of table and view metadata
//! ([`super::projection`]); nothing on the catalog's request paths reads them but commits.
//!
//! Marker documents make natural keys unique: they are created with an `exists = false`
//! precondition in the transaction that writes the entity. Times are microseconds since the
//! Unix epoch.

use std::collections::HashMap;

use lakekeeper::{
    ProjectId, WarehouseId,
    service::{NamespaceId, ServerId},
};
use serde::{Deserialize, Serialize};

use super::fs::encode_key;

pub(crate) const SERVER_COLLECTION: &str = "server";
pub(crate) const SERVER_DOC: &str = "singleton";
pub(crate) const PROJECTS: &str = "projects";
pub(crate) const WAREHOUSES: &str = "warehouses";
pub(crate) const WAREHOUSE_NAMES: &str = "warehouse_names";
pub(crate) const NAMESPACES: &str = "namespaces";
pub(crate) const NAMESPACE_NAMES: &str = "namespace_names";
pub(crate) const TABULARS: &str = "tabulars";
pub(crate) const TABULAR_NAMES: &str = "tabular_names";
pub(crate) const TABULAR_LOCATIONS: &str = "tabular_locations";
pub(crate) const TABLE_METADATA: &str = "table_metadata";
pub(crate) const TASKS: &str = "tasks";
pub(crate) const TASK_KEYS: &str = "task_keys";
pub(crate) const TASK_LOGS: &str = "task_logs";
pub(crate) const TASK_CONFIGS: &str = "task_configs";
pub(crate) const TASK_SCHEDULES: &str = "task_schedules";
pub(crate) const USERS: &str = "users";
pub(crate) const CACHE_CHANGES: &str = "cache_changes";
pub(crate) const SECRETS: &str = "secrets";
pub(crate) const TAG_DEFINITIONS: &str = "tag_definitions";
pub(crate) const TAG_DEFINITION_NAMES: &str = "tag_definition_names";
pub(crate) const TAGS: &str = "tags";
pub(crate) const IDEMPOTENCY_RECORDS: &str = "idempotency_records";
pub(crate) const IDX_TABLES: &str = "idx_tables";
pub(crate) const IDX_VIEWS: &str = "idx_views";
pub(crate) const IDX_SNAPSHOTS: &str = "idx_snapshots";
pub(crate) const IDX_FIELDS: &str = "idx_fields";
pub(crate) const IDX_PARTITION_SPECS: &str = "idx_partition_specs";

/// Version of this layout, stored in the server document for future upgrade steps.
pub(crate) const DATA_VERSION: i64 = 1;

pub(crate) fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

pub(crate) fn from_micros(micros: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ServerDoc {
    pub(crate) server_id: ServerId,
    pub(crate) open_for_bootstrap: bool,
    pub(crate) terms_accepted: bool,
    pub(crate) data_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProjectDoc {
    pub(crate) project_id: ProjectId,
    pub(crate) name: String,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NameMarker {
    pub(crate) id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WarehouseDoc {
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) project_id: ProjectId,
    pub(crate) name: String,
    /// JSON of `StorageProfile`.
    pub(crate) storage_profile: String,
    pub(crate) storage_secret_id: Option<String>,
    /// JSON of `TabularDeleteProfile`.
    pub(crate) delete_profile: String,
    /// `active` or `inactive`.
    pub(crate) status: String,
    pub(crate) protected: bool,
    /// JSON of `ManagedBy`.
    pub(crate) managed_by: String,
    /// JSON of `Vec<FormatVersion>`.
    pub(crate) allowed_format_versions: String,
    /// JSON of `Option<FormatVersion>`.
    pub(crate) default_format_version: String,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
    pub(crate) version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NamespaceDoc {
    pub(crate) namespace_id: NamespaceId,
    pub(crate) warehouse_id: WarehouseId,
    /// The stored (canonical) spelling of the full path.
    pub(crate) name: Vec<String>,
    /// [`path_key`] of the parent path; empty at the warehouse root.
    pub(crate) parent_key: String,
    pub(crate) depth: i64,
    pub(crate) protected: bool,
    pub(crate) properties: Option<HashMap<String, String>>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
    pub(crate) version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TabularDoc {
    pub(crate) tabular_id: uuid::Uuid,
    /// `table`, `view` or `generic-table`.
    pub(crate) typ: String,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) name: String,
    pub(crate) metadata_location: Option<String>,
    pub(crate) location: String,
    /// [`location_prefixes`] of `location`, to find the tabulars under a location.
    pub(crate) location_prefixes: Vec<String>,
    pub(crate) protected: bool,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
    pub(crate) deleted_at: Option<i64>,
    /// Whether the document is active (not soft-deleted); a query field.
    pub(crate) active: bool,
    /// Generic tables only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generic: Option<GenericTableDoc>,
}

/// What a generic table stores besides the tabular fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GenericTableDoc {
    pub(crate) format: String,
    pub(crate) doc: Option<String>,
    /// JSON.
    pub(crate) schema: Option<String>,
    /// JSON.
    pub(crate) statistics: Option<String>,
}

/// A tag definition, with the allowed values of an enumerated one, sorted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TagDefinitionDoc {
    pub(crate) tag_definition_id: uuid::Uuid,
    pub(crate) project_id: ProjectId,
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) scope: Vec<lakekeeper::service::TagScope>,
    pub(crate) value_kind: lakekeeper::service::TagValueKind,
    pub(crate) allowed_values: Vec<String>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
}

/// A tag definition attached to a warehouse, namespace, tabular or column: at most one of
/// `namespace_id` and `tabular_id` is set, and `field_id` only with `tabular_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TagDoc {
    pub(crate) tag_id: uuid::Uuid,
    pub(crate) tag_definition_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) namespace_id: Option<NamespaceId>,
    pub(crate) tabular_id: Option<uuid::Uuid>,
    /// The tabular's type (`table`, `view` or `generic-table`), to rebuild the target.
    pub(crate) tabular_typ: Option<String>,
    pub(crate) field_id: Option<i32>,
    /// The target's scope; a query field.
    pub(crate) kind: lakekeeper::service::TagScope,
    /// The target's key; a query field.
    pub(crate) target: String,
    pub(crate) value: Option<String>,
    pub(crate) source: lakekeeper::service::TagSource,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
}

/// Keys of one cache domain that a committed write changed; no keys for the whole domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheChangeDoc {
    pub(crate) domain: String,
    pub(crate) keys: Vec<String>,
    pub(crate) recorded_at: i64,
}

/// A user; deleted ones stay, scrubbed, with `deleted_at` set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct UserDoc {
    pub(crate) id: String,
    /// `None` for a role-provider stub that has no name yet.
    pub(crate) name: Option<String>,
    pub(crate) email: Option<String>,
    /// Upstream's `user_last_updated_with`, kebab-case.
    pub(crate) last_updated_with: String,
    pub(crate) user_type: lakekeeper::api::management::v1::user::UserType,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
    pub(crate) deleted_at: Option<i64>,
    /// Whether the user is not deleted; a query field.
    pub(crate) active: bool,
}

/// A committed success under an `Idempotency-Key`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct IdempotencyDoc {
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) idempotency_key: uuid::Uuid,
    /// [`EndpointFlat::name`](lakekeeper::api::endpoints::EndpointFlat::name).
    pub(crate) operation: String,
    pub(crate) http_status: i64,
    pub(crate) created_at: i64,
    /// A Firestore timestamp, for the TTL policy.
    #[serde(with = "firestore::serialize_as_timestamp")]
    pub(crate) expire_at: firestore::jiff::Timestamp,
}

/// The initial metadata of a staged table, which has no metadata file until its first commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StagedMetadataDoc {
    /// Gzipped JSON.
    #[serde(with = "serde_bytes_compat")]
    pub(crate) data: Vec<u8>,
}

/// Serialize `Vec<u8>` as Firestore bytes rather than an array of integers.
mod serde_bytes_compat {
    use serde::{Deserializer, Serializer, de::Visitor};

    pub(super) fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct BytesVisitor;
        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("bytes")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> {
                Ok(v.to_vec())
            }
            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                Ok(v)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> Result<Self::Value, A::Error> {
                let mut out = Vec::new();
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        d.deserialize_byte_buf(BytesVisitor)
    }
}

/// Case-folded key of a namespace path, unique per warehouse.
pub(crate) fn path_key(path: &[String]) -> String {
    path.iter()
        .map(|s| super::fs::fold_case(s))
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

pub(crate) fn warehouse_name_id(project_id: &ProjectId, name: &str) -> String {
    format!(
        "{}:{}",
        encode_key(project_id.as_str()),
        encode_key(&super::fs::fold_case(name))
    )
}

pub(crate) fn namespace_name_id(warehouse_id: WarehouseId, path: &[String]) -> String {
    format!("{warehouse_id}:{}", encode_key(&path_key(path)))
}

pub(crate) fn tabular_name_id(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    name: &str,
) -> String {
    format!(
        "{warehouse_id}:{namespace_id}:{}",
        encode_key(&super::fs::fold_case(name))
    )
}

/// Every proper prefix of a location at a `/` boundary, after the scheme:
/// `s3://b/a/c` gives `s3://b` and `s3://b/a`.
pub(crate) fn location_prefixes(location: &str) -> Vec<String> {
    let location = location.trim_end_matches('/');
    let start = location.find("://").map_or(0, |i| i + 3);
    let mut out = Vec::new();
    for (i, c) in location.char_indices().skip_while(|(i, _)| *i < start) {
        if c == '/' && i > start {
            out.push(location[..i].to_string());
        }
    }
    out
}

pub(crate) fn tabular_location_id(warehouse_id: WarehouseId, location: &str) -> String {
    format!(
        "{warehouse_id}:{}",
        encode_key(location.trim_end_matches('/'))
    )
}

/// Parts joined so that no two part lists give the same ID: each part is prefixed with its
/// length in bytes.
fn length_prefixed<'a>(parts: impl IntoIterator<Item = &'a str>) -> String {
    let mut joined = String::new();
    for p in parts {
        joined.push_str(&p.len().to_string());
        joined.push(':');
        joined.push_str(p);
    }
    encode_key(&joined)
}

/// An active task. Times are microseconds; JSON values are stored as JSON text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskDoc {
    pub(crate) task_id: uuid::Uuid,
    pub(crate) project_id: ProjectId,
    pub(crate) warehouse_id: Option<WarehouseId>,
    pub(crate) queue_name: String,
    /// `scheduled`, `running` or `should-stop`.
    pub(crate) status: String,
    /// The attempt last picked; 0 before the first pick.
    pub(crate) attempt: i32,
    pub(crate) parent_task_id: Option<uuid::Uuid>,
    pub(crate) scheduled_for: i64,
    pub(crate) picked_up_at: Option<i64>,
    pub(crate) last_heartbeat_at: Option<i64>,
    pub(crate) progress: f64,
    pub(crate) execution_details: Option<String>,
    pub(crate) task_data: String,
    /// `project`, `warehouse`, `table`, `view` or `generic-table`.
    pub(crate) entity_type: String,
    /// The tabular, or for a task produced by a schedule, the schedule.
    pub(crate) entity_id: Option<uuid::Uuid>,
    pub(crate) entity_name: Option<Vec<String>>,
    pub(crate) resource_demand: Option<i64>,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
}

/// One finished attempt of a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskLogDoc {
    pub(crate) task_id: uuid::Uuid,
    pub(crate) attempt: i32,
    pub(crate) project_id: ProjectId,
    pub(crate) warehouse_id: Option<WarehouseId>,
    pub(crate) queue_name: String,
    /// `success`, `failed` or `cancelled`.
    pub(crate) status: String,
    pub(crate) message: Option<String>,
    pub(crate) task_data: String,
    pub(crate) entity_type: String,
    pub(crate) entity_id: Option<uuid::Uuid>,
    pub(crate) entity_name: Option<Vec<String>>,
    pub(crate) started_at: Option<i64>,
    pub(crate) duration_micros: Option<i64>,
    pub(crate) progress: f64,
    pub(crate) execution_details: Option<String>,
    pub(crate) attempt_scheduled_for: i64,
    pub(crate) last_heartbeat_at: Option<i64>,
    pub(crate) parent_task_id: Option<uuid::Uuid>,
    pub(crate) task_created_at: i64,
    pub(crate) resource_demand: Option<i64>,
    pub(crate) created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskConfigDoc {
    pub(crate) project_id: ProjectId,
    pub(crate) warehouse_id: Option<WarehouseId>,
    pub(crate) queue_name: String,
    /// JSON text.
    pub(crate) config: String,
    pub(crate) max_seconds_since_last_heartbeat: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskScheduleDoc {
    pub(crate) schedule_id: uuid::Uuid,
    pub(crate) project_id: ProjectId,
    pub(crate) warehouse_id: Option<WarehouseId>,
    pub(crate) queue_name: String,
    pub(crate) schedule_key: String,
    pub(crate) interval_seconds: i64,
    pub(crate) anchor: i64,
    /// JSON text; the `task_data` of every task the schedule produces.
    pub(crate) payload: String,
    pub(crate) enabled: bool,
    pub(crate) next_run_at: i64,
    pub(crate) missed_runs: i64,
    pub(crate) spec_version: i64,
    pub(crate) created_at: i64,
    pub(crate) updated_at: Option<i64>,
}

/// At most one active task per project, warehouse, entity and exact queue name.
pub(crate) fn task_key_id(
    project_id: &str,
    warehouse_id: Option<&str>,
    entity_type: &str,
    entity_id: Option<&uuid::Uuid>,
    queue_name: &str,
) -> String {
    let entity_id = entity_id.map(ToString::to_string).unwrap_or_default();
    length_prefixed([
        project_id,
        warehouse_id.unwrap_or_default(),
        entity_type,
        &entity_id,
        queue_name,
    ])
}

pub(crate) fn task_log_id(task_id: &uuid::Uuid, attempt: i32) -> String {
    format!("{task_id}.{:010}", attempt.max(0))
}

pub(crate) fn task_config_id(
    project_id: &str,
    warehouse_id: Option<&str>,
    queue_name: &str,
) -> String {
    length_prefixed([project_id, warehouse_id.unwrap_or_default(), queue_name])
}

/// The schedule ID upstream derives (`task_schedule_id` in its Postgres migrations): a version 5 UUID
/// of the length-prefixed project, warehouse, queue and key. A schedule deleted and created
/// again keeps its ID, and with it the task it left active.
pub(crate) fn task_schedule_id(
    project_id: &str,
    warehouse_id: Option<&str>,
    queue_name: &str,
    schedule_key: &str,
) -> uuid::Uuid {
    const NAMESPACE: uuid::Uuid = uuid::uuid!("0b6f5d0e-3c1a-4f4e-9a57-4f2b8c9d1e60");
    let part = |s: &str| format!("{}:{s}", s.len());
    let name = [
        part(project_id),
        part(warehouse_id.unwrap_or_default()),
        part(queue_name),
        part(schedule_key),
    ]
    .concat();
    uuid::Uuid::new_v5(&NAMESPACE, name.as_bytes())
}
