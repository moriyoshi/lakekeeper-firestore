//! The searchable projection of table metadata (DESIGN.md, "Plan: searchable metadata
//! projection"). Pure functions; storing the documents is the caller's.
//!
//! [`project`] gives the projection of a table's metadata. [`commit_changes`] gives the documents
//! a commit changes, found part by part, by key: never a diff of the metadata JSON, and never a
//! replay. The values always come from the new metadata.
//!
//! | Part | Changes found by |
//! | --- | --- |
//! | Snapshots, partition specs | the commit's ID-set diffs |
//! | Fields | comparing the field projections before and after (see [`fields_of`]) |
//! | Refs | comparing the new refs with those of the previous [`IdxTable`] |
//! | Statistics | the commit's statistics updates, which name the snapshot |
//! | The table document | rewritten by every commit |
//!
//! Invariant: the stored projection equals [`project`] of the current metadata. The property
//! test in `tests/projection.rs` checks that [`commit_changes`] keeps it.

use std::collections::{BTreeMap, BTreeSet};

use lakekeeper::{
    WarehouseId,
    iceberg::{
        TableUpdate,
        spec::{
            PartitionSpec, SchemaRef, Snapshot, Summary, TableMetadata, Type, ViewMetadata,
            ViewRepresentation,
        },
    },
    server::tables::TableMetadataDiffs,
    service::NamespaceId,
};
use serde::{Deserialize, Serialize};

use super::fs::fold_case;

/// What identifies a table or view in its projection, apart from its metadata.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableIdentity {
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) table_id: uuid::Uuid,
    pub(crate) name: String,
    /// Whether the table is not soft-deleted.
    pub(crate) active: bool,
}

/// The numbers of a snapshot summary, by the keys Iceberg defines.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct SummaryNumbers {
    pub(crate) added_data_files: Option<i64>,
    pub(crate) deleted_data_files: Option<i64>,
    pub(crate) added_delete_files: Option<i64>,
    pub(crate) removed_delete_files: Option<i64>,
    pub(crate) added_records: Option<i64>,
    pub(crate) deleted_records: Option<i64>,
    pub(crate) added_files_size: Option<i64>,
    pub(crate) removed_files_size: Option<i64>,
    pub(crate) added_position_deletes: Option<i64>,
    pub(crate) removed_position_deletes: Option<i64>,
    pub(crate) added_equality_deletes: Option<i64>,
    pub(crate) removed_equality_deletes: Option<i64>,
    pub(crate) changed_partition_count: Option<i64>,
    pub(crate) total_records: Option<i64>,
    pub(crate) total_files_size: Option<i64>,
    pub(crate) total_data_files: Option<i64>,
    pub(crate) total_delete_files: Option<i64>,
    pub(crate) total_position_deletes: Option<i64>,
    pub(crate) total_equality_deletes: Option<i64>,
}

impl SummaryNumbers {
    fn of(summary: &Summary) -> Self {
        let n = |key: &str| {
            summary
                .additional_properties
                .get(key)
                .and_then(|v| v.parse().ok())
        };
        Self {
            added_data_files: n("added-data-files"),
            deleted_data_files: n("deleted-data-files"),
            added_delete_files: n("added-delete-files"),
            removed_delete_files: n("removed-delete-files"),
            added_records: n("added-records"),
            deleted_records: n("deleted-records"),
            added_files_size: n("added-files-size"),
            removed_files_size: n("removed-files-size"),
            added_position_deletes: n("added-position-deletes"),
            removed_position_deletes: n("removed-position-deletes"),
            added_equality_deletes: n("added-equality-deletes"),
            removed_equality_deletes: n("removed-equality-deletes"),
            changed_partition_count: n("changed-partition-count"),
            total_records: n("total-records"),
            total_files_size: n("total-files-size"),
            total_data_files: n("total-data-files"),
            total_delete_files: n("total-delete-files"),
            total_position_deletes: n("total-position-deletes"),
            total_equality_deletes: n("total-equality-deletes"),
        }
    }
}

/// `idx_tables`: one per table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IdxTable {
    pub(crate) table_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) name: String,
    /// Whether the table is not soft-deleted. Other documents of the projection do not say;
    /// searches resolve liveness and names through this one.
    pub(crate) active: bool,
    pub(crate) format_version: i64,
    pub(crate) location: String,
    pub(crate) last_updated_ms: i64,
    pub(crate) current_snapshot_id: Option<i64>,
    /// The summary of the current snapshot: the table's size and record counts.
    pub(crate) current_summary: Option<SummaryNumbers>,
    pub(crate) current_schema_id: i32,
    pub(crate) default_spec_id: i32,
    /// Ref name to snapshot ID; also the previous refs of the next commit.
    pub(crate) refs: BTreeMap<String, i64>,
    /// How many snapshot, field and partition spec documents the projection has, to decide
    /// whether a drop deletes them in its transaction.
    pub(crate) documents: i64,
    /// Whether the other documents are being rebuilt by a task and may not match this one yet.
    pub(crate) pending: bool,
}

/// A statistics or partition statistics file of a snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct StatsFile {
    pub(crate) path: String,
    pub(crate) size: i64,
}

/// `idx_snapshots`: one per snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IdxSnapshot {
    pub(crate) table_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) snapshot_id: i64,
    pub(crate) parent_snapshot_id: Option<i64>,
    pub(crate) sequence_number: i64,
    pub(crate) timestamp_ms: i64,
    pub(crate) operation: String,
    pub(crate) schema_id: Option<i32>,
    pub(crate) summary: SummaryNumbers,
    /// The summary entries that are not [`SummaryNumbers`]; not searchable.
    pub(crate) summary_other: BTreeMap<String, String>,
    pub(crate) manifest_list: String,
    /// The refs that point at this snapshot, sorted.
    pub(crate) refs: Vec<String>,
    pub(crate) statistics: Option<StatsFile>,
    pub(crate) partition_statistics: Option<StatsFile>,
}

/// `idx_fields`: one per field ID of a table or view, over all its schemas.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IdxField {
    /// The table's or view's ID.
    pub(crate) table_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    /// `table` or `view`.
    pub(crate) kind: String,
    pub(crate) field_id: i32,
    /// From the newest schema that has the field.
    pub(crate) name: String,
    pub(crate) name_folded: String,
    /// The dotted path from the root, e.g. `location.lat`.
    pub(crate) path: String,
    pub(crate) path_folded: String,
    /// A primitive type as Iceberg writes it (`long`, `decimal(10,2)`), or `struct`, `list`,
    /// `map`, `variant`.
    pub(crate) r#type: String,
    pub(crate) required: bool,
    /// Whether the current schema has the field.
    pub(crate) current: bool,
    /// The oldest schema the metadata still has with this field.
    pub(crate) first_schema_id: i32,
    /// The newest schema with this field, or `None` if the newest schema has it.
    pub(crate) last_schema_id: Option<i32>,
}

/// `idx_partition_specs`: one per partition spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IdxPartitionSpec {
    pub(crate) table_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) spec_id: i32,
    pub(crate) default: bool,
    pub(crate) names: Vec<String>,
    pub(crate) source_field_ids: Vec<i32>,
    pub(crate) transforms: Vec<String>,
    /// `{source field ID}:{transform}` per field, to search both at once.
    pub(crate) source_transforms: Vec<String>,
}

/// The projection of one table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Projection {
    pub(crate) table: IdxTable,
    pub(crate) snapshots: BTreeMap<i64, IdxSnapshot>,
    pub(crate) fields: BTreeMap<i32, IdxField>,
    pub(crate) specs: BTreeMap<i32, IdxPartitionSpec>,
}

/// `idx_views`: one per view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct IdxView {
    pub(crate) view_id: uuid::Uuid,
    pub(crate) warehouse_id: WarehouseId,
    pub(crate) namespace_id: NamespaceId,
    pub(crate) name: String,
    /// Whether the view is not soft-deleted.
    pub(crate) active: bool,
    pub(crate) location: String,
    pub(crate) current_version_id: i32,
    pub(crate) current_schema_id: i32,
    /// When the current version was created.
    pub(crate) last_updated_ms: i64,
    pub(crate) versions: i64,
    /// The SQL dialects of the current version, folded and sorted.
    pub(crate) dialects: Vec<String>,
    /// The default namespace of the current version.
    pub(crate) default_namespace: Vec<String>,
    /// How many field documents the projection has.
    pub(crate) documents: i64,
    /// Whether the field documents are being rebuilt by a task.
    pub(crate) pending: bool,
}

/// The projection of one view.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ViewProjection {
    pub(crate) view: IdxView,
    pub(crate) fields: BTreeMap<i32, IdxField>,
}

/// A change to a table's or view's projection.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Write {
    Table(IdxTable),
    View(IdxView),
    Snapshot(IdxSnapshot),
    DeleteSnapshot(i64),
    Field(IdxField),
    DeleteField(i32),
    Spec(IdxPartitionSpec),
    DeleteSpec(i32),
}

/// A commit removed schemas, so the previous fields cannot be derived from the new metadata;
/// call [`commit_changes`] again with the stored fields of the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NeedsPreviousFields;

/// The table document of the projection of `metadata`.
pub(crate) fn table_of(identity: &TableIdentity, metadata: &TableMetadata) -> IdxTable {
    let current = metadata.current_snapshot();
    let field_ids: BTreeSet<i32> = metadata
        .schemas_iter()
        .flat_map(|s| s.field_id_to_fields().keys().copied().collect::<Vec<_>>())
        .collect();
    let documents =
        metadata.snapshots().len() + field_ids.len() + metadata.partition_specs_iter().len();
    IdxTable {
        table_id: identity.table_id,
        warehouse_id: identity.warehouse_id,
        namespace_id: identity.namespace_id,
        name: identity.name.clone(),
        active: identity.active,
        format_version: match metadata.format_version() {
            lakekeeper::iceberg::spec::FormatVersion::V1 => 1,
            lakekeeper::iceberg::spec::FormatVersion::V2 => 2,
            lakekeeper::iceberg::spec::FormatVersion::V3 => 3,
        },
        location: metadata.location().to_string(),
        last_updated_ms: metadata.last_updated_ms(),
        current_snapshot_id: current.map(|s| s.snapshot_id()),
        current_summary: current.map(|s| SummaryNumbers::of(s.summary())),
        current_schema_id: metadata.current_schema_id(),
        default_spec_id: metadata.default_partition_spec_id(),
        refs: metadata
            .refs()
            .iter()
            .map(|(name, r)| (name.clone(), r.snapshot_id))
            .collect(),
        documents: i64::try_from(documents).unwrap_or(i64::MAX),
        pending: false,
    }
}

fn snapshot_of(identity: &TableIdentity, metadata: &TableMetadata, s: &Snapshot) -> IdxSnapshot {
    let id = s.snapshot_id();
    let numbers = SummaryNumbers::of(s.summary());
    // A number key whose value does not parse is kept, unsearchable, rather than lost.
    let summary_other = s
        .summary()
        .additional_properties
        .iter()
        .filter(|(k, v)| !is_number_key(k) || v.parse::<i64>().is_err())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let mut refs: Vec<String> = metadata
        .refs()
        .iter()
        .filter(|(_, r)| r.snapshot_id == id)
        .map(|(name, _)| name.clone())
        .collect();
    refs.sort();
    IdxSnapshot {
        table_id: identity.table_id,
        warehouse_id: identity.warehouse_id,
        snapshot_id: id,
        parent_snapshot_id: s.parent_snapshot_id(),
        sequence_number: s.sequence_number(),
        timestamp_ms: s.timestamp_ms(),
        operation: s.summary().operation.as_str().to_string(),
        schema_id: s.schema_id(),
        summary: numbers,
        summary_other,
        manifest_list: s.manifest_list().to_string(),
        refs,
        statistics: metadata.statistics_for_snapshot(id).map(|f| StatsFile {
            path: f.statistics_path.clone(),
            size: f.file_size_in_bytes,
        }),
        partition_statistics: metadata
            .partition_statistics_for_snapshot(id)
            .map(|f| StatsFile {
                path: f.statistics_path.clone(),
                size: f.file_size_in_bytes,
            }),
    }
}

/// Whether `key` is one of the summary keys [`SummaryNumbers`] holds.
fn is_number_key(key: &str) -> bool {
    const KEYS: [&str; 19] = [
        "added-data-files",
        "deleted-data-files",
        "added-delete-files",
        "removed-delete-files",
        "added-records",
        "deleted-records",
        "added-files-size",
        "removed-files-size",
        "added-position-deletes",
        "removed-position-deletes",
        "added-equality-deletes",
        "removed-equality-deletes",
        "changed-partition-count",
        "total-records",
        "total-files-size",
        "total-data-files",
        "total-delete-files",
        "total-position-deletes",
        "total-equality-deletes",
    ];
    KEYS.contains(&key)
}

fn type_name(t: &Type) -> String {
    match t {
        Type::Primitive(p) => p.to_string(),
        Type::Struct(_) => "struct".to_string(),
        Type::List(_) => "list".to_string(),
        Type::Map(_) => "map".to_string(),
        Type::Variant(_) => "variant".to_string(),
    }
}

/// The field projection of the schemas `schema_ids`, found by `schema_by_id`, with
/// `current_schema_id` current. A pure function of those schemas, so that the fields before a
/// commit can be computed from the metadata after it, as long as no schema was removed.
fn fields_of(
    identity: &TableIdentity,
    kind: &str,
    schema_by_id: impl Fn(i32) -> Option<SchemaRef>,
    schema_ids: &BTreeSet<i32>,
    current_schema_id: i32,
) -> BTreeMap<i32, IdxField> {
    let latest = schema_ids.last().copied();
    let mut out: BTreeMap<i32, IdxField> = BTreeMap::new();
    // Oldest first, so that each newer schema overwrites the field's attributes.
    for schema_id in schema_ids {
        let Some(schema) = schema_by_id(*schema_id) else {
            continue;
        };
        let current = *schema_id == current_schema_id;
        for (field_id, field) in schema.field_id_to_fields() {
            let path = schema
                .name_by_field_id(*field_id)
                .unwrap_or(&field.name)
                .to_string();
            let entry = out.entry(*field_id).or_insert_with(|| IdxField {
                table_id: identity.table_id,
                warehouse_id: identity.warehouse_id,
                kind: kind.to_string(),
                field_id: *field_id,
                name: String::new(),
                name_folded: String::new(),
                path: String::new(),
                path_folded: String::new(),
                r#type: String::new(),
                required: false,
                current: false,
                first_schema_id: *schema_id,
                last_schema_id: None,
            });
            entry.name.clone_from(&field.name);
            entry.name_folded = fold_case(&field.name);
            entry.path_folded = fold_case(&path);
            entry.path = path;
            entry.r#type = type_name(&field.field_type);
            entry.required = field.required;
            entry.current |= current;
            entry.last_schema_id = Some(*schema_id);
        }
    }
    for field in out.values_mut() {
        if field.last_schema_id == latest {
            field.last_schema_id = None;
        }
    }
    out
}

fn spec_of(
    identity: &TableIdentity,
    spec: &PartitionSpec,
    default_spec_id: i32,
) -> IdxPartitionSpec {
    let fields = spec.fields();
    IdxPartitionSpec {
        table_id: identity.table_id,
        warehouse_id: identity.warehouse_id,
        spec_id: spec.spec_id(),
        default: spec.spec_id() == default_spec_id,
        names: fields.iter().map(|f| f.name.clone()).collect(),
        source_field_ids: fields.iter().map(|f| f.source_id).collect(),
        transforms: fields.iter().map(|f| f.transform.to_string()).collect(),
        source_transforms: fields
            .iter()
            .map(|f| format!("{}:{}", f.source_id, f.transform))
            .collect(),
    }
}

fn schema_ids(metadata: &TableMetadata) -> BTreeSet<i32> {
    metadata.schemas_iter().map(|s| s.schema_id()).collect()
}

/// The fields of the schemas `schema_ids` of a table's `metadata`.
fn table_fields(
    identity: &TableIdentity,
    metadata: &TableMetadata,
    schema_ids: &BTreeSet<i32>,
    current_schema_id: i32,
) -> BTreeMap<i32, IdxField> {
    fields_of(
        identity,
        "table",
        |id| metadata.schema_by_id(id).cloned(),
        schema_ids,
        current_schema_id,
    )
}

/// The projection of `metadata`.
pub(crate) fn project(identity: &TableIdentity, metadata: &TableMetadata) -> Projection {
    Projection {
        table: table_of(identity, metadata),
        snapshots: metadata
            .snapshots()
            .map(|s| (s.snapshot_id(), snapshot_of(identity, metadata, s)))
            .collect(),
        fields: table_fields(
            identity,
            metadata,
            &schema_ids(metadata),
            metadata.current_schema_id(),
        ),
        specs: metadata
            .partition_specs_iter()
            .map(|s| {
                (
                    s.spec_id(),
                    spec_of(identity, s, metadata.default_partition_spec_id()),
                )
            })
            .collect(),
    }
}

/// The writes that turn the projection of a table's previous metadata into that of `metadata`,
/// the metadata after a commit with `diffs` and `updates`. `previous` is the stored table
/// document. `previous_fields`, the stored fields of the table, is needed only if the commit
/// removed schemas.
pub(crate) fn commit_changes(
    identity: &TableIdentity,
    previous: &IdxTable,
    metadata: &TableMetadata,
    diffs: &TableMetadataDiffs,
    updates: &[TableUpdate],
    previous_fields: Option<&BTreeMap<i32, IdxField>>,
) -> Result<Vec<Write>, NeedsPreviousFields> {
    let fields = field_writes(identity, previous, metadata, diffs, previous_fields)?;
    let mut writes = vec![Write::Table(table_of(identity, metadata))];

    // Snapshots: added and removed by ID; existing ones whose refs or statistics changed.
    let removed: BTreeSet<i64> = diffs.removed_snapshots.iter().copied().collect();
    let mut changed: BTreeSet<i64> = diffs.added_snapshots.iter().copied().collect();
    let new_refs = &table_of(identity, metadata).refs;
    for name in previous.refs.keys().chain(new_refs.keys()) {
        let (before, after) = (previous.refs.get(name), new_refs.get(name));
        if before != after {
            changed.extend(before.into_iter().chain(after).copied());
        }
    }
    for update in updates {
        match update {
            TableUpdate::SetStatistics { statistics } => {
                changed.insert(statistics.snapshot_id);
            }
            TableUpdate::SetPartitionStatistics {
                partition_statistics,
            } => {
                changed.insert(partition_statistics.snapshot_id);
            }
            TableUpdate::RemoveStatistics { snapshot_id }
            | TableUpdate::RemovePartitionStatistics { snapshot_id } => {
                changed.insert(*snapshot_id);
            }
            _ => {}
        }
    }
    writes.extend(removed.iter().map(|id| Write::DeleteSnapshot(*id)));
    writes.extend(
        changed
            .difference(&removed)
            .filter_map(|id| metadata.snapshot_by_id(*id))
            .map(|s| Write::Snapshot(snapshot_of(identity, metadata, s))),
    );

    writes.extend(fields);

    // Partition specs: added and removed by ID; the default flag of the previous and the new
    // default spec.
    let default_spec_id = metadata.default_partition_spec_id();
    let removed_specs: BTreeSet<i32> = diffs.removed_partition_specs.iter().copied().collect();
    let mut changed_specs: BTreeSet<i32> = diffs.added_partition_specs.iter().copied().collect();
    if previous.default_spec_id != default_spec_id {
        changed_specs.extend([previous.default_spec_id, default_spec_id]);
    }
    writes.extend(removed_specs.iter().map(|id| Write::DeleteSpec(*id)));
    writes.extend(
        changed_specs
            .difference(&removed_specs)
            .filter_map(|id| metadata.partition_spec_by_id(*id))
            .map(|s| Write::Spec(spec_of(identity, s, default_spec_id))),
    );
    Ok(writes)
}

fn field_writes(
    identity: &TableIdentity,
    previous: &IdxTable,
    metadata: &TableMetadata,
    diffs: &TableMetadataDiffs,
    previous_fields: Option<&BTreeMap<i32, IdxField>>,
) -> Result<Vec<Write>, NeedsPreviousFields> {
    let after = table_fields(
        identity,
        metadata,
        &schema_ids(metadata),
        metadata.current_schema_id(),
    );
    let computed;
    let before = if diffs.removed_schemas.is_empty() {
        // Without removals, the schemas before the commit are those after it but the added.
        let mut ids = schema_ids(metadata);
        for id in &diffs.added_schemas {
            ids.remove(id);
        }
        computed = table_fields(identity, metadata, &ids, previous.current_schema_id);
        &computed
    } else {
        previous_fields.ok_or(NeedsPreviousFields)?
    };
    Ok(field_differences(before, after))
}

/// The writes that turn the fields `before` into `after`.
fn field_differences(
    before: &BTreeMap<i32, IdxField>,
    after: BTreeMap<i32, IdxField>,
) -> Vec<Write> {
    let mut writes: Vec<Write> = before
        .keys()
        .filter(|id| !after.contains_key(id))
        .map(|id| Write::DeleteField(*id))
        .collect();
    writes.extend(
        after
            .into_iter()
            .filter(|(id, field)| before.get(id) != Some(field))
            .map(|(_, field)| Write::Field(field)),
    );
    writes
}

/// The view document of the projection of a view's `metadata`.
pub(crate) fn view_of(identity: &TableIdentity, metadata: &ViewMetadata) -> IdxView {
    let version = metadata.current_version();
    let mut dialects: Vec<String> = version
        .representations()
        .iter()
        .map(|r| match r {
            ViewRepresentation::Sql(sql) => fold_case(&sql.dialect),
        })
        .collect();
    dialects.sort();
    dialects.dedup();
    let field_ids: BTreeSet<i32> = metadata
        .schemas_iter()
        .flat_map(|s| s.field_id_to_fields().keys().copied().collect::<Vec<_>>())
        .collect();
    IdxView {
        view_id: identity.table_id,
        warehouse_id: identity.warehouse_id,
        namespace_id: identity.namespace_id,
        name: identity.name.clone(),
        active: identity.active,
        location: metadata.location().to_string(),
        current_version_id: version.version_id(),
        current_schema_id: version.schema_id(),
        last_updated_ms: version.timestamp_ms(),
        versions: i64::try_from(metadata.versions().len()).unwrap_or(i64::MAX),
        dialects,
        default_namespace: version.default_namespace().clone().inner(),
        documents: i64::try_from(field_ids.len()).unwrap_or(i64::MAX),
        pending: false,
    }
}

fn view_fields(identity: &TableIdentity, metadata: &ViewMetadata) -> BTreeMap<i32, IdxField> {
    fields_of(
        identity,
        "view",
        |id| metadata.schema_by_id(id).cloned(),
        &metadata.schemas_iter().map(|s| s.schema_id()).collect(),
        metadata.current_version().schema_id(),
    )
}

/// The projection of a view's `metadata`.
pub(crate) fn project_view(identity: &TableIdentity, metadata: &ViewMetadata) -> ViewProjection {
    ViewProjection {
        view: view_of(identity, metadata),
        fields: view_fields(identity, metadata),
    }
}

/// The writes that turn the projection of a view's `previous` metadata into that of
/// `metadata`. The core passes both to a view commit, so both field projections are computed.
pub(crate) fn view_changes(
    identity: &TableIdentity,
    previous: &ViewMetadata,
    metadata: &ViewMetadata,
) -> Vec<Write> {
    let mut writes = vec![Write::View(view_of(identity, metadata))];
    writes.extend(field_differences(
        &view_fields(identity, previous),
        view_fields(identity, metadata),
    ));
    writes
}

impl ViewProjection {
    /// The writes that store the whole projection.
    pub(crate) fn into_writes(self) -> Vec<Write> {
        let mut writes = vec![Write::View(self.view)];
        writes.extend(self.fields.into_values().map(Write::Field));
        writes
    }

    /// Apply `writes`, as storing them would.
    #[cfg(test)]
    pub(crate) fn apply(&mut self, writes: Vec<Write>) {
        for write in writes {
            match write {
                Write::View(v) => self.view = v,
                Write::Field(f) => {
                    self.fields.insert(f.field_id, f);
                }
                Write::DeleteField(id) => {
                    self.fields.remove(&id);
                }
                other => panic!("not a view write: {other:?}"),
            }
        }
    }
}

impl Projection {
    /// The writes that store the whole projection.
    pub(crate) fn into_writes(self) -> Vec<Write> {
        let mut writes = vec![Write::Table(self.table)];
        writes.extend(self.snapshots.into_values().map(Write::Snapshot));
        writes.extend(self.fields.into_values().map(Write::Field));
        writes.extend(self.specs.into_values().map(Write::Spec));
        writes
    }

    /// Apply `writes`, as storing them would.
    #[cfg(test)]
    pub(crate) fn apply(&mut self, writes: Vec<Write>) {
        for write in writes {
            match write {
                Write::Table(t) => self.table = t,
                Write::View(_) => panic!("a view write in a table projection"),
                Write::Snapshot(s) => {
                    self.snapshots.insert(s.snapshot_id, s);
                }
                Write::DeleteSnapshot(id) => {
                    self.snapshots.remove(&id);
                }
                Write::Field(f) => {
                    self.fields.insert(f.field_id, f);
                }
                Write::DeleteField(id) => {
                    self.fields.remove(&id);
                }
                Write::Spec(s) => {
                    self.specs.insert(s.spec_id, s);
                }
                Write::DeleteSpec(id) => {
                    self.specs.remove(&id);
                }
            }
        }
    }
}
