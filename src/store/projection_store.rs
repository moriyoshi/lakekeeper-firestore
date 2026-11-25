//! Storing the searchable projection of tables and views ([`super::projection`]): written in
//! the transaction that changes the table or view, so that it always matches the current
//! metadata. Its head document (`idx_tables` or `idx_views`) names the table or view; the
//! part documents hang off it by ID.
//!
//! A table has a projection once it has a metadata file: from its creation, or from the first
//! commit of a staged table. A commit to a table or view without one stores the whole
//! projection, which also covers those created before the projection existed.
//!
//! A change that would write more than [`INLINE_WRITES`] documents, or a drop that would delete
//! more, is left to the `metadata_projection` task ([`super::projection_task`]): the head
//! document is marked `pending`, and commits only rewrite it until the task has reconciled the
//! rest.

use std::collections::BTreeMap;

use lakekeeper::{
    iceberg::spec::{TableMetadata, ViewMetadata},
    service::{CatalogBackendError, TableCommit},
};

use super::{
    docs::{IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, IDX_TABLES, IDX_VIEWS, TabularDoc},
    fs::{Query, Txn, from_doc, string_value, to_doc},
    projection::{
        IdxField, IdxTable, IdxView, NeedsPreviousFields, TableIdentity, Write, commit_changes,
        project, project_view, table_of, view_changes, view_of,
    },
    projection_task,
    tabular::{GENERIC_TABLE, TABLE},
};

/// The most projection documents a transaction writes or deletes; more is left to a task.
const INLINE_WRITES: usize = 400;

/// Tests lower the limit to 16, so that they reach the task; a cost measurement raises it back
/// by setting this to `usize::MAX`.
#[cfg(test)]
pub(super) static INLINE_WRITES_IN_TESTS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(16);

fn inline_writes() -> usize {
    #[cfg(test)]
    return INLINE_WRITES_IN_TESTS
        .load(std::sync::atomic::Ordering::Relaxed)
        .min(INLINE_WRITES);
    #[cfg(not(test))]
    INLINE_WRITES
}

pub(super) fn identity(t: &TabularDoc) -> TableIdentity {
    TableIdentity {
        warehouse_id: t.warehouse_id,
        namespace_id: t.namespace_id,
        table_id: t.tabular_id,
        name: t.name.clone(),
        active: t.deleted_at.is_none(),
    }
}

fn table_name(txn: &Txn, table_id: uuid::Uuid) -> String {
    txn.db().doc_name(IDX_TABLES, &table_id.to_string())
}

fn view_name(txn: &Txn, view_id: uuid::Uuid) -> String {
    txn.db().doc_name(IDX_VIEWS, &view_id.to_string())
}

/// The head document of a tabular of type `typ`.
fn head_name(txn: &Txn, typ: &str, id: uuid::Uuid) -> String {
    if typ == TABLE {
        table_name(txn, id)
    } else {
        view_name(txn, id)
    }
}

/// The name of a document of `collection` keyed by table and `key`.
fn part_name(
    txn: &Txn,
    collection: &str,
    table_id: uuid::Uuid,
    key: impl std::fmt::Display,
) -> String {
    txn.db().doc_name(collection, &format!("{table_id}:{key}"))
}

pub(super) fn write(
    txn: &mut Txn,
    table_id: uuid::Uuid,
    w: &Write,
) -> Result<(), CatalogBackendError> {
    let doc = match w {
        Write::Table(t) => to_doc(table_name(txn, table_id), t)?,
        Write::View(v) => to_doc(view_name(txn, table_id), v)?,
        Write::Snapshot(s) => to_doc(part_name(txn, IDX_SNAPSHOTS, table_id, s.snapshot_id), s)?,
        Write::Field(f) => to_doc(part_name(txn, IDX_FIELDS, table_id, f.field_id), f)?,
        Write::Spec(s) => to_doc(part_name(txn, IDX_PARTITION_SPECS, table_id, s.spec_id), s)?,
        Write::DeleteSnapshot(id) => {
            txn.delete(part_name(txn, IDX_SNAPSHOTS, table_id, id));
            return Ok(());
        }
        Write::DeleteField(id) => {
            txn.delete(part_name(txn, IDX_FIELDS, table_id, id));
            return Ok(());
        }
        Write::DeleteSpec(id) => {
            txn.delete(part_name(txn, IDX_PARTITION_SPECS, table_id, id));
            return Ok(());
        }
    };
    txn.set(doc);
    Ok(())
}

fn write_all(
    txn: &mut Txn,
    table_id: uuid::Uuid,
    writes: impl IntoIterator<Item = Write>,
) -> Result<(), CatalogBackendError> {
    for w in writes {
        write(txn, table_id, &w)?;
    }
    Ok(())
}

async fn read_table(
    txn: &mut Txn,
    table_id: uuid::Uuid,
) -> Result<Option<IdxTable>, CatalogBackendError> {
    let name = table_name(txn, table_id);
    txn.get(&name).await?.as_ref().map(from_doc).transpose()
}

async fn read_view(
    txn: &mut Txn,
    view_id: uuid::Uuid,
) -> Result<Option<IdxView>, CatalogBackendError> {
    let name = view_name(txn, view_id);
    txn.get(&name).await?.as_ref().map(from_doc).transpose()
}

/// The documents of `collection` that belong to `table_id`.
pub(super) async fn parts(
    txn: &mut Txn,
    collection: &'static str,
    table_id: uuid::Uuid,
) -> Result<Vec<super::fs::Document>, CatalogBackendError> {
    txn.query(&Query::new(collection).eq("table_id", string_value(table_id.to_string())))
        .await
}

/// Delete the head document of a tabular of type `typ`.
pub(super) fn delete_head(txn: &mut Txn, typ: &str, id: uuid::Uuid) {
    txn.delete(head_name(txn, typ, id));
}

/// Store the projection of a table created with `metadata`.
pub(super) async fn created(
    txn: &mut Txn,
    t: &TabularDoc,
    metadata: &TableMetadata,
) -> Result<(), CatalogBackendError> {
    let writes = project(&identity(t), metadata).into_writes();
    if writes.len() > inline_writes() {
        return pend(txn, t, metadata).await;
    }
    write_all(txn, t.tabular_id, writes)
}

/// Write the table document of `metadata` as pending, and leave the rest to a task.
async fn pend(
    txn: &mut Txn,
    t: &TabularDoc,
    metadata: &TableMetadata,
) -> Result<(), CatalogBackendError> {
    let mut table = table_of(&identity(t), metadata);
    table.pending = true;
    write(txn, t.tabular_id, &Write::Table(table))?;
    projection_task::enqueue(txn, t).await
}

/// Update the projection of `t` for `commit`, with the writes [`commit_changes`] finds.
pub(super) async fn committed(
    txn: &mut Txn,
    t: &TabularDoc,
    commit: &TableCommit,
) -> Result<(), CatalogBackendError> {
    let identity = identity(t);
    let Some(previous) = read_table(txn, t.tabular_id).await? else {
        return Box::pin(created(txn, t, &commit.new_metadata)).await;
    };
    if previous.pending {
        // The other documents may not match `previous`; the task finds this commit.
        return pend(txn, t, &commit.new_metadata).await;
    }
    let changes = |fields| {
        commit_changes(
            &identity,
            &previous,
            &commit.new_metadata,
            &commit.diffs,
            &commit.updates,
            fields,
        )
    };
    // Only a commit that removed schemas needs the stored fields.
    let writes = if let Ok(writes) = changes(None) {
        writes
    } else {
        let mut fields = BTreeMap::new();
        for doc in parts(txn, IDX_FIELDS, t.tabular_id).await? {
            let field: IdxField = from_doc(&doc)?;
            fields.insert(field.field_id, field);
        }
        changes(Some(&fields)).map_err(|NeedsPreviousFields| {
            CatalogBackendError::new_unexpected(std::io::Error::other(
                "The projection asked for the stored fields although it was given them",
            ))
        })?
    };
    if writes.len() > inline_writes() {
        return pend(txn, t, &commit.new_metadata).await;
    }
    write_all(txn, t.tabular_id, writes)
}

/// Store the projection of a view created with `metadata`.
pub(super) async fn view_created(
    txn: &mut Txn,
    t: &TabularDoc,
    metadata: &ViewMetadata,
) -> Result<(), CatalogBackendError> {
    let writes = project_view(&identity(t), metadata).into_writes();
    if writes.len() > inline_writes() {
        return pend_view(txn, t, metadata).await;
    }
    write_all(txn, t.tabular_id, writes)
}

async fn pend_view(
    txn: &mut Txn,
    t: &TabularDoc,
    metadata: &ViewMetadata,
) -> Result<(), CatalogBackendError> {
    let mut view = view_of(&identity(t), metadata);
    view.pending = true;
    write(txn, t.tabular_id, &Write::View(view))?;
    projection_task::enqueue(txn, t).await
}

/// Update the projection of view `t` for its replacement of `previous` by `metadata`.
pub(super) async fn view_committed(
    txn: &mut Txn,
    t: &TabularDoc,
    previous: &ViewMetadata,
    metadata: &ViewMetadata,
) -> Result<(), CatalogBackendError> {
    let Some(stored) = read_view(txn, t.tabular_id).await? else {
        return Box::pin(view_created(txn, t, metadata)).await;
    };
    let writes = view_changes(&identity(t), previous, metadata);
    if stored.pending || writes.len() > inline_writes() {
        return pend_view(txn, t, metadata).await;
    }
    write_all(txn, t.tabular_id, writes)
}

/// Bring the head document of the projection in line with the name, namespace and
/// soft-deletion of `t`, after a rename, soft deletion or undrop.
pub(super) async fn identity_changed(
    txn: &mut Txn,
    t: &TabularDoc,
) -> Result<(), CatalogBackendError> {
    if t.typ == GENERIC_TABLE {
        return Ok(());
    }
    let identity = identity(t);
    if t.typ == TABLE {
        let Some(mut table) = read_table(txn, t.tabular_id).await? else {
            return Ok(());
        };
        table.namespace_id = identity.namespace_id;
        table.name = identity.name;
        table.active = identity.active;
        write(txn, t.tabular_id, &Write::Table(table))
    } else {
        let Some(mut view) = read_view(txn, t.tabular_id).await? else {
            return Ok(());
        };
        view.namespace_id = identity.namespace_id;
        view.name = identity.name;
        view.active = identity.active;
        write(txn, t.tabular_id, &Write::View(view))
    }
}

/// Delete the projection of `t`, which is being dropped.
pub(super) async fn dropped(txn: &mut Txn, t: &TabularDoc) -> Result<(), CatalogBackendError> {
    if t.typ == GENERIC_TABLE {
        return Ok(());
    }
    let id = t.tabular_id;
    let (pending, documents, collections): (bool, i64, &[&'static str]) = if t.typ == TABLE {
        let Some(table) = read_table(txn, id).await? else {
            return Ok(());
        };
        (
            table.pending,
            table.documents,
            &[IDX_SNAPSHOTS, IDX_FIELDS, IDX_PARTITION_SPECS],
        )
    } else {
        let Some(view) = read_view(txn, id).await? else {
            return Ok(());
        };
        (view.pending, view.documents, &[IDX_FIELDS])
    };
    delete_head(txn, &t.typ, id);
    if pending || usize::try_from(documents).unwrap_or(usize::MAX) >= inline_writes() {
        // The task finds the tabular gone and deletes the rest.
        return projection_task::enqueue(txn, t).await;
    }
    for collection in collections {
        for doc in parts(txn, collection, id).await? {
            txn.delete(doc.name);
        }
    }
    Ok(())
}
