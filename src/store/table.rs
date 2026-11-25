//! Tables: create, load and commit.
//!
//! A committed table keeps only its metadata location; loading reads the metadata file (see
//! [`super::files`]). A staged table has no file until its first commit, so its initial metadata
//! is stored as gzipped JSON in `table_metadata` until then.

use std::{io::Write as _, str::FromStr};

use lakekeeper::{
    WarehouseId,
    api::iceberg::v1::tables::{LoadTableFilters, SnapshotsQuery},
    iceberg::spec::TableMetadata,
    service::{
        CatalogBackendError, CommitTableTransactionError, ConcurrentUpdateError, CreateTableError,
        InternalParseLocationError, LoadTableError, LoadTableResponse, Location, StagedTableId,
        TableCommit, TableCreation, TableId, TableInfo, TabularAlreadyExists, TabularId,
        TabularNotFound, WarehouseVersion,
    },
};

use super::{
    docs::{StagedMetadataDoc, TABLE_METADATA, TabularDoc, location_prefixes, now_micros},
    files::{self, MetadataFiles},
    fs::{Txn, backend_error, from_doc, to_doc},
    projection_store,
    tabular::{self, Context, TABLE},
};

tabular::impl_from_info_error!(
    CreateTableError,
    LoadTableError,
    CommitTableTransactionError
);

fn staged_name(txn: &Txn, id: uuid::Uuid) -> String {
    txn.db().doc_name(TABLE_METADATA, &id.to_string())
}

fn write_staged(
    txn: &mut Txn,
    id: uuid::Uuid,
    metadata: &TableMetadata,
) -> Result<(), CatalogBackendError> {
    let json = serde_json::to_vec(metadata).map_err(backend_error)?;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&json).map_err(backend_error)?;
    let data = encoder.finish().map_err(backend_error)?;
    txn.set(to_doc(staged_name(txn, id), &StagedMetadataDoc { data })?);
    Ok(())
}

async fn read_staged(
    txn: &mut Txn,
    id: uuid::Uuid,
) -> Result<Option<TableMetadata>, CatalogBackendError> {
    let name = staged_name(txn, id);
    let Some(doc) = txn.get(&name).await? else {
        return Ok(None);
    };
    let doc: StagedMetadataDoc = from_doc(&doc)?;
    serde_json::from_reader(flate2::read::GzDecoder::new(doc.data.as_slice()))
        .map(Some)
        .map_err(backend_error)
}

/// Delete the stored metadata of `t` if it is a staged table.
pub(super) fn delete_staged(txn: &mut Txn, t: &TabularDoc) {
    if t.typ == TABLE && t.metadata_location.is_none() {
        txn.delete(staged_name(txn, t.tabular_id));
    }
}

fn location_of(metadata: &TableMetadata) -> Result<Location, InternalParseLocationError> {
    Ok(Location::from_str(metadata.location())?)
}

pub(super) async fn create(
    TableCreation {
        warehouse_id,
        namespace_id,
        table_ident,
        metadata_location,
        table_metadata,
    }: TableCreation<'_>,
    txn: &mut Txn,
) -> Result<(TableInfo, Option<StagedTableId>), CreateTableError> {
    let location = location_of(table_metadata)?;
    let id = table_metadata.uuid();

    // A staged table of the same name is replaced.
    let mut staged = None;
    if let Some(existing) =
        tabular::read_by_name(txn, warehouse_id, namespace_id, &table_ident.name).await?
    {
        if existing.typ == TABLE && existing.metadata_location.is_none() {
            tabular::delete_docs(txn, &existing);
            staged = Some(StagedTableId(TableId::from(existing.tabular_id)));
        } else {
            return Err(TabularAlreadyExists::new().into());
        }
    }
    if tabular::read(txn, id).await?.is_some() {
        return Err(TabularAlreadyExists::new().into());
    }
    if super::namespace::read(txn, namespace_id)
        .await?
        .as_ref()
        .is_none_or(|n| n.warehouse_id != warehouse_id)
    {
        return Err(backend_error(std::io::Error::other(format!(
            "Namespace {namespace_id} does not exist"
        )))
        .into());
    }
    tabular::ensure_location_available(txn, warehouse_id, id, &location).await?;

    let now = now_micros();
    let doc = TabularDoc {
        tabular_id: id,
        typ: TABLE.to_string(),
        warehouse_id,
        namespace_id,
        name: table_ident.name.clone(),
        metadata_location: metadata_location.map(|l| l.as_str().to_string()),
        location: location.as_str().trim_end_matches('/').to_string(),
        location_prefixes: location_prefixes(location.as_str()),
        protected: false,
        properties: table_metadata.properties().clone(),
        created_at: now,
        updated_at: None,
        deleted_at: None,
        active: true,
        generic: None,
    };
    tabular::write_docs(txn, &doc, true)?;
    if doc.metadata_location.is_none() {
        write_staged(txn, id, table_metadata)?;
    } else {
        projection_store::created(txn, &doc, table_metadata).await?;
    }
    let ctx = Context::load(txn, std::slice::from_ref(&doc)).await?;
    Ok((ctx.table(&doc)?, staged))
}

/// `metadata` with only the snapshots a branch or tag points to, as upstream's `snapshots=refs`
/// load returns it. The snapshot log stays whole, as upstream's does.
pub(super) fn only_referenced_snapshots(
    metadata: TableMetadata,
) -> Result<TableMetadata, CatalogBackendError> {
    let referenced: std::collections::HashSet<i64> =
        metadata.refs().values().map(|r| r.snapshot_id).collect();
    if metadata
        .snapshots()
        .all(|s| referenced.contains(&s.snapshot_id()))
    {
        return Ok(metadata);
    }
    // The metadata's fields are private: filter its JSON form.
    let mut json = serde_json::to_value(&metadata).map_err(backend_error)?;
    if let Some(snapshots) = json.get_mut("snapshots").and_then(|s| s.as_array_mut()) {
        snapshots.retain(|s| {
            s.get("snapshot-id")
                .and_then(serde_json::Value::as_i64)
                .is_some_and(|id| referenced.contains(&id))
        });
    }
    serde_json::from_value(json).map_err(backend_error)
}

pub(super) async fn load(
    warehouse_id: WarehouseId,
    tables: impl IntoIterator<Item = TableId> + Send,
    include_deleted: bool,
    filters: &LoadTableFilters,
    txn: &mut Txn,
    files: &dyn MetadataFiles,
) -> Result<Vec<LoadTableResponse>, LoadTableError> {
    let ids: Vec<TableId> = tables.into_iter().collect();
    let names: Vec<String> = ids.iter().map(|id| tabular::doc_name(txn, **id)).collect();
    let mut docs = Vec::new();
    for d in txn.get_many(&names).await?.into_iter().flatten() {
        let t: TabularDoc = from_doc(&d)?;
        if t.warehouse_id == warehouse_id
            && t.typ == TABLE
            && (include_deleted || t.deleted_at.is_none())
        {
            docs.push(t);
        }
    }
    if docs.is_empty() {
        return Ok(Vec::new());
    }
    let warehouse = super::warehouse::read(txn, warehouse_id)
        .await?
        .ok_or_else(|| {
            backend_error(std::io::Error::other(format!(
                "Warehouse {warehouse_id} is missing"
            )))
        })?;
    let mut out = Vec::with_capacity(docs.len());
    for t in docs {
        let metadata_location = t
            .metadata_location
            .as_deref()
            .map(Location::from_str)
            .transpose()
            .map_err(InternalParseLocationError::from)?;
        let table_metadata = if let Some(location) = &metadata_location {
            files::read_metadata(files, &warehouse, location).await?
        } else {
            read_staged(txn, t.tabular_id).await?.ok_or_else(|| {
                backend_error(std::io::Error::other(format!(
                    "Metadata of staged table {} is missing",
                    t.tabular_id
                )))
            })?
        };
        let table_metadata = match filters.snapshots {
            SnapshotsQuery::All => table_metadata,
            SnapshotsQuery::Refs => only_referenced_snapshots(table_metadata)?,
        };
        out.push(LoadTableResponse {
            table_id: TableId::from(t.tabular_id),
            namespace_id: t.namespace_id,
            table_metadata,
            metadata_location,
            warehouse_version: WarehouseVersion::new(warehouse.version),
        });
    }
    Ok(out)
}

pub(super) async fn commit(
    warehouse_id: WarehouseId,
    commits: impl IntoIterator<Item = TableCommit> + Send,
    txn: &mut Txn,
) -> Result<Vec<TableInfo>, CommitTableTransactionError> {
    let commits: Vec<TableCommit> = commits.into_iter().collect();
    let mut docs = Vec::new();
    for commit in commits {
        let id = commit.new_metadata.uuid();
        let tabular_id = TabularId::Table(TableId::from(id));
        let mut t = tabular::read(txn, id)
            .await?
            .filter(|t| t.warehouse_id == warehouse_id && t.typ == TABLE && t.deleted_at.is_none())
            .ok_or_else(|| TabularNotFound::new(warehouse_id, tabular_id))?;
        let previous = commit
            .previous_metadata_location
            .as_ref()
            .map(|l| l.as_str().to_string());
        if t.metadata_location != previous {
            return Err(ConcurrentUpdateError::new(warehouse_id, tabular_id).into());
        }
        let new_location = location_of(&commit.new_metadata)?;
        tabular::move_location(txn, &mut t, &new_location)
            .await
            .map_err(|e| match e {
                lakekeeper::service::CreateTabularError::CatalogBackendError(e) => {
                    CommitTableTransactionError::from(e)
                }
                other => CommitTableTransactionError::from(backend_error(std::io::Error::other(
                    other.to_string(),
                ))),
            })?;
        // The first commit of a staged table: from now on its metadata is in its file.
        delete_staged(txn, &t);
        t.metadata_location = Some(commit.new_metadata_location.as_str().to_string());
        t.properties = commit.new_metadata.properties().clone();
        t.updated_at = Some(now_micros());
        tabular::write_docs(txn, &t, false)?;
        projection_store::committed(txn, &t, &commit).await?;
        docs.push(t);
    }
    let ctx = Context::load(txn, &docs).await?;
    Ok(docs
        .iter()
        .map(|t| ctx.table(t))
        .collect::<Result<_, _>>()?)
}
