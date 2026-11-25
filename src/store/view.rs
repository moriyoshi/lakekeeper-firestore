//! Views: create, load and commit.
//!
//! A view is a tabular document of type `view` that keeps its metadata location; loading reads
//! the metadata file (see [`super::files`]). Upstream's Postgres store normalizes view versions and keeps a
//! version's default namespace as a namespace ID, so renaming that namespace renames it in the
//! view; here the metadata keeps the name it was committed with.

use std::{str::FromStr, sync::Arc};

use lakekeeper::{
    WarehouseId,
    iceberg::{TableIdent, spec::ViewMetadata},
    service::{
        CatalogView, CommitViewError, ConcurrentUpdateError, CreateViewError,
        InternalParseLocationError, LoadViewError, Location, NamespaceId, TabularAlreadyExists,
        TabularId, TabularNotFound, ViewCommit, ViewId, ViewInfo, WarehouseVersion,
    },
};

use super::{
    docs::{TabularDoc, location_prefixes, now_micros},
    files::{self, MetadataFiles},
    fs::{Txn, backend_error},
    projection_store,
    tabular::{self, Context, VIEW},
};

tabular::impl_from_info_error!(CreateViewError, LoadViewError, CommitViewError);

fn location_of(metadata: &ViewMetadata) -> Result<Location, InternalParseLocationError> {
    Ok(Location::from_str(metadata.location())?)
}

pub(super) async fn create(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    view_ident: &TableIdent,
    metadata: &ViewMetadata,
    metadata_location: &Location,
    txn: &mut Txn,
) -> Result<ViewInfo, CreateViewError> {
    let location = location_of(metadata)?;
    let id = metadata.uuid();
    if tabular::read_by_name(txn, warehouse_id, namespace_id, &view_ident.name)
        .await?
        .is_some()
        || tabular::read(txn, id).await?.is_some()
    {
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

    let doc = TabularDoc {
        tabular_id: id,
        typ: VIEW.to_string(),
        warehouse_id,
        namespace_id,
        name: view_ident.name.clone(),
        metadata_location: Some(metadata_location.as_str().to_string()),
        location: location.as_str().trim_end_matches('/').to_string(),
        location_prefixes: location_prefixes(location.as_str()),
        protected: false,
        properties: metadata.properties().clone(),
        created_at: now_micros(),
        updated_at: None,
        deleted_at: None,
        active: true,
    };
    tabular::write_docs(txn, &doc, true)?;
    projection_store::view_created(txn, &doc, metadata).await?;
    let ctx = Context::load(txn, std::slice::from_ref(&doc)).await?;
    Ok(ctx.info(&doc, ViewId::from(id))?)
}

pub(super) async fn load(
    warehouse_id: WarehouseId,
    view_id: ViewId,
    include_deleted: bool,
    txn: &mut Txn,
    files: &dyn MetadataFiles,
) -> Result<CatalogView, LoadViewError> {
    let not_found = || TabularNotFound::new(warehouse_id, TabularId::View(view_id));
    let t = tabular::read(txn, *view_id)
        .await?
        .filter(|t| {
            t.warehouse_id == warehouse_id
                && t.typ == VIEW
                && (include_deleted || t.deleted_at.is_none())
        })
        .ok_or_else(not_found)?;
    let metadata_location = t
        .metadata_location
        .as_deref()
        .ok_or_else(|| {
            backend_error(std::io::Error::other(format!(
                "View {view_id} has no metadata location"
            )))
        })
        .map(Location::from_str)?
        .map_err(InternalParseLocationError::from)?;
    let warehouse = super::warehouse::read(txn, warehouse_id)
        .await?
        .ok_or_else(|| {
            backend_error(std::io::Error::other(format!(
                "Warehouse {warehouse_id} is missing"
            )))
        })?;
    let metadata: ViewMetadata =
        files::read_metadata(files, &warehouse, &metadata_location).await?;
    let warehouse_version = warehouse.version;
    Ok(CatalogView {
        metadata_location,
        location: location_of(&metadata)?,
        metadata: Arc::new(metadata),
        warehouse_version: WarehouseVersion::new(warehouse_version),
    })
}

/// Replace the metadata of a view, if its stored metadata location is still the one the commit
/// was made against.
pub(super) async fn commit(
    ViewCommit {
        namespace_id,
        warehouse_id,
        previous_view,
        new_view,
    }: ViewCommit<'_>,
    txn: &mut Txn,
) -> Result<ViewInfo, CommitViewError> {
    let metadata = &new_view.metadata;
    let id = metadata.uuid();
    let view_id = ViewId::from(id);
    let mut t = tabular::read(txn, id)
        .await?
        .filter(|t| {
            t.warehouse_id == warehouse_id
                && t.typ == VIEW
                && t.deleted_at.is_none()
                && t.namespace_id == namespace_id
        })
        .ok_or_else(|| TabularNotFound::new(warehouse_id, TabularId::View(view_id)))?;
    if t.metadata_location.as_deref() != Some(previous_view.metadata_location.as_str()) {
        return Err(ConcurrentUpdateError::new(warehouse_id, TabularId::View(view_id)).into());
    }
    tabular::move_location(txn, &mut t, &location_of(metadata)?).await?;
    t.metadata_location = Some(new_view.metadata_location.as_str().to_string());
    t.properties = metadata.properties().clone();
    t.updated_at = Some(now_micros());
    tabular::write_docs(txn, &t, false)?;
    projection_store::view_committed(txn, &t, &previous_view.metadata, metadata).await?;
    let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
    Ok(ctx.info(&t, view_id)?)
}
