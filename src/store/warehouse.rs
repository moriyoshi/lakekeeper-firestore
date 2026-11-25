//! Warehouses.

use std::sync::Arc;

use lakekeeper::{
    ProjectId, SecretId, WarehouseId,
    api::management::v1::{
        DeleteWarehouseQuery,
        warehouse::{TabularDeleteProfile, WarehouseStatisticsResponse},
    },
    service::{
        AllowedFormatVersions, CatalogBackendError, CatalogCreateWarehouseError,
        CatalogCreateWarehouseRequest, CatalogDeleteWarehouseError, CatalogRenameWarehouseError,
        DeletedWarehouse, EnsureWarehouseSpecMutableError, ManagedBy, ProjectIdNotFoundError,
        RenamedWarehouse, ResolvedWarehouse, UpdatedWarehouseStorage, WarehouseAlreadyExists,
        WarehouseFormatVersionPolicy, WarehouseHasUnfinishedTasks, WarehouseIdAlreadyExists,
        WarehouseIdNotFound, WarehouseNotEmpty, WarehouseProtected, WarehouseSpecLocked,
        WarehouseStatus, WarehouseVersion, storage::StorageProfile,
    },
};

use super::{
    docs::{
        NAMESPACES, NameMarker, WAREHOUSE_NAMES, WAREHOUSES, WarehouseDoc, from_micros, now_micros,
        warehouse_name_id,
    },
    fs::{Query, Txn, backend_error, from_doc, string_value, to_doc},
};

pub(super) fn doc_name(txn: &Txn, warehouse_id: WarehouseId) -> String {
    txn.db().doc_name(WAREHOUSES, &warehouse_id.to_string())
}

fn marker_name(txn: &Txn, project_id: &ProjectId, name: &str) -> String {
    txn.db()
        .doc_name(WAREHOUSE_NAMES, &warehouse_name_id(project_id, name))
}

fn json<T: serde::Serialize>(value: &T) -> Result<String, CatalogBackendError> {
    serde_json::to_string(value).map_err(backend_error)
}

fn parse<T: serde::de::DeserializeOwned>(s: &str) -> Result<T, CatalogBackendError> {
    serde_json::from_str(s).map_err(backend_error)
}

fn status_str(status: WarehouseStatus) -> String {
    status.to_string()
}

pub(super) fn resolve(doc: &WarehouseDoc) -> Result<ResolvedWarehouse, CatalogBackendError> {
    let status = match doc.status.as_str() {
        "active" => WarehouseStatus::Active,
        "inactive" => WarehouseStatus::Inactive,
        other => {
            return Err(backend_error(std::io::Error::other(format!(
                "Unknown warehouse status `{other}`"
            ))));
        }
    };
    let allowed: Vec<lakekeeper::iceberg::spec::FormatVersion> =
        parse(&doc.allowed_format_versions)?;
    Ok(ResolvedWarehouse {
        warehouse_id: doc.warehouse_id,
        name: doc.name.clone(),
        project_id: Arc::new(doc.project_id.clone()),
        storage_profile: parse::<StorageProfile>(&doc.storage_profile)?,
        storage_secret_id: doc
            .storage_secret_id
            .as_deref()
            .map(|s| s.parse::<uuid::Uuid>().map(SecretId::from))
            .transpose()
            .map_err(backend_error)?,
        status,
        tabular_delete_profile: parse::<TabularDeleteProfile>(&doc.delete_profile)?,
        protected: doc.protected,
        managed_by: parse::<ManagedBy>(&doc.managed_by)?,
        allowed_format_versions: AllowedFormatVersions::try_new(allowed).unwrap_or_default(),
        default_format_version: parse(&doc.default_format_version)?,
        updated_at: doc.updated_at.map(from_micros),
        version: WarehouseVersion::new(doc.version),
    })
}

pub(super) async fn read(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
) -> Result<Option<WarehouseDoc>, CatalogBackendError> {
    let name = doc_name(txn, warehouse_id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<WarehouseDoc>(&d))
        .transpose()
}

/// The warehouse, if it exists and is active.
pub(super) async fn read_active(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
) -> Result<Option<WarehouseDoc>, CatalogBackendError> {
    Ok(read(txn, warehouse_id)
        .await?
        .filter(|w| w.status == status_str(WarehouseStatus::Active)))
}

fn write(txn: &mut Txn, doc: &WarehouseDoc) -> Result<(), CatalogBackendError> {
    let d = to_doc(doc_name(txn, doc.warehouse_id), doc)?;
    txn.set(d);
    Ok(())
}

pub(super) async fn create_warehouse(
    project_id: &ProjectId,
    request: CatalogCreateWarehouseRequest,
    txn: &mut Txn,
) -> Result<ResolvedWarehouse, CatalogCreateWarehouseError> {
    let CatalogCreateWarehouseRequest {
        warehouse_name,
        warehouse_id,
        storage_profile,
        storage_secret_id,
        delete_profile,
        format_version_policy:
            WarehouseFormatVersionPolicy {
                allowed_format_versions,
                default_format_version,
            },
        managed_by,
    } = request;
    let warehouse_id = warehouse_id.unwrap_or_else(WarehouseId::new_random);

    if !super::project::exists(txn, project_id).await? {
        return Err(ProjectIdNotFoundError::new(project_id.clone()).into());
    }
    let marker = marker_name(txn, project_id, &warehouse_name);
    if txn.get(&marker).await?.is_some() {
        return Err(WarehouseAlreadyExists::new(warehouse_name, project_id.clone()).into());
    }
    if read(txn, warehouse_id).await?.is_some() {
        return Err(WarehouseIdAlreadyExists::new(warehouse_id).into());
    }

    let doc = WarehouseDoc {
        warehouse_id,
        project_id: project_id.clone(),
        name: warehouse_name,
        storage_profile: json(&storage_profile)?,
        storage_secret_id: storage_secret_id.map(|s| s.to_string()),
        delete_profile: json(&delete_profile)?,
        status: status_str(WarehouseStatus::Active),
        protected: false,
        managed_by: json(&managed_by)?,
        allowed_format_versions: json(&allowed_format_versions.to_vec())?,
        default_format_version: json(&default_format_version)?,
        created_at: now_micros(),
        updated_at: None,
        version: 0,
    };
    txn.create(to_doc(
        marker,
        &NameMarker {
            id: warehouse_id.to_string(),
        },
    )?);
    txn.create(to_doc(doc_name(txn, warehouse_id), &doc)?);
    Ok(resolve(&doc)?)
}

pub(super) async fn delete_warehouse(
    warehouse_id: WarehouseId,
    DeleteWarehouseQuery { force }: DeleteWarehouseQuery,
    txn: &mut Txn,
) -> Result<DeletedWarehouse, CatalogDeleteWarehouseError> {
    let doc = read(txn, warehouse_id)
        .await?
        .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;
    if doc.protected && !force {
        return Err(WarehouseProtected::new().into());
    }
    let unfinished = super::tasks::active_task_counts(txn, warehouse_id).await?;
    if !unfinished.is_empty() {
        let tasks = unfinished
            .iter()
            .map(|(queue, n)| format!("{n} Tasks in queue '{queue}'"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(WarehouseHasUnfinishedTasks {
            stack: vec![format!("Unfinished tasks: {tasks}")],
        }
        .into());
    }
    let namespaces = txn
        .query(
            &Query::new(NAMESPACES)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .limit(Some(1)),
        )
        .await?;
    if !namespaces.is_empty() {
        return Err(WarehouseNotEmpty::new().into());
    }
    super::tag::delete_for_warehouse(txn, warehouse_id).await?;
    txn.delete(marker_name(txn, &doc.project_id, &doc.name));
    txn.delete(doc_name(txn, warehouse_id));
    Ok(DeletedWarehouse {
        project_id: doc.project_id,
        name: doc.name,
        storage_secret_id: doc
            .storage_secret_id
            .as_deref()
            .map(|s| s.parse::<uuid::Uuid>().map(SecretId::from))
            .transpose()
            .map_err(backend_error)?,
    })
}

pub(super) async fn rename_warehouse(
    warehouse_id: WarehouseId,
    new_name: &str,
    txn: &mut Txn,
) -> Result<RenamedWarehouse, CatalogRenameWarehouseError> {
    let mut doc = read_active(txn, warehouse_id)
        .await?
        .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;
    let previous_name = doc.name.clone();
    let old_marker = marker_name(txn, &doc.project_id, &previous_name);
    let new_marker = marker_name(txn, &doc.project_id, new_name);
    if old_marker != new_marker {
        if txn.get(&new_marker).await?.is_some() {
            return Err(backend_error(std::io::Error::other(format!(
                "A warehouse named '{new_name}' already exists in this project"
            )))
            .into());
        }
        txn.delete(old_marker);
        txn.create(to_doc(
            new_marker,
            &NameMarker {
                id: warehouse_id.to_string(),
            },
        )?);
    }
    doc.name = new_name.to_string();
    bump(&mut doc);
    write(txn, &doc)?;
    Ok(RenamedWarehouse {
        warehouse: resolve(&doc)?,
        previous_name,
    })
}

fn bump(doc: &mut WarehouseDoc) {
    doc.version += 1;
    doc.updated_at = Some(now_micros());
}

/// Read the warehouse, apply `change`, and write it with a new version.
pub(super) async fn update<E>(
    warehouse_id: WarehouseId,
    txn: &mut Txn,
    change: impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError>,
) -> Result<ResolvedWarehouse, E>
where
    E: From<WarehouseIdNotFound> + From<CatalogBackendError>,
{
    let mut doc = read(txn, warehouse_id)
        .await?
        .ok_or_else(|| WarehouseIdNotFound::new(warehouse_id))?;
    change(&mut doc)?;
    bump(&mut doc);
    write(txn, &doc)?;
    Ok(resolve(&doc)?)
}

pub(super) async fn update_storage_profile(
    warehouse_id: WarehouseId,
    storage_profile: StorageProfile,
    storage_secret_id: Option<SecretId>,
    txn: &mut Txn,
) -> Result<UpdatedWarehouseStorage, lakekeeper::service::UpdateWarehouseStorageProfileError> {
    let mut previous = None;
    let warehouse = update::<lakekeeper::service::UpdateWarehouseStorageProfileError>(
        warehouse_id,
        txn,
        |doc| {
            previous = doc.storage_secret_id.take();
            doc.storage_profile = json(&storage_profile)?;
            doc.storage_secret_id = storage_secret_id.map(|s| s.to_string());
            Ok(())
        },
    )
    .await?;
    Ok(UpdatedWarehouseStorage {
        warehouse,
        previous_secret_id: previous
            .as_deref()
            .map(|s| s.parse::<uuid::Uuid>().map(SecretId::from))
            .transpose()
            .map_err(backend_error)?,
    })
}

pub(super) fn set_delete_profile(
    profile: &TabularDeleteProfile,
) -> impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError> + '_ {
    move |doc| {
        doc.delete_profile = json(profile)?;
        Ok(())
    }
}

pub(super) fn set_status(
    status: WarehouseStatus,
) -> impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError> {
    move |doc| {
        doc.status = status_str(status);
        Ok(())
    }
}

pub(super) fn set_protected(
    protected: bool,
) -> impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError> {
    move |doc| {
        doc.protected = protected;
        Ok(())
    }
}

pub(super) fn set_format_version_policy(
    policy: &WarehouseFormatVersionPolicy,
) -> impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError> + '_ {
    move |doc| {
        doc.allowed_format_versions = json(&policy.allowed_format_versions.to_vec())?;
        doc.default_format_version = json(&policy.default_format_version)?;
        Ok(())
    }
}

pub(super) fn set_managed_by(
    managed_by: ManagedBy,
) -> impl FnOnce(&mut WarehouseDoc) -> Result<(), CatalogBackendError> {
    move |doc| {
        doc.managed_by = json(&managed_by)?;
        Ok(())
    }
}

pub(super) async fn list_warehouses(
    project_id: &ProjectId,
    status_filter: Option<Vec<WarehouseStatus>>,
    txn: &mut Txn,
) -> Result<Vec<ResolvedWarehouse>, CatalogBackendError> {
    let statuses = status_filter.unwrap_or_else(|| vec![WarehouseStatus::Active]);
    let docs = txn
        .query(&Query::new(WAREHOUSES).eq("project_id", string_value(project_id.as_str())))
        .await?;
    let mut warehouses = Vec::with_capacity(docs.len());
    for d in &docs {
        let w = resolve(&from_doc::<WarehouseDoc>(d)?)?;
        if statuses.contains(&w.status) {
            warehouses.push(w);
        }
    }
    warehouses.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(warehouses)
}

pub(super) async fn get_warehouse_by_name(
    warehouse_name: &str,
    project_id: &ProjectId,
    txn: &mut Txn,
) -> Result<Option<ResolvedWarehouse>, CatalogBackendError> {
    let marker = marker_name(txn, project_id, warehouse_name);
    let Some(marker) = txn.get(&marker).await? else {
        return Ok(None);
    };
    let marker: NameMarker = from_doc(&marker)?;
    let warehouse_id = WarehouseId::from(marker.id.parse::<uuid::Uuid>().map_err(backend_error)?);
    read(txn, warehouse_id)
        .await?
        .map(|d| resolve(&d))
        .transpose()
}

pub(super) async fn ensure_spec_mutable(
    warehouse_id: WarehouseId,
    bypass: bool,
    txn: &mut Txn,
) -> Result<(), EnsureWarehouseSpecMutableError> {
    if bypass {
        return Ok(());
    }
    if let Some(doc) = read(txn, warehouse_id).await? {
        let managed_by: ManagedBy = parse(&doc.managed_by)?;
        if managed_by.is_externally_managed() {
            return Err(WarehouseSpecLocked::new(managed_by).into());
        }
    }
    Ok(())
}

pub(super) fn empty_stats(warehouse_id: WarehouseId) -> WarehouseStatisticsResponse {
    // Statistics are not collected yet.
    WarehouseStatisticsResponse {
        warehouse_ident: *warehouse_id,
        stats: Vec::new(),
        next_page_token: None,
    }
}
