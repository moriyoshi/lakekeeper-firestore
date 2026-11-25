//! Tabulars (tables, views and generic tables): shared lookups, listing, protection and
//! deletion.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use lakekeeper::{
    WarehouseId,
    api::{iceberg::v1::PaginationQuery, management::v1::TabularType},
    service::{
        CatalogBackendError, CatalogSearchTabularInfo, CatalogSearchTabularResponse,
        ClearTabularDeletedAtError, DropTabularError, ExpirationTaskInfo,
        GetTabularInfoByLocationError, GetTabularInfoError, InternalParseLocationError,
        InvalidNamespaceIdentifier, InvalidPaginationToken, ListTabularsError, Location,
        MarkTabularAsDeletedError, NamespaceId, NamespaceIdent, NamespaceVersion,
        ProtectedTabularDeletionWithoutForce, RenameTabularError, SearchTabularError,
        SetTabularProtectionError, TableIdent, TableInfo, TabularAlreadyExists,
        TabularDeletionInfo, TabularId, TabularIdentBorrowed, TabularInfo, TabularListFlags,
        TabularNotFound, ViewOrTableDeletionInfo, ViewOrTableInfo, WarehouseVersion, tasks::TaskId,
    },
};

use super::{
    docs::{
        NameMarker, NamespaceDoc, TABULAR_LOCATIONS, TABULAR_NAMES, TABULARS, TabularDoc,
        WarehouseDoc, from_micros, location_prefixes, now_micros, tabular_location_id,
        tabular_name_id,
    },
    fs::{
        Query, Txn, backend_error, bool_value, fold_case, from_doc, int_value, string_value, to_doc,
    },
    namespace, projection_store, warehouse,
};

pub(super) const TABLE: &str = "table";
pub(super) const VIEW: &str = "view";
pub(super) const GENERIC_TABLE: &str = "generic-table";

pub(super) fn doc_name(txn: &Txn, id: uuid::Uuid) -> String {
    txn.db().doc_name(TABULARS, &id.to_string())
}

pub(super) fn name_marker(txn: &Txn, t: &TabularDoc) -> String {
    txn.db().doc_name(
        TABULAR_NAMES,
        &tabular_name_id(t.warehouse_id, t.namespace_id, &t.name),
    )
}

pub(super) fn location_marker(txn: &Txn, warehouse_id: WarehouseId, location: &str) -> String {
    txn.db().doc_name(
        TABULAR_LOCATIONS,
        &tabular_location_id(warehouse_id, location),
    )
}

pub(super) fn tabular_id(t: &TabularDoc) -> Result<TabularId, CatalogBackendError> {
    match t.typ.as_str() {
        TABLE => Ok(TabularId::Table(t.tabular_id.into())),
        VIEW => Ok(TabularId::View(t.tabular_id.into())),
        GENERIC_TABLE => Ok(TabularId::GenericTable(t.tabular_id.into())),
        other => Err(backend_error(std::io::Error::other(format!(
            "Unknown tabular type `{other}`"
        )))),
    }
}

fn matches_flags(t: &TabularDoc, flags: TabularListFlags) -> bool {
    if t.deleted_at.is_some() {
        flags.include_deleted
    } else if t.typ == TABLE && t.metadata_location.is_none() {
        flags.include_staged
    } else {
        flags.include_active
    }
}

pub(super) async fn read(
    txn: &mut Txn,
    id: uuid::Uuid,
) -> Result<Option<TabularDoc>, CatalogBackendError> {
    let name = doc_name(txn, id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<TabularDoc>(&d))
        .transpose()
}

pub(super) async fn read_in_namespace(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
) -> Result<Vec<TabularDoc>, CatalogBackendError> {
    txn.query(
        &Query::new(TABULARS)
            .eq("warehouse_id", string_value(warehouse_id.to_string()))
            .eq("namespace_id", string_value(namespace_id.to_string())),
    )
    .await?
    .iter()
    .map(from_doc)
    .collect()
}

/// The active tabular named `name` in the namespace, if any.
pub(super) async fn read_by_name(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    name: &str,
) -> Result<Option<TabularDoc>, CatalogBackendError> {
    let marker = txn.db().doc_name(
        TABULAR_NAMES,
        &tabular_name_id(warehouse_id, namespace_id, name),
    );
    let Some(marker) = txn.get(&marker).await? else {
        return Ok(None);
    };
    let marker: NameMarker = from_doc(&marker)?;
    read(txn, marker.id.parse().map_err(backend_error)?).await
}

/// Write the tabular document with its name and location markers. `is_new` makes the commit
/// fail if a marker already exists.
pub(super) fn write_docs(
    txn: &mut Txn,
    t: &TabularDoc,
    is_new: bool,
) -> Result<(), CatalogBackendError> {
    let marker = NameMarker {
        id: t.tabular_id.to_string(),
    };
    let name = to_doc(name_marker(txn, t), &marker)?;
    let location = to_doc(location_marker(txn, t.warehouse_id, &t.location), &marker)?;
    let doc = to_doc(doc_name(txn, t.tabular_id), t)?;
    if is_new {
        txn.create(name);
        txn.create(location);
        txn.create(doc);
    } else {
        txn.set(doc);
    }
    Ok(())
}

/// Delete the tabular with its markers and stored metadata.
pub(super) fn delete_docs(txn: &mut Txn, t: &TabularDoc) {
    if t.deleted_at.is_none() {
        txn.delete(name_marker(txn, t));
    }
    txn.delete(location_marker(txn, t.warehouse_id, &t.location));
    super::table::delete_staged(txn, t);
    txn.delete(doc_name(txn, t.tabular_id));
}

/// Fail with `LocationAlreadyTaken` if another tabular is stored at, above, or below `location`.
pub(super) async fn ensure_location_available(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    own_id: uuid::Uuid,
    location: &Location,
) -> Result<(), lakekeeper::service::CreateTabularError> {
    let loc = location.as_str().trim_end_matches('/').to_string();
    let mut candidates = location_prefixes(&loc);
    candidates.push(loc.clone());
    let markers: Vec<String> = candidates
        .iter()
        .map(|c| location_marker(txn, warehouse_id, c))
        .collect();
    for marker in txn.get_many(&markers).await?.into_iter().flatten() {
        let marker: NameMarker = from_doc(&marker)?;
        if marker.id != own_id.to_string() {
            return Err(lakekeeper::service::LocationAlreadyTaken::new(location.clone()).into());
        }
    }
    let below = txn
        .query(
            &Query::new(TABULARS)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .array_contains("location_prefixes", string_value(loc.clone()))
                .limit(Some(2)),
        )
        .await?;
    for d in &below {
        let t: TabularDoc = from_doc(d)?;
        if t.tabular_id != own_id {
            return Err(lakekeeper::service::LocationAlreadyTaken::new(location.clone()).into());
        }
    }
    // A pending purge would delete the files of a new tabular at, in or around its location.
    let inside = |a: &str, b: &str| a.strip_prefix(b).is_some_and(|rest| rest.starts_with('/'));
    for purged in super::tasks::purge_locations(txn, warehouse_id).await? {
        if purged == loc || inside(&loc, &purged) || inside(&purged, &loc) {
            return Err(lakekeeper::service::LocationAlreadyTaken::new(location.clone()).into());
        }
    }
    Ok(())
}

/// Move `t` to `location` if it is somewhere else: check the new location is free and move its
/// location marker. The caller writes `t`.
pub(super) async fn move_location(
    txn: &mut Txn,
    t: &mut TabularDoc,
    location: &Location,
) -> Result<(), lakekeeper::service::CreateTabularError> {
    let new_location = location.as_str().trim_end_matches('/').to_string();
    if new_location == t.location {
        return Ok(());
    }
    ensure_location_available(txn, t.warehouse_id, t.tabular_id, location).await?;
    txn.delete(location_marker(txn, t.warehouse_id, &t.location));
    txn.create(to_doc(
        location_marker(txn, t.warehouse_id, &new_location),
        &NameMarker {
            id: t.tabular_id.to_string(),
        },
    )?);
    t.location_prefixes = location_prefixes(&new_location);
    t.location = new_location;
    Ok(())
}

/// The warehouse and namespace documents a tabular info is built from.
pub(super) struct Context {
    warehouses: HashMap<WarehouseId, WarehouseDoc>,
    namespaces: HashMap<NamespaceId, NamespaceDoc>,
}

impl Context {
    pub(super) async fn load(
        txn: &mut Txn,
        tabulars: &[TabularDoc],
    ) -> Result<Self, CatalogBackendError> {
        let mut warehouses = HashMap::new();
        let mut namespaces = HashMap::new();
        for t in tabulars {
            if !warehouses.contains_key(&t.warehouse_id)
                && let Some(w) = warehouse::read(txn, t.warehouse_id).await?
            {
                warehouses.insert(t.warehouse_id, w);
            }
        }
        let mut ns_ids: Vec<NamespaceId> = tabulars.iter().map(|t| t.namespace_id).collect();
        ns_ids.sort();
        ns_ids.dedup();
        let names: Vec<String> = ns_ids
            .iter()
            .map(|id| namespace::doc_name(txn, *id))
            .collect();
        for d in txn.get_many(&names).await?.into_iter().flatten() {
            let ns: NamespaceDoc = from_doc(&d)?;
            namespaces.insert(ns.namespace_id, ns);
        }
        Ok(Self {
            warehouses,
            namespaces,
        })
    }

    pub(super) fn info<T: std::fmt::Debug + PartialEq + Copy>(
        &self,
        t: &TabularDoc,
        id: T,
    ) -> Result<TabularInfo<T>, InfoError> {
        let ns = self.namespaces.get(&t.namespace_id).ok_or_else(|| {
            backend_error(std::io::Error::other(format!(
                "Namespace {} of tabular {} is missing",
                t.namespace_id, t.tabular_id
            )))
        })?;
        let warehouse_version = self
            .warehouses
            .get(&t.warehouse_id)
            .map_or(0, |w| w.version);
        let ns_ident = NamespaceIdent::from_vec(ns.name.clone()).map_err(|_| {
            InvalidNamespaceIdentifier::new(t.warehouse_id, format!("{:?}", ns.name))
        })?;
        Ok(TabularInfo {
            warehouse_id: t.warehouse_id,
            namespace_id: t.namespace_id,
            namespace_version: NamespaceVersion::new(ns.version),
            warehouse_version: WarehouseVersion::new(warehouse_version),
            tabular_ident: TableIdent::new(ns_ident, t.name.clone()),
            tabular_id: id,
            location: Location::from_str(&t.location).map_err(InternalParseLocationError::from)?,
            metadata_location: t
                .metadata_location
                .as_deref()
                .map(Location::from_str)
                .transpose()
                .map_err(InternalParseLocationError::from)?,
            protected: t.protected,
            properties: t.properties.clone(),
            updated_at: t.updated_at.map(from_micros),
        })
    }

    pub(super) fn view_or_table(&self, t: &TabularDoc) -> Result<ViewOrTableInfo, InfoError> {
        Ok(match tabular_id(t)? {
            TabularId::Table(id) => ViewOrTableInfo::Table(self.info(t, id)?),
            TabularId::View(id) => ViewOrTableInfo::View(self.info(t, id)?),
            TabularId::GenericTable(id) => ViewOrTableInfo::GenericTable(self.info(t, id)?),
        })
    }

    pub(super) fn table(&self, t: &TabularDoc) -> Result<TableInfo, InfoError> {
        self.info(t, t.tabular_id.into())
    }

    /// The active soft-deletion task of each soft-deleted tabular among `tabulars`.
    pub(super) async fn expiration_tasks(
        &self,
        txn: &mut Txn,
        tabulars: &[TabularDoc],
    ) -> Result<HashMap<uuid::Uuid, ExpirationTaskInfo>, CatalogBackendError> {
        let mut out = HashMap::new();
        for (warehouse_id, w) in &self.warehouses {
            let deleted: Vec<&TabularDoc> = tabulars
                .iter()
                .filter(|t| t.warehouse_id == *warehouse_id && t.deleted_at.is_some())
                .collect();
            if !deleted.is_empty() {
                out.extend(super::tasks::expiration_tasks(txn, &w.project_id, &deleted).await?);
            }
        }
        Ok(out)
    }

    fn deletion_info(
        &self,
        t: &TabularDoc,
        expiration_task: Option<ExpirationTaskInfo>,
    ) -> Result<ViewOrTableDeletionInfo, InfoError> {
        let created_at = from_micros(t.created_at);
        let deleted_at = t.deleted_at.map(from_micros);
        Ok(match self.view_or_table(t)? {
            ViewOrTableInfo::Table(tabular) => {
                ViewOrTableDeletionInfo::Table(TabularDeletionInfo {
                    tabular,
                    created_at,
                    deleted_at,
                    expiration_task,
                })
            }
            ViewOrTableInfo::View(tabular) => ViewOrTableDeletionInfo::View(TabularDeletionInfo {
                tabular,
                created_at,
                deleted_at,
                expiration_task,
            }),
            ViewOrTableInfo::GenericTable(tabular) => {
                ViewOrTableDeletionInfo::GenericTable(TabularDeletionInfo {
                    tabular,
                    created_at,
                    deleted_at,
                    expiration_task,
                })
            }
        })
    }
}

/// The errors building a tabular info can produce; every tabular error type has them.
#[derive(Debug)]
pub(super) enum InfoError {
    Backend(CatalogBackendError),
    Namespace(InvalidNamespaceIdentifier),
    Location(InternalParseLocationError),
}

impl From<CatalogBackendError> for InfoError {
    fn from(e: CatalogBackendError) -> Self {
        Self::Backend(e)
    }
}
impl From<InvalidNamespaceIdentifier> for InfoError {
    fn from(e: InvalidNamespaceIdentifier) -> Self {
        Self::Namespace(e)
    }
}
impl From<InternalParseLocationError> for InfoError {
    fn from(e: InternalParseLocationError) -> Self {
        Self::Location(e)
    }
}

macro_rules! impl_from_info_error {
    ($($t:ty),*) => {$(
        impl From<crate::store::tabular::InfoError> for $t {
            fn from(e: crate::store::tabular::InfoError) -> Self {
                use crate::store::tabular::InfoError;
                match e {
                    InfoError::Backend(e) => e.into(),
                    InfoError::Namespace(e) => e.into(),
                    InfoError::Location(e) => e.into(),
                }
            }
        }
    )*};
}
pub(super) use impl_from_info_error;

impl_from_info_error!(
    GetTabularInfoError,
    GetTabularInfoByLocationError,
    ListTabularsError,
    SetTabularProtectionError,
    MarkTabularAsDeletedError,
    DropTabularError,
    ClearTabularDeletedAtError,
    RenameTabularError,
    SearchTabularError
);

pub(super) async fn get_by_ident(
    warehouse_id: WarehouseId,
    tabulars: &[TabularIdentBorrowed<'_>],
    flags: TabularListFlags,
    txn: &mut Txn,
) -> Result<HashMap<TableIdent, ViewOrTableInfo>, GetTabularInfoError> {
    let mut out = HashMap::new();
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(out);
    }
    for requested in tabulars {
        let (ident, typ) = match requested {
            TabularIdentBorrowed::Table(i) => (*i, TABLE),
            TabularIdentBorrowed::View(i) => (*i, VIEW),
            TabularIdentBorrowed::GenericTable(i) => (*i, GENERIC_TABLE),
        };
        let Some(ns) = namespace::read_by_path(txn, warehouse_id, ident.namespace.as_ref()).await?
        else {
            continue;
        };
        let Some(t) = read_by_name(txn, warehouse_id, ns.namespace_id, &ident.name).await? else {
            continue;
        };
        if t.typ != typ || !matches_flags(&t, flags) {
            continue;
        }
        let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
        out.insert(ident.clone(), ctx.view_or_table(&t)?);
    }
    Ok(out)
}

pub(super) async fn get_by_id(
    warehouse_id: WarehouseId,
    ids: &[TabularId],
    flags: TabularListFlags,
    txn: &mut Txn,
) -> Result<Vec<ViewOrTableInfo>, GetTabularInfoError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = ids.iter().map(|id| doc_name(txn, **id)).collect();
    let mut docs = Vec::new();
    for d in txn.get_many(&names).await?.into_iter().flatten() {
        let t: TabularDoc = from_doc(&d)?;
        if t.warehouse_id == warehouse_id
            && ids.contains(&tabular_id(&t)?)
            && matches_flags(&t, flags)
        {
            docs.push(t);
        }
    }
    let ctx = Context::load(txn, &docs).await?;
    Ok(docs
        .iter()
        .map(|t| ctx.view_or_table(t))
        .collect::<Result<_, _>>()?)
}

pub(super) async fn get_by_location(
    warehouse_id: WarehouseId,
    location: &Location,
    flags: TabularListFlags,
    txn: &mut Txn,
) -> Result<Option<ViewOrTableInfo>, GetTabularInfoByLocationError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(None);
    }
    let loc = location.as_str().trim_end_matches('/').to_string();
    let mut candidates = location_prefixes(&loc);
    candidates.push(loc);
    let markers: Vec<String> = candidates
        .iter()
        .map(|c| location_marker(txn, warehouse_id, c))
        .collect();
    for marker in txn.get_many(&markers).await?.into_iter().flatten() {
        let marker: NameMarker = from_doc(&marker)?;
        if let Some(t) = read(txn, marker.id.parse().map_err(backend_error)?).await?
            && matches_flags(&t, flags)
        {
            let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
            return Ok(Some(ctx.view_or_table(&t)?));
        }
    }
    Ok(None)
}

fn page_token(t: &TabularDoc) -> String {
    format!("{}|{}", t.created_at, t.tabular_id)
}

fn parse_page_token(token: &str) -> Result<(i64, uuid::Uuid), InvalidPaginationToken> {
    let invalid = || InvalidPaginationToken {
        message: "Invalid page token".to_string(),
        value: token.to_string(),
        stack: Vec::new(),
    };
    let (ts, id) = token.split_once('|').ok_or_else(invalid)?;
    Ok((
        ts.parse().map_err(|_| invalid())?,
        id.parse().map_err(|_| invalid())?,
    ))
}

pub(super) async fn list(
    warehouse_id: WarehouseId,
    namespace_id: Option<NamespaceId>,
    flags: TabularListFlags,
    txn: &mut Txn,
    typ: Option<TabularType>,
    pagination: PaginationQuery,
) -> Result<
    lakekeeper::api::iceberg::v1::PaginatedMapping<TabularId, ViewOrTableDeletionInfo>,
    ListTabularsError,
> {
    let page_size =
        usize::try_from(lakekeeper::CONFIG.page_size_or_pagination_max(pagination.page_size))
            .unwrap_or(usize::MAX);
    let mut out = lakekeeper::api::iceberg::v1::PaginatedMapping::with_capacity(0);
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(out);
    }
    let start_after = pagination
        .page_token
        .as_option()
        .map(parse_page_token)
        .transpose()?
        .map(|(ts, id)| (int_value(ts), doc_name(txn, id)));
    let mut query = Query::new(TABULARS)
        .eq("warehouse_id", string_value(warehouse_id.to_string()))
        .order_by("created_at")
        .start_after(start_after)
        .limit(Some(page_size));
    if let Some(ns) = namespace_id {
        query = query.eq("namespace_id", string_value(ns.to_string()));
    }
    let typ = typ.map(|t| match t {
        TabularType::Table => TABLE,
        TabularType::View => VIEW,
        TabularType::GenericTable => GENERIC_TABLE,
    });
    if let Some(typ) = typ {
        query = query.eq("typ", string_value(typ));
    }
    let docs: Vec<TabularDoc> = txn
        .query(&query)
        .await?
        .iter()
        .map(from_doc)
        .collect::<Result<_, _>>()?;
    let docs: Vec<TabularDoc> = docs
        .into_iter()
        .filter(|t| matches_flags(t, flags))
        .collect();
    let ctx = Context::load(txn, &docs).await?;
    let expiration_tasks = ctx.expiration_tasks(txn, &docs).await?;
    for t in &docs {
        let expiration_task = expiration_tasks.get(&t.tabular_id).copied();
        out.insert(
            tabular_id(t)?,
            ctx.deletion_info(t, expiration_task)?,
            page_token(t),
        );
    }
    Ok(out)
}

pub(super) async fn set_protected(
    warehouse_id: WarehouseId,
    id: TabularId,
    protect: bool,
    txn: &mut Txn,
) -> Result<ViewOrTableInfo, SetTabularProtectionError> {
    let mut t = read(txn, *id)
        .await?
        .filter(|t| t.warehouse_id == warehouse_id)
        .ok_or_else(|| TabularNotFound::new(warehouse_id, id))?;
    t.protected = protect;
    t.updated_at = Some(now_micros());
    write_docs(txn, &t, false)?;
    let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
    Ok(ctx.view_or_table(&t)?)
}

pub(super) async fn mark_as_deleted(
    warehouse_id: WarehouseId,
    id: TabularId,
    force: bool,
    txn: &mut Txn,
) -> Result<ViewOrTableInfo, MarkTabularAsDeletedError> {
    let mut t = read(txn, *id)
        .await?
        .filter(|t| t.warehouse_id == warehouse_id && t.deleted_at.is_none())
        .ok_or_else(|| TabularNotFound::new(warehouse_id, id))?;
    if t.protected && !force {
        return Err(ProtectedTabularDeletionWithoutForce::new(warehouse_id, id).into());
    }
    // The name is free again once the tabular is soft-deleted.
    txn.delete(name_marker(txn, &t));
    t.deleted_at = Some(now_micros());
    t.active = false;
    write_docs(txn, &t, false)?;
    projection_store::identity_changed(txn, &t).await?;
    let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
    Ok(ctx.view_or_table(&t)?)
}

/// Undrop `ids`: clear their soft-deletion marker and take their name back. With `task_ids`,
/// only the soft-deleted ones whose active soft-deletion task is one of `task_ids`, skipping
/// the others; without, every ID must exist in the warehouse. Returns the undropped tabulars
/// with their soft-deletion task, which the caller cancels.
pub(super) async fn clear_deleted_at(
    warehouse_id: WarehouseId,
    ids: &[TabularId],
    task_ids: Option<&[TaskId]>,
    txn: &mut Txn,
) -> Result<Vec<ViewOrTableDeletionInfo>, ClearTabularDeletedAtError> {
    let names: Vec<String> = ids.iter().map(|id| doc_name(txn, **id)).collect();
    let mut docs = Vec::new();
    for d in txn.get_many(&names).await?.into_iter().flatten() {
        let t: TabularDoc = from_doc(&d)?;
        if t.warehouse_id == warehouse_id && ids.contains(&tabular_id(&t)?) {
            docs.push(t);
        }
    }
    if task_ids.is_none()
        && let Some(missing) = ids
            .iter()
            .find(|id| !docs.iter().any(|t| t.tabular_id == ***id))
    {
        return Err(TabularNotFound::new(warehouse_id, *missing).into());
    }
    let ctx = Context::load(txn, &docs).await?;
    let expiration_tasks = ctx.expiration_tasks(txn, &docs).await?;
    let mut out = Vec::new();
    for mut t in docs {
        let expiration_task = expiration_tasks.get(&t.tabular_id).copied();
        if let Some(task_ids) = task_ids
            && !expiration_task.is_some_and(|e| task_ids.contains(&e.task_id))
        {
            continue;
        }
        if t.deleted_at.is_some() {
            let marker = name_marker(txn, &t);
            if txn.get(&marker).await?.is_some() {
                return Err(TabularAlreadyExists::new().into());
            }
            txn.create(to_doc(
                marker,
                &NameMarker {
                    id: t.tabular_id.to_string(),
                },
            )?);
            t.deleted_at = None;
            t.active = true;
            t.updated_at = Some(now_micros());
            write_docs(txn, &t, false)?;
            projection_store::identity_changed(txn, &t).await?;
        }
        out.push(ctx.deletion_info(&t, expiration_task)?);
    }
    Ok(out)
}

/// Rename `id` to `destination`, possibly into another namespace. The tabular must still be
/// in `source_namespace_id` under `source`'s name, and `destination_namespace_id` must still
/// bear `destination`'s namespace path; otherwise the rename lost a race and finds nothing.
/// Names are compared case-insensitively, as the name markers fold them.
pub(super) async fn rename(
    warehouse_id: WarehouseId,
    id: TabularId,
    source_namespace_id: NamespaceId,
    destination_namespace_id: NamespaceId,
    source: &TableIdent,
    destination: &TableIdent,
    txn: &mut Txn,
) -> Result<ViewOrTableInfo, RenameTabularError> {
    let not_found = |detail: &str| {
        RenameTabularError::from(TabularNotFound::new(warehouse_id, id).append_detail(detail))
    };
    let same = |a: &str, b: &str| fold_case(a) == fold_case(b);
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(not_found("The warehouse is not active."));
    }
    let mut t = read(txn, *id)
        .await?
        .filter(|t| {
            t.warehouse_id == warehouse_id
                && tabular_id(t).is_ok_and(|typ| typ == id)
                && (t.metadata_location.is_some() || t.typ == GENERIC_TABLE)
                && t.deleted_at.is_none()
                && t.namespace_id == source_namespace_id
                && same(&t.name, &source.name)
        })
        .ok_or_else(|| {
            not_found("The source tabular is not under the given namespace and name.")
        })?;
    let destination_path = destination.namespace.as_ref();
    namespace::read(txn, destination_namespace_id)
        .await?
        .filter(|n| {
            n.warehouse_id == warehouse_id
                && n.name.len() == destination_path.len()
                && n.name.iter().zip(destination_path).all(|(a, b)| same(a, b))
        })
        .ok_or_else(|| not_found("The destination namespace no longer bears the given name."))?;

    let old_marker = name_marker(txn, &t);
    t.namespace_id = destination_namespace_id;
    t.name.clone_from(&destination.name);
    let new_marker = name_marker(txn, &t);
    // A change of case only keeps the marker, which folds case.
    if new_marker != old_marker {
        if txn.get(&new_marker).await?.is_some() {
            return Err(TabularAlreadyExists::new().into());
        }
        txn.delete(old_marker);
        txn.create(to_doc(
            new_marker,
            &NameMarker {
                id: t.tabular_id.to_string(),
            },
        )?);
    }
    t.updated_at = Some(now_micros());
    write_docs(txn, &t, false)?;
    projection_store::identity_changed(txn, &t).await?;
    let ctx = Context::load(txn, std::slice::from_ref(&t)).await?;
    Ok(ctx.view_or_table(&t)?)
}

pub(super) async fn drop(
    warehouse_id: WarehouseId,
    id: TabularId,
    force: bool,
    txn: &mut Txn,
) -> Result<Location, DropTabularError> {
    let t = read(txn, *id)
        .await?
        .filter(|t| t.warehouse_id == warehouse_id)
        .ok_or_else(|| TabularNotFound::new(warehouse_id, id))?;
    if t.protected && !force {
        return Err(ProtectedTabularDeletionWithoutForce::new(warehouse_id, id).into());
    }
    delete_docs(txn, &t);
    projection_store::dropped(txn, &t).await?;
    super::tag::delete_for_tabular(txn, warehouse_id, t.tabular_id).await?;
    Ok(Location::from_str(&t.location).map_err(InternalParseLocationError::from)?)
}

/// Upstream's search returns at most this many tabulars.
const SEARCH_LIMIT: usize = 10;

/// `pg_trgm`'s trigrams of `s`: of each alphanumeric word, lowercased and padded with two
/// spaces before and one after.
fn trigrams(s: &str) -> HashSet<[char; 3]> {
    let mut out = HashSet::new();
    for word in s
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        let padded: Vec<char> = "  "
            .chars()
            .chain(word.to_lowercase().chars())
            .chain(" ".chars())
            .collect();
        for w in padded.windows(3) {
            out.insert([w[0], w[1], w[2]]);
        }
    }
    out
}

/// `pg_trgm`'s distance (`<->`): one minus the share of trigrams the two have in common.
#[allow(clippy::cast_precision_loss)]
fn trigram_distance(a: &HashSet<[char; 3]>, b: &str) -> f32 {
    let b = trigrams(b);
    let common = a.intersection(&b).count();
    let all = a.len() + b.len() - common;
    if all == 0 {
        return 1.0;
    }
    1.0 - common as f32 / all as f32
}

/// The management API's tabular search, following upstream's Postgres store: a UUID finds the
/// tabular with that ID and the tabulars of the namespace with that ID; any other term finds
/// the ten tabulars whose `namespace.name` is nearest to it by trigram distance (below 1).
/// Reads every active tabular of the warehouse, where upstream uses a trigram index.
pub(super) async fn search(
    warehouse_id: WarehouseId,
    search_term: &str,
    txn: &mut Txn,
) -> Result<CatalogSearchTabularResponse, SearchTabularError> {
    let mut search_results = Vec::new();
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(CatalogSearchTabularResponse { search_results });
    }
    let live = |t: &TabularDoc| {
        t.warehouse_id == warehouse_id
            && t.deleted_at.is_none()
            && (t.metadata_location.is_some() || t.typ == GENERIC_TABLE)
    };
    let mut found: Vec<(TabularDoc, Option<f32>)> = Vec::new();
    if let Ok(id) = uuid::Uuid::try_parse(search_term) {
        if let Some(t) = read(txn, id).await?.filter(live) {
            found.push((t, None));
        }
        let in_namespace = read_in_namespace(txn, warehouse_id, NamespaceId::from(id)).await?;
        found.extend(
            in_namespace
                .into_iter()
                .filter(|t| live(t) && t.tabular_id != id)
                .map(|t| (t, None)),
        );
        found.truncate(SEARCH_LIMIT);
    } else {
        let docs: Vec<TabularDoc> = txn
            .query(
                &Query::new(TABULARS)
                    .eq("warehouse_id", string_value(warehouse_id.to_string()))
                    .eq("active", bool_value(true)),
            )
            .await?
            .iter()
            .map(from_doc)
            .collect::<Result<_, _>>()?;
        let docs: Vec<TabularDoc> = docs.into_iter().filter(live).collect();
        let ctx = Context::load(txn, &docs).await?;
        let term = trigrams(search_term);
        for t in docs {
            let Some(ns) = ctx.namespaces.get(&t.namespace_id) else {
                continue;
            };
            let full = format!("{}.{}", ns.name.join("."), t.name);
            let distance = trigram_distance(&term, &full);
            if distance < 1.0 {
                found.push((t, Some(distance)));
            }
        }
        found.sort_by(|a, b| a.1.unwrap_or(1.0).total_cmp(&b.1.unwrap_or(1.0)));
        found.truncate(SEARCH_LIMIT);
    }
    let docs: Vec<TabularDoc> = found.iter().map(|(t, _)| t.clone()).collect();
    let ctx = Context::load(txn, &docs).await?;
    for (t, distance) in &found {
        search_results.push(CatalogSearchTabularInfo {
            tabular: ctx.view_or_table(t)?,
            distance: *distance,
        });
    }
    Ok(CatalogSearchTabularResponse { search_results })
}
