//! Generic tables: create, load, list and drop.
//!
//! A generic table is a tabular document of type `generic-table`, carrying its format, doc,
//! schema and statistics in [`GenericTableDoc`]. It shares the tabular name and location
//! markers with tables and views, so the shared tabular code renames, protects, soft-deletes
//! and lists it. It has no metadata file, and is never staged.

use std::str::FromStr;

use lakekeeper::{
    CONFIG, WarehouseId,
    api::iceberg::v1::PaginatedMapping,
    iceberg::NamespaceIdent,
    service::{
        CatalogBackendError, CreateGenericTableError, CreateTabularError, DropGenericTableError,
        DropTabularError, GenericTableAlreadyExists, GenericTableCreation, GenericTableFormat,
        GenericTableId, GenericTableInfo, GenericTableListEntry, GenericTableNotFound,
        InvalidPaginationToken, ListGenericTablesError, LoadGenericTableError, NamespaceId,
        TableIdent, TabularId,
    },
};

use super::{
    docs::{GenericTableDoc, TABULARS, TabularDoc, from_micros, location_prefixes, now_micros},
    fs::{Query, Txn, backend_error, bool_value, from_doc, int_value, string_value},
    tabular::{self, Context, GENERIC_TABLE, InfoError},
    warehouse,
};

impl From<InfoError> for CreateGenericTableError {
    fn from(e: InfoError) -> Self {
        match e {
            InfoError::Backend(e) => e.into(),
            InfoError::Namespace(e) => e.into(),
            InfoError::Location(e) => e.into(),
        }
    }
}

/// Loading has no variant for a bad stored location or namespace path: they are backend errors.
impl From<InfoError> for LoadGenericTableError {
    fn from(e: InfoError) -> Self {
        match e {
            InfoError::Backend(e) => e.into(),
            InfoError::Namespace(e) => backend_error(e).into(),
            InfoError::Location(e) => backend_error(e).into(),
        }
    }
}

fn json_string(value: Option<&serde_json::Value>) -> Result<Option<String>, CatalogBackendError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(backend_error)
}

fn json_value(value: Option<&str>) -> Result<Option<serde_json::Value>, CatalogBackendError> {
    value
        .map(serde_json::from_str)
        .transpose()
        .map_err(backend_error)
}

fn generic(t: &TabularDoc) -> Result<&GenericTableDoc, CatalogBackendError> {
    t.generic.as_ref().ok_or_else(|| {
        backend_error(std::io::Error::other(format!(
            "Generic table {} has no generic table fields",
            t.tabular_id
        )))
    })
}

fn format(g: &GenericTableDoc) -> Result<GenericTableFormat, CatalogBackendError> {
    GenericTableFormat::from_str(&g.format).map_err(backend_error)
}

async fn info(txn: &mut Txn, t: &TabularDoc) -> Result<GenericTableInfo, InfoError> {
    let g = generic(t)?;
    let ctx = Context::load(txn, std::slice::from_ref(t)).await?;
    let info = ctx.info(t, GenericTableId::from(t.tabular_id))?;
    Ok(GenericTableInfo {
        generic_table_id: info.tabular_id,
        warehouse_id: info.warehouse_id,
        warehouse_version: info.warehouse_version,
        namespace_id: info.namespace_id,
        namespace_version: info.namespace_version,
        namespace_ident: info.tabular_ident.namespace.clone(),
        name: t.name.clone(),
        tabular_ident: info.tabular_ident,
        location: info.location,
        properties: info.properties,
        protected: info.protected,
        format: format(g)?,
        doc: g.doc.clone(),
        schema: json_value(g.schema.as_deref())?,
        statistics: json_value(g.statistics.as_deref())?,
    })
}

pub(super) async fn create(
    creation: GenericTableCreation,
    txn: &mut Txn,
) -> Result<GenericTableInfo, CreateGenericTableError> {
    let GenericTableCreation {
        generic_table_id,
        namespace_id,
        warehouse_id,
        name,
        format,
        location,
        doc,
        schema,
        statistics,
        properties,
    } = creation;
    let id = *generic_table_id;
    if tabular::read_by_name(txn, warehouse_id, namespace_id, &name)
        .await?
        .is_some()
        || tabular::read(txn, id).await?.is_some()
    {
        return Err(GenericTableAlreadyExists::new().into());
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
    tabular::ensure_location_available(txn, warehouse_id, id, &location)
        .await
        .map_err(|e| match e {
            CreateTabularError::TabularAlreadyExists(_) => GenericTableAlreadyExists::new().into(),
            CreateTabularError::CatalogBackendError(e) => e.into(),
            CreateTabularError::InternalParseLocationError(e) => e.into(),
            CreateTabularError::LocationAlreadyTaken(e) => e.into(),
            CreateTabularError::InvalidNamespaceIdentifier(e) => CreateGenericTableError::from(e),
        })?;
    let doc = TabularDoc {
        tabular_id: id,
        typ: GENERIC_TABLE.to_string(),
        warehouse_id,
        namespace_id,
        name,
        metadata_location: None,
        location: location.as_str().trim_end_matches('/').to_string(),
        location_prefixes: location_prefixes(location.as_str()),
        protected: false,
        properties,
        created_at: now_micros(),
        updated_at: None,
        deleted_at: None,
        active: true,
        generic: Some(GenericTableDoc {
            format: format.as_str().to_string(),
            doc,
            schema: json_string(schema.as_ref())?,
            statistics: json_string(statistics.as_ref())?,
        }),
    };
    tabular::write_docs(txn, &doc, true)?;
    Ok(info(txn, &doc).await?)
}

/// The active generic table named `name` in the namespace of an active warehouse.
async fn read_by_name(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    name: &str,
) -> Result<Option<TabularDoc>, CatalogBackendError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(None);
    }
    Ok(tabular::read_by_name(txn, warehouse_id, namespace_id, name)
        .await?
        .filter(|t| t.typ == GENERIC_TABLE))
}

pub(super) async fn load(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    name: &str,
    txn: &mut Txn,
) -> Result<GenericTableInfo, LoadGenericTableError> {
    let t = read_by_name(txn, warehouse_id, namespace_id, name)
        .await?
        .ok_or_else(GenericTableNotFound::new)?;
    Ok(info(txn, &t).await?)
}

pub(super) async fn load_by_id(
    warehouse_id: WarehouseId,
    id: GenericTableId,
    txn: &mut Txn,
) -> Result<GenericTableInfo, LoadGenericTableError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(GenericTableNotFound::new().into());
    }
    let t = tabular::read(txn, *id)
        .await?
        .filter(|t| {
            t.warehouse_id == warehouse_id && t.typ == GENERIC_TABLE && t.deleted_at.is_none()
        })
        .ok_or_else(GenericTableNotFound::new)?;
    Ok(info(txn, &t).await?)
}

fn token_of(t: &TabularDoc) -> String {
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

/// The active generic tables of the namespace, oldest first.
pub(super) async fn list(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    namespace_ident: &NamespaceIdent,
    page_size: Option<i64>,
    page_token: Option<&str>,
    txn: &mut Txn,
) -> Result<PaginatedMapping<GenericTableId, GenericTableListEntry>, ListGenericTablesError> {
    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_default(page_size)).unwrap_or(usize::MAX);
    let start_after = page_token
        .map(parse_page_token)
        .transpose()?
        .map(|(ts, id)| (int_value(ts), tabular::doc_name(txn, id)));
    let mut out = PaginatedMapping::with_capacity(0);
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(out);
    }
    let docs = txn
        .query(
            &Query::new(TABULARS)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .eq("namespace_id", string_value(namespace_id.to_string()))
                .eq("typ", string_value(GENERIC_TABLE))
                .eq("active", bool_value(true))
                .order_by("created_at")
                .start_after(start_after)
                .limit(Some(page_size)),
        )
        .await?;
    for d in &docs {
        let t: TabularDoc = from_doc(d)?;
        let entry = GenericTableListEntry {
            generic_table_id: t.tabular_id.into(),
            warehouse_id,
            namespace_id,
            tabular_ident: TableIdent::new(namespace_ident.clone(), t.name.clone()),
            format: format(generic(&t)?)?,
            namespace_ident: namespace_ident.clone(),
            protected: t.protected,
            created_at: from_micros(t.created_at),
            name: t.name.clone(),
        };
        out.insert(t.tabular_id.into(), entry, token_of(&t));
    }
    Ok(out)
}

/// Drop the active generic table named `name`; protection is not overridden, as upstream.
pub(super) async fn drop(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    name: &str,
    txn: &mut Txn,
) -> Result<GenericTableId, DropGenericTableError> {
    let t = read_by_name(txn, warehouse_id, namespace_id, name)
        .await?
        .ok_or_else(GenericTableNotFound::new)?;
    let id = GenericTableId::from(t.tabular_id);
    tabular::drop(warehouse_id, TabularId::GenericTable(id), false, txn)
        .await
        .map_err(|e| match e {
            DropTabularError::TabularNotFound(_) => GenericTableNotFound::new().into(),
            DropTabularError::CatalogBackendError(e) => e.into(),
            DropTabularError::InvalidNamespaceIdentifier(e) => e.into(),
            DropTabularError::InternalParseLocationError(e) => e.into(),
            DropTabularError::ProtectedTabularDeletionWithoutForce(e) => e.into(),
            DropTabularError::ConcurrentUpdateError(e) => DropGenericTableError::from(e),
        })?;
    Ok(id)
}
