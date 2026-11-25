//! Namespaces.
//!
//! A namespace document stores its full path in its stored spelling. The marker
//! `namespace_names/{warehouse}:{folded path}` makes the path unique per warehouse and resolves
//! a path to an ID; lookups by path read the markers of every prefix, then the namespaces.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
};

use lakekeeper::{
    CONFIG, WarehouseId,
    api::iceberg::v1::{PaginatedMapping, namespace::NamespaceDropFlags},
    service::{
        CatalogBackendError, CatalogCreateNamespaceError, CatalogGetNamespaceError,
        CatalogListNamespaceError, CatalogListNamespacesResponse, CatalogMoveNamespaceError,
        CatalogNamespaceDropError, CatalogSetNamespaceProtectedError,
        CatalogUpdateNamespacePropertiesError, ChildNamespaceProtected, ChildTabularProtected,
        CreateNamespaceRequest, InvalidNamespaceIdentifier, InvalidPaginationToken,
        ListNamespacesQuery, Location, MovedNamespace, Namespace, NamespaceAlreadyExists,
        NamespaceCannotMoveIntoSelf, NamespaceDropInfo, NamespaceHasChildren,
        NamespaceHasRunningTabularExpirations, NamespaceId, NamespaceIdent, NamespaceNotEmpty,
        NamespaceNotFound, NamespaceProtected, NamespaceVersion, NamespaceWithParent,
        UpdatedNamespaceProperties, WarehouseIdNotFound,
    },
};

use super::{
    docs::{
        NAMESPACE_NAMES, NAMESPACES, NameMarker, NamespaceDoc, from_micros, namespace_name_id,
        now_micros, path_key,
    },
    fs::{Query, Txn, backend_error, from_doc, int_value, string_value, to_doc},
    tabular, warehouse,
};

pub(super) fn doc_name(txn: &Txn, namespace_id: NamespaceId) -> String {
    txn.db().doc_name(NAMESPACES, &namespace_id.to_string())
}

fn marker_name(txn: &Txn, warehouse_id: WarehouseId, path: &[String]) -> String {
    txn.db()
        .doc_name(NAMESPACE_NAMES, &namespace_name_id(warehouse_id, path))
}

fn prefixes(path: &[String]) -> impl Iterator<Item = &[String]> {
    (1..=path.len()).map(|n| &path[..n])
}

fn ident(
    warehouse_id: WarehouseId,
    path: &[String],
) -> Result<NamespaceIdent, InvalidNamespaceIdentifier> {
    NamespaceIdent::from_vec(path.to_vec())
        .map_err(|_| InvalidNamespaceIdentifier::new(warehouse_id, format!("{path:?}")))
}

fn build(
    doc: &NamespaceDoc,
    parent: Option<&NamespaceDoc>,
    requested: Option<&[String]>,
) -> Result<NamespaceWithParent, InvalidNamespaceIdentifier> {
    let requested_ident = match requested {
        Some(r) if r != doc.name.as_slice() => Some(ident(doc.warehouse_id, r)?),
        _ => None,
    };
    Ok(NamespaceWithParent {
        namespace: Arc::new(Namespace {
            namespace_ident: ident(doc.warehouse_id, &doc.name)?,
            protected: doc.protected,
            namespace_id: doc.namespace_id,
            warehouse_id: doc.warehouse_id,
            properties: doc.properties.clone().filter(|p| !p.is_empty()),
            created_at: from_micros(doc.created_at),
            updated_at: doc.updated_at.map(from_micros),
            version: NamespaceVersion::new(doc.version),
        }),
        parent: parent.map(|p| (p.namespace_id, NamespaceVersion::new(p.version))),
        requested_ident,
    })
}

pub(super) async fn read(
    txn: &mut Txn,
    namespace_id: NamespaceId,
) -> Result<Option<NamespaceDoc>, CatalogBackendError> {
    let name = doc_name(txn, namespace_id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<NamespaceDoc>(&d))
        .transpose()
}

/// The namespaces at `paths`, by folded path key; missing ones are absent.
async fn read_by_paths(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    paths: &[Vec<String>],
) -> Result<HashMap<String, NamespaceDoc>, CatalogBackendError> {
    let markers: Vec<String> = paths
        .iter()
        .map(|p| marker_name(txn, warehouse_id, p))
        .collect();
    let mut ids = Vec::new();
    for marker in txn.get_many(&markers).await?.into_iter().flatten() {
        let marker: NameMarker = from_doc(&marker)?;
        let id = NamespaceId::from(marker.id.parse::<uuid::Uuid>().map_err(backend_error)?);
        ids.push(doc_name(txn, id));
    }
    let mut out = HashMap::new();
    for doc in txn.get_many(&ids).await?.into_iter().flatten() {
        let doc: NamespaceDoc = from_doc(&doc)?;
        out.insert(path_key(&doc.name), doc);
    }
    Ok(out)
}

/// The namespace at `path`, if any.
pub(super) async fn read_by_path(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    path: &[String],
) -> Result<Option<NamespaceDoc>, CatalogBackendError> {
    Ok(read_by_paths(txn, warehouse_id, &[path.to_vec()])
        .await?
        .remove(&path_key(path)))
}

fn parent_of<'a>(
    doc: &NamespaceDoc,
    by_key: &'a HashMap<String, NamespaceDoc>,
) -> Option<&'a NamespaceDoc> {
    (doc.name.len() > 1)
        .then(|| by_key.get(&path_key(&doc.name[..doc.name.len() - 1])))
        .flatten()
}

pub(super) async fn get_by_ident(
    warehouse_id: WarehouseId,
    namespaces: &[&NamespaceIdent],
    txn: &mut Txn,
) -> Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(Vec::new());
    }
    // Every prefix of every requested path, in the requested spelling.
    let mut requested: Vec<Vec<String>> = Vec::new();
    let mut seen = HashSet::new();
    for ns in namespaces {
        for prefix in prefixes(ns.as_ref()) {
            if seen.insert(prefix.to_vec()) {
                requested.push(prefix.to_vec());
            }
        }
    }
    let by_key = read_by_paths(txn, warehouse_id, &requested).await?;

    // Like Postgres: return the requested namespaces that exist, with all their parents, once
    // per requested spelling.
    let mut wanted: HashSet<String> = HashSet::new();
    for ns in namespaces {
        if by_key.contains_key(&path_key(ns.as_ref())) {
            for prefix in prefixes(ns.as_ref()) {
                wanted.insert(path_key(prefix));
            }
        }
    }
    let mut out = Vec::new();
    for path in &requested {
        let key = path_key(path);
        if !wanted.contains(&key) {
            continue;
        }
        if let Some(doc) = by_key.get(&key) {
            out.push(build(doc, parent_of(doc, &by_key), Some(path))?);
        }
    }
    Ok(out)
}

pub(super) async fn get_by_id(
    warehouse_id: WarehouseId,
    namespace_ids: &[NamespaceId],
    txn: &mut Txn,
) -> Result<Vec<NamespaceWithParent>, CatalogGetNamespaceError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = namespace_ids.iter().map(|id| doc_name(txn, *id)).collect();
    let mut paths = Vec::new();
    for doc in txn.get_many(&names).await?.into_iter().flatten() {
        let doc: NamespaceDoc = from_doc(&doc)?;
        if doc.warehouse_id == warehouse_id {
            paths.extend(prefixes(&doc.name).map(<[String]>::to_vec));
        }
    }
    paths.sort();
    paths.dedup();
    let by_key = read_by_paths(txn, warehouse_id, &paths).await?;
    let mut out = Vec::with_capacity(by_key.len());
    for doc in by_key.values() {
        out.push(build(doc, parent_of(doc, &by_key), None)?);
    }
    Ok(out)
}

fn page_token(doc: &NamespaceDoc) -> String {
    format!("{}|{}", doc.created_at, doc.namespace_id)
}

fn parse_page_token(token: &str) -> Result<(i64, NamespaceId), InvalidPaginationToken> {
    let invalid = || InvalidPaginationToken {
        message: "Invalid page token".to_string(),
        value: token.to_string(),
        stack: Vec::new(),
    };
    let (ts, id) = token.split_once('|').ok_or_else(invalid)?;
    Ok((
        ts.parse().map_err(|_| invalid())?,
        NamespaceId::from(uuid::Uuid::from_str(id).map_err(|_| invalid())?),
    ))
}

pub(super) async fn list(
    warehouse_id: WarehouseId,
    query: &ListNamespacesQuery,
    txn: &mut Txn,
) -> Result<CatalogListNamespacesResponse, CatalogListNamespaceError> {
    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_max(query.page_size)).unwrap_or(usize::MAX);
    let parent: Vec<String> = query
        .parent
        .as_ref()
        .map(|p| p.as_ref().clone())
        .unwrap_or_default();
    let empty = CatalogListNamespacesResponse {
        parent_namespaces: HashMap::new(),
        namespaces: PaginatedMapping::with_capacity(0),
    };
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Ok(empty);
    }
    let start_after = query
        .page_token
        .as_option()
        .map(parse_page_token)
        .transpose()?
        .map(|(ts, id)| (int_value(ts), doc_name(txn, id)));
    let docs = txn
        .query(
            &Query::new(NAMESPACES)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .eq("parent_key", string_value(path_key(&parent)))
                .order_by("created_at")
                .start_after(start_after)
                .limit(Some(page_size)),
        )
        .await?;
    let docs = docs
        .iter()
        .map(from_doc::<NamespaceDoc>)
        .collect::<Result<Vec<_>, _>>()?;
    if docs.is_empty() {
        return Ok(empty);
    }
    // Ancestors of the listed namespaces share one path: the parent's.
    let ancestor_paths: Vec<Vec<String>> = prefixes(&docs[0].name[..docs[0].name.len() - 1])
        .map(<[String]>::to_vec)
        .collect();
    let mut by_key = read_by_paths(txn, warehouse_id, &ancestor_paths).await?;
    let mut namespaces = PaginatedMapping::with_capacity(docs.len());
    for doc in &docs {
        let ns = build(doc, parent_of(doc, &by_key), None)?;
        namespaces.insert(doc.namespace_id, ns, page_token(doc));
    }
    for doc in docs {
        by_key.insert(path_key(&doc.name), doc);
    }
    let mut parent_namespaces = HashMap::with_capacity(by_key.len());
    for doc in by_key.values() {
        parent_namespaces.insert(doc.namespace_id, build(doc, parent_of(doc, &by_key), None)?);
    }
    Ok(CatalogListNamespacesResponse {
        parent_namespaces,
        namespaces,
    })
}

pub(super) async fn create(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    request: CreateNamespaceRequest,
    txn: &mut Txn,
) -> Result<NamespaceWithParent, CatalogCreateNamespaceError> {
    let CreateNamespaceRequest {
        namespace,
        properties,
    } = request;
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(WarehouseIdNotFound::new(warehouse_id).into());
    }
    let path = namespace.as_ref().clone();
    let parent = match namespace.parent() {
        Some(parent_ident) => Some(
            read_by_path(txn, warehouse_id, parent_ident.as_ref())
                .await?
                .ok_or_else(|| NamespaceNotFound::new(warehouse_id, parent_ident.clone()))?,
        ),
        None => None,
    };
    // The parent's stored spelling plus the caller's spelling of the leaf.
    let written: Vec<String> = match &parent {
        Some(p) => p.name.iter().cloned().chain(path.last().cloned()).collect(),
        None => path.clone(),
    };
    let marker = marker_name(txn, warehouse_id, &written);
    if txn.get(&marker).await?.is_some() {
        return Err(NamespaceAlreadyExists::new(warehouse_id, namespace).into());
    }
    if read(txn, namespace_id).await?.is_some() {
        return Err(NamespaceAlreadyExists::new(warehouse_id, namespace).into());
    }
    let doc = NamespaceDoc {
        namespace_id,
        warehouse_id,
        parent_key: path_key(&written[..written.len() - 1]),
        depth: i64::try_from(written.len()).unwrap_or(i64::MAX),
        name: written,
        protected: false,
        properties,
        created_at: now_micros(),
        updated_at: None,
        version: 0,
    };
    txn.create(to_doc(
        marker,
        &NameMarker {
            id: namespace_id.to_string(),
        },
    )?);
    txn.create(to_doc(doc_name(txn, namespace_id), &doc)?);
    Ok(build(&doc, parent.as_ref(), None)?)
}

/// The namespace and every namespace below it.
async fn subtree(
    txn: &mut Txn,
    root: &NamespaceDoc,
) -> Result<Vec<NamespaceDoc>, CatalogBackendError> {
    let root_key = path_key(&root.name);
    let docs = txn
        .query(
            &Query::new(NAMESPACES).eq("warehouse_id", string_value(root.warehouse_id.to_string())),
        )
        .await?;
    let mut out = Vec::new();
    for d in &docs {
        let doc: NamespaceDoc = from_doc(d)?;
        let key = path_key(&doc.name);
        if key == root_key || key.starts_with(&format!("{root_key}\u{1f}")) {
            out.push(doc);
        }
    }
    Ok(out)
}

pub(super) async fn drop(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    NamespaceDropFlags {
        force,
        purge: _,
        recursive,
    }: NamespaceDropFlags,
    txn: &mut Txn,
) -> Result<NamespaceDropInfo, CatalogNamespaceDropError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(NamespaceNotFound::new(warehouse_id, namespace_id).into());
    }
    let root = read(txn, namespace_id)
        .await?
        .filter(|d| d.warehouse_id == warehouse_id)
        .ok_or_else(|| NamespaceNotFound::new(warehouse_id, namespace_id))?;
    let root_ident = ident(warehouse_id, &root.name)?;
    let namespaces = subtree(txn, &root).await?;
    let children: Vec<&NamespaceDoc> = namespaces
        .iter()
        .filter(|n| n.namespace_id != namespace_id)
        .collect();

    let mut tabulars = Vec::new();
    for ns in &namespaces {
        tabulars.extend(tabular::read_in_namespace(txn, warehouse_id, ns.namespace_id).await?);
    }
    let reported: Vec<_> = tabulars
        .iter()
        .filter(|t| t.metadata_location.is_some() || t.typ != "table")
        .collect();
    let active = reported.iter().filter(|t| t.deleted_at.is_none()).count();
    let deleted = reported.len() - active;
    if !recursive && (!reported.is_empty() || !children.is_empty()) {
        return Err(NamespaceNotEmpty::new(warehouse_id, root_ident)
            .append_detail(format!(
                "Contains {active} tables/views, {deleted} soft-deleted tables/views and {} child namespaces.",
                children.len()
            ))
            .append_detail("Use 'recursive' flag to delete all content.")
            .into());
    }
    if !force && root.protected {
        return Err(NamespaceProtected::new(warehouse_id, root_ident).into());
    }
    if !force && children.iter().any(|c| c.protected) {
        return Err(ChildNamespaceProtected::new(warehouse_id, root_ident).into());
    }
    if !force && reported.iter().any(|t| t.protected) {
        return Err(ChildTabularProtected::new(warehouse_id, root_ident).into());
    }

    let tabular_ids: Vec<uuid::Uuid> = tabulars.iter().map(|t| t.tabular_id).collect();
    let open_tasks =
        super::tasks::active_tasks_of_entities(txn, warehouse_id, &tabular_ids).await?;
    if open_tasks
        .iter()
        .any(|t| super::tasks::is_soft_deletion(t) && super::tasks::is_running(t))
    {
        return Err(NamespaceHasRunningTabularExpirations::new(warehouse_id, root_ident).into());
    }

    let ns_by_id: HashMap<NamespaceId, &NamespaceDoc> =
        namespaces.iter().map(|n| (n.namespace_id, n)).collect();
    let mut child_tables = Vec::new();
    for t in reported.iter().filter(|t| t.deleted_at.is_none()) {
        let ns_path = ns_by_id
            .get(&t.namespace_id)
            .map(|n| n.name.clone())
            .unwrap_or_default();
        child_tables.push((
            tabular::tabular_id(t)?,
            Location::from_str(&t.location)
                .map_err(|e| backend_error(std::io::Error::other(e.to_string())))?,
            lakekeeper::service::TableIdent::new(ident(warehouse_id, &ns_path)?, t.name.clone()),
        ));
    }
    let namespace_locations = namespaces
        .iter()
        .filter_map(|n| {
            let loc = n.properties.as_ref()?.get("location")?;
            Location::from_str(loc).ok().map(|l| (n.namespace_id, l))
        })
        .collect();

    for t in &tabulars {
        tabular::delete_docs(txn, t);
        super::projection_store::dropped(txn, t).await?;
    }
    for ns in &namespaces {
        txn.delete(marker_name(txn, warehouse_id, &ns.name));
        txn.delete(doc_name(txn, ns.namespace_id));
    }
    super::tag::delete_for_namespaces(
        txn,
        warehouse_id,
        &namespaces.iter().map(|n| n.namespace_id).collect(),
        &tabulars.iter().map(|t| t.tabular_id).collect(),
    )
    .await?;
    Ok(NamespaceDropInfo {
        child_namespaces: children.iter().map(|c| c.namespace_id).collect(),
        child_tables,
        // The core cancels these after the drop. Not the projection's: they delete what the
        // dropped tables leave in it.
        open_tasks: open_tasks
            .iter()
            .filter(|t| t.queue_name != super::projection_task::QUEUE_NAME.as_str())
            .map(|t| t.task_id.into())
            .collect(),
        namespace_locations,
    })
}

/// Read the namespace, apply `change`, write it with a new version, and return it with its
/// parent.
async fn update<E>(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    txn: &mut Txn,
    change: impl FnOnce(&mut NamespaceDoc),
) -> Result<(NamespaceDoc, NamespaceWithParent), E>
where
    E: From<NamespaceNotFound> + From<CatalogBackendError> + From<InvalidNamespaceIdentifier>,
{
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(NamespaceNotFound::new(warehouse_id, namespace_id).into());
    }
    let mut doc = read(txn, namespace_id)
        .await?
        .filter(|d| d.warehouse_id == warehouse_id)
        .ok_or_else(|| NamespaceNotFound::new(warehouse_id, namespace_id))?;
    let before = doc.clone();
    change(&mut doc);
    doc.version += 1;
    doc.updated_at = Some(now_micros());
    txn.set(to_doc(doc_name(txn, namespace_id), &doc)?);
    let parent = if doc.name.len() > 1 {
        read_by_path(txn, warehouse_id, &doc.name[..doc.name.len() - 1]).await?
    } else {
        None
    };
    Ok((before, build(&doc, parent.as_ref(), None)?))
}

pub(super) async fn update_properties(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    updates: &HashMap<String, String>,
    removals: &[String],
    txn: &mut Txn,
) -> Result<UpdatedNamespaceProperties, CatalogUpdateNamespacePropertiesError> {
    let (before, namespace) =
        update::<CatalogUpdateNamespacePropertiesError>(warehouse_id, namespace_id, txn, |doc| {
            let mut props = doc.properties.take().unwrap_or_default();
            for key in removals {
                props.remove(key);
            }
            props.extend(updates.iter().map(|(k, v)| (k.clone(), v.clone())));
            doc.properties = Some(props);
        })
        .await?;
    Ok(UpdatedNamespaceProperties {
        namespace,
        previous_properties: before.properties,
    })
}

pub(super) async fn set_protected(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    protect: bool,
    txn: &mut Txn,
) -> Result<NamespaceWithParent, CatalogSetNamespaceProtectedError> {
    let (_, namespace) =
        update::<CatalogSetNamespaceProtectedError>(warehouse_id, namespace_id, txn, |doc| {
            doc.protected = protect;
        })
        .await?;
    Ok(namespace)
}

/// Move the namespace to `destination`, following upstream's Postgres store: only a namespace
/// without children moves, so only its document and name marker change. Its tabulars refer to
/// it by ID.
pub(super) async fn move_to(
    warehouse_id: WarehouseId,
    namespace_id: NamespaceId,
    destination: &NamespaceIdent,
    force: bool,
    txn: &mut Txn,
) -> Result<MovedNamespace, CatalogMoveNamespaceError> {
    if warehouse::read_active(txn, warehouse_id).await?.is_none() {
        return Err(NamespaceNotFound::new(warehouse_id, namespace_id).into());
    }
    let source = read(txn, namespace_id)
        .await?
        .filter(|d| d.warehouse_id == warehouse_id)
        .ok_or_else(|| NamespaceNotFound::new(warehouse_id, namespace_id))?;
    let source_parent = if source.name.len() > 1 {
        read_by_path(txn, warehouse_id, &source.name[..source.name.len() - 1]).await?
    } else {
        None
    };
    let previous_ident = ident(warehouse_id, &source.name)?;
    let previous_parent = source_parent.as_ref().map(|p| p.namespace_id);
    // The same path, byte for byte: a retried move succeeds.
    if source.name == destination.as_ref()[..] {
        return Ok(MovedNamespace {
            namespace: build(&source, source_parent.as_ref(), None)?,
            previous_ident,
            previous_parent,
        });
    }
    if source.protected && !force {
        return Err(NamespaceProtected::new(warehouse_id, namespace_id).into());
    }
    // Every descendant is below a child, so one child is enough to refuse.
    let children = txn
        .query(
            &Query::new(NAMESPACES)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .eq("parent_key", string_value(path_key(&source.name)))
                .limit(Some(1)),
        )
        .await?;
    if !children.is_empty() {
        return Err(NamespaceHasChildren::new(warehouse_id, namespace_id).into());
    }
    let parent = match destination.parent() {
        Some(parent_ident) => {
            let parent = read_by_path(txn, warehouse_id, parent_ident.as_ref())
                .await?
                .ok_or_else(|| NamespaceNotFound::new(warehouse_id, parent_ident.clone()))?;
            if parent.namespace_id == namespace_id {
                return Err(NamespaceCannotMoveIntoSelf::new(warehouse_id, namespace_id).into());
            }
            Some(parent)
        }
        None => None,
    };
    // The parent's stored spelling plus the caller's spelling of the leaf, as in `create`.
    let written: Vec<String> = match &parent {
        Some(p) => p
            .name
            .iter()
            .cloned()
            .chain(destination.as_ref().last().cloned())
            .collect(),
        None => destination.as_ref().clone(),
    };
    // Differs only in the case of the ancestors: nothing to write.
    if written == source.name {
        let namespace = build(&source, parent.as_ref(), None)?;
        return Ok(MovedNamespace {
            namespace,
            previous_ident,
            previous_parent,
        });
    }
    let old_marker = marker_name(txn, warehouse_id, &source.name);
    let new_marker = marker_name(txn, warehouse_id, &written);
    // A change of case alone keeps the marker.
    if new_marker != old_marker {
        if txn.get(&new_marker).await?.is_some() {
            return Err(NamespaceAlreadyExists::new(warehouse_id, destination.clone()).into());
        }
        txn.delete(old_marker);
        txn.create(to_doc(
            new_marker,
            &NameMarker {
                id: namespace_id.to_string(),
            },
        )?);
    }
    let mut doc = source;
    doc.parent_key = path_key(&written[..written.len() - 1]);
    doc.depth = i64::try_from(written.len()).unwrap_or(i64::MAX);
    doc.name = written;
    doc.version += 1;
    doc.updated_at = Some(now_micros());
    txn.set(to_doc(doc_name(txn, namespace_id), &doc)?);
    Ok(MovedNamespace {
        namespace: build(&doc, parent.as_ref(), None)?,
        previous_ident,
        previous_parent,
    })
}
