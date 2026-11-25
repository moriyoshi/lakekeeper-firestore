//! Tags, following upstream's Postgres store: project-scoped definitions, and attachments
//! of a definition to a warehouse, namespace, tabular or column.
//!
//! A definition is a `tag_definitions` document holding its allowed values; the marker
//! `tag_definition_names/{project}:{folded name}` makes names unique per project ignoring
//! case. An attachment is a `tags` document whose ID is its unique key (target, definition,
//! source), so applying a tag again finds it. Attachments go when their target is deleted,
//! as upstream's cascading foreign keys make them go: with a dropped tabular, a dropped
//! namespace and what was in it, and a deleted warehouse. A soft-deleted tabular keeps them.
//!
//! Upstream also deletes a column's tags once the field leaves the table's last schema, and
//! refuses a column tag for a field the table never had. Neither is done here: the core
//! resolves the column in the current schema before it applies a tag, and fields are only
//! dropped from old schemas, which nothing removes yet.

use std::collections::{HashMap, HashSet};

use lakekeeper::{
    CONFIG, ProjectId, WarehouseId,
    api::iceberg::v1::PaginationQuery,
    service::{
        ApplyTagError, CatalogBackendError, CatalogCreateTagDefinitionRequest,
        CreateTagDefinitionError, DeleteTagDefinitionError, EffectiveTagCandidate,
        EffectiveTagSource, InvalidPaginationToken, ListTagAttachmentsError,
        ListTagAttachmentsResponse, ListTagDefinitionsError, ListTagDefinitionsResponse,
        NamespaceId, ProjectIdNotFoundError, RemoveTagError, TabularId, Tag, TagAttachmentFilter,
        TagDefinition, TagDefinitionId, TagDefinitionIdNotFound, TagDefinitionInUse, TagId,
        TagNameAlreadyExists, TagNotFound, TagScope, TagSource, TagTarget, TagTargetNotFound,
        TagWithName, UpdateTagDefinitionError, UpdateTagDefinitionRequest,
    },
};

use super::{
    docs::{
        NameMarker, TAG_DEFINITION_NAMES, TAG_DEFINITIONS, TAGS, TagDefinitionDoc, TagDoc,
        from_micros, now_micros,
    },
    fs::{
        Query, Txn, backend_error, encode_key, fold_case, from_doc, int_value, string_value, to_doc,
    },
    tabular::{GENERIC_TABLE, TABLE, VIEW},
};

// ---------------- Definitions ----------------

fn definition_name(txn: &Txn, id: TagDefinitionId) -> String {
    txn.db().doc_name(TAG_DEFINITIONS, &id.to_string())
}

fn name_marker(txn: &Txn, project_id: &ProjectId, name: &str) -> String {
    txn.db().doc_name(
        TAG_DEFINITION_NAMES,
        &format!(
            "{}:{}",
            encode_key(project_id.as_str()),
            encode_key(&fold_case(name))
        ),
    )
}

fn definition(doc: &TagDefinitionDoc) -> TagDefinition {
    TagDefinition {
        tag_definition_id: doc.tag_definition_id.into(),
        project_id: doc.project_id.clone(),
        name: doc.name.clone(),
        description: doc.description.clone(),
        scope: doc.scope.clone(),
        value_kind: doc.value_kind,
        created_at: from_micros(doc.created_at),
        updated_at: doc.updated_at.map(from_micros),
    }
}

async fn read_definition(
    txn: &mut Txn,
    id: TagDefinitionId,
) -> Result<Option<TagDefinitionDoc>, CatalogBackendError> {
    let name = definition_name(txn, id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<TagDefinitionDoc>(&d))
        .transpose()
}

/// The definitions `ids`, by ID; missing ones are absent.
async fn read_definitions(
    txn: &mut Txn,
    ids: impl IntoIterator<Item = uuid::Uuid>,
) -> Result<HashMap<uuid::Uuid, TagDefinitionDoc>, CatalogBackendError> {
    let ids: HashSet<uuid::Uuid> = ids.into_iter().collect();
    let names: Vec<String> = ids
        .iter()
        .map(|id| definition_name(txn, (*id).into()))
        .collect();
    let mut out = HashMap::new();
    for doc in txn.get_many(&names).await?.into_iter().flatten() {
        let doc: TagDefinitionDoc = from_doc(&doc)?;
        out.insert(doc.tag_definition_id, doc);
    }
    Ok(out)
}

fn sorted(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut values: Vec<String> = values.into_iter().collect();
    values.sort();
    values.dedup();
    values
}

pub(super) async fn create_definition(
    project_id: &ProjectId,
    request: CatalogCreateTagDefinitionRequest<'_>,
    txn: &mut Txn,
) -> Result<TagDefinition, CreateTagDefinitionError> {
    let CatalogCreateTagDefinitionRequest {
        tag_definition_id,
        name,
        description,
        scope,
        value_spec,
    } = request;
    if !super::project::exists(txn, project_id).await? {
        return Err(ProjectIdNotFoundError::new(project_id.clone()).into());
    }
    let marker = name_marker(txn, project_id, name);
    if txn.get(&marker).await?.is_some() {
        return Err(TagNameAlreadyExists::new().into());
    }
    let doc = TagDefinitionDoc {
        tag_definition_id: *tag_definition_id,
        project_id: project_id.clone(),
        name: name.to_string(),
        description: description.map(ToString::to_string),
        scope: scope.to_vec(),
        value_kind: value_spec.kind(),
        allowed_values: sorted(value_spec.allowed_values().iter().map(ToString::to_string)),
        created_at: now_micros(),
        updated_at: None,
    };
    txn.create(to_doc(
        marker,
        &NameMarker {
            id: tag_definition_id.to_string(),
        },
    )?);
    txn.create(to_doc(definition_name(txn, tag_definition_id), &doc)?);
    Ok(definition(&doc))
}

pub(super) async fn get_definition(
    project_id: &ProjectId,
    id: TagDefinitionId,
    txn: &mut Txn,
) -> Result<Option<TagDefinition>, CatalogBackendError> {
    Ok(read_definition(txn, id)
        .await?
        .filter(|d| d.project_id == *project_id)
        .as_ref()
        .map(definition))
}

pub(super) async fn get_definition_by_name(
    project_id: &ProjectId,
    name: &str,
    txn: &mut Txn,
) -> Result<Option<TagDefinition>, CatalogBackendError> {
    let marker = name_marker(txn, project_id, name);
    let Some(marker) = txn.get(&marker).await? else {
        return Ok(None);
    };
    let marker: NameMarker = from_doc(&marker)?;
    let id = marker.id.parse::<uuid::Uuid>().map_err(backend_error)?;
    get_definition(project_id, id.into(), txn).await
}

fn parse_page_token(token: &str) -> Result<(i64, String), InvalidPaginationToken> {
    let invalid = || InvalidPaginationToken {
        message: "Invalid page token".to_string(),
        value: token.to_string(),
        stack: Vec::new(),
    };
    let (ts, id) = token.split_once('|').ok_or_else(invalid)?;
    Ok((ts.parse().map_err(|_| invalid())?, id.to_string()))
}

pub(super) async fn list_definitions(
    project_id: &ProjectId,
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    txn: &mut Txn,
) -> Result<ListTagDefinitionsResponse, ListTagDefinitionsError> {
    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_default(page_size)).unwrap_or(usize::MAX);
    let start_after = page_token
        .as_option()
        .map(parse_page_token)
        .transpose()?
        .map(|(ts, id)| (int_value(ts), txn.db().doc_name(TAG_DEFINITIONS, &id)));
    let docs = txn
        .query(
            &Query::new(TAG_DEFINITIONS)
                .eq("project_id", string_value(project_id.as_str()))
                .order_by("created_at")
                .start_after(start_after)
                .limit(Some(page_size)),
        )
        .await?;
    let docs: Vec<TagDefinitionDoc> = docs.iter().map(from_doc).collect::<Result<_, _>>()?;
    let next_page_token = docs
        .last()
        .map(|d| format!("{}|{}", d.created_at, d.tag_definition_id));
    Ok(ListTagDefinitionsResponse {
        tag_definitions: docs.iter().map(definition).collect(),
        next_page_token,
    })
}

pub(super) async fn allowed_values(
    id: TagDefinitionId,
    txn: &mut Txn,
) -> Result<Vec<String>, CatalogBackendError> {
    Ok(read_definition(txn, id)
        .await?
        .map(|d| d.allowed_values)
        .unwrap_or_default())
}

/// Replace the name, description and scope, and add allowed values. The caller enforces
/// that the scope only widens and the kind stays.
pub(super) async fn update_definition(
    project_id: &ProjectId,
    id: TagDefinitionId,
    UpdateTagDefinitionRequest {
        name,
        description,
        scope,
        add_allowed_values,
    }: UpdateTagDefinitionRequest<'_>,
    txn: &mut Txn,
) -> Result<(TagDefinition, Vec<String>), UpdateTagDefinitionError> {
    let mut doc = read_definition(txn, id)
        .await?
        .filter(|d| d.project_id == *project_id)
        .ok_or_else(|| TagDefinitionIdNotFound::new(id))?;
    let old_marker = name_marker(txn, project_id, &doc.name);
    let new_marker = name_marker(txn, project_id, name);
    // A change of case alone keeps the marker.
    if new_marker != old_marker {
        if txn.get(&new_marker).await?.is_some() {
            return Err(TagNameAlreadyExists::new().into());
        }
        txn.delete(old_marker);
        txn.create(to_doc(new_marker, &NameMarker { id: id.to_string() })?);
    }
    doc.name = name.to_string();
    doc.description = description.map(ToString::to_string);
    doc.scope = scope.to_vec();
    doc.allowed_values = sorted(
        std::mem::take(&mut doc.allowed_values)
            .into_iter()
            .chain(add_allowed_values.iter().map(ToString::to_string)),
    );
    doc.updated_at = Some(now_micros());
    txn.set(to_doc(definition_name(txn, id), &doc)?);
    Ok((definition(&doc), doc.allowed_values))
}

/// Delete a definition that no tag uses.
pub(super) async fn delete_definition(
    project_id: &ProjectId,
    id: TagDefinitionId,
    txn: &mut Txn,
) -> Result<(), DeleteTagDefinitionError> {
    let doc = read_definition(txn, id)
        .await?
        .filter(|d| d.project_id == *project_id)
        .ok_or_else(|| TagDefinitionIdNotFound::new(id))?;
    let used = txn
        .query(
            &Query::new(TAGS)
                .eq("tag_definition_id", string_value(id.to_string()))
                .limit(Some(1)),
        )
        .await?;
    if !used.is_empty() {
        return Err(TagDefinitionInUse::new().into());
    }
    txn.delete(name_marker(txn, project_id, &doc.name));
    txn.delete(definition_name(txn, id));
    Ok(())
}

/// Delete the definitions of a project being deleted. Its warehouses, and so its tags, are
/// already gone.
pub(super) async fn delete_project_definitions(
    txn: &mut Txn,
    project_id: &ProjectId,
) -> Result<(), CatalogBackendError> {
    let docs = txn
        .query(&Query::new(TAG_DEFINITIONS).eq("project_id", string_value(project_id.as_str())))
        .await?;
    for d in &docs {
        let doc: TagDefinitionDoc = from_doc(d)?;
        txn.delete(name_marker(txn, project_id, &doc.name));
        txn.delete(d.name.clone());
    }
    Ok(())
}

// ---------------- Attachments ----------------

/// The key of a target, which a tag stores to be found by it.
fn target_key(target: TagTarget) -> String {
    match target {
        TagTarget::Warehouse(w) => w.to_string(),
        TagTarget::Namespace {
            warehouse_id,
            namespace_id,
        } => format!("{warehouse_id}:ns:{namespace_id}"),
        TagTarget::Tabular {
            warehouse_id,
            tabular_id,
        } => format!("{warehouse_id}:t:{}", *tabular_id),
        TagTarget::Column {
            warehouse_id,
            tabular_id,
            field_id,
        } => format!("{warehouse_id}:t:{}:f:{field_id}", *tabular_id),
    }
}

fn source_str(source: TagSource) -> &'static str {
    match source {
        TagSource::Manual => "manual",
    }
}

fn tag_name(
    txn: &Txn,
    target: TagTarget,
    definition: TagDefinitionId,
    source: TagSource,
) -> String {
    txn.db().doc_name(
        TAGS,
        &format!("{}:{definition}:{}", target_key(target), source_str(source)),
    )
}

fn tabular_typ(id: TabularId) -> &'static str {
    match id {
        TabularId::Table(_) => TABLE,
        TabularId::View(_) => VIEW,
        TabularId::GenericTable(_) => GENERIC_TABLE,
    }
}

fn tabular_id(typ: &str, id: uuid::Uuid) -> Result<TabularId, CatalogBackendError> {
    Ok(match typ {
        TABLE => TabularId::Table(id.into()),
        VIEW => TabularId::View(id.into()),
        GENERIC_TABLE => TabularId::GenericTable(id.into()),
        other => {
            return Err(backend_error(std::io::Error::other(format!(
                "Unknown tabular type `{other}` of a tag"
            ))));
        }
    })
}

/// The target a tag document names.
fn stored_target(doc: &TagDoc) -> Result<TagTarget, CatalogBackendError> {
    let warehouse_id = doc.warehouse_id;
    if let Some(namespace_id) = doc.namespace_id {
        return Ok(TagTarget::Namespace {
            warehouse_id,
            namespace_id,
        });
    }
    let Some(id) = doc.tabular_id else {
        return Ok(TagTarget::Warehouse(warehouse_id));
    };
    let tabular_id = tabular_id(doc.tabular_typ.as_deref().unwrap_or_default(), id)?;
    Ok(match doc.field_id {
        Some(field_id) => TagTarget::Column {
            warehouse_id,
            tabular_id,
            field_id,
        },
        None => TagTarget::Tabular {
            warehouse_id,
            tabular_id,
        },
    })
}

fn tag(doc: &TagDoc, target: TagTarget) -> Tag {
    Tag {
        tag_id: doc.tag_id.into(),
        tag_definition_id: doc.tag_definition_id.into(),
        target,
        value: doc.value.clone(),
        source: doc.source,
        created_at: from_micros(doc.created_at),
        updated_at: doc.updated_at.map(from_micros),
    }
}

/// Whether `target` exists: a soft-deleted tabular does.
async fn target_exists(txn: &mut Txn, target: TagTarget) -> Result<bool, CatalogBackendError> {
    let warehouse_id = target.warehouse_id();
    Ok(match target {
        TagTarget::Warehouse(_) => super::warehouse::read(txn, warehouse_id).await?.is_some(),
        TagTarget::Namespace { namespace_id, .. } => super::namespace::read(txn, namespace_id)
            .await?
            .is_some_and(|n| n.warehouse_id == warehouse_id),
        TagTarget::Tabular { tabular_id, .. } | TagTarget::Column { tabular_id, .. } => {
            super::tabular::read(txn, *tabular_id)
                .await?
                .is_some_and(|t| t.warehouse_id == warehouse_id && t.typ == tabular_typ(tabular_id))
        }
    })
}

/// Attach the definition to the target, or change the value of the attachment. Returns
/// whether anything changed: the same value again changes nothing.
pub(super) async fn apply(
    tag_id: TagId,
    tag_definition_id: TagDefinitionId,
    target: TagTarget,
    value: Option<&str>,
    source: TagSource,
    txn: &mut Txn,
) -> Result<(Tag, bool), ApplyTagError> {
    if read_definition(txn, tag_definition_id).await?.is_none() {
        return Err(TagDefinitionIdNotFound::new(tag_definition_id).into());
    }
    if !target_exists(txn, target).await? {
        return Err(TagTargetNotFound::new().into());
    }
    let name = tag_name(txn, target, tag_definition_id, source);
    if let Some(existing) = txn.get(&name).await? {
        let mut doc: TagDoc = from_doc(&existing)?;
        if doc.value.as_deref() == value {
            return Ok((tag(&doc, target), false));
        }
        doc.value = value.map(ToString::to_string);
        doc.updated_at = Some(now_micros());
        txn.set(to_doc(name, &doc)?);
        return Ok((tag(&doc, target), true));
    }
    let (namespace_id, tabular, field_id) = match target {
        TagTarget::Warehouse(_) => (None, None, None),
        TagTarget::Namespace { namespace_id, .. } => (Some(namespace_id), None, None),
        TagTarget::Tabular { tabular_id, .. } => (None, Some(tabular_id), None),
        TagTarget::Column {
            tabular_id,
            field_id,
            ..
        } => (None, Some(tabular_id), Some(field_id)),
    };
    let doc = TagDoc {
        tag_id: *tag_id,
        tag_definition_id: *tag_definition_id,
        warehouse_id: target.warehouse_id(),
        namespace_id,
        tabular_id: tabular.map(|t| *t),
        tabular_typ: tabular.map(|t| tabular_typ(t).to_string()),
        field_id,
        kind: target.scope(),
        target: target_key(target),
        value: value.map(ToString::to_string),
        source,
        created_at: now_micros(),
        updated_at: None,
    };
    txn.create(to_doc(name, &doc)?);
    Ok((tag(&doc, target), true))
}

pub(super) async fn remove(tag_id: TagId, txn: &mut Txn) -> Result<(), RemoveTagError> {
    let docs = txn
        .query(
            &Query::new(TAGS)
                .eq("tag_id", string_value(tag_id.to_string()))
                .limit(Some(1)),
        )
        .await?;
    let Some(doc) = docs.into_iter().next() else {
        return Err(TagNotFound::new(tag_id).into());
    };
    txn.delete(doc.name);
    Ok(())
}

/// Delete the attachment of the definition to the target, if any, and return it.
pub(super) async fn remove_for_target(
    target: TagTarget,
    tag_definition_id: TagDefinitionId,
    source: TagSource,
    txn: &mut Txn,
) -> Result<Option<Tag>, RemoveTagError> {
    let name = tag_name(txn, target, tag_definition_id, source);
    let Some(doc) = txn.get(&name).await? else {
        return Ok(None);
    };
    let doc: TagDoc = from_doc(&doc)?;
    txn.delete(name);
    Ok(Some(tag(&doc, target)))
}

/// The tags directly on `target`, oldest first.
async fn on_target(txn: &mut Txn, target: TagTarget) -> Result<Vec<TagDoc>, CatalogBackendError> {
    let mut docs: Vec<TagDoc> = txn
        .query(
            &Query::new(TAGS)
                .eq(
                    "warehouse_id",
                    string_value(target.warehouse_id().to_string()),
                )
                .eq("target", string_value(target_key(target))),
        )
        .await?
        .iter()
        .map(from_doc)
        .collect::<Result<_, _>>()?;
    docs.sort_by_key(|a| (a.created_at, a.tag_id));
    Ok(docs)
}

/// `docs` with their definitions' names; a tag whose definition is gone is left out.
async fn with_names(
    txn: &mut Txn,
    docs: Vec<(TagDoc, TagTarget)>,
) -> Result<Vec<TagWithName>, CatalogBackendError> {
    let definitions = read_definitions(txn, docs.iter().map(|(d, _)| d.tag_definition_id)).await?;
    Ok(docs
        .into_iter()
        .filter_map(|(doc, target)| {
            let name = definitions.get(&doc.tag_definition_id)?.name.clone();
            Some(TagWithName {
                tag: tag(&doc, target),
                definition_name: name,
            })
        })
        .collect())
}

pub(super) async fn list_for_target(
    target: TagTarget,
    txn: &mut Txn,
) -> Result<Vec<TagWithName>, CatalogBackendError> {
    let docs = on_target(txn, target).await?;
    with_names(txn, docs.into_iter().map(|d| (d, target)).collect()).await
}

/// The direct tags of each target, echoing the caller's target. One query per distinct
/// target; of two entries naming the same object, the later one takes its rows.
pub(super) async fn list_on_targets(
    targets: &[TagTarget],
    txn: &mut Txn,
) -> Result<Vec<TagWithName>, CatalogBackendError> {
    let mut by_key: Vec<(String, TagTarget)> = Vec::new();
    for target in targets {
        let key = target_key(*target);
        match by_key.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = *target,
            None => by_key.push((key, *target)),
        }
    }
    let mut docs = Vec::new();
    for (_, target) in by_key {
        docs.extend(
            on_target(txn, target)
                .await?
                .into_iter()
                .map(|d| (d, target)),
        );
    }
    with_names(txn, docs).await
}

/// Every column tag of the tabular, ordered by field ID.
pub(super) async fn list_column_tags(
    warehouse_id: WarehouseId,
    tabular_id: TabularId,
    txn: &mut Txn,
) -> Result<Vec<TagWithName>, CatalogBackendError> {
    let mut docs: Vec<TagDoc> = txn
        .query(
            &Query::new(TAGS)
                .eq("warehouse_id", string_value(warehouse_id.to_string()))
                .eq("tabular_id", string_value(tabular_id.to_string()))
                .eq("kind", string_value(TagScope::Column.as_str())),
        )
        .await?
        .iter()
        .map(from_doc)
        .collect::<Result<_, _>>()?;
    docs.sort_by_key(|a| (a.field_id, a.created_at, a.tag_id));
    let docs = docs
        .into_iter()
        .filter_map(|d| {
            let field_id = d.field_id?;
            Some((
                d,
                TagTarget::Column {
                    warehouse_id,
                    tabular_id,
                    field_id,
                },
            ))
        })
        .collect();
    with_names(txn, docs).await
}

/// The targets the definition is attached to, filtered, oldest first. Tags on soft-deleted
/// tabulars are left out, as upstream does. Reads the definition's tags a page at a time and
/// filters them in memory; the page token is `{created_at}|{document ID}`.
pub(super) async fn list_attachments(
    tag_definition_id: TagDefinitionId,
    filter: &TagAttachmentFilter,
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    txn: &mut Txn,
) -> Result<ListTagAttachmentsResponse, ListTagAttachmentsError> {
    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_default(page_size)).unwrap_or(usize::MAX);
    let batch = page_size.clamp(1, 300);
    let mut after = page_token.as_option().map(parse_page_token).transpose()?;
    let created_after = filter.created_after.map(|t| t.timestamp_micros());
    let created_before = filter.created_before.map(|t| t.timestamp_micros());
    let mut found: Vec<(Tag, String)> = Vec::new();
    'pages: loop {
        let cursor = after
            .as_ref()
            .map(|(ts, id)| (int_value(*ts), txn.db().doc_name(TAGS, id)));
        let docs = txn
            .query(
                &Query::new(TAGS)
                    .eq(
                        "tag_definition_id",
                        string_value(tag_definition_id.to_string()),
                    )
                    .order_by("created_at")
                    .start_after(cursor)
                    .limit(Some(batch)),
            )
            .await?;
        let exhausted = docs.len() < batch;
        let parsed: Vec<(String, TagDoc)> = docs
            .iter()
            .map(|d| {
                let id = d.name.rsplit('/').next().unwrap_or_default().to_string();
                from_doc::<TagDoc>(d).map(|t| (id, t))
            })
            .collect::<Result<_, _>>()?;
        // The tabulars of this batch, to leave out the soft-deleted ones.
        let tabular_names: Vec<String> = parsed
            .iter()
            .filter_map(|(_, t)| t.tabular_id)
            .map(|id| super::tabular::doc_name(txn, id))
            .collect();
        let mut deleted = HashSet::new();
        for d in txn.get_many(&tabular_names).await?.into_iter().flatten() {
            let t: super::docs::TabularDoc = from_doc(&d)?;
            if t.deleted_at.is_some() {
                deleted.insert(t.tabular_id);
            }
        }
        for (id, doc) in parsed {
            after = Some((doc.created_at, id.clone()));
            let keep = doc.tabular_id.is_none_or(|t| !deleted.contains(&t))
                && filter
                    .value
                    .as_deref()
                    .is_none_or(|v| doc.value.as_deref() == Some(v))
                && filter.warehouse_id.is_none_or(|w| doc.warehouse_id == w)
                && filter.target_type.is_none_or(|k| doc.kind == k)
                && created_after.is_none_or(|t| doc.created_at >= t)
                && created_before.is_none_or(|t| doc.created_at <= t);
            if keep {
                let target = stored_target(&doc)?;
                found.push((tag(&doc, target), format!("{}|{id}", doc.created_at)));
                if found.len() == page_size {
                    break 'pages;
                }
            }
        }
        if exhausted {
            break;
        }
    }
    let next_page_token = found.last().map(|(_, token)| token.clone());
    Ok(ListTagAttachmentsResponse {
        tags: found.into_iter().map(|(t, _)| t).collect(),
        next_page_token,
    })
}

/// The tags `target` has and inherits, with their containment distance: a namespace's
/// parents and its warehouse, or a tabular's namespace, that namespace's parents and the
/// warehouse. A column has only its own. Nothing for a target that does not exist.
pub(super) async fn effective_candidates(
    target: TagTarget,
    txn: &mut Txn,
) -> Result<Vec<EffectiveTagCandidate>, CatalogBackendError> {
    let warehouse_id = target.warehouse_id();
    // (target, distance, origin), the target itself first.
    let mut chain: Vec<(TagTarget, i32, EffectiveTagSource)> =
        vec![(target, 0, EffectiveTagSource::Direct)];
    let anchor = match target {
        TagTarget::Warehouse(_) | TagTarget::Column { .. } => None,
        TagTarget::Namespace { namespace_id, .. } => Some((namespace_id, 0)),
        TagTarget::Tabular { tabular_id, .. } => {
            let Some(t) = super::tabular::read(txn, *tabular_id)
                .await?
                .filter(|t| t.warehouse_id == warehouse_id)
            else {
                return Ok(Vec::new());
            };
            Some((t.namespace_id, 1))
        }
    };
    if let Some((namespace_id, offset)) = anchor {
        let Some(ns) = super::namespace::read(txn, namespace_id)
            .await?
            .filter(|n| n.warehouse_id == warehouse_id)
        else {
            return Ok(Vec::new());
        };
        let depth = i32::try_from(ns.name.len()).unwrap_or(i32::MAX);
        // The namespace at each prefix of the path; for a namespace target, its own entry is
        // the target already.
        for len in (1..=ns.name.len()).rev() {
            let at = if len == ns.name.len() {
                Some(ns.clone())
            } else {
                super::namespace::read_by_path(txn, warehouse_id, &ns.name[..len]).await?
            };
            let Some(at) = at else { continue };
            let ancestor = TagTarget::Namespace {
                warehouse_id,
                namespace_id: at.namespace_id,
            };
            if ancestor == target {
                continue;
            }
            let distance = depth - i32::try_from(len).unwrap_or(i32::MAX) + offset;
            chain.push((
                ancestor,
                distance,
                EffectiveTagSource::Namespace {
                    warehouse_id,
                    namespace_id: at.namespace_id,
                },
            ));
        }
        chain.push((
            TagTarget::Warehouse(warehouse_id),
            depth + offset,
            EffectiveTagSource::Warehouse { warehouse_id },
        ));
    }
    let mut rows = Vec::new();
    for (at, distance, origin) in chain {
        for doc in on_target(txn, at).await? {
            rows.push((doc, distance, origin));
        }
    }
    let definitions =
        read_definitions(txn, rows.iter().map(|(d, _, _)| d.tag_definition_id)).await?;
    Ok(rows
        .into_iter()
        .filter_map(|(doc, distance, origin)| {
            Some(EffectiveTagCandidate {
                tag_id: doc.tag_id.into(),
                tag_definition_id: doc.tag_definition_id.into(),
                name: definitions.get(&doc.tag_definition_id)?.name.clone(),
                value: doc.value,
                source: doc.source,
                created_at: from_micros(doc.created_at),
                updated_at: doc.updated_at.map(from_micros),
                distance,
                origin,
            })
        })
        .collect())
}

// ---------------- Deletes of targets ----------------

/// Delete the tags in the warehouse that `matches` selects. Reads every tag of the
/// warehouse: called when deleting a warehouse, namespace or tabular.
async fn delete_where(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    query: Query,
    matches: impl Fn(&TagDoc) -> bool,
) -> Result<(), CatalogBackendError> {
    let docs = txn
        .query(&query.eq("warehouse_id", string_value(warehouse_id.to_string())))
        .await?;
    for d in docs {
        let doc: TagDoc = from_doc(&d)?;
        if matches(&doc) {
            txn.delete(d.name);
        }
    }
    Ok(())
}

/// Delete the tags of a tabular and its columns.
pub(super) async fn delete_for_tabular(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    tabular_id: uuid::Uuid,
) -> Result<(), CatalogBackendError> {
    delete_where(
        txn,
        warehouse_id,
        Query::new(TAGS).eq("tabular_id", string_value(tabular_id.to_string())),
        |_| true,
    )
    .await
}

/// Delete the tags of the namespaces and tabulars of a namespace being dropped.
pub(super) async fn delete_for_namespaces(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
    namespaces: &HashSet<NamespaceId>,
    tabulars: &HashSet<uuid::Uuid>,
) -> Result<(), CatalogBackendError> {
    delete_where(txn, warehouse_id, Query::new(TAGS), |t| {
        t.namespace_id.is_some_and(|n| namespaces.contains(&n))
            || t.tabular_id.is_some_and(|id| tabulars.contains(&id))
    })
    .await
}

/// Delete the tags of a warehouse being deleted.
pub(super) async fn delete_for_warehouse(
    txn: &mut Txn,
    warehouse_id: WarehouseId,
) -> Result<(), CatalogBackendError> {
    delete_where(txn, warehouse_id, Query::new(TAGS), |_| true).await
}
