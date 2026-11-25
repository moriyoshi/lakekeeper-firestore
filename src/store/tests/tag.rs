//! Tags on the emulator.

use lakekeeper::{
    ProjectId, WarehouseId,
    api::{
        ErrorModel,
        iceberg::v1::{PageToken, PaginationQuery, namespace::NamespaceDropFlags},
    },
    service::{
        CatalogCreateTagDefinitionRequest, EffectiveTagSource, TableId, TabularId,
        TagAttachmentFilter, TagDefinition, TagDefinitionId, TagId, TagScope, TagSource, TagTarget,
        TagValueSpec, UpdateTagDefinitionRequest,
    },
};

use super::{
    db,
    table::{create as create_table, namespace, small},
};
use crate::store::{
    fs::{Db, Txn},
    namespace as ns_store, tabular, tag, warehouse,
};

async fn project_of(db: &Db, warehouse_id: WarehouseId) -> ProjectId {
    warehouse::read(&mut Txn::latest(db.clone()), warehouse_id)
        .await
        .unwrap()
        .unwrap()
        .project_id
}

async fn define(
    db: &Db,
    project_id: &ProjectId,
    name: &str,
    scope: &[TagScope],
    value_spec: TagValueSpec<'_>,
) -> Result<TagDefinition, ErrorModel> {
    let mut txn = Txn::read_write(db.clone());
    let request = CatalogCreateTagDefinitionRequest::builder()
        .tag_definition_id(TagDefinitionId::new_random())
        .name(name)
        .scope(scope)
        .value_spec(value_spec)
        .build();
    match tag::create_definition(project_id, request, &mut txn).await {
        Ok(d) => {
            txn.commit().await?;
            Ok(d)
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(e.into())
        }
    }
}

async fn apply(
    db: &Db,
    definition: TagDefinitionId,
    target: TagTarget,
    value: Option<&str>,
) -> Result<bool, ErrorModel> {
    let mut txn = Txn::read_write(db.clone());
    match tag::apply(
        TagId::new_random(),
        definition,
        target,
        value,
        TagSource::Manual,
        &mut txn,
    )
    .await
    {
        Ok((_, changed)) => {
            txn.commit().await?;
            Ok(changed)
        }
        Err(e) => {
            txn.rollback().await.unwrap();
            Err(e.into())
        }
    }
}

async fn names_on(db: &Db, target: TagTarget) -> Vec<(String, Option<String>)> {
    tag::list_for_target(target, &mut Txn::latest(db.clone()))
        .await
        .unwrap()
        .into_iter()
        .map(|t| (t.definition_name, t.tag.value))
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn definitions() {
    let Some(db) = db().await else { return };
    let (warehouse_id, ns) = namespace(&db).await;
    let project_id = project_of(&db, warehouse_id).await;

    let pii = define(
        &db,
        &project_id,
        "PII",
        &[TagScope::Table],
        TagValueSpec::Enumerated {
            allowed_values: &["low", "high"],
        },
    )
    .await
    .unwrap();
    let taken = define(
        &db,
        &project_id,
        "pii",
        &[TagScope::Table],
        TagValueSpec::Marker,
    )
    .await
    .unwrap_err();
    assert_eq!(taken.r#type, "TagNameAlreadyExists");
    let missing = define(
        &db,
        &ProjectId::new_random(),
        "pii",
        &[TagScope::Table],
        TagValueSpec::Marker,
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code, 404);

    let mut txn = Txn::latest(db.clone());
    let by_name = tag::get_definition_by_name(&project_id, "Pii", &mut txn)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(by_name, pii);
    assert_eq!(
        tag::get_definition(&ProjectId::new_random(), pii.tag_definition_id, &mut txn)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        tag::allowed_values(pii.tag_definition_id, &mut txn)
            .await
            .unwrap(),
        ["high", "low"]
    );

    // Rename, widen and add values.
    let mut txn = Txn::read_write(db.clone());
    let (updated, values) = tag::update_definition(
        &project_id,
        pii.tag_definition_id,
        UpdateTagDefinitionRequest::builder()
            .name("sensitivity")
            .description(Some("How sensitive"))
            .scope(&[TagScope::Table, TagScope::Column])
            .add_allowed_values(&["medium", "low"])
            .build(),
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(updated.name, "sensitivity");
    assert_eq!(values, ["high", "low", "medium"]);
    let mut txn = Txn::latest(db.clone());
    assert!(
        tag::get_definition_by_name(&project_id, "pii", &mut txn)
            .await
            .unwrap()
            .is_none()
    );
    // The old name is free again.
    define(
        &db,
        &project_id,
        "pii",
        &[TagScope::Table],
        TagValueSpec::Marker,
    )
    .await
    .unwrap();
    define(
        &db,
        &project_id,
        "owner",
        &[TagScope::Table],
        TagValueSpec::FreeText,
    )
    .await
    .unwrap();

    // Listing, two at a time, oldest first.
    let mut names = Vec::new();
    let mut token = PageToken::Empty;
    loop {
        let page = tag::list_definitions(
            &project_id,
            PaginationQuery {
                page_token: token,
                page_size: Some(2),
            },
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap();
        let done = page.tag_definitions.len() < 2;
        names.extend(page.tag_definitions.into_iter().map(|d| d.name));
        match page.next_page_token {
            Some(t) if !done => token = PageToken::Present(t),
            _ => break,
        }
    }
    assert_eq!(names, ["sensitivity", "pii", "owner"]);

    // A definition in use cannot be deleted.
    let table = small("s3://b/tags/defs");
    let table_id = table.uuid();
    create_table(
        &db,
        (warehouse_id, ns),
        "t",
        Some("s3://b/m/defs.json"),
        &table,
    )
    .await
    .unwrap();
    let target = TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::Table(TableId::from(table_id)),
    };
    apply(&db, updated.tag_definition_id, target, Some("high"))
        .await
        .unwrap();
    let mut txn = Txn::read_write(db.clone());
    let in_use = tag::delete_definition(&project_id, updated.tag_definition_id, &mut txn)
        .await
        .unwrap_err();
    txn.rollback().await.unwrap();
    assert_eq!(ErrorModel::from(in_use).r#type, "TagDefinitionInUse");
    let mut txn = Txn::read_write(db.clone());
    tag::remove_for_target(
        target,
        updated.tag_definition_id,
        TagSource::Manual,
        &mut txn,
    )
    .await
    .unwrap()
    .unwrap();
    tag::delete_definition(&project_id, updated.tag_definition_id, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(
        tag::get_definition(
            &project_id,
            updated.tag_definition_id,
            &mut Txn::latest(db.clone())
        )
        .await
        .unwrap(),
        None
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn attachments_inheritance_and_deletes() {
    let Some(db) = db().await else { return };
    let (warehouse_id, ns) = namespace(&db).await;
    let project_id = project_of(&db, warehouse_id).await;
    let all = [
        TagScope::Warehouse,
        TagScope::Namespace,
        TagScope::Table,
        TagScope::Column,
    ];
    let owner = define(&db, &project_id, "owner", &all, TagValueSpec::FreeText)
        .await
        .unwrap()
        .tag_definition_id;
    let pii = define(&db, &project_id, "pii", &all, TagValueSpec::Marker)
        .await
        .unwrap()
        .tag_definition_id;

    let table = small("s3://b/tags/orders");
    let table_id = TabularId::Table(TableId::from(table.uuid()));
    create_table(
        &db,
        (warehouse_id, ns),
        "orders",
        Some("s3://b/m/orders.json"),
        &table,
    )
    .await
    .unwrap();
    let on_warehouse = TagTarget::Warehouse(warehouse_id);
    let on_namespace = TagTarget::Namespace {
        warehouse_id,
        namespace_id: ns,
    };
    let on_table = TagTarget::Tabular {
        warehouse_id,
        tabular_id: table_id,
    };
    let on_column = TagTarget::Column {
        warehouse_id,
        tabular_id: table_id,
        field_id: 1,
    };

    assert!(
        apply(&db, owner, on_warehouse, Some("platform"))
            .await
            .unwrap()
    );
    assert!(
        apply(&db, owner, on_namespace, Some("sales"))
            .await
            .unwrap()
    );
    assert!(apply(&db, pii, on_column, None).await.unwrap());
    assert!(apply(&db, owner, on_table, Some("alice")).await.unwrap());
    // The same value again changes nothing; another value does.
    assert!(!apply(&db, owner, on_table, Some("alice")).await.unwrap());
    assert!(apply(&db, owner, on_table, Some("bob")).await.unwrap());
    let missing = TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::Table(TableId::new_random()),
    };
    assert_eq!(
        apply(&db, owner, missing, Some("x"))
            .await
            .unwrap_err()
            .r#type,
        "TagTargetNotFound"
    );
    // A table is not a view.
    let as_view = TagTarget::Tabular {
        warehouse_id,
        tabular_id: TabularId::View(table.uuid().into()),
    };
    assert_eq!(
        apply(&db, owner, as_view, Some("x"))
            .await
            .unwrap_err()
            .code,
        404
    );

    assert_eq!(
        names_on(&db, on_table).await,
        [("owner".to_string(), Some("bob".to_string()))]
    );
    assert_eq!(names_on(&db, on_column).await, [("pii".to_string(), None)]);
    let on_targets = tag::list_on_targets(
        &[on_warehouse, on_table, on_namespace],
        &mut Txn::latest(db.clone()),
    )
    .await
    .unwrap();
    assert_eq!(on_targets.len(), 3);
    assert!(on_targets.iter().all(|t| t.definition_name == "owner"));
    let columns = tag::list_column_tags(warehouse_id, table_id, &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].tag.target, on_column);

    // Inherited: the table's own, its namespace's, then the warehouse's.
    let mut candidates = tag::effective_candidates(on_table, &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    candidates.sort_by_key(|c| c.distance);
    assert_eq!(
        candidates
            .iter()
            .map(|c| (c.distance, c.origin, c.value.clone().unwrap()))
            .collect::<Vec<_>>(),
        [
            (0, EffectiveTagSource::Direct, "bob".to_string()),
            (
                1,
                EffectiveTagSource::Namespace {
                    warehouse_id,
                    namespace_id: ns
                },
                "sales".to_string()
            ),
            (
                2,
                EffectiveTagSource::Warehouse { warehouse_id },
                "platform".to_string()
            ),
        ]
    );
    // A column has only its own.
    let column = tag::effective_candidates(on_column, &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    assert_eq!(column.len(), 1);

    // Reverse lookup, by value and by kind.
    let attachments = |filter: TagAttachmentFilter| {
        let db = db.clone();
        async move {
            tag::list_attachments(
                owner,
                &filter,
                PaginationQuery::new_with_page_size(1),
                &mut Txn::latest(db),
            )
            .await
            .unwrap()
        }
    };
    let first = attachments(TagAttachmentFilter::builder().build()).await;
    assert_eq!(first.tags[0].target, on_warehouse);
    let mut all_targets = Vec::new();
    let mut token = PageToken::Empty;
    loop {
        let page = tag::list_attachments(
            owner,
            &TagAttachmentFilter::builder().build(),
            PaginationQuery {
                page_token: token,
                page_size: Some(1),
            },
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap();
        let done = page.tags.is_empty();
        all_targets.extend(page.tags.iter().map(|t| t.target));
        match page.next_page_token {
            Some(t) if !done => token = PageToken::Present(t),
            _ => break,
        }
    }
    assert_eq!(all_targets, [on_warehouse, on_namespace, on_table]);
    let by_value = attachments(
        TagAttachmentFilter::builder()
            .value(Some("sales".to_string()))
            .build(),
    )
    .await;
    assert_eq!(by_value.tags[0].target, on_namespace);
    let by_kind = attachments(
        TagAttachmentFilter::builder()
            .target_type(Some(TagScope::Table))
            .build(),
    )
    .await;
    assert_eq!(by_kind.tags[0].target, on_table);

    // A soft-deleted table keeps its tags but leaves the reverse lookup.
    let mut txn = Txn::read_write(db.clone());
    tabular::mark_as_deleted(warehouse_id, table_id, false, &mut txn)
        .await
        .unwrap();
    txn.commit().await.unwrap();
    assert_eq!(names_on(&db, on_table).await.len(), 1);
    let by_kind = attachments(
        TagAttachmentFilter::builder()
            .target_type(Some(TagScope::Table))
            .build(),
    )
    .await;
    assert_eq!(by_kind.tags.len(), 0);

    // Dropping the namespace takes its tags and its tables' tags.
    let mut txn = Txn::read_write(db.clone());
    ns_store::drop(
        warehouse_id,
        ns,
        NamespaceDropFlags {
            force: true,
            purge: false,
            recursive: true,
        },
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    for target in [on_namespace, on_table, on_column] {
        assert_eq!(names_on(&db, target).await.len(), 0);
    }
    assert_eq!(names_on(&db, on_warehouse).await.len(), 1);
    // Removing by ID.
    let id = tag::list_for_target(on_warehouse, &mut Txn::latest(db.clone()))
        .await
        .unwrap()[0]
        .tag
        .tag_id;
    let mut txn = Txn::read_write(db.clone());
    tag::remove(id, &mut txn).await.unwrap();
    txn.commit().await.unwrap();
    let mut txn = Txn::read_write(db.clone());
    assert_eq!(
        ErrorModel::from(tag::remove(id, &mut txn).await.unwrap_err()).code,
        404
    );
    txn.rollback().await.unwrap();
}
