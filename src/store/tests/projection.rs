//! The searchable projection (phase 1): changes found part by part keep the projection equal to
//! the projection of the current metadata. Needs no emulator.

use std::{collections::HashMap, sync::Arc};

use lakekeeper::{
    WarehouseId,
    iceberg::{
        Result as IcebergResult,
        spec::{
            MAIN_BRANCH, NestedField, Operation, PartitionStatisticsFile, PrimitiveType, Schema,
            Snapshot, SnapshotReference, SqlViewRepresentation, StatisticsFile, StructType,
            Summary, TableMetadata, TableMetadataBuilder, Transform, Type, UnboundPartitionField,
            UnboundPartitionSpec, ViewFormatVersion, ViewMetadata, ViewMetadataBuilder,
            ViewRepresentation, ViewRepresentations, ViewVersion,
        },
    },
    server::tables::calculate_diffs,
    service::NamespaceId,
};

use super::table::small;
use crate::store::projection::{
    Projection, TableIdentity, ViewProjection, Write, commit_changes, project, project_view,
    view_changes,
};

/// A table and its projection, kept up to date commit by commit.
struct Run {
    identity: TableIdentity,
    metadata: TableMetadata,
    projection: Projection,
    commits: usize,
    /// Commits that needed the stored fields because they removed schemas.
    field_reads: usize,
}

impl Run {
    fn new() -> Self {
        let metadata = small("gs://b/wh/ns/t");
        let identity = TableIdentity {
            warehouse_id: WarehouseId::new_random(),
            namespace_id: NamespaceId::new_random(),
            table_id: metadata.uuid(),
            name: "t".to_string(),
            active: true,
        };
        let projection = project(&identity, &metadata);
        Self {
            identity,
            metadata,
            projection,
            commits: 0,
            field_reads: 0,
        }
    }

    /// Commit what `f` builds, update the projection with the changes found, check it against
    /// the projection of the new metadata, and return the writes.
    fn commit(
        &mut self,
        f: impl FnOnce(TableMetadataBuilder) -> IcebergResult<TableMetadataBuilder>,
    ) -> Vec<Write> {
        self.commits += 1;
        let previous = self.metadata.clone();
        let location = format!("gs://b/wh/ns/t/metadata/{}.json", self.commits);
        let built = f(TableMetadataBuilder::new_from_metadata(
            previous.clone(),
            Some(location),
        ))
        .unwrap()
        .build()
        .unwrap();
        let diffs = calculate_diffs(
            &built.metadata,
            &previous,
            1,
            built.expired_metadata_logs.len(),
        );
        let changes = |fields| {
            commit_changes(
                &self.identity,
                &self.projection.table,
                &built.metadata,
                &diffs,
                &built.changes,
                fields,
            )
        };
        let writes = if let Ok(writes) = changes(None) {
            writes
        } else {
            self.field_reads += 1;
            changes(Some(&self.projection.fields)).unwrap()
        };
        self.projection.apply(writes.clone());
        assert_eq!(
            self.projection,
            project(&self.identity, &built.metadata),
            "after commit {}",
            self.commits
        );
        self.metadata = built.metadata;
        writes
    }
}

fn snapshot(id: i64, parent: Option<i64>, schema_id: i32) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(id)
        .with_parent_snapshot_id(parent)
        .with_sequence_number(id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis() + id * 1000)
        .with_manifest_list(format!("gs://b/wh/ns/t/metadata/snap-{id}.avro"))
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: HashMap::from([
                ("added-records".to_string(), "10".to_string()),
                ("total-records".to_string(), (10 * id).to_string()),
                ("total-files-size".to_string(), (1000 * id).to_string()),
                ("engine".to_string(), "test".to_string()),
            ]),
        })
        .with_schema_id(schema_id)
        .build()
}

fn moved(metadata: &TableMetadata, name: &str, snapshot_id: i64) -> SnapshotReference {
    SnapshotReference {
        snapshot_id,
        retention: metadata.refs()[name].retention.clone(),
    }
}

/// How many writes of each kind: (table, snapshot, field, spec), deletes included.
fn kinds(writes: &[Write]) -> (usize, usize, usize, usize) {
    let mut out = (0, 0, 0, 0);
    for w in writes {
        match w {
            Write::Table(_) | Write::View(_) => out.0 += 1,
            Write::Snapshot(_) | Write::DeleteSnapshot(_) => out.1 += 1,
            Write::Field(_) | Write::DeleteField(_) => out.2 += 1,
            Write::Spec(_) | Write::DeleteSpec(_) => out.3 += 1,
        }
    }
    out
}

/// The first schema with `id` renamed to `key`, and a struct with two fields added.
fn evolved_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::optional(
                1,
                "key",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                2,
                "Location",
                Type::Struct(StructType::new(vec![
                    Arc::new(NestedField::required(
                        3,
                        "lat",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                    Arc::new(NestedField::required(
                        4,
                        "lon",
                        Type::Primitive(PrimitiveType::Double),
                    )),
                ])),
            )),
        ])
        .build()
        .unwrap()
}

#[test]
fn commits_keep_the_projection_of_the_current_metadata() {
    let mut run = Run::new();

    // Appends: the new snapshot, and the one `main` moved away from.
    let writes = run.commit(|b| b.set_branch_snapshot(snapshot(1, None, 0), MAIN_BRANCH));
    assert_eq!(kinds(&writes), (1, 1, 0, 0));
    let writes = run.commit(|b| b.set_branch_snapshot(snapshot(2, Some(1), 0), MAIN_BRANCH));
    assert_eq!(kinds(&writes), (1, 2, 0, 0));
    assert_eq!(
        run.projection
            .table
            .current_summary
            .as_ref()
            .unwrap()
            .total_records,
        Some(20)
    );
    assert_eq!(
        run.projection.snapshots[&2].summary_other,
        [("engine".to_string(), "test".to_string())].into()
    );

    // Schema evolution: `id` renamed to `key`, and a struct with two fields added. Only the
    // fields that changed are written.
    let writes = run.commit(|b| b.add_current_schema(evolved_schema()));
    assert_eq!(kinds(&writes), (1, 0, 4, 0));
    let lat = &run.projection.fields[&3];
    assert_eq!(
        (lat.path_folded.as_str(), lat.r#type.as_str(), lat.current),
        ("location.lat", "double", true)
    );
    assert_eq!(run.projection.fields[&1].name, "key");
    run.commit(|b| b.set_branch_snapshot(snapshot(3, Some(2), 1), MAIN_BRANCH));

    // A branch, then `main` rolled back: refs move without a new snapshot.
    let dev = moved(&run.metadata, MAIN_BRANCH, 2);
    run.commit(|b| b.set_ref("dev", dev));
    let back = moved(&run.metadata, MAIN_BRANCH, 1);
    let writes = run.commit(|b| b.set_ref(MAIN_BRANCH, back));
    assert_eq!(kinds(&writes), (1, 2, 0, 0));
    assert_eq!(run.projection.snapshots[&1].refs, vec![MAIN_BRANCH]);

    // Statistics, replaced for the same snapshot, which the ID diffs do not show.
    let stats = |path: &str| StatisticsFile {
        snapshot_id: 2,
        statistics_path: path.to_string(),
        file_size_in_bytes: 100,
        file_footer_size_in_bytes: 10,
        key_metadata: None,
        blob_metadata: vec![],
    };
    run.commit(|b| Ok(b.set_statistics(stats("gs://b/s/a.puffin"))));
    let writes = run.commit(|b| Ok(b.set_statistics(stats("gs://b/s/b.puffin"))));
    assert_eq!(kinds(&writes), (1, 1, 0, 0));
    run.commit(|b| {
        Ok(b.set_partition_statistics(PartitionStatisticsFile {
            snapshot_id: 3,
            statistics_path: "gs://b/s/p.parquet".to_string(),
            file_size_in_bytes: 50,
        }))
    });
    run.commit(|b| Ok(b.remove_statistics(2)));

    // Partition evolution and back; the previous default spec is removed afterwards.
    let bucket = UnboundPartitionSpec::builder()
        .add_partition_field(
            UnboundPartitionField::builder()
                .source_ids(vec![1])
                .name("key_bucket".to_string())
                .transform(Transform::Bucket(4))
                .build()
                .unwrap(),
        )
        .unwrap()
        .build();
    let writes = run.commit(|b| b.add_default_partition_spec(bucket));
    assert_eq!(kinds(&writes), (1, 0, 0, 2));
    assert_eq!(
        run.projection.specs[&1].source_transforms,
        vec!["1:bucket[4]"]
    );
    run.commit(|b| b.set_default_partition_spec(0));
    run.commit(|b| b.remove_partition_specs(&[1]));

    // The schema rolled back, then the newer schema removed: only then are the stored fields
    // needed.
    run.commit(|b| b.set_current_schema(0));
    assert!(!run.projection.fields[&3].current);
    assert_eq!(run.field_reads, 0);
    run.commit(|b| b.remove_schemas(&[1]));
    assert_eq!(run.field_reads, 1);
    assert!(!run.projection.fields.contains_key(&3));

    // Expiry, and a branch dropped.
    run.commit(|b| Ok(b.remove_snapshots(&[3])));
    run.commit(|b| Ok(b.remove_ref("dev")));
    run.commit(|b| {
        b.set_properties(HashMap::from([(
            "owner".to_string(),
            "errtraq".to_string(),
        )]))
    });
    assert_eq!(
        run.projection.table.refs.keys().collect::<Vec<_>>(),
        vec![MAIN_BRANCH]
    );
}

/// Replace the view with what `f` builds, update its projection with the changes found, check
/// it against the projection of the new metadata, and return the writes.
fn replace(
    identity: &TableIdentity,
    (metadata, projection): &mut (ViewMetadata, ViewProjection),
    f: impl FnOnce(ViewMetadataBuilder) -> ViewMetadataBuilder,
) -> Vec<Write> {
    let next = f(ViewMetadataBuilder::new_from_metadata(metadata.clone()))
        .build()
        .unwrap()
        .metadata;
    let writes = view_changes(identity, metadata, &next);
    projection.apply(writes.clone());
    assert_eq!(*projection, project_view(identity, &next));
    *metadata = next;
    writes
}

fn view_version(sql: &str, dialect: &str) -> ViewVersion {
    ViewVersion::builder()
        .with_schema_id(0)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_representations(
            ViewRepresentations::builder()
                .add_all_representations(vec![ViewRepresentation::Sql(SqlViewRepresentation {
                    sql: sql.to_string(),
                    dialect: dialect.to_string(),
                })])
                .build()
                .unwrap(),
        )
        .with_default_namespace(lakekeeper::iceberg::NamespaceIdent::new("ns".to_string()))
        .build()
}

/// View replacements keep the view projection equal to the projection of the current
/// metadata, writing only the fields that changed.
#[test]
fn view_replacements_keep_the_projection_of_the_current_metadata() {
    let id_only = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let metadata = ViewMetadataBuilder::new(
        "gs://b/wh/ns/v".to_string(),
        id_only.clone(),
        view_version("select 1 as id", "Spark"),
        ViewFormatVersion::V1,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata;
    let identity = TableIdentity {
        warehouse_id: WarehouseId::new_random(),
        namespace_id: NamespaceId::new_random(),
        table_id: metadata.uuid(),
        name: "v".to_string(),
        active: true,
    };
    let projection = project_view(&identity, &metadata);
    assert_eq!(projection.view.dialects, vec!["spark"]);
    let mut view = (metadata, projection);
    // A new version with a column added: the view document and the one new field.
    let wider = evolved_schema();
    let writes = replace(&identity, &mut view, |b| {
        b.set_current_version(
            view_version("select 1 as key, 2 as location", "spark"),
            wider.clone(),
        )
        .unwrap()
    });
    assert_eq!(
        kinds(&writes),
        (1, 0, 4, 0),
        "key renamed, location and its two fields"
    );
    // Back to the first version: the three fields only the newer schema has are no longer
    // current; `key`, in both, keeps the attributes of the newest schema.
    let writes = replace(&identity, &mut view, |b| {
        b.set_current_version_id(1).unwrap()
    });
    assert_eq!(kinds(&writes), (1, 0, 3, 0));
    assert!(view.1.fields[&1].current && !view.1.fields[&2].current);
    assert_eq!(view.1.view.dialects, vec!["spark"]);
    // Only properties: the view document alone.
    let writes = replace(&identity, &mut view, |b| {
        b.set_properties(HashMap::from([("k".to_string(), "v".to_string())]))
            .unwrap()
    });
    assert_eq!(kinds(&writes), (1, 0, 0, 0));
}
