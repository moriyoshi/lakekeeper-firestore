//! `lakekeeper-firestore search`: the companion CLI that searches the metadata projection
//! (DESIGN.md, "Plan: searchable metadata projection").
//!
//! It reads Firestore directly, with the process's Google credentials (or the emulator), and
//! bypasses Lakekeeper's authorizer: whoever can read the database sees all metadata. Tables are
//! named and filtered for liveness through `idx_tables`; soft-deleted tables are left out unless
//! asked for, and a table whose projection is being rebuilt is marked `(pending)`.

mod indexes;
mod query;

use std::collections::{BTreeMap, BTreeSet, HashMap};

use clap::{Args, Subcommand, ValueEnum};
use firestore::gcloud_sdk::google::firestore::v1::structured_query::field_filter::Operator;
use lakekeeper::{WarehouseId, service::NamespaceId};
use serde_json::{Value as Json, json};

use self::query::{Direction, Search, boolean, int, segment, string};
use super::{
    docs::{
        IDX_FIELDS, IDX_PARTITION_SPECS, IDX_SNAPSHOTS, IDX_TABLES, IDX_VIEWS, NAMESPACE_NAMES,
        NAMESPACES, NameMarker, NamespaceDoc, TABULAR_NAMES, TABULARS, TabularDoc, WAREHOUSES,
        WarehouseDoc, namespace_name_id, tabular_name_id,
    },
    fs::{Db, Document, Txn, fold_case, from_doc},
    projection::{IdxField, IdxPartitionSpec, IdxSnapshot, IdxTable, IdxView, SummaryNumbers},
    tabular::TABLE,
};

pub(crate) use indexes::firestore_indexes;

/// Documents read per query page; small in tests, so that they cross pages.
const PAGE: usize = if cfg!(test) { 2 } else { 300 };

#[derive(Debug, Args)]
pub(crate) struct SearchArgs {
    /// Print one JSON object per row instead of a table.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

/// Tables are given as `namespace.table` (dots separate namespace levels) or by ID; warehouses
/// by name or ID.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Snapshots of a warehouse or a table, newest first.
    Snapshots {
        #[arg(long)]
        warehouse: String,
        /// Only the snapshots of this table.
        #[arg(long)]
        table: Option<String>,
        /// Committed at or after this time (RFC 3339, or a date).
        #[arg(long)]
        since: Option<String>,
        /// Committed before this time (RFC 3339, or a date).
        #[arg(long)]
        until: Option<String>,
        /// `append`, `overwrite`, `replace` or `delete`.
        #[arg(long)]
        operation: Option<String>,
        /// A condition on a summary number, such as `added_records>=1000`; repeatable. Applied
        /// to the query's results, so a selective time range or table keeps it fast.
        #[arg(long = "where", value_name = "CONDITION")]
        conditions: Vec<String>,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        include_deleted: bool,
    },
    /// A table's snapshots in commit order, with their sizes and refs.
    History {
        #[arg(long)]
        warehouse: String,
        table: String,
    },
    /// Tables and views with a column of this name, in any schema they still have.
    Columns {
        /// The column name; with `--path`, its dotted path such as `location.lat`. Case is
        /// ignored.
        name: String,
        #[arg(long)]
        warehouse: Option<String>,
        /// Match the dotted path instead of the name.
        #[arg(long)]
        path: bool,
        /// Only columns of this type, as Iceberg writes it: `long`, `string`, `struct`,
        /// `decimal(10,2)`.
        #[arg(long = "type")]
        r#type: Option<String>,
        /// Only columns of the current schema.
        #[arg(long)]
        current: bool,
        /// Only columns of tables, or only of views.
        #[arg(long, value_enum)]
        kind: Option<Kind>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        include_deleted: bool,
    },
    /// When each column of a table was added and dropped, by schema.
    SchemaChanges {
        #[arg(long)]
        warehouse: String,
        table: String,
    },
    /// Tables partitioned by a transform or a column, in their default partition spec.
    Partitioned {
        #[arg(long)]
        warehouse: Option<String>,
        /// A transform as Iceberg writes it: `identity`, `day`, `bucket[16]`, `truncate[4]`.
        #[arg(long)]
        transform: Option<String>,
        /// A source column name; case is ignored.
        #[arg(long)]
        column: Option<String>,
        /// Search every partition spec the tables have, not only the default one.
        #[arg(long)]
        all_specs: bool,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        include_deleted: bool,
    },
    /// Tables and views with a property, or with a property of a value.
    Properties {
        key: String,
        value: Option<String>,
        #[arg(long)]
        warehouse: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        include_deleted: bool,
    },
    /// The largest tables, by their current snapshot.
    Largest {
        #[arg(long)]
        warehouse: Option<String>,
        #[arg(long, value_enum, default_value_t = Measure::FilesSize)]
        by: Measure,
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        include_deleted: bool,
    },
    /// Print `firestore.indexes.json` for the configured collection prefix: the composite
    /// indexes the catalog and these searches need, and the fields kept out of indexes.
    Indexes,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum Kind {
    Table,
    View,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum Measure {
    FilesSize,
    Records,
    DataFiles,
    DeleteFiles,
}

impl Measure {
    fn field(self) -> &'static str {
        indexes::MEASURES[self as usize]
    }
}

/// Rows of a search, in column order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Output {
    pub(crate) columns: Vec<&'static str>,
    pub(crate) rows: Vec<Vec<Json>>,
}

impl Output {
    fn new(columns: &[&'static str]) -> Self {
        Self {
            columns: columns.to_vec(),
            rows: Vec::new(),
        }
    }

    /// The values of `column`, for tests.
    #[cfg(test)]
    pub(crate) fn column(&self, column: &str) -> Vec<Json> {
        let i = self.columns.iter().position(|c| *c == column).unwrap();
        self.rows.iter().map(|r| r[i].clone()).collect()
    }

    fn print(&self, json: bool) {
        if json {
            for row in &self.rows {
                let object: serde_json::Map<String, Json> = self
                    .columns
                    .iter()
                    .map(|c| (*c).to_string())
                    .zip(row.iter().cloned())
                    .collect();
                println!("{}", Json::Object(object));
            }
            return;
        }
        let cell = |v: &Json| match v {
            Json::Null => String::new(),
            Json::String(s) => s.clone(),
            other => other.to_string(),
        };
        let cells: Vec<Vec<String>> = self
            .rows
            .iter()
            .map(|r| r.iter().map(cell).collect())
            .collect();
        let widths: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                cells
                    .iter()
                    .map(|r| r[i].chars().count())
                    .chain([c.len()])
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let line = |values: Vec<&str>| {
            let padded: Vec<String> = values
                .iter()
                .zip(&widths)
                .map(|(v, w)| format!("{v:<w$}"))
                .collect();
            println!("{}", padded.join("  ").trim_end());
        };
        line(self.columns.clone());
        for row in &cells {
            line(row.iter().map(String::as_str).collect());
        }
    }
}

pub(crate) async fn run(args: SearchArgs) -> anyhow::Result<()> {
    if let Command::Indexes = args.command {
        let prefix = crate::CONFIG_BIN
            .firestore
            .collection_prefix
            .clone()
            .unwrap_or_default();
        println!(
            "{}",
            serde_json::to_string_pretty(&firestore_indexes(&prefix))?
        );
        return Ok(());
    }
    let db = super::connect_from_config().await?;
    execute(&db, args.command).await?.print(args.json);
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(crate) async fn execute(db: &Db, command: Command) -> anyhow::Result<Output> {
    match command {
        Command::Snapshots {
            warehouse,
            table,
            since,
            until,
            operation,
            conditions,
            limit,
            include_deleted,
        } => {
            let warehouse_id = resolve_warehouse(db, &warehouse).await?;
            let table_id = match table {
                Some(t) => Some(resolve_table(db, warehouse_id, &t).await?),
                None => None,
            };
            let conditions = conditions
                .iter()
                .map(|c| Condition::parse(c))
                .collect::<anyhow::Result<Vec<_>>>()?;
            let mut search = match table_id {
                Some(id) => Search::default().eq("table_id", string(id.to_string())),
                None => Search::default().eq("warehouse_id", string(warehouse_id.to_string())),
            }
            .eq_opt("operation", operation.map(string));
            if let Some(since) = since {
                search = search.filter(
                    "timestamp_ms",
                    Operator::GreaterThanOrEqual,
                    int(parse_time(&since)?),
                );
            }
            if let Some(until) = until {
                search =
                    search.filter("timestamp_ms", Operator::LessThan, int(parse_time(&until)?));
            }
            let search = search
                .order_by("timestamp_ms", Direction::Descending)
                .order_by("__name__", Direction::Descending);
            let (docs, tables) = scan(db, IDX_SNAPSHOTS, &search, limit, include_deleted, |doc| {
                from_doc::<IdxSnapshot>(doc)
                    .is_ok_and(|s| conditions.iter().all(|c| c.matches(&s.summary)))
            })
            .await?;
            snapshot_rows(&docs, &tables, false)
        }
        Command::History { warehouse, table } => {
            let warehouse_id = resolve_warehouse(db, &warehouse).await?;
            let table_id = resolve_table(db, warehouse_id, &table).await?;
            let search = Search::default()
                .eq("table_id", string(table_id.to_string()))
                .order_by("sequence_number", Direction::Ascending)
                .order_by("__name__", Direction::Ascending);
            let (docs, tables) =
                scan(db, IDX_SNAPSHOTS, &search, usize::MAX, true, |_| true).await?;
            snapshot_rows(&docs, &tables, true)
        }
        Command::Columns {
            name,
            warehouse,
            path,
            r#type,
            current,
            kind,
            limit,
            include_deleted,
        } => {
            let warehouse_id = resolve_optional_warehouse(db, warehouse.as_deref()).await?;
            let search = Search::default()
                .eq(
                    if path { "path_folded" } else { "name_folded" },
                    string(fold_case(&name)),
                )
                .eq_opt("warehouse_id", warehouse_id.map(|w| string(w.to_string())))
                .eq_opt("type", r#type.map(string))
                .eq_opt("current", current.then(|| boolean(true)))
                .eq_opt(
                    "kind",
                    kind.map(|k| {
                        string(match k {
                            Kind::Table => "table",
                            Kind::View => "view",
                        })
                    }),
                )
                .order_by("__name__", Direction::Ascending);
            let (docs, tables) =
                scan(db, IDX_FIELDS, &search, limit, include_deleted, |_| true).await?;
            let mut out = Output::new(&[
                "table",
                "kind",
                "field_id",
                "path",
                "type",
                "required",
                "current",
                "first_schema_id",
                "last_schema_id",
            ]);
            for doc in &docs {
                let f: IdxField = from_doc(doc)?;
                out.rows.push(vec![
                    json!(tables.label(f.table_id)),
                    json!(f.kind),
                    json!(f.field_id),
                    json!(f.path),
                    json!(f.r#type),
                    json!(f.required),
                    json!(f.current),
                    json!(f.first_schema_id),
                    json!(f.last_schema_id),
                ]);
            }
            Ok(out)
        }
        Command::SchemaChanges { warehouse, table } => {
            let warehouse_id = resolve_warehouse(db, &warehouse).await?;
            let table_id = resolve_table(db, warehouse_id, &table).await?;
            schema_changes(db, table_id).await
        }
        Command::Partitioned {
            warehouse,
            transform,
            column,
            all_specs,
            limit,
            include_deleted,
        } => {
            let warehouse_id = resolve_optional_warehouse(db, warehouse.as_deref()).await?;
            partitioned(
                db,
                warehouse_id,
                transform,
                column,
                all_specs,
                limit,
                include_deleted,
            )
            .await
        }
        Command::Properties {
            key,
            value,
            warehouse,
            limit,
            include_deleted,
        } => {
            let warehouse_id = resolve_optional_warehouse(db, warehouse.as_deref()).await?;
            properties(db, &key, value, warehouse_id, limit, include_deleted).await
        }
        Command::Largest {
            warehouse,
            by,
            limit,
            include_deleted,
        } => {
            let warehouse_id = resolve_optional_warehouse(db, warehouse.as_deref()).await?;
            let docs = Search::default()
                .eq_opt("warehouse_id", warehouse_id.map(|w| string(w.to_string())))
                .eq_opt("active", (!include_deleted).then(|| boolean(true)))
                .order_by(
                    format!("current_summary.{}", by.field()),
                    Direction::Descending,
                )
                .limit(Some(limit))
                .run(db, IDX_TABLES)
                .await?;
            let mut tables = Tables::default();
            tables.load(db, docs.iter().filter_map(table_id_of)).await?;
            let mut out = Output::new(&[
                "table",
                "total_files_size",
                "total_records",
                "total_data_files",
                "total_delete_files",
                "snapshot_id",
                "last_updated",
            ]);
            for doc in &docs {
                let t: IdxTable = from_doc(doc)?;
                let s = t.current_summary.clone().unwrap_or_default();
                out.rows.push(vec![
                    json!(tables.label(t.table_id)),
                    json!(s.total_files_size),
                    json!(s.total_records),
                    json!(s.total_data_files),
                    json!(s.total_delete_files),
                    json!(t.current_snapshot_id),
                    json!(time(t.last_updated_ms)),
                ]);
            }
            Ok(out)
        }
        Command::Indexes => anyhow::bail!("`indexes` needs no database"),
    }
}

fn snapshot_rows(docs: &[Document], tables: &Tables, history: bool) -> anyhow::Result<Output> {
    let mut out = Output::new(if history {
        &[
            "sequence_number",
            "time",
            "snapshot_id",
            "parent_id",
            "operation",
            "refs",
            "schema_id",
            "added_records",
            "deleted_records",
            "total_records",
            "total_data_files",
            "total_delete_files",
            "total_files_size",
        ]
    } else {
        &[
            "time",
            "table",
            "snapshot_id",
            "operation",
            "refs",
            "added_records",
            "deleted_records",
            "total_records",
            "total_delete_files",
            "total_files_size",
        ]
    });
    for doc in docs {
        let s: IdxSnapshot = from_doc(doc)?;
        let n = &s.summary;
        out.rows.push(if history {
            vec![
                json!(s.sequence_number),
                json!(time(s.timestamp_ms)),
                json!(s.snapshot_id),
                json!(s.parent_snapshot_id),
                json!(s.operation),
                json!(s.refs.join(",")),
                json!(s.schema_id),
                json!(n.added_records),
                json!(n.deleted_records),
                json!(n.total_records),
                json!(n.total_data_files),
                json!(n.total_delete_files),
                json!(n.total_files_size),
            ]
        } else {
            vec![
                json!(time(s.timestamp_ms)),
                json!(tables.label(s.table_id)),
                json!(s.snapshot_id),
                json!(s.operation),
                json!(s.refs.join(",")),
                json!(n.added_records),
                json!(n.deleted_records),
                json!(n.total_records),
                json!(n.total_delete_files),
                json!(n.total_files_size),
            ]
        });
    }
    Ok(out)
}

/// One row per column added and per column dropped, by schema, with the time of the oldest
/// snapshot still kept that was written with that schema (or a newer one, for drops). Renames
/// and type changes are not tracked: a field keeps the attributes of its newest schema.
async fn schema_changes(db: &Db, table_id: uuid::Uuid) -> anyhow::Result<Output> {
    let fields = Search::default()
        .eq("table_id", string(table_id.to_string()))
        .run(db, IDX_FIELDS)
        .await?;
    let snapshots = Search::default()
        .eq("table_id", string(table_id.to_string()))
        .order_by("sequence_number", Direction::Ascending)
        .run(db, IDX_SNAPSHOTS)
        .await?;
    // The oldest kept snapshot of each schema.
    let mut first_use: BTreeMap<i32, i64> = BTreeMap::new();
    for doc in &snapshots {
        let s: IdxSnapshot = from_doc(doc)?;
        if let Some(schema_id) = s.schema_id {
            let t = first_use.entry(schema_id).or_insert(s.timestamp_ms);
            *t = (*t).min(s.timestamp_ms);
        }
    }
    let used_from = |schema_id: i32, after: bool| {
        first_use
            .iter()
            .filter(|(id, _)| {
                if after {
                    **id > schema_id
                } else {
                    **id == schema_id
                }
            })
            .map(|(_, t)| *t)
            .min()
            .map(time)
    };
    let mut changes = Vec::new();
    for doc in &fields {
        let f: IdxField = from_doc(doc)?;
        changes.push((
            f.first_schema_id,
            0,
            f.field_id,
            "added",
            used_from(f.first_schema_id, false),
            f.clone(),
        ));
        if let Some(last) = f.last_schema_id {
            changes.push((
                last,
                1,
                f.field_id,
                "dropped after",
                used_from(last, true),
                f,
            ));
        }
    }
    changes.sort_by_key(|(schema_id, order, field_id, ..)| (*schema_id, *order, *field_id));
    let mut out = Output::new(&[
        "schema_id",
        "change",
        "first_seen",
        "field_id",
        "path",
        "type",
        "required",
    ]);
    for (schema_id, _, field_id, change, seen, f) in changes {
        out.rows.push(vec![
            json!(schema_id),
            json!(change),
            json!(seen),
            json!(field_id),
            json!(f.path),
            json!(f.r#type),
            json!(f.required),
        ]);
    }
    Ok(out)
}

#[allow(clippy::too_many_lines)]
async fn partitioned(
    db: &Db,
    warehouse_id: Option<WarehouseId>,
    transform: Option<String>,
    column: Option<String>,
    all_specs: bool,
    limit: usize,
    include_deleted: bool,
) -> anyhow::Result<Output> {
    let default = (!all_specs).then(|| boolean(true));
    let (specs, tables) = match (column, transform) {
        (None, None) => anyhow::bail!("Give --transform, --column, or both"),
        (None, Some(transform)) => {
            let search = Search::default()
                .filter("transforms", Operator::ArrayContains, string(transform))
                .eq_opt("warehouse_id", warehouse_id.map(|w| string(w.to_string())))
                .eq_opt("default", default)
                .order_by("__name__", Direction::Ascending);
            scan(
                db,
                IDX_PARTITION_SPECS,
                &search,
                limit,
                include_deleted,
                |_| true,
            )
            .await?
        }
        (Some(column), transform) => {
            // The column's field ID differs from table to table.
            let search = Search::default()
                .eq("name_folded", string(fold_case(&column)))
                .eq("kind", string("table"))
                .eq_opt("warehouse_id", warehouse_id.map(|w| string(w.to_string())))
                .order_by("__name__", Direction::Ascending);
            let (fields, tables) =
                scan(db, IDX_FIELDS, &search, usize::MAX, include_deleted, |_| {
                    true
                })
                .await?;
            let mut specs = Vec::new();
            for doc in &fields {
                let f: IdxField = from_doc(doc)?;
                let found = Search::default()
                    .eq("table_id", string(f.table_id.to_string()))
                    .filter(
                        "source_field_ids",
                        Operator::ArrayContains,
                        int(i64::from(f.field_id)),
                    )
                    .eq_opt("default", default.clone())
                    .run(db, IDX_PARTITION_SPECS)
                    .await?;
                for spec in found {
                    let s: IdxPartitionSpec = from_doc(&spec)?;
                    let wanted = format!("{}:{}", f.field_id, transform.as_deref().unwrap_or(""));
                    if transform.is_none() || s.source_transforms.contains(&wanted) {
                        specs.push(spec);
                    }
                }
                if specs.len() >= limit {
                    break;
                }
            }
            specs.truncate(limit);
            (specs, tables)
        }
    };
    // The source columns by path, from the field documents of the specs' tables.
    let mut source_names: Vec<String> = Vec::new();
    let mut parsed = Vec::new();
    for doc in &specs {
        let s: IdxPartitionSpec = from_doc(doc)?;
        for id in &s.source_field_ids {
            source_names.push(db.doc_name(IDX_FIELDS, &format!("{}:{id}", s.table_id)));
        }
        parsed.push(s);
    }
    let mut paths: HashMap<(uuid::Uuid, i32), String> = HashMap::new();
    for doc in Txn::latest(db.clone())
        .get_many(&source_names)
        .await?
        .into_iter()
        .flatten()
    {
        let f: IdxField = from_doc(&doc)?;
        paths.insert((f.table_id, f.field_id), f.path);
    }
    let mut out = Output::new(&["table", "spec_id", "default", "fields"]);
    for s in parsed {
        let fields: Vec<String> = s
            .names
            .iter()
            .zip(&s.source_field_ids)
            .zip(&s.transforms)
            .map(|((name, id), transform)| {
                let source = paths
                    .get(&(s.table_id, *id))
                    .cloned()
                    .unwrap_or_else(|| id.to_string());
                format!("{name}={transform}({source})")
            })
            .collect();
        out.rows.push(vec![
            json!(tables.label(s.table_id)),
            json!(s.spec_id),
            json!(s.default),
            json!(fields.join(", ")),
        ]);
    }
    Ok(out)
}

/// From the tabular documents, which keep the properties of tables and views.
async fn properties(
    db: &Db,
    key: &str,
    value: Option<String>,
    warehouse_id: Option<WarehouseId>,
    limit: usize,
    include_deleted: bool,
) -> anyhow::Result<Output> {
    let field = format!("properties.{}", segment(key));
    let docs = if let Some(value) = value {
        // Equalities only: served by the single-field indexes of the map's entries.
        Search::default()
            .eq(field, string(value))
            .eq_opt("warehouse_id", warehouse_id.map(|w| string(w.to_string())))
            .eq_opt("active", (!include_deleted).then(|| boolean(true)))
            .limit(Some(limit))
            .run(db, TABULARS)
            .await?
    } else {
        // Any value: an inequality on the key alone needs no composite index, so the rest is
        // filtered here.
        let mut docs = Search::default()
            .filter(field.clone(), Operator::GreaterThanOrEqual, string(""))
            .order_by(field, Direction::Ascending)
            .run(db, TABULARS)
            .await?;
        docs.retain(|doc| {
            from_doc::<TabularDoc>(doc).is_ok_and(|t| {
                warehouse_id.is_none_or(|w| t.warehouse_id == w) && (include_deleted || t.active)
            })
        });
        docs.truncate(limit);
        docs
    };
    let tabulars: Vec<TabularDoc> = docs.iter().map(from_doc).collect::<Result<_, _>>()?;
    let namespaces = namespace_paths(db, tabulars.iter().map(|t| t.namespace_id)).await?;
    let mut out = Output::new(&["type", "name", "value", "id", "deleted"]);
    for t in tabulars {
        let mut name = namespaces
            .get(&t.namespace_id.to_string())
            .cloned()
            .unwrap_or_default();
        name.push(t.name.clone());
        out.rows.push(vec![
            json!(t.typ),
            json!(name.join(".")),
            json!(t.properties.get(key)),
            json!(t.tabular_id.to_string()),
            json!(t.deleted_at.is_some()),
        ]);
    }
    Ok(out)
}

fn table_id_of(doc: &Document) -> Option<uuid::Uuid> {
    use firestore::gcloud_sdk::google::firestore::v1::value::ValueType;
    match doc.fields.get("table_id")?.value_type.as_ref()? {
        ValueType::StringValue(s) => s.parse().ok(),
        _ => None,
    }
}

/// Pages of `search` until `limit` documents of live tables pass `keep`.
async fn scan(
    db: &Db,
    collection: &str,
    search: &Search,
    limit: usize,
    include_deleted: bool,
    keep: impl Fn(&Document) -> bool,
) -> anyhow::Result<(Vec<Document>, Tables)> {
    let mut tables = Tables::default();
    let mut out = Vec::new();
    let mut after: Option<Document> = None;
    loop {
        let page = search.page(db, collection, after.as_ref(), PAGE).await?;
        tables.load(db, page.iter().filter_map(table_id_of)).await?;
        for doc in &page {
            if out.len() >= limit {
                break;
            }
            if tables.live(table_id_of(doc), include_deleted) && keep(doc) {
                out.push(doc.clone());
            }
        }
        if out.len() >= limit || page.len() < PAGE {
            return Ok((out, tables));
        }
        after = page.last().cloned();
    }
}

/// What the head document of a table or view says about it.
#[derive(Debug)]
struct Head {
    namespace_id: NamespaceId,
    name: String,
    active: bool,
    pending: bool,
}

/// The head documents of tables and views in the projection, with the paths of their
/// namespaces.
#[derive(Debug, Default)]
struct Tables {
    tables: HashMap<uuid::Uuid, Head>,
    namespaces: HashMap<String, Vec<String>>,
}

impl Tables {
    async fn load(
        &mut self,
        db: &Db,
        ids: impl IntoIterator<Item = uuid::Uuid>,
    ) -> anyhow::Result<()> {
        let missing: BTreeSet<uuid::Uuid> = ids
            .into_iter()
            .filter(|id| !self.tables.contains_key(id))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        let mut txn = Txn::latest(db.clone());
        let names: Vec<String> = missing
            .iter()
            .map(|id| db.doc_name(IDX_TABLES, &id.to_string()))
            .collect();
        for doc in txn.get_many(&names).await?.into_iter().flatten() {
            let t: IdxTable = from_doc(&doc)?;
            self.tables.insert(
                t.table_id,
                Head {
                    namespace_id: t.namespace_id,
                    name: t.name,
                    active: t.active,
                    pending: t.pending,
                },
            );
        }
        // Those that are not tables are views, or gone.
        let names: Vec<String> = missing
            .iter()
            .filter(|id| !self.tables.contains_key(id))
            .map(|id| db.doc_name(IDX_VIEWS, &id.to_string()))
            .collect();
        for doc in txn.get_many(&names).await?.into_iter().flatten() {
            let v: IdxView = from_doc(&doc)?;
            self.tables.insert(
                v.view_id,
                Head {
                    namespace_id: v.namespace_id,
                    name: v.name,
                    active: v.active,
                    pending: v.pending,
                },
            );
        }
        let namespaces = self
            .tables
            .values()
            .map(|t| t.namespace_id)
            .filter(|n| !self.namespaces.contains_key(&n.to_string()))
            .collect::<Vec<_>>();
        self.namespaces
            .extend(namespace_paths(db, namespaces).await?);
        Ok(())
    }

    /// Whether the table or view exists, and is not soft-deleted unless `include_deleted`. A
    /// part whose head document is gone belongs to a dropped one whose cleanup is pending.
    fn live(&self, id: Option<uuid::Uuid>, include_deleted: bool) -> bool {
        id.and_then(|id| self.tables.get(&id))
            .is_some_and(|t| include_deleted || t.active)
    }

    fn label(&self, id: uuid::Uuid) -> String {
        let Some(t) = self.tables.get(&id) else {
            return id.to_string();
        };
        let mut name = self
            .namespaces
            .get(&t.namespace_id.to_string())
            .cloned()
            .unwrap_or_default();
        name.push(t.name.clone());
        let mut label = name.join(".");
        if !t.active {
            label.push_str(" (deleted)");
        }
        if t.pending {
            label.push_str(" (pending)");
        }
        label
    }
}

/// Namespace ID to path.
async fn namespace_paths(
    db: &Db,
    ids: impl IntoIterator<Item = NamespaceId>,
) -> anyhow::Result<HashMap<String, Vec<String>>> {
    let ids: BTreeSet<String> = ids.into_iter().map(|n| n.to_string()).collect();
    let names: Vec<String> = ids.iter().map(|id| db.doc_name(NAMESPACES, id)).collect();
    let mut out = HashMap::new();
    for doc in Txn::latest(db.clone())
        .get_many(&names)
        .await?
        .into_iter()
        .flatten()
    {
        let n: NamespaceDoc = from_doc(&doc)?;
        out.insert(n.namespace_id.to_string(), n.name);
    }
    Ok(out)
}

async fn resolve_optional_warehouse(
    db: &Db,
    warehouse: Option<&str>,
) -> anyhow::Result<Option<WarehouseId>> {
    match warehouse {
        Some(w) => Ok(Some(resolve_warehouse(db, w).await?)),
        None => Ok(None),
    }
}

async fn resolve_warehouse(db: &Db, warehouse: &str) -> anyhow::Result<WarehouseId> {
    if let Ok(id) = warehouse.parse::<uuid::Uuid>() {
        return Ok(WarehouseId::from(id));
    }
    let docs = Search::default()
        .eq("name", string(warehouse))
        .run(db, WAREHOUSES)
        .await?;
    match docs.as_slice() {
        [] => anyhow::bail!("No warehouse is named `{warehouse}`"),
        [doc] => Ok(from_doc::<WarehouseDoc>(doc)?.warehouse_id),
        _ => anyhow::bail!(
            "Several projects have a warehouse named `{warehouse}`; give its ID instead"
        ),
    }
}

/// A table by ID, or by `namespace.table` through the name markers of live tables.
async fn resolve_table(
    db: &Db,
    warehouse_id: WarehouseId,
    table: &str,
) -> anyhow::Result<uuid::Uuid> {
    if let Ok(id) = table.parse::<uuid::Uuid>() {
        return Ok(id);
    }
    let parts: Vec<String> = table.split('.').map(str::to_string).collect();
    let Some((name, path)) = parts.split_last().filter(|(_, path)| !path.is_empty()) else {
        anyhow::bail!("Give the table as `namespace.table`, or its ID");
    };
    let mut txn = Txn::latest(db.clone());
    let marker = |doc: Option<Document>| -> anyhow::Result<Option<String>> {
        Ok(doc
            .map(|d| from_doc::<NameMarker>(&d))
            .transpose()?
            .map(|m| m.id))
    };
    let namespace = marker(
        txn.get(&db.doc_name(NAMESPACE_NAMES, &namespace_name_id(warehouse_id, path)))
            .await?,
    )?
    .ok_or_else(|| anyhow::anyhow!("No namespace `{}`", path.join(".")))?;
    let namespace_id = NamespaceId::from(namespace.parse::<uuid::Uuid>()?);
    let id = marker(
        txn.get(&db.doc_name(
            TABULAR_NAMES,
            &tabular_name_id(warehouse_id, namespace_id, name),
        ))
        .await?,
    )?
    .ok_or_else(|| anyhow::anyhow!("No table `{table}`"))?;
    let id: uuid::Uuid = id.parse()?;
    let t = txn
        .get(&db.doc_name(TABULARS, &id.to_string()))
        .await?
        .map(|d| from_doc::<TabularDoc>(&d))
        .transpose()?;
    if t.is_none_or(|t| t.typ != TABLE) {
        anyhow::bail!("`{table}` is not a table");
    }
    Ok(id)
}

/// RFC 3339, or a date (midnight UTC), as milliseconds.
fn parse_time(s: &str) -> anyhow::Result<i64> {
    if let Ok(t) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(t.timestamp_millis());
    }
    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|_| anyhow::anyhow!("`{s}` is neither an RFC 3339 time nor a date"))?;
    Ok(date
        .and_hms_opt(0, 0, 0)
        .unwrap_or_default()
        .and_utc()
        .timestamp_millis())
}

fn time(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// `field op number` on a [`SummaryNumbers`] field, with `op` one of `>=`, `<=`, `>`, `<`,
/// `=`, `!=`. Field names may use dashes, as the summary does.
#[derive(Debug, Clone, PartialEq)]
struct Condition {
    field: String,
    op: &'static str,
    value: i64,
}

impl Condition {
    fn parse(s: &str) -> anyhow::Result<Self> {
        const OPS: [&str; 6] = [">=", "<=", "!=", ">", "<", "="];
        let (at, op) = OPS
            .iter()
            .filter_map(|op| s.find(op).map(|at| (at, *op)))
            .min_by_key(|(at, op)| (*at, std::cmp::Reverse(op.len())))
            .ok_or_else(|| anyhow::anyhow!("`{s}` has no comparison such as `>=`"))?;
        let field = s[..at].trim().replace('-', "_");
        let known = serde_json::to_value(SummaryNumbers::default())?;
        if known.get(&field).is_none() {
            let names: Vec<&String> = known
                .as_object()
                .map(|o| o.keys().collect())
                .unwrap_or_default();
            anyhow::bail!("`{field}` is not a summary number; one of {names:?}");
        }
        let value = s[at + op.len()..].trim().parse()?;
        Ok(Self { field, op, value })
    }

    fn matches(&self, summary: &SummaryNumbers) -> bool {
        let Some(actual) = serde_json::to_value(summary)
            .ok()
            .and_then(|v| v.get(&self.field).and_then(Json::as_i64))
        else {
            return false;
        };
        match self.op {
            ">=" => actual >= self.value,
            "<=" => actual <= self.value,
            ">" => actual > self.value,
            "<" => actual < self.value,
            "=" => actual == self.value,
            _ => actual != self.value,
        }
    }
}
