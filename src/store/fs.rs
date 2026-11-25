//! Firestore access: document names, serde conversion, queries, and the transaction overlay.
//!
//! Firestore transactions buffer writes on the client until `Commit`, so a read inside a
//! transaction does not see the transaction's own earlier writes. [`Txn`] keeps the pending
//! writes and answers reads from them first; queries are re-filtered locally against them.

use std::{cmp::Ordering, collections::HashMap};

use firestore::gcloud_sdk::{
    google::firestore::v1::{
        self as pb, BatchGetDocumentsRequest, BeginTransactionRequest, CommitRequest,
        RollbackRequest, RunQueryRequest, StructuredQuery, TransactionOptions,
        batch_get_documents_request, batch_get_documents_response, precondition, run_query_request,
        structured_query::{self, field_filter, filter},
        transaction_options,
        value::ValueType,
        write,
    },
    tonic,
};
use lakekeeper::{
    api::ErrorModel,
    service::{COMMIT_STATE_UNKNOWN_ERROR_TYPE, CatalogBackendError, CatalogBackendErrorType},
    tracing,
};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) type Document = pb::Document;
pub(crate) type Value = pb::Value;

/// A Firestore database plus the collection prefix of this catalog.
#[derive(Clone, Debug)]
pub(crate) struct Db {
    db: firestore::FirestoreDb,
    prefix: String,
}

impl Db {
    pub(crate) async fn connect(
        project_id: String,
        database_id: Option<String>,
        prefix: String,
    ) -> anyhow::Result<Self> {
        let mut options = firestore::FirestoreDbOptions::new(project_id);
        if let Some(database_id) = database_id {
            options = options.with_database_id(database_id);
        }
        let db = firestore::FirestoreDb::with_options(options).await?;
        Ok(Self { db, prefix })
    }

    fn database(&self) -> String {
        self.db.get_database_path().clone()
    }

    fn documents(&self) -> &str {
        self.db.get_documents_path()
    }

    fn collection_id(&self, collection: &str) -> String {
        format!("{}{collection}", self.prefix)
    }

    /// The full name of document `id` in top-level `collection`.
    pub(crate) fn doc_name(&self, collection: &str, id: &str) -> String {
        format!(
            "{}/{}/{}",
            self.documents(),
            self.collection_id(collection),
            id
        )
    }

    /// Run `query` on top-level `collection` outside any transaction, for reads beyond what
    /// [`Query`] covers. No pending writes overlay it.
    pub(crate) async fn run_query(
        &self,
        collection: &str,
        mut query: StructuredQuery,
    ) -> Result<Vec<Document>, CatalogBackendError> {
        query.from = vec![structured_query::CollectionSelector {
            collection_id: self.collection_id(collection),
            all_descendants: false,
        }];
        let mut stream = self
            .client()
            .run_query(RunQueryRequest {
                parent: self.documents().to_string(),
                explain_options: None,
                request_options: None,
                query_type: Some(run_query_request::QueryType::StructuredQuery(query)),
                consistency_selector: None,
            })
            .await
            .map_err(status_error)?
            .into_inner();
        let mut docs = Vec::new();
        while let Some(response) = stream.message().await.map_err(status_error)? {
            docs.extend(response.document);
        }
        #[cfg(test)]
        cost::query(collection, docs.len());
        Ok(docs)
    }

    /// Listen to `query` on top-level `collection` as target `target_id`, from the start or
    /// after `resume_token`. The stream stays open until the server ends it.
    pub(crate) async fn listen(
        &self,
        collection: &str,
        mut query: StructuredQuery,
        target_id: i32,
        resume_token: Option<Vec<u8>>,
    ) -> Result<tonic::codec::Streaming<pb::ListenResponse>, tonic::Status> {
        use futures::StreamExt as _;
        query.from = vec![structured_query::CollectionSelector {
            collection_id: self.collection_id(collection),
            all_descendants: false,
        }];
        let request = pb::ListenRequest {
            database: self.database(),
            labels: HashMap::new(),
            request_options: None,
            target_change: Some(pb::listen_request::TargetChange::AddTarget(pb::Target {
                target_id,
                once: false,
                expected_count: None,
                target_type: Some(pb::target::TargetType::Query(pb::target::QueryTarget {
                    parent: self.documents().to_string(),
                    query_type: Some(pb::target::query_target::QueryType::StructuredQuery(query)),
                })),
                resume_type: resume_token.map(pb::target::ResumeType::ResumeToken),
            })),
        };
        // The request stream must stay open: closing it ends the listen.
        let requests = futures::stream::iter([request]).chain(futures::stream::pending());
        Ok(self.client().listen(requests).await?.into_inner())
    }

    fn client(
        &self,
    ) -> firestore::gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient<
        firestore::gcloud_sdk::GoogleAuthMiddleware,
    > {
        self.db.client().get()
    }
}

/// Encode a natural key for use inside a document ID: `/` is not allowed there.
pub(crate) fn encode_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len());
    for c in key.chars() {
        match c {
            '%' => out.push_str("%25"),
            '/' => out.push_str("%2F"),
            c => out.push(c),
        }
    }
    out
}

/// The case-insensitive form of a name, matching Postgres' `case_insensitive` collation.
pub(crate) fn fold_case(name: &str) -> String {
    name.to_lowercase()
}

pub(crate) fn backend_error(
    e: impl std::error::Error + Send + Sync + 'static,
) -> CatalogBackendError {
    CatalogBackendError::new_unexpected(e)
}

fn status_error(status: tonic::Status) -> CatalogBackendError {
    let r#type = if status.code() == tonic::Code::Aborted {
        CatalogBackendErrorType::ConcurrentModification
    } else {
        CatalogBackendErrorType::Unexpected
    };
    CatalogBackendError::new(status, r#type)
}

pub(crate) fn to_doc<T: Serialize>(
    name: String,
    value: &T,
) -> Result<Document, CatalogBackendError> {
    firestore::firestore_document_from_serializable(name, value).map_err(backend_error)
}

pub(crate) fn from_doc<T: DeserializeOwned>(doc: &Document) -> Result<T, CatalogBackendError> {
    firestore::firestore_document_to_serializable(doc).map_err(|e| {
        backend_error(e).append_detail(format!("Cannot decode Firestore document {}", doc.name))
    })
}

pub(crate) fn string_value(s: impl Into<String>) -> Value {
    Value {
        value_type: Some(ValueType::StringValue(s.into())),
    }
}

pub(crate) fn bool_value(b: bool) -> Value {
    Value {
        value_type: Some(ValueType::BooleanValue(b)),
    }
}

pub(crate) fn int_value(i: i64) -> Value {
    Value {
        value_type: Some(ValueType::IntegerValue(i)),
    }
}

/// How a [`Query`] filter compares a field with its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Equal,
    /// The field is an array that contains the value.
    ArrayContains,
    /// The field is at most the value. Firestore requires the query to order by this field
    /// first.
    LessThanOrEqual,
}

/// A query on one top-level collection: filters joined by AND, ordered by one field
/// (then by document name), with an optional cursor and limit. Kept this small so that the
/// overlay can evaluate it locally.
#[derive(Debug, Clone)]
pub(crate) struct Query {
    pub(crate) collection: &'static str,
    pub(crate) filters: Vec<(&'static str, Op, Value)>,
    pub(crate) order_by: Option<&'static str>,
    /// Return only documents strictly after this `(order value, document name)`.
    pub(crate) start_after: Option<(Value, String)>,
    pub(crate) limit: Option<usize>,
}

impl Query {
    pub(crate) fn new(collection: &'static str) -> Self {
        Self {
            collection,
            filters: Vec::new(),
            order_by: None,
            start_after: None,
            limit: None,
        }
    }

    #[must_use]
    pub(crate) fn eq(mut self, field: &'static str, value: Value) -> Self {
        self.filters.push((field, Op::Equal, value));
        self
    }

    #[must_use]
    pub(crate) fn array_contains(mut self, field: &'static str, value: Value) -> Self {
        self.filters.push((field, Op::ArrayContains, value));
        self
    }

    #[must_use]
    pub(crate) fn at_most(mut self, field: &'static str, value: Value) -> Self {
        self.filters.push((field, Op::LessThanOrEqual, value));
        self
    }

    #[must_use]
    pub(crate) fn order_by(mut self, field: &'static str) -> Self {
        self.order_by = Some(field);
        self
    }

    #[must_use]
    pub(crate) fn start_after(mut self, cursor: Option<(Value, String)>) -> Self {
        self.start_after = cursor;
        self
    }

    #[must_use]
    pub(crate) fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    fn matches(&self, doc: &Document) -> bool {
        self.filters.iter().all(|(field, op, value)| {
            let actual = doc.fields.get(*field);
            match op {
                Op::Equal => actual == Some(value),
                Op::ArrayContains => matches!(
                    actual.and_then(|v| v.value_type.as_ref()),
                    Some(ValueType::ArrayValue(a)) if a.values.contains(value)
                ),
                Op::LessThanOrEqual => {
                    actual.is_some() && value_cmp(actual, Some(value)) != Ordering::Greater
                }
            }
        }) && match (&self.start_after, self.order_by) {
            (Some((value, name)), Some(field)) => {
                sort_key_cmp(doc, field, value, name) == Ordering::Greater
            }
            _ => true,
        }
    }

    fn to_structured(&self, db: &Db, extra: usize) -> StructuredQuery {
        let field_filter = |field: &str, op: Op, value: &Value| structured_query::Filter {
            filter_type: Some(filter::FilterType::FieldFilter(
                structured_query::FieldFilter {
                    field: Some(structured_query::FieldReference {
                        field_path: field.to_string(),
                    }),
                    op: match op {
                        Op::Equal => field_filter::Operator::Equal,
                        Op::ArrayContains => field_filter::Operator::ArrayContains,
                        Op::LessThanOrEqual => field_filter::Operator::LessThanOrEqual,
                    }
                    .into(),
                    value: Some(value.clone()),
                },
            )),
        };
        let r#where = match self.filters.as_slice() {
            [] => None,
            [(field, op, value)] => Some(field_filter(field, *op, value)),
            filters => Some(structured_query::Filter {
                filter_type: Some(filter::FilterType::CompositeFilter(
                    structured_query::CompositeFilter {
                        op: structured_query::composite_filter::Operator::And.into(),
                        filters: filters
                            .iter()
                            .map(|(f, op, v)| field_filter(f, *op, v))
                            .collect(),
                    },
                )),
            }),
        };
        let order = |field: &str| structured_query::Order {
            field: Some(structured_query::FieldReference {
                field_path: field.to_string(),
            }),
            direction: structured_query::Direction::Ascending.into(),
        };
        let order_by = match self.order_by {
            Some(field) => vec![order(field), order("__name__")],
            None => vec![],
        };
        let start_at = match (&self.start_after, self.order_by) {
            (Some((value, name)), Some(_)) => Some(pb::Cursor {
                values: vec![
                    value.clone(),
                    Value {
                        value_type: Some(ValueType::ReferenceValue(name.clone())),
                    },
                ],
                before: false,
            }),
            _ => None,
        };
        StructuredQuery {
            from: vec![structured_query::CollectionSelector {
                collection_id: db.collection_id(self.collection),
                all_descendants: false,
            }],
            r#where,
            order_by,
            start_at,
            limit: self
                .limit
                .map(|l| i32::try_from(l + extra).unwrap_or(i32::MAX)),
            ..Default::default()
        }
    }
}

fn value_cmp(a: Option<&Value>, b: Option<&Value>) -> Ordering {
    use ValueType::{IntegerValue, StringValue};
    match (
        a.and_then(|v| v.value_type.as_ref()),
        b.and_then(|v| v.value_type.as_ref()),
    ) {
        (Some(IntegerValue(a)), Some(IntegerValue(b))) => a.cmp(b),
        (Some(StringValue(a)), Some(StringValue(b))) => a.cmp(b),
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        // Only integer and string order fields are used.
        _ => Ordering::Equal,
    }
}

fn sort_key_cmp(doc: &Document, field: &str, value: &Value, name: &str) -> Ordering {
    value_cmp(doc.fields.get(field), Some(value)).then_with(|| doc.name.as_str().cmp(name))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Reads through the state: each read sees the latest committed data.
    Latest,
    ReadOnly,
    ReadWrite,
}

#[derive(Debug)]
struct PendingWrite {
    /// `None` deletes the document.
    doc: Option<Document>,
    must_not_exist: bool,
}

/// A Firestore transaction with read-your-writes.
#[derive(Debug)]
pub(crate) struct Txn {
    db: Db,
    mode: Mode,
    /// Started lazily by the first read.
    id: Option<Vec<u8>>,
    pending: HashMap<String, PendingWrite>,
    order: Vec<String>,
}

impl Txn {
    pub(crate) fn latest(db: Db) -> Self {
        Self::new(db, Mode::Latest)
    }

    pub(crate) fn read_only(db: Db) -> Self {
        Self::new(db, Mode::ReadOnly)
    }

    pub(crate) fn read_write(db: Db) -> Self {
        Self::new(db, Mode::ReadWrite)
    }

    fn new(db: Db, mode: Mode) -> Self {
        Self {
            db,
            mode,
            id: None,
            pending: HashMap::new(),
            order: Vec::new(),
        }
    }

    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    fn options(&self) -> Option<TransactionOptions> {
        let mode = match self.mode {
            Mode::Latest => return None,
            Mode::ReadOnly => {
                transaction_options::Mode::ReadOnly(transaction_options::ReadOnly::default())
            }
            Mode::ReadWrite => {
                transaction_options::Mode::ReadWrite(transaction_options::ReadWrite::default())
            }
        };
        Some(TransactionOptions { mode: Some(mode) })
    }

    /// Start the transaction if it has not been started. Used before a read whose RPC cannot
    /// start one itself.
    async fn ensure_started(&mut self) -> Result<(), CatalogBackendError> {
        if self.id.is_some() {
            return Ok(());
        }
        let Some(options) = self.options() else {
            return Ok(());
        };
        let response = self
            .db
            .client()
            .begin_transaction(BeginTransactionRequest {
                database: self.db.database(),
                options: Some(options),
                request_options: None,
            })
            .await
            .map_err(status_error)?;
        self.id = Some(response.into_inner().transaction);
        Ok(())
    }

    pub(crate) async fn get(
        &mut self,
        name: &str,
    ) -> Result<Option<Document>, CatalogBackendError> {
        Ok(self.get_many(&[name.to_string()]).await?.pop().flatten())
    }

    /// The documents `names`, in order; `None` for a missing one.
    pub(crate) async fn get_many(
        &mut self,
        names: &[String],
    ) -> Result<Vec<Option<Document>>, CatalogBackendError> {
        let to_fetch: Vec<String> = names
            .iter()
            .filter(|n| !self.pending.contains_key(*n))
            .cloned()
            .collect();
        let mut fetched: HashMap<String, Document> = HashMap::new();
        #[cfg(test)]
        cost::read_documents(&to_fetch);
        if !to_fetch.is_empty() {
            let consistency_selector = match (&self.id, self.options()) {
                (Some(id), _) => {
                    Some(batch_get_documents_request::ConsistencySelector::Transaction(id.clone()))
                }
                (None, Some(options)) => {
                    Some(batch_get_documents_request::ConsistencySelector::NewTransaction(options))
                }
                (None, None) => None,
            };
            let mut stream = self
                .db
                .client()
                .batch_get_documents(BatchGetDocumentsRequest {
                    database: self.db.database(),
                    documents: to_fetch,
                    mask: None,
                    request_options: None,
                    consistency_selector,
                })
                .await
                .map_err(status_error)?
                .into_inner();
            while let Some(response) = stream.message().await.map_err(status_error)? {
                if !response.transaction.is_empty() {
                    self.id = Some(response.transaction);
                }
                if let Some(batch_get_documents_response::Result::Found(doc)) = response.result {
                    fetched.insert(doc.name.clone(), doc);
                }
            }
        }
        Ok(names
            .iter()
            .map(|name| match self.pending.get(name) {
                Some(w) => w.doc.clone(),
                None => fetched.get(name).cloned(),
            })
            .collect())
    }

    pub(crate) async fn query(
        &mut self,
        query: &Query,
    ) -> Result<Vec<Document>, CatalogBackendError> {
        let collection_prefix = format!(
            "{}/{}/",
            self.db.documents(),
            self.db.collection_id(query.collection)
        );
        let touched: Vec<&String> = self
            .order
            .iter()
            .filter(|n| n.starts_with(&collection_prefix))
            .collect();
        let structured = query.to_structured(&self.db, touched.len());

        let consistency_selector = match (&self.id, self.options()) {
            (Some(id), _) => Some(run_query_request::ConsistencySelector::Transaction(
                id.clone(),
            )),
            (None, Some(options)) => Some(run_query_request::ConsistencySelector::NewTransaction(
                options,
            )),
            (None, None) => None,
        };
        let mut stream = self
            .db
            .client()
            .run_query(RunQueryRequest {
                parent: self.db.documents().to_string(),
                explain_options: None,
                request_options: None,
                query_type: Some(run_query_request::QueryType::StructuredQuery(structured)),
                consistency_selector,
            })
            .await
            .map_err(status_error)?
            .into_inner();
        let mut docs = Vec::new();
        while let Some(response) = stream.message().await.map_err(status_error)? {
            if !response.transaction.is_empty() {
                self.id = Some(response.transaction);
            }
            if let Some(doc) = response.document {
                docs.push(doc);
            }
        }

        #[cfg(test)]
        cost::query(query.collection, docs.len());
        if touched.is_empty() {
            return Ok(docs);
        }
        // Overlay: drop stale versions of written documents, add the written ones that match.
        docs.retain(|d| !self.pending.contains_key(&d.name));
        for name in touched {
            if let Some(doc) = self.pending.get(name).and_then(|w| w.doc.as_ref())
                && query.matches(doc)
            {
                docs.push(doc.clone());
            }
        }
        if let Some(field) = query.order_by {
            docs.sort_by(|a, b| {
                value_cmp(a.fields.get(field), b.fields.get(field))
                    .then_with(|| a.name.cmp(&b.name))
            });
        }
        if let Some(limit) = query.limit {
            docs.truncate(limit);
        }
        Ok(docs)
    }

    fn push(&mut self, name: String, doc: Option<Document>, must_not_exist: bool) {
        debug_assert_eq!(
            self.mode,
            Mode::ReadWrite,
            "write outside a write transaction"
        );
        if let Some(existing) = self.pending.get_mut(&name) {
            // One write per document and commit; a create stays a create.
            existing.doc = doc;
        } else {
            self.order.push(name.clone());
            self.pending.insert(
                name,
                PendingWrite {
                    doc,
                    must_not_exist,
                },
            );
        }
    }

    /// Write `doc`, replacing any existing document.
    pub(crate) fn set(&mut self, doc: Document) {
        self.push(doc.name.clone(), Some(doc), false);
    }

    /// Write `doc`; the commit fails if it already exists.
    pub(crate) fn create(&mut self, doc: Document) {
        self.push(doc.name.clone(), Some(doc), true);
    }

    pub(crate) fn delete(&mut self, name: String) {
        self.push(name, None, false);
    }

    fn writes(&mut self) -> Vec<pb::Write> {
        let mut pending = std::mem::take(&mut self.pending);
        std::mem::take(&mut self.order)
            .into_iter()
            .filter_map(|name| {
                let w = pending.remove(&name)?;
                let current_document = w.must_not_exist.then_some(pb::Precondition {
                    condition_type: Some(precondition::ConditionType::Exists(false)),
                });
                let operation = match w.doc {
                    Some(doc) => write::Operation::Update(doc),
                    // Created and deleted again in this transaction: nothing to write.
                    None if w.must_not_exist => return None,
                    None => write::Operation::Delete(name),
                };
                Some(pb::Write {
                    update_mask: None,
                    update_transforms: Vec::new(),
                    current_document,
                    operation: Some(operation),
                })
            })
            .collect()
    }

    /// # Errors
    /// An error of type [`COMMIT_STATE_UNKNOWN_ERROR_TYPE`] if the outcome is unknown.
    pub(crate) async fn commit(mut self) -> Result<(), ErrorModel> {
        let writes = self.writes();
        if writes.is_empty() {
            return self.rollback().await.map_err(ErrorModel::from);
        }
        #[cfg(test)]
        let counted = writes.clone();
        let result = self
            .db
            .client()
            .commit(CommitRequest {
                database: self.db.database(),
                writes,
                transaction: self.id.take().unwrap_or_default(),
                request_options: None,
            })
            .await;
        match result {
            Ok(_) => {
                #[cfg(test)]
                cost::committed(&counted);
                Ok(())
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::DeadlineExceeded | tonic::Code::Unavailable | tonic::Code::Unknown
                ) =>
            {
                Err(ErrorModel::internal(
                    "Outcome of the transaction commit is unknown",
                    COMMIT_STATE_UNKNOWN_ERROR_TYPE,
                    Some(Box::new(status)),
                ))
            }
            Err(status)
                if matches!(
                    status.code(),
                    tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition
                ) =>
            {
                // A `create` precondition failed (`ALREADY_EXISTS`): another transaction
                // created the document.
                Err(ErrorModel::conflict(
                    "A concurrent transaction created the same entity",
                    "ConcurrentModification",
                    Some(Box::new(status)),
                ))
            }
            Err(status) => Err(status_error(status).into()),
        }
    }

    pub(crate) async fn rollback(mut self) -> Result<(), CatalogBackendError> {
        let Some(id) = self.id.take() else {
            return Ok(());
        };
        self.db
            .client()
            .rollback(RollbackRequest {
                database: self.db.database(),
                transaction: id,
                request_options: None,
            })
            .await
            .map_err(status_error)?;
        Ok(())
    }

    /// Whether a read-only RPC works; used by health checks.
    pub(crate) async fn ping(&mut self) -> Result<(), CatalogBackendError> {
        let name = self
            .db
            .doc_name(super::docs::SERVER_COLLECTION, super::docs::SERVER_DOC);
        self.ensure_started().await?;
        self.get(&name).await.map(|_| ())
    }
}

impl Drop for Txn {
    /// A transaction dropped without commit or rollback, as on an error path, still holds its
    /// read locks until Firestore expires it, and other transactions wait for them. It is
    /// rolled back in the background instead.
    fn drop(&mut self) {
        let Some(id) = self.id.take() else {
            return;
        };
        let Ok(runtime) = lakekeeper::tokio::runtime::Handle::try_current() else {
            return;
        };
        let db = self.db.clone();
        runtime.spawn(async move {
            let request = RollbackRequest {
                database: db.database(),
                transaction: id,
                request_options: None,
            };
            if let Err(status) = db.client().rollback(request).await {
                tracing::debug!(%status, "Rollback of a dropped Firestore transaction failed");
            }
        });
    }
}

/// Counts of the operations Firestore bills, by collection, for cost measurements in tests.
/// Global: a measurement runs alone (`tests/projection_cost.rs`).
#[cfg(test)]
pub(crate) mod cost {
    use std::{collections::BTreeMap, sync::Mutex};

    use super::{pb, write};

    /// `(collection, operation)` to count; operations are `read`, `write` and `delete`.
    static COUNTS: Mutex<BTreeMap<(String, &'static str), u64>> = Mutex::new(BTreeMap::new());

    fn add(collection: &str, operation: &'static str, n: u64) {
        *COUNTS
            .lock()
            .unwrap()
            .entry((collection.to_string(), operation))
            .or_default() += n;
    }

    /// The collection of a document name, without a test's `t{uuid}_` prefix.
    fn collection_of(name: &str) -> String {
        let collection = name
            .split_once("/documents/")
            .and_then(|(_, rest)| rest.split('/').next())
            .unwrap_or_default();
        match collection.strip_prefix('t') {
            Some(rest) if rest.len() > 33 && rest.as_bytes()[32] == b'_' => rest[33..].to_string(),
            _ => collection.to_string(),
        }
    }

    /// A get bills a read per document asked for, found or not.
    pub(crate) fn read_documents(names: &[String]) {
        for name in names {
            add(&collection_of(name), "read", 1);
        }
    }

    /// A query bills a read per document returned, and one if none is.
    pub(crate) fn query(collection: &str, returned: usize) {
        add(
            collection,
            "read",
            u64::try_from(returned.max(1)).unwrap_or(u64::MAX),
        );
    }

    pub(crate) fn committed(writes: &[pb::Write]) {
        for w in writes {
            match &w.operation {
                Some(write::Operation::Update(doc)) => add(&collection_of(&doc.name), "write", 1),
                Some(write::Operation::Delete(name)) => add(&collection_of(name), "delete", 1),
                _ => {}
            }
        }
    }

    /// The counts since the last call.
    pub(crate) fn take() -> BTreeMap<(String, &'static str), u64> {
        std::mem::take(&mut *COUNTS.lock().unwrap())
    }
}
