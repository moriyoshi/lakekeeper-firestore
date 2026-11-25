//! The cache change log: what each write changed in the cached data, so that other instances
//! evict it.
//!
//! A write records its changed keys in `cache_changes` documents, in its own transaction: one
//! document per domain and up to 1000 keys, with no keys for a whole domain. Each instance
//! reads them through a Firestore listener rather than by polling: the listener delivers each
//! committed document once, in consistent snapshots, which a query by a timestamp field
//! cannot promise (a field written in a transaction does not hold its commit time). Changes
//! wait in the listener until the poller takes them.
//!
//! The listener's query takes the documents recorded after ten minutes before it starts; the
//! documents in its first snapshot are the state the log starts at, so the window only bounds
//! that first read. A listener that loses its stream resumes from its last consistent point;
//! one that cannot resume starts again, and the next poll reports a reset.

use std::{collections::HashSet, time::Duration};

use firestore::gcloud_sdk::google::firestore::v1::{
    self as pb, StructuredQuery, listen_response::ResponseType, structured_query,
    target_change::TargetChangeType,
};
use lakekeeper::{
    service::{
        CatalogBackendError,
        cache_invalidation::{
            self, CacheChangeLog, ChangeLogError, ChangeSet, LoggedChange, PollOutcome, ResetReason,
        },
    },
    tokio::{
        self,
        sync::mpsc::{self, UnboundedReceiver, UnboundedSender, error::TryRecvError},
        task::JoinHandle,
    },
    tracing,
};

use super::{
    docs::{CACHE_CHANGES, CacheChangeDoc, now_micros},
    fs::{Db, Query, Txn, from_doc, int_value, to_doc},
};

const KEYS_PER_DOC: usize = 1000;
/// How far before its start a fresh listener reads; see the module docs.
const WINDOW: Duration = Duration::from_mins(10);
const TARGET_ID: i32 = 1;
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Documents deleted per commit when pruning, and commits per run.
const PRUNE_BATCH: usize = 400;
const PRUNE_BATCHES: usize = 25;

/// Record `change_set` in `txn`.
pub(super) fn record(txn: &mut Txn, change_set: &ChangeSet) -> Result<(), CatalogBackendError> {
    if !cache_invalidation::recording_enabled() || change_set.is_empty() {
        return Ok(());
    }
    let recorded_at = now_micros();
    let mut write = |domain: &str, keys: Vec<String>| {
        let doc = CacheChangeDoc {
            domain: domain.to_string(),
            keys,
            recorded_at,
        };
        let name = txn
            .db()
            .doc_name(CACHE_CHANGES, &uuid::Uuid::now_v7().to_string());
        txn.create(to_doc(name, &doc)?);
        Ok::<_, CatalogBackendError>(())
    };
    for (domain, keys) in change_set.encoded() {
        let Some(keys) = keys else {
            write(domain.as_str(), Vec::new())?;
            continue;
        };
        let keys: Vec<String> = keys.map(ToString::to_string).collect();
        for chunk in keys.chunks(KEYS_PER_DOC) {
            write(domain.as_str(), chunk.to_vec())?;
        }
    }
    Ok(())
}

/// Delete the documents recorded more than `retention` ago, up to 10 000 per run.
pub(super) async fn prune(db: &Db, retention: Duration) -> Result<u64, CatalogBackendError> {
    let cutoff = now_micros() - i64::try_from(retention.as_micros()).unwrap_or(i64::MAX);
    let mut deleted = 0;
    for _ in 0..PRUNE_BATCHES {
        let docs = Txn::latest(db.clone())
            .query(
                &Query::new(CACHE_CHANGES)
                    .at_most("recorded_at", int_value(cutoff))
                    .order_by("recorded_at")
                    .limit(Some(PRUNE_BATCH)),
            )
            .await?;
        let n = docs.len();
        // A transaction without reads commits as a plain batch, which another pruner's
        // deletes of the same documents cannot conflict with.
        let mut txn = Txn::read_write(db.clone());
        for doc in docs {
            txn.delete(doc.name);
        }
        txn.commit()
            .await
            .map_err(|e| CatalogBackendError::new_unexpected(std::io::Error::other(e.message)))?;
        deleted += n as u64;
        if n < PRUNE_BATCH {
            break;
        }
    }
    Ok(deleted)
}

/// What the listener tells the poller.
#[derive(Debug)]
enum Event {
    /// The first snapshot is current: the log starts here.
    Ready,
    /// Changes of one consistent snapshot.
    Changes(Vec<LoggedChange>),
    /// The listener started again from a new snapshot: what changed in between is unknown.
    Reset,
}

/// The change log of one instance, read by a listener task it starts on the first poll.
#[derive(Debug)]
pub(crate) struct FirestoreChangeLog {
    db: Db,
    events: Option<UnboundedReceiver<Event>>,
    listener: Option<JoinHandle<()>>,
    ready: bool,
}

impl FirestoreChangeLog {
    pub(crate) fn new(db: Db) -> Self {
        Self {
            db,
            events: None,
            listener: None,
            ready: false,
        }
    }
}

impl Drop for FirestoreChangeLog {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}

fn closed() -> ChangeLogError {
    ChangeLogError::Query(Box::new(std::io::Error::other(
        "The cache change log listener stopped",
    )))
}

#[async_trait::async_trait]
impl CacheChangeLog for FirestoreChangeLog {
    async fn poll(&mut self) -> Result<PollOutcome, ChangeLogError> {
        if self.events.is_none() {
            let (tx, rx) = mpsc::unbounded_channel();
            self.listener = Some(tokio::spawn(listen(self.db.clone(), tx)));
            self.events = Some(rx);
        }
        let events = self.events.as_mut().ok_or_else(closed)?;
        // The first poll waits for the listener's first snapshot (the poller bounds the wait).
        if !self.ready {
            return match events.recv().await {
                Some(Event::Ready) => {
                    self.ready = true;
                    Ok(PollOutcome::Changes(Vec::new()))
                }
                Some(event) => Err(ChangeLogError::Query(Box::new(std::io::Error::other(
                    format!("Unexpected first event of the change log listener: {event:?}"),
                )))),
                None => Err(closed()),
            };
        }
        let mut changes = Vec::new();
        loop {
            match events.try_recv() {
                Ok(Event::Changes(more)) => changes.extend(more),
                // Supersedes the changes before it; those after it stay for the next poll.
                Ok(Event::Reset) => return Ok(PollOutcome::Reset(ResetReason::ServerChanged)),
                Ok(Event::Ready) => {}
                Err(TryRecvError::Disconnected) if changes.is_empty() => return Err(closed()),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => {
                    return Ok(PollOutcome::Changes(changes));
                }
            }
        }
    }
}

/// The documents recorded after `since` (microseconds).
fn query(since: i64) -> StructuredQuery {
    StructuredQuery {
        r#where: Some(structured_query::Filter {
            filter_type: Some(structured_query::filter::FilterType::FieldFilter(
                structured_query::FieldFilter {
                    field: Some(structured_query::FieldReference {
                        field_path: "recorded_at".to_string(),
                    }),
                    op: structured_query::field_filter::Operator::GreaterThan.into(),
                    value: Some(int_value(since)),
                },
            )),
        }),
        ..Default::default()
    }
}

/// Where a listener (re)starts: the query of its target, and the token to resume it from.
struct Position {
    query: StructuredQuery,
    resume_token: Option<Vec<u8>>,
}

/// Listen until the poller goes away, restarting the stream when it ends.
async fn listen(db: Db, events: UnboundedSender<Event>) {
    let mut position: Option<Position> = None;
    let mut started = false;
    let mut known = HashSet::new();
    let mut backoff = Duration::from_secs(1);
    loop {
        let fresh = position.is_none();
        let Position {
            query,
            resume_token,
        } = position.take().unwrap_or_else(|| Position {
            query: query(now_micros() - i64::try_from(WINDOW.as_micros()).unwrap_or(0)),
            resume_token: None,
        });
        if fresh {
            known.clear();
        }
        let outcome = match db
            .listen(
                CACHE_CHANGES,
                query.clone(),
                TARGET_ID,
                resume_token.clone(),
            )
            .await
        {
            Ok(stream) => {
                let mut session = Session {
                    events: &events,
                    known: &mut known,
                    // Resumed: the poller has the documents known so far.
                    initialized: !fresh,
                    announce: if started { Event::Reset } else { Event::Ready },
                    current: false,
                    buffer: Vec::new(),
                    token: resume_token,
                };
                let outcome = session.run(stream).await;
                started |= session.initialized;
                if session.initialized {
                    backoff = Duration::from_secs(1);
                    // A stream that never became consistent cannot be resumed.
                    if let Some(token) = session.token {
                        position = Some(Position {
                            query,
                            resume_token: Some(token),
                        });
                    }
                }
                outcome
            }
            Err(status) => {
                // Not connected: try the same position again.
                if !fresh {
                    position = Some(Position {
                        query,
                        resume_token,
                    });
                }
                Err(status)
            }
        };
        if events.is_closed() {
            return;
        }
        match outcome {
            Ok(()) => tracing::debug!("The cache change log stream ended; listening again"),
            Err(status) => tracing::warn!(
                %status,
                "The cache change log stream failed; listening again in {backoff:?}"
            ),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One listen stream.
struct Session<'a> {
    events: &'a UnboundedSender<Event>,
    /// The documents of the target the poller has: the first snapshot's and those sent since.
    /// A document the server sends again (after a target reset, which the emulator sends on
    /// every change) is not new.
    known: &'a mut HashSet<String>,
    /// Whether the poller has the first snapshot.
    initialized: bool,
    /// What to send when a fresh stream's first snapshot is current.
    announce: Event,
    /// Whether the target has caught up with its first snapshot.
    current: bool,
    /// New documents since the last consistent snapshot, by name.
    buffer: Vec<(String, LoggedChange)>,
    /// The token of the last consistent snapshot.
    token: Option<Vec<u8>>,
}

impl Session<'_> {
    async fn run(
        &mut self,
        mut stream: firestore::gcloud_sdk::tonic::codec::Streaming<pb::ListenResponse>,
    ) -> Result<(), firestore::gcloud_sdk::tonic::Status> {
        while let Some(response) = stream.message().await? {
            if self.events.is_closed() {
                return Ok(());
            }
            match response.response_type {
                Some(ResponseType::DocumentChange(change)) => {
                    if !change.target_ids.contains(&TARGET_ID) {
                        continue;
                    }
                    let Some(doc) = change.document else { continue };
                    if self.known.contains(&doc.name)
                        || self.buffer.iter().any(|(name, _)| *name == doc.name)
                    {
                        continue;
                    }
                    match from_doc::<CacheChangeDoc>(&doc) {
                        Ok(change) => self.buffer.push((
                            doc.name,
                            LoggedChange {
                                domain: change.domain,
                                keys: change.keys,
                            },
                        )),
                        Err(error) => {
                            tracing::warn!(?error, "Skipping an unreadable cache change document");
                        }
                    }
                }
                Some(ResponseType::TargetChange(change)) => {
                    if !self.target_change(&change) {
                        return Ok(());
                    }
                }
                Some(
                    ResponseType::DocumentDelete(pb::DocumentDelete { document, .. })
                    | ResponseType::DocumentRemove(pb::DocumentRemove { document, .. }),
                ) => {
                    self.known.remove(&document);
                }
                // Filters count documents, which nothing here needs.
                Some(ResponseType::Filter(_)) | None => {}
            }
        }
        Ok(())
    }

    /// Handle a target change; `false` ends the stream.
    fn target_change(&mut self, change: &pb::TargetChange) -> bool {
        match TargetChangeType::try_from(change.target_change_type) {
            Ok(TargetChangeType::Current) => self.current = true,
            // The server sends the target's documents again; `known` tells the new ones.
            Ok(TargetChangeType::Reset) => {
                if !self.initialized {
                    self.buffer.clear();
                    self.current = false;
                }
            }
            Ok(TargetChangeType::Remove) => {
                tracing::warn!(cause = ?change.cause, "The cache change log target was removed");
                self.initialized = false;
                return false;
            }
            Ok(TargetChangeType::Add) | Err(_) => {}
            Ok(TargetChangeType::NoChange) => {
                // A consistent snapshot of every target.
                if change.target_ids.is_empty() && change.read_time.is_some() {
                    return self.consistent(change);
                }
            }
        }
        true
    }

    fn consistent(&mut self, change: &pb::TargetChange) -> bool {
        if self.initialized {
            if !change.resume_token.is_empty() {
                self.token = Some(change.resume_token.clone());
            }
            let buffer = std::mem::take(&mut self.buffer);
            if buffer.is_empty() {
                return true;
            }
            let mut changes = Vec::with_capacity(buffer.len());
            for (name, logged) in buffer {
                self.known.insert(name);
                changes.push(logged);
            }
            self.events.send(Event::Changes(changes)).is_ok()
        } else if self.current {
            // The documents of the first snapshot are where the log starts.
            if !change.resume_token.is_empty() {
                self.token = Some(change.resume_token.clone());
            }
            self.known
                .extend(std::mem::take(&mut self.buffer).into_iter().map(|(n, _)| n));
            self.initialized = true;
            let announce = std::mem::replace(&mut self.announce, Event::Reset);
            self.events.send(announce).is_ok()
        } else {
            true
        }
    }
}
