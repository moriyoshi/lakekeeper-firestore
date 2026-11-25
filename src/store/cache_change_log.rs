//! The cache change log. Not written yet: see DESIGN.md.

use lakekeeper::service::cache_invalidation::{CacheChangeLog, ChangeLogError, PollOutcome};

/// A log that never reports a change. Correct only for a single instance, whose own writes
/// evict its caches directly.
#[derive(Debug)]
pub(crate) struct EmptyChangeLog;

#[async_trait::async_trait]
impl CacheChangeLog for EmptyChangeLog {
    async fn poll(&mut self) -> Result<PollOutcome, ChangeLogError> {
        Ok(PollOutcome::Changes(Vec::new()))
    }
}
