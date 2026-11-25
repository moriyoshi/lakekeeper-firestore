//! The Firestore catalog store.

use std::sync::Arc;

use lakekeeper::{
    api::ErrorModel,
    service::{
        CatalogBackendError, Result, StateOrTransaction, StateOrTransactionEnum, Transaction,
        health::{Health, HealthExt, HealthStatus},
    },
    tokio::sync::RwLock,
    tracing,
};

mod cache_change_log;
mod catalog;
mod docs;
mod endpoint_statistics;
mod files;
mod fs;
mod generic_table;
mod idempotency;
mod namespace;
mod project;
mod projection;
mod projection_store;
mod projection_task;
pub(crate) mod search;
mod server;
mod table;
mod tabular;
mod tag;
mod task_admin;
mod tasks;
#[cfg(test)]
mod tests;
mod user;
mod view;
mod warehouse;

pub(crate) use files::{MetadataFiles, StorageFiles};
use fs::{Db, Txn};
pub(crate) use projection_task::register as register_projection_queue;

use crate::CONFIG_BIN;

/// The [`lakekeeper::service::CatalogStore`] backed by Firestore.
#[derive(Debug, Clone)]
pub(crate) struct FirestoreBackend;

/// Shared handle to the Firestore database, and to the storage the metadata files are in.
#[derive(Debug, Clone)]
pub(crate) struct FirestoreState {
    db: Db,
    files: Arc<dyn MetadataFiles>,
    health: Arc<RwLock<Vec<Health>>>,
}

impl FirestoreState {
    pub(crate) async fn from_config(files: Arc<dyn MetadataFiles>) -> anyhow::Result<Self> {
        let db = connect_from_config().await?;
        Ok(Self {
            db,
            files,
            health: Arc::new(RwLock::new(vec![Health::now(
                "firestore",
                HealthStatus::Unknown,
            )])),
        })
    }

    /// A transaction-less handle whose reads see the latest committed data.
    pub(crate) fn latest(&self) -> Txn {
        Txn::latest(self.db.clone())
    }
}

#[async_trait::async_trait]
impl HealthExt for FirestoreState {
    async fn health(&self) -> Vec<Health> {
        self.health.read().await.clone()
    }

    async fn update_health(&self) {
        let status = match self.latest().ping().await {
            Ok(()) => HealthStatus::Healthy,
            Err(e) => {
                tracing::warn!(?e, "Firestore is unhealthy");
                HealthStatus::Unhealthy
            }
        };
        *self.health.write().await = vec![Health::now("firestore", status)];
    }
}

/// The database of the configuration.
async fn connect_from_config() -> anyhow::Result<Db> {
    let config = &CONFIG_BIN.firestore;
    let project_id = config
        .project_id
        .clone()
        .or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Set LAKEKEEPER__FIRESTORE__PROJECT_ID (or GOOGLE_CLOUD_PROJECT) to the Google Cloud project of the Firestore database"
            )
        })?;
    Db::connect(
        project_id,
        config.database_id.clone(),
        config.collection_prefix.clone().unwrap_or_default(),
    )
    .await
}

/// A catalog transaction.
#[derive(Debug)]
pub(crate) struct FirestoreTransaction {
    txn: Txn,
    files: Arc<dyn MetadataFiles>,
}

pub(crate) type FirestoreTransactionHandle<'a> = &'a mut FirestoreTransaction;

#[async_trait::async_trait]
impl Transaction<FirestoreState> for FirestoreTransaction {
    type Transaction<'a> = FirestoreTransactionHandle<'a>;

    async fn begin_write(db_state: FirestoreState) -> Result<Self> {
        Ok(Self {
            txn: Txn::read_write(db_state.db),
            files: db_state.files,
        })
    }

    async fn begin_read(db_state: FirestoreState) -> Result<Self> {
        Ok(Self {
            txn: Txn::read_only(db_state.db),
            files: db_state.files,
        })
    }

    async fn commit(self) -> Result<()> {
        self.txn.commit().await.map_err(Into::into)
    }

    async fn rollback(self) -> Result<()> {
        self.txn
            .rollback()
            .await
            .map_err(|e| ErrorModel::from(e).into())
    }

    fn transaction(&mut self) -> Self::Transaction<'_> {
        self
    }
}

impl<'txn> StateOrTransaction<FirestoreState, FirestoreTransactionHandle<'txn>> for FirestoreState {
    fn as_enum_mut(
        &mut self,
    ) -> StateOrTransactionEnum<'_, FirestoreState, FirestoreTransactionHandle<'txn>> {
        StateOrTransactionEnum::State(self.clone())
    }
}

impl<'txn> StateOrTransaction<FirestoreState, FirestoreTransactionHandle<'txn>>
    for FirestoreTransactionHandle<'txn>
{
    fn as_enum_mut(
        &mut self,
    ) -> StateOrTransactionEnum<'_, FirestoreState, FirestoreTransactionHandle<'txn>> {
        StateOrTransactionEnum::Transaction(self)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("`{0}` is not supported by the Firestore catalog store yet")]
pub(crate) struct Unsupported(&'static str);

/// The error of a store method that is not implemented yet.
pub(crate) fn unsupported(method: &'static str) -> CatalogBackendError {
    CatalogBackendError::new_unexpected(Unsupported(method))
}
