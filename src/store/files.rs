//! Reading the metadata files the core writes to object storage.
//!
//! Firestore keeps only the metadata location of a table or view; loading reads the file, as an
//! Iceberg pointer-swap catalog does. The core writes every metadata file under a new name, and
//! before the transaction that points to it commits, and never rewrites one. A pointer read in
//! a transaction therefore always names a complete file that does not change. Staged tables
//! have no file yet; see [`super::table`].

use std::{str::FromStr, sync::Arc};

use lakekeeper::{
    SecretId,
    service::{
        CatalogBackendError, Location, SecretStore,
        storage::{StorageCredential, StorageProfile},
    },
};
use lakekeeper_io::LakekeeperStorage as _;
use serde::de::DeserializeOwned;

use super::{docs::WarehouseDoc, fs::backend_error};

fn error(what: String) -> CatalogBackendError {
    backend_error(std::io::Error::other(what))
}

/// Where metadata files are read from.
#[async_trait::async_trait]
pub(crate) trait MetadataFiles: std::fmt::Debug + Send + Sync {
    /// The stored bytes of the file at `location` in the storage of `warehouse`.
    async fn read(
        &self,
        warehouse: &WarehouseDoc,
        location: &Location,
    ) -> Result<Vec<u8>, CatalogBackendError>;
}

/// The warehouse's storage, reached with its stored credential, as the core reaches it.
#[derive(Debug, Clone)]
pub(crate) struct StorageFiles<S> {
    secrets: S,
}

impl<S> StorageFiles<S> {
    pub(crate) fn new(secrets: S) -> Self {
        Self { secrets }
    }
}

#[async_trait::async_trait]
impl<S: SecretStore> MetadataFiles for StorageFiles<S> {
    async fn read(
        &self,
        warehouse: &WarehouseDoc,
        location: &Location,
    ) -> Result<Vec<u8>, CatalogBackendError> {
        let profile: StorageProfile =
            serde_json::from_str(&warehouse.storage_profile).map_err(backend_error)?;
        let credential: Option<Arc<StorageCredential>> = match &warehouse.storage_secret_id {
            Some(id) => {
                let id = SecretId::from(uuid::Uuid::from_str(id).map_err(backend_error)?);
                let secret = self
                    .secrets
                    .require_storage_secret_by_id(id)
                    .await
                    .map_err(|e| {
                        error(format!(
                            "Cannot read the storage credential of warehouse {}: {}",
                            warehouse.warehouse_id, e.error.message
                        ))
                    })?;
                Some(secret.secret)
            }
            None => None,
        };
        let io = profile.file_io(credential.as_deref()).await.map_err(|e| {
            error(format!(
                "Cannot reach the storage of warehouse {}: {e}",
                warehouse.warehouse_id
            ))
        })?;
        let object = io
            .read(location.as_str())
            .await
            .map_err(|e| error(format!("Cannot read metadata file {location}: {e}")))?;
        Ok(object.bytes.into())
    }
}

/// Read and parse the metadata file at `location`, gunzipping a `.gz.metadata.json` file as the
/// core does.
pub(super) async fn read_metadata<T: DeserializeOwned + Send + 'static>(
    files: &dyn MetadataFiles,
    warehouse: &WarehouseDoc,
    location: &Location,
) -> Result<T, CatalogBackendError> {
    let bytes = files.read(warehouse, location).await?;
    let gzipped = location.as_str().ends_with(".gz.metadata.json");
    let what = location.to_string();
    // Metadata can be megabytes; parse it off the async workers.
    lakekeeper::tokio::task::spawn_blocking(move || {
        let parsed = if gzipped {
            serde_json::from_reader(flate2::read::GzDecoder::new(bytes.as_slice()))
        } else {
            serde_json::from_slice(&bytes)
        };
        parsed.map_err(|e| error(format!("Cannot parse metadata file {what}: {e}")))
    })
    .await
    .map_err(backend_error)?
}
