//! The secret store: storage credentials in Firestore (see `store::secrets`), which keeps only
//! credentials without secret material. `LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS=true` keeps any
//! credential in process memory instead, where a restart loses it, for local testing with keys.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use lakekeeper::{
    SecretId,
    api::ErrorModel,
    service::{
        SecretStore,
        health::{Health, HealthExt, HealthStatus},
        secrets::{Secret, SecretInStorage},
    },
    tracing,
};

use crate::store::FirestoreSecrets;

type Memory = Arc<Mutex<HashMap<SecretId, (serde_json::Value, chrono::DateTime<chrono::Utc>)>>>;

#[derive(Debug, Clone)]
enum Backend {
    Firestore(FirestoreSecrets),
    Memory(Memory),
}

#[derive(Debug, Clone)]
pub(crate) struct CatalogSecretStore {
    backend: Backend,
}

impl CatalogSecretStore {
    pub(crate) async fn from_config() -> anyhow::Result<Self> {
        let backend = if crate::CONFIG_BIN.debug.in_memory_secrets {
            tracing::warn!(
                "Storage secrets are kept in memory and are lost on restart (LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS). For testing only."
            );
            Backend::Memory(Memory::default())
        } else {
            Backend::Firestore(FirestoreSecrets::from_config().await?)
        };
        Ok(Self { backend })
    }
}

fn internal(e: impl std::error::Error + Send + Sync + 'static) -> ErrorModel {
    ErrorModel::internal(
        "Secret serialization failed",
        "SecretSerializationError",
        Some(Box::new(e)),
    )
}

#[async_trait]
impl SecretStore for CatalogSecretStore {
    async fn get_secret_by_id_impl<S: SecretInStorage + serde::de::DeserializeOwned>(
        &self,
        secret_id: SecretId,
    ) -> lakekeeper::api::Result<Option<Secret<S>>> {
        let entry = match &self.backend {
            Backend::Firestore(store) => store.get(secret_id).await?,
            Backend::Memory(memory) => memory
                .lock()
                .expect("secret store lock poisoned")
                .get(&secret_id)
                .cloned(),
        };
        let Some((value, created_at)) = entry else {
            return Ok(None);
        };
        Ok(Some(Secret {
            secret_id,
            secret: serde_json::from_value(value).map_err(internal)?,
            created_at,
            updated_at: None,
        }))
    }

    async fn create_secret_impl<
        S: SecretInStorage + Send + Sync + serde::Serialize + std::fmt::Debug,
    >(
        &self,
        secret: S,
    ) -> lakekeeper::api::Result<SecretId> {
        let value = serde_json::to_value(&secret).map_err(internal)?;
        match &self.backend {
            Backend::Firestore(store) => Ok(store.create(&value).await?),
            Backend::Memory(memory) => {
                let secret_id = SecretId::from(uuid::Uuid::now_v7());
                memory
                    .lock()
                    .expect("secret store lock poisoned")
                    .insert(secret_id, (value, chrono::Utc::now()));
                Ok(secret_id)
            }
        }
    }

    async fn delete_secret_impl(&self, secret_id: &SecretId) -> lakekeeper::api::Result<()> {
        match &self.backend {
            Backend::Firestore(store) => Ok(store.delete(*secret_id).await?),
            Backend::Memory(memory) => {
                memory
                    .lock()
                    .expect("secret store lock poisoned")
                    .remove(secret_id);
                Ok(())
            }
        }
    }
}

#[async_trait]
impl HealthExt for CatalogSecretStore {
    async fn health(&self) -> Vec<Health> {
        vec![Health::now("secrets", HealthStatus::Healthy)]
    }

    async fn update_health(&self) {}
}
