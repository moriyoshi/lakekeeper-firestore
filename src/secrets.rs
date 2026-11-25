//! Placeholder secret store.
//!
//! By default every operation fails, so warehouses whose storage profile needs a stored
//! credential cannot be created. `LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS=true` keeps secrets in
//! process memory instead, for local testing. Google Secret Manager is planned; see DESIGN.md.

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

type Memory = Arc<Mutex<HashMap<SecretId, (serde_json::Value, chrono::DateTime<chrono::Utc>)>>>;

#[derive(Debug, Clone)]
pub(crate) struct PlaceholderSecretStore {
    memory: Option<Memory>,
}

impl PlaceholderSecretStore {
    pub(crate) fn from_config() -> Self {
        let memory = crate::CONFIG_BIN.debug.in_memory_secrets.then(|| {
            tracing::warn!(
                "Storage secrets are kept in memory and are lost on restart (LAKEKEEPER__DEBUG__IN_MEMORY_SECRETS). For testing only."
            );
            Memory::default()
        });
        Self { memory }
    }

    fn memory(&self) -> Result<&Memory, ErrorModel> {
        self.memory.as_ref().ok_or_else(|| {
            ErrorModel::builder()
                .code(lakekeeper::axum::http::StatusCode::NOT_IMPLEMENTED.into())
                .message("This catalog has no secret store yet".to_string())
                .r#type("SecretStoreUnsupported".to_string())
                .build()
        })
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
impl SecretStore for PlaceholderSecretStore {
    async fn get_secret_by_id_impl<S: SecretInStorage + serde::de::DeserializeOwned>(
        &self,
        secret_id: SecretId,
    ) -> lakekeeper::api::Result<Option<Secret<S>>> {
        let entry = self
            .memory()?
            .lock()
            .expect("secret store lock poisoned")
            .get(&secret_id)
            .cloned();
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
        let secret_id = SecretId::from(uuid::Uuid::now_v7());
        self.memory()?
            .lock()
            .expect("secret store lock poisoned")
            .insert(secret_id, (value, chrono::Utc::now()));
        Ok(secret_id)
    }

    async fn delete_secret_impl(&self, secret_id: &SecretId) -> lakekeeper::api::Result<()> {
        self.memory()?
            .lock()
            .expect("secret store lock poisoned")
            .remove(secret_id);
        Ok(())
    }
}

#[async_trait]
impl HealthExt for PlaceholderSecretStore {
    async fn health(&self) -> Vec<Health> {
        vec![Health::now("secrets", HealthStatus::Healthy)]
    }

    async fn update_health(&self) {}
}
