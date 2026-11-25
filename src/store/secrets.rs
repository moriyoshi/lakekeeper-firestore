//! Storage secrets as Firestore documents, `secrets/{secret ID}`, holding the credential's JSON.
//!
//! Only credentials without secret material are kept: those that tell the server to use its
//! own identity (`gcp-system-identity`, `aws-system-identity`, `azure-system-identity`). A
//! credential with a key or password is refused, as Firestore documents are readable by
//! anyone who can read the database. On Cloud Run, the service account reaches the buckets.

use lakekeeper::{SecretId, api::ErrorModel, axum::http::StatusCode};
use serde::{Deserialize, Serialize};

use super::{
    connect_from_config,
    docs::{SECRETS, from_micros, now_micros},
    fs::{Db, Txn, from_doc, to_doc},
};

/// The credential types that carry no secret material.
const KEYLESS: [&str; 3] = [
    "gcp-system-identity",
    "aws-system-identity",
    "azure-system-identity",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecretDoc {
    /// JSON of the credential.
    secret: String,
    created_at: i64,
}

/// A stored secret: its JSON, and when it was created.
pub(crate) type StoredSecret = (serde_json::Value, chrono::DateTime<chrono::Utc>);

#[derive(Debug, Clone)]
pub(crate) struct FirestoreSecrets {
    db: Db,
}

fn internal(e: impl std::error::Error + Send + Sync + 'static) -> ErrorModel {
    ErrorModel::internal(
        "Secret store failure",
        "SecretStoreError",
        Some(Box::new(e)),
    )
}

impl FirestoreSecrets {
    pub(crate) async fn from_config() -> anyhow::Result<Self> {
        Ok(Self {
            db: connect_from_config().await?,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(db: Db) -> Self {
        Self { db }
    }

    fn name(&self, id: SecretId) -> String {
        self.db.doc_name(SECRETS, &id.to_string())
    }

    pub(crate) async fn get(&self, id: SecretId) -> Result<Option<StoredSecret>, ErrorModel> {
        let Some(doc) = Txn::latest(self.db.clone()).get(&self.name(id)).await? else {
            return Ok(None);
        };
        let doc: SecretDoc = from_doc(&doc)?;
        let value = serde_json::from_str(&doc.secret).map_err(internal)?;
        Ok(Some((value, from_micros(doc.created_at))))
    }

    /// Store `secret`, or refuse it if it carries secret material.
    pub(crate) async fn create(&self, secret: &serde_json::Value) -> Result<SecretId, ErrorModel> {
        let credential_type = secret
            .get("credential-type")
            .and_then(serde_json::Value::as_str);
        if !credential_type.is_some_and(|t| KEYLESS.contains(&t)) {
            return Err(ErrorModel::builder()
                .code(StatusCode::BAD_REQUEST.into())
                .message(format!(
                    "This catalog stores only credentials without secret material ({}); got `{}`. \
                     Grant the server's own identity access to the storage instead.",
                    KEYLESS.join(", "),
                    credential_type.unwrap_or("none"),
                ))
                .r#type("StorageCredentialNotStorable".to_string())
                .build());
        }
        let id = SecretId::from(uuid::Uuid::now_v7());
        let doc = SecretDoc {
            secret: serde_json::to_string(secret).map_err(internal)?,
            created_at: now_micros(),
        };
        let mut txn = Txn::read_write(self.db.clone());
        txn.create(to_doc(self.name(id), &doc)?);
        txn.commit().await?;
        Ok(id)
    }

    pub(crate) async fn delete(&self, id: SecretId) -> Result<(), ErrorModel> {
        let mut txn = Txn::read_write(self.db.clone());
        txn.delete(self.name(id));
        txn.commit().await
    }
}
