//! Storage secrets on the emulator.

use super::db;
use crate::store::FirestoreSecrets;

#[tokio::test]
async fn keyless_credentials_only() {
    let Some(db) = db().await else { return };
    let secrets = FirestoreSecrets::new(db);
    let identity = serde_json::json!({"type": "gcs", "credential-type": "gcp-system-identity"});
    let id = secrets.create(&identity).await.unwrap();
    let (stored, _) = secrets.get(id).await.unwrap().unwrap();
    assert_eq!(stored, identity);

    let key = serde_json::json!({
        "type": "gcs",
        "credential-type": "service-account-key",
        "key": {"private-key": "-----BEGIN PRIVATE KEY-----"},
    });
    let refused = secrets.create(&key).await.unwrap_err();
    assert_eq!(
        (refused.code, refused.r#type.as_str()),
        (400, "StorageCredentialNotStorable")
    );

    secrets.delete(id).await.unwrap();
    assert!(secrets.get(id).await.unwrap().is_none());
}
