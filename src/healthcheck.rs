// Adapted from Lakekeeper v0.15.1, crates/lakekeeper-bin/src/healthcheck.rs.
// Copyright 2024-2026 Vakamo Inc. Licensed under the Apache License, Version 2.0:
// https://www.apache.org/licenses/LICENSE-2.0
// Modifications: the database check probes the Firestore catalog store instead of Postgres
// pools.

use anyhow::Context;
use lakekeeper::{
    CONFIG,
    service::health::{HealthExt, HealthState, HealthStatus},
    tracing,
};

use crate::store::FirestoreState;

pub(crate) async fn health(check_db: bool, check_server: bool) -> anyhow::Result<()> {
    tracing::info!("Checking health...");
    if check_db {
        match db_health_check().await {
            Ok(()) => {
                tracing::info!("Database is healthy.");
            }
            Err(details) => {
                tracing::info!(?details, "Database is not healthy.");
                std::process::exit(1);
            }
        }
    }

    if check_server {
        let client = reqwest::Client::new();
        let response = client
            .get(format!("http://localhost:{}/health", CONFIG.listen_port))
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            tracing::info!("Server is not healthy: StatusCode: '{}'", status);
            std::process::exit(1);
        }
        let body = response.json::<HealthState>().await?;
        // Fail with an error if the server is not healthy
        if matches!(body.health, HealthStatus::Healthy) {
            tracing::info!("Server is healthy.");
        } else {
            tracing::info!(?body, "Server is not healthy: StatusCode: '{}'", status,);
            std::process::exit(1);
        }
    }
    Ok(())
}

pub(crate) fn normalize_checks(
    check_all: bool,
    check_db: bool,
    check_server: bool,
) -> (bool, bool) {
    let check_db = check_db || check_all;
    let check_server = check_server || check_all;

    if !check_db && !check_server {
        (false, true)
    } else {
        (check_db, check_server)
    }
}

pub(crate) async fn db_health_check() -> anyhow::Result<()> {
    // The health check reads no metadata files.
    let files =
        crate::store::StorageFiles::new(crate::secrets::CatalogSecretStore::from_config().await?);
    let db = FirestoreState::from_config(std::sync::Arc::new(files))
        .await
        .with_context(|| "Connecting to Firestore failed.")?;
    db.update_health().await;
    let mut db_healthy = true;

    for h in db.health().await {
        tracing::info!("{:?}", h);
        db_healthy = db_healthy && matches!(h.status(), HealthStatus::Healthy);
    }
    if db_healthy {
        Ok(())
    } else {
        Err(anyhow::anyhow!("Database is not healthy."))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn bare_healthcheck_checks_server() {
        assert_eq!(super::normalize_checks(false, false, false), (false, true));
    }

    #[test]
    fn check_all_checks_db_and_server() {
        assert_eq!(super::normalize_checks(true, false, false), (true, true));
    }

    #[test]
    fn explicit_checks_are_preserved() {
        assert_eq!(super::normalize_checks(false, true, false), (true, false));
        assert_eq!(super::normalize_checks(false, false, true), (false, true));
    }
}
