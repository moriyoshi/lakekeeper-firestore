// Adapted from Lakekeeper v0.15.1, crates/lakekeeper-bin/src/config.rs.
// Copyright 2024-2026 Vakamo Inc. Licensed under the Apache License, Version 2.0:
// https://www.apache.org/licenses/LICENSE-2.0
// Modifications: dropped `migrate_before_serve` (Firestore has no migrations); added the
// Firestore connection settings.

use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

pub(crate) static CONFIG_BIN: LazyLock<DynAppConfig> = LazyLock::new(get_config);

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub(crate) struct DynAppConfig {
    pub(crate) debug: DebugConfig,
    pub(crate) firestore: FirestoreConfig,
}

#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub(crate) struct DebugConfig {
    /// Run the serve command unless another command is specified.
    pub(crate) auto_serve: bool,
    /// Include file names and line numbers in log output.
    /// This is useful for debugging but can be disabled for cleaner logs in production.
    pub(crate) extended_logs: bool,
    /// Keep storage secrets in process memory, where a restart loses them. For local testing
    /// only, until a real secret store exists.
    pub(crate) in_memory_secrets: bool,
}

/// `LAKEKEEPER__FIRESTORE__*`. The emulator is selected by the standard
/// `FIRESTORE_EMULATOR_HOST` environment variable.
#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub(crate) struct FirestoreConfig {
    /// Google Cloud project that holds the Firestore database.
    pub(crate) project_id: Option<String>,
    /// Firestore database ID; `None` selects `(default)`.
    pub(crate) database_id: Option<String>,
    /// Prefix for every top-level collection, so several catalogs can share one database.
    pub(crate) collection_prefix: Option<String>,
    /// Workers of the `metadata_projection` task queue in this process; default 2, as for
    /// upstream's queues.
    pub(crate) projection_workers: Option<usize>,
}

fn get_config() -> DynAppConfig {
    let defaults = figment::providers::Serialized::defaults(DynAppConfig::default());

    #[cfg(not(test))]
    let prefixes = &["LAKEKEEPER__"];
    #[cfg(test)]
    let prefixes = &["LAKEKEEPER_TEST__"];

    let mut config = figment::Figment::from(defaults);
    for prefix in prefixes {
        let env = figment::providers::Env::prefixed(prefix).split("__");
        config = config.merge(env);
    }

    match config.extract::<DynAppConfig>() {
        Ok(c) => c,
        Err(e) => {
            panic!("Failed to extract Lakekeeper Binary config: {e}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_serve_env_vars() {
        figment::Jail::expect_with(|_jail| {
            let config = get_config();
            assert!(!config.debug.auto_serve);
            Ok(())
        });

        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__DEBUG__AUTO_SERVE", "true");
            let config = get_config();
            assert!(config.debug.auto_serve);
            Ok(())
        });
    }

    #[test]
    fn test_firestore_env_vars() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("LAKEKEEPER_TEST__FIRESTORE__PROJECT_ID", "demo-project");
            jail.set_env("LAKEKEEPER_TEST__FIRESTORE__COLLECTION_PREFIX", "lk_");
            let config = get_config();
            assert_eq!(config.firestore.project_id.as_deref(), Some("demo-project"));
            assert_eq!(config.firestore.database_id, None);
            assert_eq!(config.firestore.collection_prefix.as_deref(), Some("lk_"));
            Ok(())
        });
    }
}
