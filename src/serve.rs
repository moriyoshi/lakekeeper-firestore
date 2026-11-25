// Adapted from Lakekeeper v0.15.1, crates/lakekeeper-bin/src/serve.rs.
// Copyright 2024-2026 Vakamo Inc. Licensed under the Apache License, Version 2.0:
// https://www.apache.org/licenses/LICENSE-2.0
// Modifications: serves the Firestore catalog store; only the allow-all authorizer; no
// endpoint statistics sink and no NATS/Kafka event backends.

use std::sync::Arc;

use lakekeeper::{
    limes::{Authenticator, AuthenticatorEnum},
    serve::{RegisterTaskQueueFn, ServeConfiguration, serve},
    service::{
        CatalogStore, SecretStore,
        authn::{BuiltInAuthenticators, get_default_authenticator_from_config},
        authz::{AllowAllAuthorizer, Authorizer},
        endpoint_statistics::EndpointStatisticsSink,
        events::{CloudEventBackend, EventDispatcher, maybe_tracing_cloud_event_backend},
    },
    tracing,
};

#[cfg(feature = "ui")]
use crate::ui;
use crate::{
    secrets::CatalogSecretStore,
    store::{FirestoreBackend, FirestoreState, StorageFiles, register_projection_queue},
};

pub(crate) async fn serve_default(bind_addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let secrets = CatalogSecretStore::from_config().await?;
    let catalog = FirestoreState::from_config(Arc::new(StorageFiles::new(secrets.clone()))).await?;
    let server_id = <FirestoreBackend as CatalogStore>::get_server_info(catalog.clone())
        .await?
        .server_id();
    // Events implement interior mutability.
    let events = EventDispatcher::new(vec![]);
    if lakekeeper::CONFIG.authz_backend != lakekeeper::AuthZBackend::AllowAll {
        anyhow::bail!(
            "Only the allow-all authorizer is supported by this build; unset LAKEKEEPER__AUTHZ_BACKEND"
        );
    }
    tracing::info!("Using AllowAll authorizer");
    let authz = AllowAllAuthorizer { server_id };
    // Endpoint statistics need a store of their own; none is written yet.
    let stats = vec![];
    let queues: Vec<RegisterTaskQueueFn<_, FirestoreBackend, _>> =
        vec![Box::new(|registry, context| {
            Box::pin(async move {
                register_projection_queue(&registry, context.v1_state.catalog).await;
                Ok(())
            })
        })];

    serve_with_authn::<FirestoreBackend, _, _>(
        bind_addr, secrets, catalog, authz, stats, events, queues,
    )
    .await
}

async fn serve_with_authn<C: CatalogStore, S: SecretStore, A: Authorizer>(
    bind: std::net::SocketAddr,
    secret: S,
    catalog: C::State,
    authz: A,
    stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
    events: EventDispatcher,
    queues: Vec<RegisterTaskQueueFn<A, C, S>>,
) -> anyhow::Result<()> {
    // A headless worker (serve_http_api disabled) serves no authenticated
    // requests, so it skips the authenticator.
    //
    // Otherwise use the upstream config-driven authenticator — supports both
    // single-provider (OPENID_PROVIDER_URI) and multi-provider (OPENID_PROVIDERS).
    let authentication = if lakekeeper::CONFIG.serve_http_api {
        get_default_authenticator_from_config().await?
    } else {
        None
    };

    match authentication {
        None => {
            serve_inner::<C, _, _, AuthenticatorEnum>(
                bind, secret, catalog, authz, None, stats, events, queues,
            )
            .await
        }
        Some(BuiltInAuthenticators::Chain(authn)) => {
            serve_inner::<C, _, _, _>(
                bind,
                secret,
                catalog,
                authz,
                Some(authn),
                stats,
                events,
                queues,
            )
            .await
        }
        Some(BuiltInAuthenticators::Single(authn)) => {
            serve_inner::<C, _, _, _>(
                bind,
                secret,
                catalog,
                authz,
                Some(authn),
                stats,
                events,
                queues,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_inner<C: CatalogStore, S: SecretStore, A: Authorizer, N: Authenticator + 'static>(
    bind: std::net::SocketAddr,
    secrets: S,
    catalog: C::State,
    authorizer: A,
    authenticator: Option<N>,
    stats: Vec<Arc<dyn EndpointStatisticsSink + 'static>>,
    events: EventDispatcher,
    queues: Vec<RegisterTaskQueueFn<A, C, S>>,
) -> anyhow::Result<()> {
    let mut cloud_event_sinks: Vec<Arc<dyn CloudEventBackend + Sync + Send>> = Vec::new();
    if let Some(tracing) = maybe_tracing_cloud_event_backend() {
        cloud_event_sinks.push(tracing);
    } else {
        tracing::info!("Running without publisher.");
    }

    let config = ServeConfiguration::<C, _, _, _>::builder()
        .bind_addr(bind)
        .secrets_state(secrets)
        .catalog_state(catalog)
        .authorizer(authorizer)
        .authenticator(authenticator)
        .stats(stats)
        .modify_router_fn(Some(add_ui_routes))
        .cloud_event_sinks(cloud_event_sinks)
        .event_dispatcher(Some(events))
        .register_additional_task_queues_fn(queues)
        .build();

    serve(config).await
}

fn add_ui_routes(router: lakekeeper::axum::Router) -> lakekeeper::axum::Router {
    #[cfg(feature = "ui")]
    {
        let ui_router = ui::get_ui_router();
        router.merge(ui_router)
    }

    #[cfg(not(feature = "ui"))]
    router
}
