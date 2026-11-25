// Adapted from Lakekeeper v0.15.1, crates/lakekeeper-bin/src/main.rs.
// Copyright 2024-2026 Vakamo Inc. Licensed under the Apache License, Version 2.0:
// https://www.apache.org/licenses/LICENSE-2.0
// Modifications: serves the Firestore catalog store. Dropped the Postgres-only commands
// (`migrate`, `wait-for-db`, `reopen-bootstrap`), the OpenFGA commands, the OpenAPI dump
// commands and the jemalloc allocator.

//! Lakekeeper with a Firestore catalog store. See DESIGN.md.

#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![forbid(unsafe_code)]
#![allow(clippy::module_name_repetitions, clippy::similar_names)]

use clap::{Parser, Subcommand};
use lakekeeper::{CONFIG, tokio, tracing};
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

mod config;
mod healthcheck;
mod secrets;
mod serve;
mod store;
#[cfg(feature = "ui")]
mod ui;

pub(crate) use config::CONFIG_BIN;
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the server
    Serve {},
    /// Check the health endpoint of the server
    Healthcheck {
        #[clap(
            default_value = "false",
            short = 'a',
            help = "Check all services, implies -d and -s."
        )]
        check_all: bool,
        #[clap(
            default_value = "false",
            short = 'd',
            help = "Only test the Firestore connection.",
            conflicts_with("check_all")
        )]
        check_db: bool,
        #[clap(
            default_value = "false",
            short = 's',
            help = "Check health endpoint.",
            conflicts_with("check_all")
        )]
        check_server: bool,
    },
    /// Print the version of the server
    Version {},
    /// Search the metadata of the catalog's tables in Firestore directly, bypassing the
    /// server and its authorizer.
    Search(store::search::SearchArgs),
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    lakekeeper::audit::log_format(CONFIG_BIN.debug.extended_logs)
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    // After the subscriber is installed, so it inspects the filter in force. Writes to
    // standard error: a filter that selects nothing would suppress a logged warning.
    lakekeeper::audit::warn_on_retired_audit_filter();

    match cli.command {
        Some(Commands::Serve {}) => {
            print_info();
            serve().await?;
        }
        Some(Commands::Healthcheck {
            check_all,
            check_db,
            check_server,
        }) => {
            let (check_db, check_server) =
                healthcheck::normalize_checks(check_all, check_db, check_server);
            healthcheck::health(check_db, check_server).await?;
        }
        Some(Commands::Version {}) => {
            println!("{VERSION}");
        }
        Some(Commands::Search(args)) => {
            store::search::run(args).await?;
        }
        None => {
            if CONFIG_BIN.debug.auto_serve {
                print_info();
                serve().await?;
            } else {
                // Error out if no subcommand is provided.
                eprintln!("No subcommand provided. Use --help for more information.");
                anyhow::bail!("No subcommand provided");
            }
        }
    }

    Ok(())
}

async fn serve() -> anyhow::Result<()> {
    if CONFIG.serve_http_api {
        tracing::info!(
            "Starting server on {}:{}...",
            CONFIG.bind_ip,
            CONFIG.listen_port
        );
    } else {
        tracing::info!(
            "Starting headless worker (serve_http_api disabled): running background task-queue \
             workers; only /health is served on {}:{}.",
            CONFIG.bind_ip,
            CONFIG.listen_port
        );
    }
    let bind_addr = std::net::SocketAddr::from((CONFIG.bind_ip, CONFIG.listen_port));
    serve::serve_default(bind_addr).await
}

fn print_info() {
    println!("Lakekeeper (Firestore catalog store) {VERSION}\n");
    tracing::info!("Lakekeeper Firestore Version: {VERSION}");
}
