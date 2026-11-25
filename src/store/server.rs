//! Server info and bootstrap.

use lakekeeper::{
    api::ErrorModel,
    service::{Result, ServerId, ServerInfo},
};

use super::{
    FirestoreState,
    docs::{DATA_VERSION, SERVER_COLLECTION, SERVER_DOC, ServerDoc},
    fs::{Txn, from_doc, to_doc},
};

fn server_doc_name(txn: &Txn) -> String {
    txn.db().doc_name(SERVER_COLLECTION, SERVER_DOC)
}

async fn read_server(txn: &mut Txn) -> Result<Option<ServerDoc>, ErrorModel> {
    let name = server_doc_name(txn);
    txn.get(&name)
        .await?
        .map(|doc| from_doc::<ServerDoc>(&doc))
        .transpose()
        .map_err(Into::into)
}

/// The server document, created on first use: Firestore has no migration step that could
/// create it.
pub(super) async fn get_server_info(state: &FirestoreState) -> Result<ServerInfo, ErrorModel> {
    let server = if let Some(server) = read_server(&mut state.latest()).await? {
        server
    } else {
        let mut txn = Txn::read_write(state.db.clone());
        let server = if let Some(server) = read_server(&mut txn).await? {
            server
        } else {
            let server = ServerDoc {
                server_id: ServerId::new_random(),
                open_for_bootstrap: true,
                terms_accepted: false,
                data_version: DATA_VERSION,
            };
            txn.create(to_doc(server_doc_name(&txn), &server)?);
            server
        };
        match txn.commit().await {
            Ok(()) => server,
            // Another instance created it first.
            Err(_) => read_server(&mut state.latest()).await?.ok_or_else(|| {
                ErrorModel::internal("Cannot create the server document", "ServerIdMissing", None)
            })?,
        }
    };
    if server.data_version > DATA_VERSION {
        return Err(ErrorModel::failed_dependency(
            format!(
                "The Firestore data version {} is newer than this binary supports ({DATA_VERSION})",
                server.data_version
            ),
            "DataVersionTooNew",
            None,
        ));
    }
    Ok(ServerInfo {
        server_id: server.server_id,
        terms_accepted: server.terms_accepted,
        open_for_bootstrap: server.open_for_bootstrap,
    })
}

pub(super) async fn bootstrap(terms_accepted: bool, txn: &mut Txn) -> Result<bool> {
    let Some(mut server) = read_server(txn).await? else {
        return Err(ErrorModel::failed_dependency(
            "No server document found; start the server once before bootstrapping",
            "ServerIdMissing",
            None,
        )
        .into());
    };
    if !server.open_for_bootstrap {
        return Ok(false);
    }
    server.open_for_bootstrap = false;
    server.terms_accepted = terms_accepted;
    txn.set(to_doc(server_doc_name(txn), &server).map_err(ErrorModel::from)?);
    Ok(true)
}

pub(super) async fn reopen_for_bootstrap(state: &FirestoreState) -> Result<ServerId> {
    let mut txn = Txn::read_write(state.db.clone());
    let server = read_server(&mut txn).await?;
    let server = match server {
        Some(server) if !server.open_for_bootstrap => server,
        _ => {
            return Err(ErrorModel::bad_request(
                "Catalog is already open for bootstrap, or no server document exists.",
                "CatalogAlreadyOpenForBootstrap",
                None,
            )
            .into());
        }
    };
    let server_id = server.server_id;
    let server = ServerDoc {
        open_for_bootstrap: true,
        ..server
    };
    txn.set(to_doc(server_doc_name(&txn), &server).map_err(ErrorModel::from)?);
    txn.commit().await?;
    Ok(server_id)
}
