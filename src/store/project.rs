//! Projects.

use std::{collections::HashSet, sync::Arc};

use lakekeeper::{
    ProjectId,
    api::ErrorModel,
    service::{CatalogBackendError, GetProjectResponse, Result, authn::UserId},
};

use super::{
    docs::{PROJECTS, ProjectDoc, WAREHOUSES, now_micros},
    fs::{Query, Txn, from_doc, string_value, to_doc},
};

fn name(txn: &Txn, project_id: &ProjectId) -> String {
    txn.db()
        .doc_name(PROJECTS, &super::fs::encode_key(project_id.as_str()))
}

fn response(doc: ProjectDoc) -> GetProjectResponse {
    GetProjectResponse {
        project_id: Arc::new(doc.project_id),
        name: doc.name,
    }
}

async fn read(
    txn: &mut Txn,
    project_id: &ProjectId,
) -> Result<Option<ProjectDoc>, CatalogBackendError> {
    let name = name(txn, project_id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<ProjectDoc>(&d))
        .transpose()
}

pub(super) async fn exists(
    txn: &mut Txn,
    project_id: &ProjectId,
) -> Result<bool, CatalogBackendError> {
    Ok(read(txn, project_id).await?.is_some())
}

pub(super) async fn create_project(
    project_id: &ProjectId,
    project_name: String,
    txn: &mut Txn,
) -> Result<()> {
    if read(txn, project_id)
        .await
        .map_err(ErrorModel::from)?
        .is_some()
    {
        return Err(ErrorModel::conflict(
            format!("Project with id '{project_id}' already exists"),
            "ProjectAlreadyExists",
            None,
        )
        .into());
    }
    let doc = ProjectDoc {
        project_id: project_id.clone(),
        name: project_name,
        created_at: now_micros(),
        updated_at: None,
    };
    txn.create(to_doc(name(txn, project_id), &doc).map_err(ErrorModel::from)?);
    Ok(())
}

pub(super) async fn delete_project(project_id: &ProjectId, txn: &mut Txn) -> Result<Vec<UserId>> {
    if read(txn, project_id)
        .await
        .map_err(ErrorModel::from)?
        .is_none()
    {
        return Err(ErrorModel::not_found(
            format!("Project with id '{project_id}' not found"),
            "ProjectNotFound",
            None,
        )
        .into());
    }
    let warehouses = txn
        .query(
            &Query::new(WAREHOUSES)
                .eq("project_id", string_value(project_id.as_str()))
                .limit(Some(1)),
        )
        .await
        .map_err(ErrorModel::from)?;
    if !warehouses.is_empty() {
        return Err(ErrorModel::conflict(
            "Project is not empty: delete its warehouses first",
            "ProjectNotEmpty",
            None,
        )
        .into());
    }
    super::tasks::delete_project_tasks(txn, project_id)
        .await
        .map_err(ErrorModel::from)?;
    txn.delete(name(txn, project_id));
    // No role assignments are stored yet, so no user is affected.
    Ok(Vec::new())
}

pub(super) async fn get_project(
    project_id: &ProjectId,
    txn: &mut Txn,
) -> Result<Option<GetProjectResponse>> {
    Ok(read(txn, project_id)
        .await
        .map_err(ErrorModel::from)?
        .map(response))
}

pub(super) async fn list_projects(
    project_ids: Option<HashSet<ProjectId>>,
    txn: &mut Txn,
) -> Result<Vec<GetProjectResponse>> {
    let docs = match project_ids {
        Some(ids) => {
            let names: Vec<String> = ids.iter().map(|id| name(txn, id)).collect();
            txn.get_many(&names)
                .await
                .map_err(ErrorModel::from)?
                .into_iter()
                .flatten()
                .collect()
        }
        None => txn
            .query(&Query::new(PROJECTS))
            .await
            .map_err(ErrorModel::from)?,
    };
    docs.iter()
        .map(|d| Ok(response(from_doc(d).map_err(ErrorModel::from)?)))
        .collect()
}

pub(super) async fn rename_project(
    project_id: &ProjectId,
    new_name: &str,
    txn: &mut Txn,
) -> Result<()> {
    let Some(mut doc) = read(txn, project_id).await.map_err(ErrorModel::from)? else {
        return Err(ErrorModel::not_found(
            format!("Project with id '{project_id}' not found"),
            "ProjectNotFound",
            None,
        )
        .into());
    };
    doc.name = new_name.to_string();
    doc.updated_at = Some(now_micros());
    txn.set(to_doc(name(txn, project_id), &doc).map_err(ErrorModel::from)?);
    Ok(())
}
