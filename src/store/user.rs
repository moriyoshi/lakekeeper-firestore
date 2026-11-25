//! Users, following upstream's Postgres store. A deleted user stays as a scrubbed document
//! with `deleted_at` set, as upstream soft-deletes; no role assignments are stored yet, so a
//! delete affects no role.
//!
//! Name filters and searches read every active user and match in memory: Firestore has no
//! substring or similarity match. Upstream's search ranks by trigram similarity; here an exact
//! ID match comes first, then users whose name or email contains the term, ignoring case.

use lakekeeper::{
    CONFIG,
    api::{
        ErrorModel,
        iceberg::v1::PaginationQuery,
        management::v1::user::{
            ListUsersResponse, SearchUser, SearchUserResponse, User, UserLastUpdatedWith, UserType,
        },
    },
    service::{
        CatalogBackendError, CreateOrUpdateUserResponse, DeletedUser, Result, UserId,
        UserUpsertMode,
    },
};

use super::{
    docs::{USERS, UserDoc, from_micros, now_micros},
    fs::{Query, Txn, backend_error, bool_value, encode_key, from_doc, int_value, to_doc},
};

/// Upstream's search returns at most this many users.
const SEARCH_LIMIT: usize = 10;

fn doc_name(txn: &Txn, id: &str) -> String {
    txn.db().doc_name(USERS, &encode_key(id))
}

fn last_updated_with_str(u: UserLastUpdatedWith) -> &'static str {
    match u {
        UserLastUpdatedWith::CreateEndpoint => "create-endpoint",
        UserLastUpdatedWith::ConfigCallCreation => "config-call-creation",
        UserLastUpdatedWith::UpdateEndpoint => "update-endpoint",
        UserLastUpdatedWith::RoleProvider => "role-provider",
    }
}

fn last_updated_with(s: &str) -> Result<UserLastUpdatedWith, CatalogBackendError> {
    Ok(match s {
        "create-endpoint" => UserLastUpdatedWith::CreateEndpoint,
        "config-call-creation" => UserLastUpdatedWith::ConfigCallCreation,
        "update-endpoint" => UserLastUpdatedWith::UpdateEndpoint,
        "role-provider" => UserLastUpdatedWith::RoleProvider,
        other => {
            return Err(backend_error(std::io::Error::other(format!(
                "Unknown `last_updated_with` `{other}`"
            ))));
        }
    })
}

/// Upstream's placeholder for a role-provider stub, which has no name yet.
fn display_name(doc: &UserDoc) -> String {
    doc.name
        .clone()
        .unwrap_or_else(|| format!("Nameless User with id {}", doc.id))
}

fn user(doc: &UserDoc) -> Result<User> {
    Ok(User {
        name: display_name(doc),
        email: doc.email.clone(),
        id: UserId::try_from(doc.id.as_str())?,
        user_type: doc.user_type,
        last_updated_with: last_updated_with(&doc.last_updated_with).map_err(ErrorModel::from)?,
        created_at: from_micros(doc.created_at),
        updated_at: doc.updated_at.map(from_micros),
    })
}

/// The fields a user lifecycle event reports a change of, as upstream compares them.
fn identity(doc: &UserDoc) -> (Option<&str>, Option<&str>, UserType, bool) {
    (
        doc.name.as_deref(),
        doc.email.as_deref(),
        doc.user_type,
        doc.deleted_at.is_some(),
    )
}

async fn read(txn: &mut Txn, id: &str) -> Result<Option<UserDoc>, CatalogBackendError> {
    let name = doc_name(txn, id);
    txn.get(&name)
        .await?
        .map(|d| from_doc::<UserDoc>(&d))
        .transpose()
}

pub(super) async fn create_or_update(
    user_id: &UserId,
    name: &str,
    email: Option<&str>,
    updated_with: UserLastUpdatedWith,
    user_type: UserType,
    mode: UserUpsertMode,
    txn: &mut Txn,
) -> Result<CreateOrUpdateUserResponse> {
    let id = user_id.to_string();
    let before = read(txn, &id).await.map_err(ErrorModel::from)?;
    let Some(before) = before else {
        let doc = UserDoc {
            id: id.clone(),
            name: Some(name.to_string()),
            email: email.map(ToString::to_string),
            last_updated_with: last_updated_with_str(updated_with).to_string(),
            user_type,
            created_at: now_micros(),
            updated_at: None,
            deleted_at: None,
            active: true,
        };
        txn.create(to_doc(doc_name(txn, &id), &doc).map_err(ErrorModel::from)?);
        return Ok(CreateOrUpdateUserResponse::Created(user(&doc)?));
    };
    let backfill = mode == UserUpsertMode::BackfillUnnamedStub;
    // A backfill only names a role-provider stub; a real name is never replaced.
    if backfill
        && !(before.name.is_none()
            && before.last_updated_with == last_updated_with_str(UserLastUpdatedWith::RoleProvider))
    {
        return Ok(CreateOrUpdateUserResponse::Unchanged(user(&before)?));
    }
    let mut after = before.clone();
    after.name = Some(name.to_string());
    // A backfill keeps the provider's email when the token has none.
    after.email = match email {
        Some(e) => Some(e.to_string()),
        None if backfill => before.email.clone(),
        None => None,
    };
    after.last_updated_with = last_updated_with_str(updated_with).to_string();
    after.user_type = user_type;
    after.deleted_at = None;
    after.active = true;
    after.updated_at = Some(now_micros());
    txn.set(to_doc(doc_name(txn, &id), &after).map_err(ErrorModel::from)?);
    Ok(if identity(&before) == identity(&after) {
        CreateOrUpdateUserResponse::Unchanged(user(&after)?)
    } else {
        CreateOrUpdateUserResponse::Updated {
            user: user(&after)?,
            previous: Some(user(&before)?),
        }
    })
}

fn token_of(doc: &UserDoc) -> String {
    format!("{}|{}", doc.created_at, doc.id)
}

fn parse_page_token(token: &str) -> Result<(i64, String)> {
    let invalid = || {
        ErrorModel::bad_request(
            format!("Invalid page token `{token}`"),
            "InvalidPaginationToken",
            None,
        )
    };
    let (ts, id) = token.split_once('|').ok_or_else(invalid)?;
    Ok((ts.parse().map_err(|_| invalid())?, id.to_string()))
}

fn contains_folded(haystack: Option<&str>, needle: &str) -> bool {
    haystack.is_some_and(|h| h.to_lowercase().contains(needle))
}

/// Active users in `(created_at, id)` order after `after`: the first `limit` that `keep`
/// accepts.
async fn scan(
    txn: &mut Txn,
    mut after: Option<(i64, String)>,
    limit: usize,
    keep: impl Fn(&UserDoc) -> bool,
) -> Result<Vec<UserDoc>, CatalogBackendError> {
    let batch = limit.clamp(1, 300);
    let mut out = Vec::new();
    loop {
        let cursor = after
            .as_ref()
            .map(|(ts, id)| (int_value(*ts), doc_name(txn, id)));
        let docs = txn
            .query(
                &Query::new(USERS)
                    .eq("active", bool_value(true))
                    .order_by("created_at")
                    .start_after(cursor)
                    .limit(Some(batch)),
            )
            .await?;
        let exhausted = docs.len() < batch;
        for d in &docs {
            let doc: UserDoc = from_doc(d)?;
            after = Some((doc.created_at, doc.id.clone()));
            if keep(&doc) {
                out.push(doc);
                if out.len() == limit {
                    return Ok(out);
                }
            }
        }
        if exhausted {
            return Ok(out);
        }
    }
}

pub(super) async fn list(
    filter_user_id: Option<Vec<UserId>>,
    filter_name: Option<String>,
    PaginationQuery {
        page_token,
        page_size,
    }: PaginationQuery,
    txn: &mut Txn,
) -> Result<ListUsersResponse> {
    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_default(page_size)).unwrap_or(usize::MAX);
    let after = page_token.as_option().map(parse_page_token).transpose()?;
    let name = filter_name
        .filter(|n| !n.is_empty())
        .map(|n| n.to_lowercase());
    let matches_name = |doc: &UserDoc| {
        name.as_deref()
            .is_none_or(|n| contains_folded(doc.name.as_deref(), n))
    };
    let users: Vec<UserDoc> = if let Some(ids) = filter_user_id {
        // The configuration call's lookup of the caller: read the documents directly.
        let names: Vec<String> = ids
            .iter()
            .map(|id| doc_name(txn, &id.to_string()))
            .collect();
        let mut docs: Vec<UserDoc> = txn
            .get_many(&names)
            .await
            .map_err(ErrorModel::from)?
            .into_iter()
            .flatten()
            .map(|d| from_doc::<UserDoc>(&d))
            .collect::<Result<_, _>>()
            .map_err(ErrorModel::from)?;
        docs.retain(|d| {
            d.deleted_at.is_none()
                && matches_name(d)
                && after
                    .as_ref()
                    .is_none_or(|(ts, id)| (d.created_at, &d.id) > (*ts, id))
        });
        docs.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
        docs.truncate(page_size);
        docs
    } else {
        scan(txn, after, page_size, matches_name)
            .await
            .map_err(ErrorModel::from)?
    };
    let next_page_token = users.last().map(token_of);
    Ok(ListUsersResponse {
        users: users.iter().map(user).collect::<Result<_>>()?,
        next_page_token,
    })
}

pub(super) async fn search(search_term: &str, txn: &mut Txn) -> Result<SearchUserResponse> {
    let term = search_term.to_lowercase();
    let mut found: Vec<UserDoc> = read(txn, search_term)
        .await
        .map_err(ErrorModel::from)?
        .filter(|d| d.deleted_at.is_none())
        .into_iter()
        .collect();
    let exact = found.first().map(|d| d.id.clone());
    found.extend(
        scan(txn, None, SEARCH_LIMIT, |d| {
            Some(&d.id) != exact.as_ref()
                && (contains_folded(d.name.as_deref(), &term)
                    || contains_folded(d.email.as_deref(), &term))
        })
        .await
        .map_err(ErrorModel::from)?,
    );
    found.truncate(SEARCH_LIMIT);
    Ok(SearchUserResponse {
        users: found
            .iter()
            .map(|d| {
                Ok(SearchUser {
                    name: display_name(d),
                    id: UserId::try_from(d.id.as_str())?,
                    user_type: d.user_type,
                    email: d.email.clone(),
                })
            })
            .collect::<Result<_>>()?,
    })
}

/// Soft-delete the user, scrubbing its name and email. `None` if no active user has the ID.
pub(super) async fn delete(user_id: &UserId, txn: &mut Txn) -> Result<Option<DeletedUser>> {
    let id = user_id.to_string();
    let Some(before) = read(txn, &id)
        .await
        .map_err(ErrorModel::from)?
        .filter(|d| d.deleted_at.is_none())
    else {
        return Ok(None);
    };
    let mut after = before.clone();
    after.name = Some("Deleted User".to_string());
    after.email = None;
    after.deleted_at = Some(now_micros());
    after.active = false;
    txn.set(to_doc(doc_name(txn, &id), &after).map_err(ErrorModel::from)?);
    Ok(Some(DeletedUser {
        user: user(&before)?,
        affected_roles: Vec::new(),
    }))
}
