//! Users on the emulator. Users are not scoped to a project, so each test names its users with
//! a fresh tag and filters by it.

use lakekeeper::{
    api::{
        iceberg::v1::{PageToken, PaginationQuery},
        management::v1::user::{UserLastUpdatedWith, UserType},
    },
    service::{CreateOrUpdateUserResponse, UserId, UserUpsertMode},
};

use super::db;
use crate::store::{
    docs::{USERS, UserDoc},
    fs::{Db, Txn, encode_key, to_doc},
    user,
};

fn tag() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

fn user_id(tag: &str, n: u32) -> UserId {
    UserId::try_from(format!("oidc~{tag}-{n}").as_str()).unwrap()
}

async fn upsert(
    db: &Db,
    id: &UserId,
    name: &str,
    email: Option<&str>,
    mode: UserUpsertMode,
) -> CreateOrUpdateUserResponse {
    let mut txn = Txn::read_write(db.clone());
    let response = user::create_or_update(
        id,
        name,
        email,
        UserLastUpdatedWith::UpdateEndpoint,
        UserType::Human,
        mode,
        &mut txn,
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();
    response
}

async fn by_id(db: &Db, id: &UserId) -> Vec<String> {
    user::list(
        Some(vec![id.clone()]),
        None,
        PaginationQuery::new_with_page_size(10),
        &mut Txn::latest(db.clone()),
    )
    .await
    .unwrap()
    .users
    .into_iter()
    .map(|u| u.name)
    .collect()
}

#[tokio::test]
async fn create_update_and_delete() {
    let Some(db) = db().await else { return };
    let tag = tag();
    let id = user_id(&tag, 1);
    let name = format!("Alice {tag}");
    assert_eq!(by_id(&db, &id).await.len(), 0);

    let created = upsert(
        &db,
        &id,
        &name,
        Some("a@example.com"),
        UserUpsertMode::Overwrite,
    )
    .await;
    assert!(matches!(created, CreateOrUpdateUserResponse::Created(_)));
    assert_eq!(by_id(&db, &id).await, std::slice::from_ref(&name));
    // The same again changes nothing.
    let same = upsert(
        &db,
        &id,
        &name,
        Some("a@example.com"),
        UserUpsertMode::Overwrite,
    )
    .await;
    assert!(matches!(same, CreateOrUpdateUserResponse::Unchanged(_)));
    // A first login never renames a named user.
    let backfill = upsert(&db, &id, "Other", None, UserUpsertMode::BackfillUnnamedStub).await;
    assert!(matches!(backfill, CreateOrUpdateUserResponse::Unchanged(_)));
    // An overwrite does, and clears the email.
    let renamed = format!("Alicia {tag}");
    match upsert(&db, &id, &renamed, None, UserUpsertMode::Overwrite).await {
        CreateOrUpdateUserResponse::Updated { user, previous } => {
            assert_eq!(user.name, renamed);
            assert_eq!(user.email, None);
            assert_eq!(previous.unwrap().name, name);
        }
        other => panic!("{other:?}"),
    }

    let mut txn = Txn::read_write(db.clone());
    let deleted = user::delete(&id, &mut txn).await.unwrap().unwrap();
    txn.commit().await.unwrap();
    assert_eq!(deleted.user.name, renamed);
    assert_eq!(deleted.affected_roles.len(), 0);
    assert_eq!(by_id(&db, &id).await.len(), 0);
    let mut txn = Txn::read_write(db.clone());
    assert!(user::delete(&id, &mut txn).await.unwrap().is_none());
    txn.rollback().await.unwrap();
    // Created again: an update of the deleted user.
    let again = upsert(&db, &id, &name, None, UserUpsertMode::Overwrite).await;
    assert!(matches!(again, CreateOrUpdateUserResponse::Updated { .. }));
    assert_eq!(by_id(&db, &id).await, [name]);
}

#[tokio::test]
async fn a_first_login_names_a_role_provider_stub() {
    let Some(db) = db().await else { return };
    let tag = tag();
    let id = user_id(&tag, 1);
    let stub = UserDoc {
        id: id.to_string(),
        name: None,
        email: Some("synced@example.com".to_string()),
        last_updated_with: "role-provider".to_string(),
        user_type: UserType::Human,
        created_at: crate::store::docs::now_micros(),
        updated_at: None,
        deleted_at: None,
        active: true,
    };
    let mut txn = Txn::read_write(db.clone());
    txn.set(to_doc(db.doc_name(USERS, &encode_key(&id.to_string())), &stub).unwrap());
    txn.commit().await.unwrap();
    assert_eq!(
        by_id(&db, &id).await,
        [format!("Nameless User with id {id}")]
    );

    match upsert(&db, &id, "Bob", None, UserUpsertMode::BackfillUnnamedStub).await {
        CreateOrUpdateUserResponse::Updated { user, .. } => {
            assert_eq!(user.name, "Bob");
            // The provider's email stays when the token has none.
            assert_eq!(user.email.as_deref(), Some("synced@example.com"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn list_by_name_and_search() {
    let Some(db) = db().await else { return };
    let tag = tag();
    for n in 1..=5 {
        let name = if n == 3 {
            format!("Carol {tag}")
        } else {
            format!("User {} {n}", tag.to_uppercase())
        };
        let email = format!("u{n}-{tag}@example.com");
        upsert(
            &db,
            &user_id(&tag, n),
            &name,
            Some(&email),
            UserUpsertMode::Overwrite,
        )
        .await;
    }

    // A name filter, two at a time; it matches ignoring case.
    let mut names = Vec::new();
    let mut token = PageToken::Empty;
    loop {
        let page = user::list(
            None,
            Some(format!("user {tag}")),
            PaginationQuery {
                page_token: token,
                page_size: Some(2),
            },
            &mut Txn::latest(db.clone()),
        )
        .await
        .unwrap();
        let done = page.users.len() < 2;
        names.extend(page.users.into_iter().map(|u| u.name));
        match page.next_page_token {
            Some(t) if !done => token = PageToken::Present(t),
            _ => break,
        }
    }
    let upper = tag.to_uppercase();
    assert_eq!(
        names,
        [1, 2, 4, 5].map(|n| format!("User {upper} {n}")).to_vec()
    );

    // An exact ID first, then names and emails containing the term.
    let found = user::search(&user_id(&tag, 4).to_string(), &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    assert_eq!(found.users[0].id, user_id(&tag, 4));
    let found = user::search(&format!("u3-{tag}"), &mut Txn::latest(db.clone()))
        .await
        .unwrap();
    assert_eq!(
        found
            .users
            .iter()
            .map(|u| u.name.clone())
            .collect::<Vec<_>>(),
        [format!("Carol {tag}")]
    );
}
