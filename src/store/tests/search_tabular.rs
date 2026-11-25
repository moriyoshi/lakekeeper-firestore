//! The management API's tabular search on the emulator.

use super::{
    db,
    table::{create, namespace, small},
};
use crate::store::{fs::Txn, tabular};

#[tokio::test]
async fn search_by_name_and_id() {
    let Some(db) = db().await else { return };
    let ns = namespace(&db).await;
    let mut ids = Vec::new();
    for name in ["orders", "order_items", "customers"] {
        let metadata = small(&format!("s3://b/search/{name}"));
        ids.push(metadata.uuid());
        create(
            &db,
            ns,
            name,
            Some(&format!("s3://b/m/{name}.json")),
            &metadata,
        )
        .await
        .unwrap();
    }
    let names = |term: &str| {
        let db = db.clone();
        let term = term.to_string();
        async move {
            tabular::search(ns.0, &term, &mut Txn::latest(db))
                .await
                .unwrap()
                .search_results
                .into_iter()
                .map(|r| r.tabular.tabular_ident().name.clone())
                .collect::<Vec<_>>()
        }
    };
    // Nearest first, by trigram distance of `namespace.name`; the namespace counts too.
    assert_eq!(
        names("ns.orders").await,
        ["orders", "order_items", "customers"]
    );
    assert_eq!(names("order").await, ["orders", "order_items"]);
    assert_eq!(names("CUSTOMER").await[0], "customers");
    assert_eq!(names("zzz").await.len(), 0);
    // A table ID, or a namespace ID.
    assert_eq!(names(&ids[2].to_string()).await, ["customers"]);
    let mut all = names(&ns.1.to_string()).await;
    all.sort();
    assert_eq!(all, ["customers", "order_items", "orders"]);
}
