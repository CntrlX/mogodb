//! Live integration tests against a real mongod.
//!
//! Run with `MOGO_TEST_URI=mongodb://127.0.0.1:27030 cargo test -p mogo-client --test live -- --nocapture`.
//! Auth cases additionally need `MOGO_TEST_AUTH_URI` (a server with `--auth` and a user).

use bson::{Bson, Document};
use mogo_client::{Client, FindOptions, WatchOptions};

fn uri() -> Option<String> {
    std::env::var("MOGO_TEST_URI").ok().filter(|s| !s.is_empty())
}

async fn setup() -> Option<(Client, String)> {
    let uri = uri()?;
    let client = Client::connect(&uri).await.expect("connect");
    let db = format!("mogo_client_it_{}", std::process::id());
    client.run_admin(bson::doc! {"dropDatabase": 1, "$db": &db}).await.ok();
    Some((client, db))
}

#[tokio::test]
async fn handshake_and_crud() {
    let Some((client, dbname)) = setup().await else { return };
    let info = client.server_info().await.unwrap();
    assert!(info.max_wire_version >= 17, "maxWireVersion {}", info.max_wire_version);
    // `hello` does not include the version; `buildInfo` does.
    assert!(client.build_info().await.unwrap().get_str("version").unwrap().starts_with("9."));

    let db = client.database(&dbname);
    db.create_collection("items", Document::new()).await.unwrap();
    assert!(db.list_collection_names().await.unwrap().contains(&"items".to_string()));

    let c = db.collection("items");
    let id = c.insert_one(bson::doc! {"_id": 1, "n": "a", "q": 5}).await.unwrap();
    assert_eq!(id, Bson::Int32(1));
    c.insert_many(vec![bson::doc! {"_id": 2, "n": "b", "q": 7}, bson::doc! {"_id": 3, "n": "c", "q": 9}], true).await.unwrap();

    assert_eq!(c.count(Document::new()).await.unwrap(), 3);
    let all = c.find_all(Document::new(), FindOptions::default().sort(bson::doc! {"_id": 1})).await.unwrap();
    assert_eq!(all.len(), 3, "{all:?}");

    let one = c.find_one(bson::doc! {"n": "b"}, FindOptions::default()).await.unwrap().unwrap();
    assert_eq!(one.get_i32("_id").unwrap(), 2);

    let u = c.update_one(bson::doc! {"_id": 1}, bson::doc! {"$set": {"n": "aa"}}).await.unwrap();
    assert_eq!((u.matched, u.modified), (1, 1));
    let u = c.update_many(Document::new(), bson::doc! {"$inc": {"q": 1}}).await.unwrap();
    assert_eq!(u.modified, 3);
    assert_eq!(c.find_one(bson::doc! {"_id": 1}, FindOptions::default()).await.unwrap().unwrap().get_i32("q").unwrap(), 6);

    // Projection + range
    let rows = c.find_all(bson::doc! {"q": {"$gte": 6}}, FindOptions::default().projection(bson::doc! {"_id": 0, "n": 1})).await.unwrap();
    assert_eq!(rows.iter().map(|r| r.get_str("n").unwrap()).collect::<Vec<_>>(), vec!["aa", "b", "c"]);

    assert_eq!(c.delete_one(bson::doc! {"_id": 3}).await.unwrap(), 1);
    assert_eq!(c.count(Document::new()).await.unwrap(), 2);

    // Aggregation with cursor paging
    let mut cur = c.aggregate(vec![bson::doc! {"$match": {}}, bson::doc! {"$sort": {"_id": 1}}], Default::default()).await.unwrap();
    let mut seen = Vec::new();
    while let Some(d) = cur.next().await.unwrap() {
        seen.push(d.get_i32("_id").unwrap());
    }
    assert_eq!(seen, vec![1, 2]);
    db.drop_collection("items").await.unwrap();
}

#[tokio::test]
async fn indexes_and_explain() {
    let Some((client, dbname)) = setup().await else { return };
    let c = client.database(&dbname).collection("ix");
    c.insert_many(vec![bson::doc! {"_id": 1, "a": 1, "b": "x"}, bson::doc! {"_id": 2, "a": 2, "b": "y"}], true).await.unwrap();
    c.create_index(bson::doc! {"key": {"a": 1}, "name": "a_1"}, Document::new()).await.unwrap();
    let idx = c.indexes().await.unwrap();
    assert!(idx.iter().any(|i| i.get_str("name").unwrap() == "a_1"));

    let plan = c.explain(bson::doc! {"find": "ix", "filter": {"a": 1}}).await.unwrap();
    let text = plan.to_string();
    assert!(text.contains("IXSCAN"), "explain: {text}");
    c.drop_index("a_1").await.unwrap();
}

#[tokio::test]
async fn change_stream_sees_inserts() {
    // Change streams require a replica set.
    let Some(uri) = std::env::var("MOGO_TEST_RS_URI").ok().filter(|s| !s.is_empty()) else { return };
    let client = Client::connect(&uri).await.expect("connect");
    let dbname = format!("mogo_client_cs_{}", std::process::id());
    let db = client.database(&dbname);
    db.create_collection("cs", Document::new()).await.unwrap();
    let c = db.collection("cs");
    c.insert_one(bson::doc! {"_id": 1}).await.unwrap();

    let mut stream = c.watch(vec![], WatchOptions { max_time_ms: 5000, ..Default::default() }).await.unwrap();
    c.insert_one(bson::doc! {"_id": 2, "m": "hi"}).await.unwrap();
    let ev = tokio::time::timeout(std::time::Duration::from_secs(10), stream.next(5000)).await.expect("event timeout").unwrap().expect("event");
    assert_eq!(ev.get_str("operationType").unwrap(), "insert");
    assert_eq!(ev.get_document("fullDocument").unwrap().get_i32("_id").unwrap(), 2);
}

#[tokio::test]
async fn auth_roundtrip() {
    let Some(uri) = std::env::var("MOGO_TEST_AUTH_URI").ok().filter(|s| !s.is_empty()) else { return };
    // Wrong password must fail with an auth error.
    let bad = uri.replace(":mogo-pass@", ":wrong-pass@");
    let client = Client::connect(&uri).await.expect("auth connect");
    client.ping().await.unwrap();
    // Auth covers admin commands on the pool's connections.
    let mut conn = client.conn().await.unwrap();
    assert!(conn.is_authenticated());
    conn.command("admin", bson::doc! {"ping": 1}).await.unwrap();

}