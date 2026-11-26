//! The worker thread: owns one `mogo-client` connection per server and executes commands.

use crate::model::{Cmd, CollectionInfo, CollectionKind, DatabaseInfo, IndexInfo, QueryRequest, Reply, Response};
use bson::{Bson, Document};
use mogo_client::{AggregateOptions, Client, ClientOptions, Collection, FindOptions};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Instant;

pub fn spawn(rx: Receiver<(u64, Cmd)>, tx: Sender<Response>) {
    std::thread::Builder::new()
        .name("mogo-compass-worker".into())
        .spawn(move || {
            let mut rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = e;
                    return;
                }
            };
            let mut conns: HashMap<usize, Client> = HashMap::new();
            while let Ok((id, cmd)) = rx.recv() {
                let started = Instant::now();
                let result = rt.block_on(handle(&mut conns, cmd));
                if tx.send(Response { id, started, result }).is_err() {
                    break;
                }
            }
        })
        .expect("spawn worker");
}

fn invalid(msg: impl Into<String>) -> Result<Reply, String> {
    Err(msg.into())
}

async fn handle(conns: &mut HashMap<usize, Client>, cmd: Cmd) -> Result<Reply, String> {
    match cmd {
        Cmd::Connect { server, uri, password } => {
            let mut opts = ClientOptions::from_uri(&uri).map_err(|e| e.to_string())?;
            if let Some(p) = password {
                opts.password = Some(p);
            }
            let client = Client::with_options(opts).await.map_err(|e| e.to_string())?;
            conns.insert(server, client);
            Ok(Reply::Unit)
        }
        Cmd::Disconnect(server) => {
            conns.remove(&server);
            Ok(Reply::Unit)
        }
        Cmd::ListDatabases(server) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let mut out = Vec::new();
            let dbs = client.list_database_names().await.map_err(|e| e.to_string())?;
            for name in dbs {
                let db = client.database(&name);
                let colls = db.list_collection_names().await.unwrap_or_default();
                out.push(DatabaseInfo { name, collections: Vec::new(), size_on_disk: 0, empty: colls.is_empty() });
            }
            Ok(Reply::Databases(out))
        }
        Cmd::ListCollections(server, db) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let d = client.database(&db);
            let infos = d.list_collections(Document::new()).await.map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            for info in infos {
                let name = info.get_str("name").unwrap_or("").to_string();
                let kind = match info.get_str("type").unwrap_or("collection") {
                    "view" => CollectionKind::View,
                    "timeseries" => CollectionKind::TimeSeries,
                    _ => CollectionKind::Collection,
                };
                let count = if kind == CollectionKind::Collection {
                    d.collection(&name).count(Document::new()).await.unwrap_or(0)
                } else {
                    0
                };
                let indexes = if kind == CollectionKind::View {
                    Vec::new()
                } else {
                    index_info(&d.collection(&name)).await.unwrap_or_default()
                };
                out.push(CollectionInfo { name, kind, document_count: count, storage_size: 0, indexes });
            }
            Ok(Reply::Collections(db, out))
        }
        Cmd::CollectionMeta(server, db, coll) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&db).collection(&coll);
            let kind = match client.database(&db).list_collections(bson::doc! {"name": coll}).await {
                Ok(mut v) if !v.is_empty() => match v.remove(0).get_str("type").unwrap_or("collection") {
                    "view" => CollectionKind::View,
                    "timeseries" => CollectionKind::TimeSeries,
                    _ => CollectionKind::Collection,
                },
                _ => CollectionKind::Collection,
            };
            collection_info(&c, kind).await
        }
        Cmd::RunQuery(server, q) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let opts = FindOptions {
                projection: q.projection.clone(),
                sort: q.sort.clone(),
                skip: q.skip,
                limit: q.limit,
                collation: q.collation.clone(),
                ..Default::default()
            };
            let docs = c.find_all(q.filter.clone(), opts).await.map_err(|e| e.to_string())?;
            Ok(Reply::Docs(docs))
        }
        Cmd::CountDocuments(server, q) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let n = c.count(q.filter.clone()).await.map_err(|e| e.to_string())?;
            Ok(Reply::Count(n.max(0) as u64))
        }
        Cmd::Distinct(server, q, field) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let values = c.distinct(&field, q.filter.clone()).await.map_err(|e| e.to_string())?;
            Ok(Reply::Values(values))
        }
        Cmd::Explain(server, q) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let mut cmd = bson::doc! {"find": q.coll.clone(), "filter": q.filter.clone()};
            if let Some(s) = &q.sort {
                cmd.insert("sort", s.clone());
            }
            if let Some(p) = &q.projection {
                cmd.insert("projection", p.clone());
            }
            if q.skip > 0 {
                cmd.insert("skip", q.skip);
            }
            if q.limit != 0 {
                cmd.insert("limit", q.limit);
            }
            c.explain(cmd).await.map(Reply::Explain).map_err(|e| e.to_string())
        }
        Cmd::Insert(server, q, docs) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let r = c.insert_many(docs, true).await.map_err(|e| e.to_string())?;
            Ok(Reply::Count(r.inserted_ids.len() as u64))
        }
        Cmd::UpdateMany(server, q, update) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let r = c.update_many(q.filter.clone(), update).await.map_err(|e| e.to_string())?;
            Ok(Reply::Count(r.modified as u64))
        }
        Cmd::DeleteMany(server, q) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            let n = c.delete_many(q.filter.clone()).await.map_err(|e| e.to_string())?;
            Ok(Reply::Count(n as u64))
        }
        Cmd::DropCollection(server, q) => {
            let client = conns.get(&server).ok_or("not connected")?;
            client.database(&q.db).drop_collection(&q.coll).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::CreateCollection(server, db, name, options) => {
            let client = conns.get(&server).ok_or("not connected")?;
            client.database(&db).create_collection(&name, options).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::DropDatabase(server, db) => {
            let client = conns.get(&server).ok_or("not connected")?;
            client.run_admin(bson::doc! {"dropDatabase": 1, "$db": db}).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::CreateIndex(server, q, spec, opts) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            c.create_index(spec, opts).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::DropIndex(server, q, name) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let c = client.database(&q.db).collection(&q.coll);
            c.drop_index(&name).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::ServerStatus(server) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let d = client.run_admin(bson::doc! {"serverStatus": 1}).await.map_err(|e| e.to_string())?;
            Ok(Reply::Status(d))
        }
        Cmd::CurrentOps(server) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let d = client.run_admin(bson::doc! {"currentOp": 1, "$all": true}).await.map_err(|e| e.to_string())?;
            let ops = d
                .get_array("inprog")
                .map(|a| a.iter().filter_map(Bson::as_document).cloned().collect())
                .unwrap_or_default();
            Ok(Reply::Ops(ops))
        }
        Cmd::KillOp(server, op) => {
            let client = conns.get(&server).ok_or("not connected")?;
            client.run_admin(bson::doc! {"killOp": 1, "op": op}).await.map_err(|e| e.to_string())?;
            Ok(Reply::Unit)
        }
        Cmd::Command(server, db, cmd) => {
            let client = conns.get(&server).ok_or("not connected")?;
            let mut c = cmd;
            if !c.contains_key("$db") {
                c.insert("$db", db);
            }
            let d = client.run_admin(c).await.map_err(|e| e.to_string())?;
            Ok(Reply::Docs(vec![d]))
        }
    }
}

async fn count(c: &Collection<'_>, q: &QueryRequest) -> Result<Reply, String> {
    let n = c.count(q.filter.clone()).await.map_err(|e| e.to_string())?;
    Ok(Reply::Count(n.max(0) as u64))
}

async fn index_info(c: &Collection<'_>) -> Result<Vec<IndexInfo>, String> {
    let list = c.indexes().await.map_err(|e| e.to_string())?;
    Ok(list
        .iter()
        .map(|d| IndexInfo {
            name: d.get_str("name").unwrap_or("").to_string(),
            key: d.get_document("key").cloned().unwrap_or_default(),
            unique: d.get_bool("unique").unwrap_or(false),
            sparse: d.get_bool("sparse").unwrap_or(false),
            collation: d.get_document("collation").ok().cloned(),
        })
        .collect())
}

async fn collection_info(c: &Collection<'_>, kind: CollectionKind) -> Result<Reply, String> {
    let stats = c.stats().await.map_err(|e| e.to_string())?;
    Ok(Reply::Collection(CollectionInfo {
        name: c.name().to_string(),
        kind,
        document_count: stats.get_i64("count").unwrap_or(0),
        storage_size: stats.get_i64("size").unwrap_or(0),
        indexes: index_info(c).await?,
    }))
}

/// Aggregation pipelines used by the GUI (schema analysis).
pub async fn sample_docs(c: &Collection<'_>, limit: i64) -> Result<Vec<Document>, String> {
    let docs = c
        .find_all(
            Document::new(),
            FindOptions { limit, ..Default::default() },
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(docs)
}

pub async fn distinct_values(c: &Collection<'_>, field: &str) -> Result<Vec<Bson>, String> {
    let mut out = Vec::new();
    let mut skip = 0i64;
    loop {
        let docs = c
            .find_all(Document::new(), FindOptions { projection: Some(bson::doc! {field: 1}), limit: 1000, skip, ..Default::default() })
            .await
            .map_err(|e| e.to_string())?;
        if docs.is_empty() {
            break;
        }
        skip += docs.len() as i64;
        for d in &docs {
            if let Some(v) = d.get(field) {
                if !out.contains(v) {
                    out.push(v.clone());
                }
            }
        }
        if docs.len() < 1000 {
            break;
        }
    }
    Ok(out)
}

pub async fn explain_aggregate(c: &Collection<'_>, pipeline: Vec<Document>) -> Result<Document, String> {
    c.aggregate_all(pipeline, AggregateOptions { allow_disk_use: true, ..Default::default() })
        .await
        .map(|_| Document::new())
        .map_err(|e| e.to_string())
}

pub fn invalid_(m: &str) -> Result<Reply, String> {
    invalid(m)
}

pub fn _unused(_: &Bson) {}