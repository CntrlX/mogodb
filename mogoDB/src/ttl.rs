//! TTL monitor: deletes documents whose indexed date is older than `expireAfterSeconds`.

use crate::error::Result;
use crate::query::matcher::Matcher;
use crate::server::Server;
use bson::{Bson, Document, doc};

/// One TTL sweep over every collection with a TTL index. Returns documents deleted.
pub fn run_pass(srv: &Server) -> Result<u64> {
    let store = &srv.store;
    let mut deleted = 0u64;
    let now = bson::DateTime::now().timestamp_millis();
    for c in store.list_colls(None) {
        if c.is_view() {
            continue;
        }
        // Time series collections expire by the collection-level `expireAfterSeconds`.
        let ts_job = crate::timeseries::ttl_filter(&c.options, now).map(|f| (f, true));
        let idx_jobs = c.indexes().into_iter().filter_map(|idx| {
            let secs = idx.expire_after?;
            if idx.fields.len() != 1 {
                return None;
            }
            let cutoff = bson::DateTime::from_millis(now - (secs * 1000.0) as i64);
            Some((doc! {idx.fields[0].0.as_str(): {"$lt": Bson::DateTime(cutoff)}}, false))
        });
        let jobs: Vec<Document> = ts_job.into_iter().chain(idx_jobs).map(|(f, _)| f).collect();
        for filter in jobs {
            let m = Matcher::new(&filter)?;
            let plan = store.plan(&c, &filter, None)?;
            let snap = store.snapshot();
            let victims: Vec<u64> = store
                .execute(&snap, &c, &plan)?
                .filter_map(|r| r.ok())
                .filter(|(_, d)| m.matches(d).unwrap_or(false))
                .map(|(rid, _)| rid)
                .collect();
            for chunk in victims.chunks(500) {
                deleted += store.with_retry(|t| {
                    let mut n = 0;
                    for rid in chunk {
                        if let Some(d) = store.get_rid(&t.tx, &c, *rid)? {
                            if m.matches(&d)? {
                                store.delete(t, &c, *rid, &d)?;
                                n += 1;
                            }
                        }
                    }
                    Ok(n)
                })?;
            }
        }
    }
    Ok(deleted)
}
