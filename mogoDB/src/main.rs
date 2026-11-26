//! `mogod`: MongoDB-compatible server binary.

use mogodb::server::{self, Config, Server};
use mogodb::storage::{Store, StoreOptions};
use std::sync::Arc;
use std::time::Duration;

const USAGE: &str = "mogod - lightweight MongoDB-compatible server

Usage: mogod [options]
  --port <n>            listen port (default 27017)
  --bind_ip <ips>       comma-separated addresses (default 127.0.0.1)
  --bind_ip_all         listen on all IPv4/IPv6 addresses
  --dbpath <dir>        data directory (default ./data/db)
  --auth                require SCRAM authentication
  --replSet <name>      present as a single-node replica set (PRIMARY)
  --cacheSizeMB <n>     block cache size (default 16)
  --oplogSizeMB <n>     oplog ring buffer size (default 64)
  -v, --verbose         log every command
  --version             print version
  -h, --help            this help";

fn parse_args() -> Result<Config, String> {
    let argv: Vec<String> = std::env::args().collect();
    let mut cfg = Config {
        port: 27017,
        bind_ip: vec!["127.0.0.1".into()],
        dbpath: "./data/db".into(),
        auth: false,
        repl_set: None,
        cache_mb: 16,
        oplog_mb: 64,
        verbose: false,
        argv: argv.clone(),
    };
    let mut it = argv.iter().skip(1);
    while let Some(a) = it.next() {
        let (key, inline) = match a.split_once('=') {
            Some((k, v)) if k.starts_with("--") => (k.to_string(), Some(v.to_string())),
            _ => (a.clone(), None),
        };
        let mut val = |name: &str| -> Result<String, String> {
            inline.clone().or_else(|| it.next().cloned()).ok_or_else(|| format!("missing value for {name}"))
        };
        match key.as_str() {
            "--port" => cfg.port = val("--port")?.parse().map_err(|e| format!("--port: {e}"))?,
            "--bind_ip" => cfg.bind_ip = val("--bind_ip")?.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            "--bind_ip_all" => cfg.bind_ip = vec!["0.0.0.0".into()],
            "--dbpath" => cfg.dbpath = val("--dbpath")?.into(),
            "--auth" => cfg.auth = true,
            "--noauth" => cfg.auth = false,
            "--replSet" => cfg.repl_set = Some(val("--replSet")?),
            "--cacheSizeMB" | "--wiredTigerCacheSizeGB" => {
                let v: f64 = val(&key)?.parse().map_err(|e| format!("{key}: {e}"))?;
                cfg.cache_mb = if key == "--wiredTigerCacheSizeGB" { (v * 1024.0) as u64 } else { v as u64 };
            }
            "--oplogSizeMB" => cfg.oplog_mb = val("--oplogSizeMB")?.parse().map_err(|e| format!("--oplogSizeMB: {e}"))?,
            "-v" | "--verbose" | "-vv" | "-vvv" => cfg.verbose = true,
            "--quiet" | "--nounixsocket" | "--fork" | "--journal" | "--nojournal" => {}
            "--logpath" | "--pidfilepath" | "--storageEngine" | "--keyFile" | "--setParameter" => {
                let _ = val(&key)?;
            }
            "--version" => {
                println!("db version v{}\nmogodb {}", server::VERSION, env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unrecognised option '{other}'")),
        }
    }
    Ok(cfg)
}

fn main() {
    let cfg = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error parsing command line: {e}\ntry 'mogod --help' for more information");
            std::process::exit(2);
        }
    };
    let threads = std::thread::available_parallelism().map(|n| n.get().min(4)).unwrap_or(2);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().thread_stack_size(4 << 20).build().expect("tokio runtime");
    rt.block_on(async move {
        let store = match Store::open(&cfg.dbpath, StoreOptions { cache_mb: cfg.cache_mb, oplog_mb: cfg.oplog_mb }) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to open dbpath {}: {e}", cfg.dbpath.display());
                std::process::exit(100);
            }
        };
        let srv = Server::new(store, cfg);
        srv.log(format!("mogod {} starting: dbpath={} port={} auth={} replSet={:?}", server::VERSION, srv.cfg.dbpath.display(), srv.cfg.port, srv.cfg.auth, srv.cfg.repl_set));
        spawn_maintenance(srv.clone());
        let s2 = srv.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            s2.log("received interrupt, shutting down");
            s2.shutdown.notify_waiters();
        });
        if let Err(e) = server::run(srv.clone()).await {
            eprintln!("listen failed: {e}");
            std::process::exit(48);
        }
        let _ = srv.store.flush();
        srv.log("shutdown complete");
    });
}

/// Background workers: TTL deletion (60s), cursor/session reaping and oplog trimming (10s).
fn spawn_maintenance(srv: Arc<Server>) {
    let s = srv.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let timeout = s.parameters.lock().ok().and_then(|p| p.get("cursorTimeoutMillis").and_then(mogodb::bsonutil::num_i64)).unwrap_or(600_000);
            s.cursors.reap(Duration::from_millis(timeout.max(1) as u64));
            s.sessions.reap();
            if let Err(e) = s.store.trim_oplog() {
                s.log(format!("oplog trim failed: {e}"));
            }
            let st = s.store.clone();
            if let Ok(Err(e)) = tokio::task::spawn_blocking(move || st.refresh_histograms()).await {
                s.log(format!("histogram refresh failed: {e}"));
            }
        }
    });
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let s2 = srv.clone();
            let r = tokio::task::spawn_blocking(move || mogodb::ttl::run_pass(&s2)).await;
            if let Ok(Err(e)) = r {
                srv.log(format!("TTL monitor failed: {e}"));
            }
        }
    });
}
