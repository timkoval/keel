//! One process owns the database. The Unix socket is the only mutation path in
//! this slice (`POST /v1/items`). `127.0.0.1` serves GET document routes only.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::thread::JoinHandle;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

use crate::config::Config;
use crate::db::{open, Error, Result};
use crate::http::{self, Request, Response};
use crate::render::{self, CardView, PointView, QueueLink, QueueView};
use crate::state::{self, RawIntake};

const LOCK_EX: i32 = 2;
const LOCK_NB: i32 = 4;
/// Same cap the state machine enforces. Status only reports it.
const LEASE_CAP: i64 = 4;

pub(crate) const ITEM_STATES: &[&str] = &[
    "inbox",
    "planning",
    "awaiting_plan_review",
    "revision_requested",
    "revising",
    "awaiting_implementation",
    "implementing",
    "awaiting_merge",
    "merge_conflict",
    "merged",
    "dropped",
];

extern "C" {
    fn flock(fd: i32, op: i32) -> i32;
}

struct Shared {
    db: Mutex<Connection>,
    config: Config,
}

struct LockGuard {
    file: File,
    pid: u32,
    start: String,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Blank our pid before the flock drops so a later start in this same
        // process is not refused. A crash skips this and leaves a dead pid.
        if let Ok(Some((pid, start))) = read_lock_file(&mut self.file) {
            if pid == self.pid && start == self.start {
                let _ = self.file.set_len(0);
                let _ = self.file.sync_all();
            }
        }
    }
}

enum Owner {
    Alive,
    Dead,
    /// `/proc` could not be read. Do not steal the database.
    Unverified,
}

pub(crate) fn lock_owner_alive(path: &Path) -> bool {
    let Ok(text) = fs::read_to_string(path) else {
        return false;
    };
    match parse_lock(&text) {
        Some((pid, start)) => matches!(owner_state(pid, &start), Owner::Alive | Owner::Unverified),
        None => false,
    }
}

pub(crate) fn serve(config: &Config, shutdown: &AtomicBool) -> Result<()> {
    ensure_private_dir(&config.runtime_dir)?;
    let _lock = acquire_lock(&config.lock_path)?;
    // The previous owner is dead, or this is the first start. The socket file
    // is not a listener anymore; replace it.
    let _ = fs::remove_file(&config.socket_path);
    let unix = bind_socket(&config.socket_path)?;
    let tcp = match TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], config.port))) {
        Ok(listener) => listener,
        Err(err) => {
            drop(unix);
            let _ = fs::remove_file(&config.socket_path);
            return Err(Error::Io(std::io::Error::new(
                err.kind(),
                format!("bind 127.0.0.1:{}: {err}", config.port),
            )));
        }
    };
    unix.set_nonblocking(true)?;
    tcp.set_nonblocking(true)?;
    ensure_private_dir(&config.data_dir)?;
    let db = open(&config.db_path)?;
    let mut db_perms = fs::metadata(&config.db_path)?.permissions();
    db_perms.set_mode(0o600);
    fs::set_permissions(&config.db_path, db_perms)?;
    let shared = Arc::new(Shared {
        db: Mutex::new(db),
        config: config.clone(),
    });
    // Polled accept so a test can stop the thread without a signal.
    while !shutdown.load(Ordering::SeqCst) {
        let mut idle = true;
        match unix.accept() {
            Ok((stream, _)) => {
                idle = false;
                let shared = Arc::clone(&shared);
                thread::spawn(move || handle_unix(stream, shared));
            }
            Err(err) if is_idle(&err) => {}
            Err(err) => {
                let _ = fs::remove_file(&config.socket_path);
                return Err(err.into());
            }
        }
        match tcp.accept() {
            Ok((stream, _)) => {
                idle = false;
                let shared = Arc::clone(&shared);
                thread::spawn(move || handle_tcp(stream, shared));
            }
            Err(err) if is_idle(&err) => {}
            Err(err) => {
                let _ = fs::remove_file(&config.socket_path);
                return Err(err.into());
            }
        }
        if idle {
            thread::sleep(Duration::from_millis(20));
        }
    }
    let _ = fs::remove_file(&config.socket_path);
    Ok(())
}

#[cfg(test)]
pub(crate) struct Running {
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
}

#[cfg(test)]
impl Running {
    pub(crate) fn start(config: Config) -> Result<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&shutdown);
        let cfg = config.clone();
        let mut thread = Some(thread::spawn(move || serve(&cfg, &flag)));
        let started = std::time::Instant::now();
        loop {
            if UnixStream::connect(&config.socket_path).is_ok() {
                return Ok(Running { shutdown, thread });
            }
            let finished = thread.as_ref().is_some_and(|handle| handle.is_finished());
            if finished {
                let err = match thread.take().unwrap().join() {
                    Ok(Ok(())) => Error::Invalid("daemon exited before listening".into()),
                    Ok(Err(err)) => err,
                    Err(_) => Error::Invalid("daemon panicked".into()),
                };
                return Err(err);
            }
            if started.elapsed() > Duration::from_secs(3) {
                shutdown.store(true, Ordering::SeqCst);
                if let Some(handle) = thread.take() {
                    let _ = handle.join();
                }
                return Err(Error::Invalid("timed out waiting for the socket".into()));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

#[cfg(test)]
impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_unix(mut stream: UnixStream, shared: Arc<Shared>) {
    prepare_stream(&stream);
    let req = match http::read_request(&mut stream) {
        Ok(req) => req,
        Err(err) if quiet(&err) => return,
        Err(err) => {
            let _ = http::write_response(&mut stream, &http::error_response(400, &err), true);
            return;
        }
    };
    let include_body = req.method != "HEAD";
    let response = route_unix(&req, &shared);
    let _ = http::write_response(&mut stream, &response, include_body);
}

fn handle_tcp(mut stream: TcpStream, shared: Arc<Shared>) {
    prepare_stream(&stream);
    let req = match http::read_request(&mut stream) {
        Ok(req) => req,
        Err(err) if quiet(&err) => return,
        Err(err) => {
            let _ = http::write_response(&mut stream, &http::error_response(400, &err), true);
            return;
        }
    };
    let include_body = req.method != "HEAD";
    let response = route_tcp(&req, &shared);
    let _ = http::write_response(&mut stream, &response, include_body);
}

fn prepare_stream(stream: &impl Silence) {
    stream.prepare();
}

trait Silence {
    fn prepare(&self);
}

impl Silence for UnixStream {
    fn prepare(&self) {
        let _ = self.set_nonblocking(false);
        let _ = self.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = self.set_write_timeout(Some(Duration::from_secs(5)));
    }
}

impl Silence for TcpStream {
    fn prepare(&self) {
        let _ = self.set_nonblocking(false);
        let _ = self.set_read_timeout(Some(Duration::from_secs(5)));
        let _ = self.set_write_timeout(Some(Duration::from_secs(5)));
    }
}

fn quiet(err: &Error) -> bool {
    match err {
        Error::Invalid(msg) if msg == "eof" => true,
        Error::Io(err) => matches!(
            err.kind(),
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        ),
        _ => false,
    }
}

fn route_unix(req: &Request, shared: &Shared) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/v1/items") => create_response(shared, &req.body),
        ("GET" | "HEAD", "/v1/items") => list_response(shared, http::query(&req.query, "state")),
        ("GET" | "HEAD", "/v1/status") => status_response(shared),
        ("GET" | "HEAD", path) if path.starts_with("/v1/items/") => {
            let id = &path["/v1/items/".len()..];
            if id.contains('/') || !render::safe_id(id) {
                return missing(id);
            }
            item_response(shared, id)
        }
        ("GET" | "HEAD", _) => missing(&req.path),
        ("POST", _) => missing(&req.path),
        _ => method_not_allowed("GET, POST"),
    }
}

/// The document port has no create route. Tailscale Serve would forward the whole port.
fn route_tcp(req: &Request, shared: &Shared) -> Response {
    if req.method != "GET" && req.method != "HEAD" {
        return method_not_allowed("GET");
    }
    match req.path.as_str() {
        "/health" => Response::json(200, json!({ "ok": true })),
        "/queue/plan-review" => queue_response(shared, QueueKind::Plan),
        "/queue/feedback" => queue_response(shared, QueueKind::Feedback),
        other => match card_id(other) {
            Some(id) => card_response(shared, id),
            None => missing(other),
        },
    }
}

fn method_not_allowed(allow: &str) -> Response {
    let mut response = http::error_response(405, &Error::Invalid("method not allowed".into()));
    response.extra.push(("Allow".into(), allow.into()));
    response
}

fn missing(id: &str) -> Response {
    http::error_response(404, &Error::NotFound(id.to_string()))
}

fn fail(err: Error) -> Response {
    let status = http::status_code(&err);
    http::error_response(status, &err)
}

#[derive(Clone, Copy)]
enum QueueKind {
    Plan,
    Feedback,
}

fn create_response(shared: &Shared, body: &[u8]) -> Response {
    let raw = match intake_from_json(body) {
        Ok(raw) => raw,
        Err(err) => return fail(err),
    };
    let id = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::create_item(&mut conn, raw) {
            Ok(id) => id,
            Err(err) => return fail(err),
        }
    };
    if let Err(err) = publish_card(shared, &id) {
        return fail(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("item {id} stored but card was not written: {err}"),
        )));
    }
    Response::json(
        200,
        json!({
            "id": id,
            "card_url": shared.config.card_url(&id),
        }),
    )
}

fn intake_from_json(body: &[u8]) -> Result<RawIntake> {
    let value: Value =
        serde_json::from_slice(body).map_err(|err| Error::Invalid(format!("json: {err}")))?;
    let obj = value
        .as_object()
        .ok_or_else(|| Error::Invalid("expected a JSON object".into()))?;
    let title = required_string(obj, "title")?;
    let body = optional_string(obj, "body")?.unwrap_or_default();
    let source = optional_string(obj, "source")?.unwrap_or_else(|| "cli".into());
    let external_id = optional_string(obj, "external_id")?;
    let repo = optional_string(obj, "repo")?.filter(|value| !value.is_empty());
    let target_branch = optional_string(obj, "target_branch")?.filter(|value| !value.is_empty());
    Ok(RawIntake {
        source,
        external_id,
        title,
        body,
        repo: repo.map(PathBuf::from),
        target_branch,
    })
}

fn required_string(obj: &serde_json::Map<String, Value>, key: &str) -> Result<String> {
    match obj.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(Error::Invalid(format!("{key} is required"))),
    }
}

fn optional_string(obj: &serde_json::Map<String, Value>, key: &str) -> Result<Option<String>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::Invalid(format!("{key} must be a string"))),
    }
}

fn list_response(shared: &Shared, state: Option<&str>) -> Response {
    let state = state.filter(|value| !value.is_empty()).map(str::to_string);
    let conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return fail(err),
    };
    match list_items(&conn, state.as_deref()) {
        Ok(items) => Response::json(200, json!({ "items": items })),
        Err(err) => fail(err),
    }
}

fn item_response(shared: &Shared, id: &str) -> Response {
    let conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return fail(err),
    };
    match load_item_json(&conn, id, &shared.config) {
        Ok(item) => Response::json(200, item),
        Err(err) => fail(err),
    }
}

fn status_response(shared: &Shared) -> Response {
    let conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return fail(err),
    };
    match load_status(&conn, &shared.config) {
        Ok(status) => Response::json(200, status),
        Err(err) => fail(err),
    }
}

fn card_response(shared: &Shared, id: &str) -> Response {
    let path = shared.config.docs_dir.join(id).join("card.html");
    if let Ok(text) = fs::read_to_string(&path) {
        return Response::html(text);
    }
    if let Err(err) = publish_card(shared, id) {
        return fail(err);
    }
    match fs::read_to_string(&path) {
        Ok(text) => Response::html(text),
        Err(err) => fail(err.into()),
    }
}

fn queue_response(shared: &Shared, kind: QueueKind) -> Response {
    let view = {
        let conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match load_queue(&conn, &shared.config, kind) {
            Ok(view) => view,
            Err(err) => return fail(err),
        }
    };
    Response::html(render::queue_page(&view))
}

fn publish_card(shared: &Shared, id: &str) -> Result<()> {
    if !render::safe_id(id) {
        return Err(Error::NotFound(id.to_string()));
    }
    let card = {
        let conn = lock_db(&shared.db)?;
        load_card(&conn, id)?
    };
    let html = render::card_page(&card);
    let canonical = shared.config.docs_dir.join(&card.id).join("card.html");
    // The database lock is not held across the disk write.
    write_atomic(&canonical, html.as_bytes())?;
    {
        let conn = lock_db(&shared.db)?;
        let rel = format!("docs/{}/card.html", card.id);
        conn.execute(
            "UPDATE items SET card_relpath = ?1 WHERE id = ?2",
            params![rel, card.id],
        )?;
    }
    let published = shared
        .config
        .publish_root
        .join("keel")
        .join(&card.id)
        .join("card.html");
    // The artifact copy is best-effort. The canonical file is the record.
    if let Err(err) = write_atomic(&published, html.as_bytes()) {
        eprintln!("keel: publish {} failed: {err}", card.id);
    }
    Ok(())
}

fn load_card(conn: &Connection, id: &str) -> Result<CardView> {
    let row = conn
        .query_row(
            "SELECT id, title, body, source, repo, target_branch, state, summary,
                    plan_rev, created_at, updated_at
             FROM items WHERE id = ?1",
            params![id],
            |row| {
                Ok(CardView {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    body: row.get(2)?,
                    source: row.get(3)?,
                    repo: row.get(4)?,
                    target_branch: row.get(5)?,
                    state: row.get(6)?,
                    summary: row.get(7)?,
                    plan_rev: row.get(8)?,
                    created_at: row.get(9)?,
                    updated_at: row.get(10)?,
                    open_points: Vec::new(),
                })
            },
        )
        .optional()?;
    let Some(mut card) = row else {
        return Err(Error::NotFound(id.to_string()));
    };
    let mut stmt =
        conn.prepare("SELECT id, text, status FROM open_points WHERE item_id = ?1 ORDER BY id")?;
    let points = stmt.query_map(params![id], |row| {
        Ok(PointView {
            id: row.get(0)?,
            text: row.get(1)?,
            status: row.get(2)?,
        })
    })?;
    for point in points {
        card.open_points.push(point?);
    }
    Ok(card)
}

fn list_items(conn: &Connection, state: Option<&str>) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT id, title, state, source, summary, repo, target_branch, created_at, updated_at
         FROM items
         WHERE (?1 IS NULL OR state = ?1)
         ORDER BY updated_at DESC, id DESC",
    )?;
    let rows = stmt.query_map(params![state], |row| {
        Ok(json!({
            "id": row.get::<_, String>(0)?,
            "title": row.get::<_, String>(1)?,
            "state": row.get::<_, String>(2)?,
            "source": row.get::<_, String>(3)?,
            "summary": row.get::<_, String>(4)?,
            "repo": row.get::<_, Option<String>>(5)?,
            "target_branch": row.get::<_, Option<String>>(6)?,
            "created_at": row.get::<_, String>(7)?,
            "updated_at": row.get::<_, String>(8)?,
        }))
    })?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

fn load_item_json(conn: &Connection, id: &str, config: &Config) -> Result<Value> {
    let row = conn
        .query_row(
            "SELECT id, title, body, source, external_id, repo, target_branch, state,
                    summary, plan_rev, created_at, updated_at
             FROM items WHERE id = ?1",
            params![id],
            |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "title": row.get::<_, String>(1)?,
                    "body": row.get::<_, String>(2)?,
                    "source": row.get::<_, String>(3)?,
                    "external_id": row.get::<_, Option<String>>(4)?,
                    "repo": row.get::<_, Option<String>>(5)?,
                    "target_branch": row.get::<_, Option<String>>(6)?,
                    "state": row.get::<_, String>(7)?,
                    "summary": row.get::<_, String>(8)?,
                    "plan_rev": row.get::<_, i64>(9)?,
                    "created_at": row.get::<_, String>(10)?,
                    "updated_at": row.get::<_, String>(11)?,
                }))
            },
        )
        .optional()?;
    let Some(mut item) = row else {
        return Err(Error::NotFound(id.to_string()));
    };
    item["card_url"] = json!(config.card_url(id));
    item["events"] = json!(load_events(conn, id)?);
    Ok(item)
}

fn load_events(conn: &Connection, id: &str) -> Result<Vec<Value>> {
    let mut stmt = conn.prepare(
        "SELECT id, at, actor, job_id, kind, from_state, to_state, detail
         FROM events
         WHERE item_id = ?1
         ORDER BY id DESC
         LIMIT 10",
    )?;
    let rows = stmt.query_map(params![id], |row| {
        Ok(json!({
            "id": row.get::<_, i64>(0)?,
            "at": row.get::<_, String>(1)?,
            "actor": row.get::<_, String>(2)?,
            "job_id": row.get::<_, Option<String>>(3)?,
            "kind": row.get::<_, String>(4)?,
            "from_state": row.get::<_, Option<String>>(5)?,
            "to_state": row.get::<_, Option<String>>(6)?,
            "detail": row.get::<_, Option<String>>(7)?,
        }))
    })?;
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

fn load_status(conn: &Connection, config: &Config) -> Result<Value> {
    let mut counts = serde_json::Map::new();
    for state in ITEM_STATES {
        counts.insert((*state).to_string(), json!(0));
    }
    let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM items GROUP BY state")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (state, count) = row?;
        counts.insert(state, json!(count));
    }
    let mut lease_stmt = conn.prepare(
        "SELECT 'item', id, claim_worker, claim_role, claim_pid, claim_starttime, lease_until
         FROM items WHERE claim_token IS NOT NULL
         UNION ALL
         SELECT 'job', id, claim_worker, claim_role, claim_pid, claim_starttime, lease_until
         FROM jobs WHERE claim_token IS NOT NULL
         ORDER BY 1, 2",
    )?;
    let leases = lease_stmt.query_map([], |row| {
        Ok(json!({
            "kind": row.get::<_, String>(0)?,
            "id": row.get::<_, String>(1)?,
            "worker": row.get::<_, Option<String>>(2)?,
            "role": row.get::<_, Option<String>>(3)?,
            "pid": row.get::<_, Option<i64>>(4)?,
            "starttime": row.get::<_, Option<String>>(5)?,
            "lease_until": row.get::<_, Option<String>>(6)?,
        }))
    })?;
    let mut lease_rows = Vec::new();
    for lease in leases {
        lease_rows.push(lease?);
    }
    Ok(json!({
        "profile": config.profile,
        "pid": std::process::id(),
        "socket": config.socket_path,
        "port": config.port,
        "lease_cap": LEASE_CAP,
        "counts": counts,
        "leases": lease_rows,
    }))
}

fn load_queue(conn: &Connection, config: &Config, kind: QueueKind) -> Result<QueueView> {
    match kind {
        QueueKind::Plan => {
            let mut stmt = conn.prepare(
                "SELECT id, title, summary FROM items
                 WHERE state = 'awaiting_plan_review'
                 ORDER BY updated_at, id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            let mut links = Vec::new();
            for row in rows {
                let (id, title, summary) = row?;
                if !render::safe_id(&id) {
                    continue;
                }
                links.push(QueueLink {
                    url: format!("{}/i/{id}/plan", config.origin()),
                    id,
                    title,
                    summary,
                });
            }
            Ok(QueueView {
                heading: "awaiting plan review".into(),
                name: "plan-review".into(),
                rows: links,
            })
        }
        QueueKind::Feedback => {
            let mut stmt = conn.prepare(
                "SELECT j.id, j.item_id, j.title, i.title
                 FROM jobs j
                 JOIN items i ON i.id = j.item_id
                 WHERE j.state = 'feedback_pending'
                 ORDER BY j.updated_at, j.id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?;
            let mut links = Vec::new();
            for row in rows {
                let (job_id, item_id, title, item_title) = row?;
                if !render::safe_id(&job_id) || !render::safe_id(&item_id) {
                    continue;
                }
                links.push(QueueLink {
                    url: format!("{}/i/{item_id}/jobs/{job_id}/review", config.origin()),
                    id: job_id,
                    title,
                    summary: item_title,
                });
            }
            Ok(QueueView {
                heading: "awaiting feedback".into(),
                name: "feedback".into(),
                rows: links,
            })
        }
    }
}

fn card_id(path: &str) -> Option<&str> {
    let rest = path.strip_prefix("/i/")?;
    let (id, tail) = rest.split_once('/')?;
    if tail == "card" && render::safe_id(id) {
        Some(id)
    } else {
        None
    }
}

fn lock_db(db: &Mutex<Connection>) -> Result<std::sync::MutexGuard<'_, Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp = PathBuf::from(tmp_name);
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

fn bind_socket(path: &Path) -> Result<UnixListener> {
    let listener = UnixListener::bind(path)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(path, perms)?;
    Ok(listener)
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)?;
    Ok(())
}

fn acquire_lock(path: &Path) -> Result<LockGuard> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .open(path)?;
    let mut perms = file.metadata()?.permissions();
    perms.set_mode(0o600);
    file.set_permissions(perms)?;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            let pid = fs::read_to_string(path)
                .ok()
                .and_then(|text| parse_lock(&text))
                .map(|(pid, _)| pid.to_string())
                .unwrap_or_else(|| "unknown".into());
            return Err(Error::Invalid(format!(
                "daemon already running (pid {pid})"
            )));
        }
        return Err(err.into());
    }
    let existing = read_lock_file(&mut file)?;
    if let Some((pid, start)) = existing {
        match owner_state(pid, &start) {
            // Matching starttime means that process incarnation is still the owner.
            // PID reuse is the mismatch arm and is not a lock.
            Owner::Alive => {
                return Err(Error::Invalid(format!(
                    "daemon already running (pid {pid})"
                )));
            }
            Owner::Unverified => {
                return Err(Error::Invalid(format!(
                    "lock pid {pid} could not be verified"
                )));
            }
            Owner::Dead => {}
        }
    }
    let pid = std::process::id();
    let start = read_proc_starttime(pid)
        .ok_or_else(|| Error::Invalid("could not read this process starttime from /proc".into()))?;
    write_lock(&mut file, pid, &start)?;
    Ok(LockGuard { file, pid, start })
}

fn write_lock(file: &mut File, pid: u32, start: &str) -> Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    writeln!(file, "{pid} {start}")?;
    file.sync_all()?;
    Ok(())
}

fn read_lock_file(file: &mut File) -> Result<Option<(u32, String)>> {
    file.seek(SeekFrom::Start(0))?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(parse_lock(&text))
}

fn parse_lock(text: &str) -> Option<(u32, String)> {
    let mut parts = text.split_whitespace();
    let pid = parts.next()?.parse::<u32>().ok()?;
    let start = parts.next()?.to_string();
    if pid == 0 || start.is_empty() {
        return None;
    }
    Some((pid, start))
}

fn owner_state(pid: u32, start: &str) -> Owner {
    match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(text) => match starttime_after_comm(&text) {
            Some(actual) if actual == start => Owner::Alive,
            Some(_) => Owner::Dead,
            None => Owner::Unverified,
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Owner::Dead,
        Err(_) => Owner::Unverified,
    }
}

pub(crate) fn read_proc_starttime(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    starttime_after_comm(&text)
}

/// Field 22 of `/proc/pid/stat`. `comm` may contain spaces, so count after `)`.
fn starttime_after_comm(text: &str) -> Option<String> {
    let end = text.rfind(')')?;
    let fields: Vec<&str> = text[end + 1..].split_whitespace().collect();
    fields.get(19).map(|value| (*value).to_string())
}

fn is_idle(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
    )
}

#[cfg(test)]
pub(crate) struct Rm(pub PathBuf);

#[cfg(test)]
impl Drop for Rm {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
pub(crate) fn scratch_dir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "keel-d-{}-{}-{}",
        std::process::id(),
        n,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

#[cfg(test)]
pub(crate) fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

#[cfg(test)]
pub(crate) fn boot_at(root: &Path) -> Result<(Config, Running)> {
    let mut last = None;
    for _ in 0..5 {
        let cfg = crate::config::isolated(root, free_port());
        match Running::start(cfg.clone()) {
            Ok(running) => return Ok((cfg, running)),
            Err(err) => last = Some(err),
        }
    }
    Err(last.unwrap_or_else(|| Error::Invalid("daemon did not start".into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::isolated;
    use crate::state::RawIntake;
    use std::os::unix::net::UnixListener as StdUnix;
    use std::process::Command;

    fn api(cfg: &Config, method: &str, path: &str, body: Option<&[u8]>) -> Response {
        let mut stream = UnixStream::connect(&cfg.socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        http::exchange(&mut stream, method, path, body).unwrap()
    }

    fn tcp(cfg: &Config, method: &str, path: &str, body: Option<&[u8]>) -> Response {
        let mut stream = TcpStream::connect(("127.0.0.1", cfg.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        http::exchange(&mut stream, method, path, body).unwrap()
    }

    fn post_item(cfg: &Config, title: &str, body: &str) -> Value {
        let payload = json!({
            "source": "cli",
            "title": title,
            "body": body,
        });
        let response = api(
            cfg,
            "POST",
            "/v1/items",
            Some(payload.to_string().as_bytes()),
        );
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        serde_json::from_slice(&response.body).unwrap()
    }

    #[test]
    fn dead_pid_and_stale_socket_are_replaced() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        fs::create_dir_all(&cfg.runtime_dir).unwrap();
        fs::write(&cfg.lock_path, "2147483646 1\n").unwrap();
        assert!(!Path::new("/proc/2147483646").exists());
        let listener = StdUnix::bind(&cfg.socket_path).unwrap();
        drop(listener);
        let running = Running::start(cfg.clone()).unwrap();
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        let start = read_proc_starttime(std::process::id()).unwrap();
        assert_eq!(text.trim(), format!("{} {start}", std::process::id()));
        let mode = fs::metadata(&cfg.socket_path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let dir_mode = fs::metadata(&cfg.data_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert!(loopback_only(cfg.port));
        drop(running);
    }

    #[test]
    fn live_lock_pid_does_not_open_the_database() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        fs::create_dir_all(&cfg.runtime_dir).unwrap();
        let mut child = Command::new("sleep").arg("30").spawn().unwrap();
        let pid = child.id();
        let start = read_proc_starttime(pid).unwrap();
        fs::write(&cfg.lock_path, format!("{pid} {start}\n")).unwrap();
        let err = match Running::start(cfg.clone()) {
            Err(err) => err,
            Ok(_running) => panic!("daemon started while the lock pid was live"),
        };
        assert!(err.to_string().contains("already running"), "{err}");
        assert!(!cfg.db_path.exists());
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        assert!(text.starts_with(&format!("{pid} ")), "{text}");
        let _ = child.kill();
        let _ = child.wait();
    }

    #[test]
    fn starttime_mismatch_is_not_a_live_lock() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        fs::create_dir_all(&cfg.runtime_dir).unwrap();
        let real = read_proc_starttime(std::process::id()).unwrap();
        assert_ne!(real, "1");
        fs::write(&cfg.lock_path, format!("{} 1\n", std::process::id())).unwrap();
        let running = Running::start(cfg.clone()).unwrap();
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        assert_eq!(text.trim(), format!("{} {real}", std::process::id()));
        let stop = AtomicBool::new(false);
        let again = serve(&cfg, &stop).expect_err("second daemon");
        assert!(again.to_string().contains("already running"), "{again}");
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        assert_eq!(text.trim(), format!("{} {real}", std::process::id()));
        drop(running);
    }

    #[test]
    fn intake_card_queue_and_get_only_port() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let created = post_item(&cfg, "Pin <walk>", "</script> & more");
        let id = created["id"].as_str().unwrap().to_string();
        let card_url = created["card_url"].as_str().unwrap();
        assert_eq!(card_url, cfg.card_url(&id));

        let canonical = fs::read_to_string(cfg.docs_dir.join(&id).join("card.html")).unwrap();
        let published =
            fs::read_to_string(cfg.publish_root.join("keel").join(&id).join("card.html")).unwrap();
        assert_eq!(canonical, published);
        assert!(canonical.contains("kind"));
        assert!(canonical.contains(&id));
        assert!(!canonical.contains("</script><script>"));
        assert!(canonical.contains("Pin &lt;walk&gt;"));

        let card = tcp(&cfg, "GET", &format!("/i/{id}/card"), None);
        assert_eq!(card.status, 200);
        assert_eq!(card.body, canonical.as_bytes());

        let health = tcp(&cfg, "GET", "/health", None);
        assert_eq!(health.status, 200);
        assert_eq!(health.body, br#"{"ok":true}"#);

        let posted = tcp(
            &cfg,
            "POST",
            "/v1/items",
            Some(br#"{"title":"nope","body":"x"}"#),
        );
        assert_eq!(posted.status, 405);
        let sneaky = tcp(&cfg, "POST", "/health", Some(b"{}"));
        assert_eq!(sneaky.status, 405);
        let counts_on_tcp = tcp(&cfg, "GET", "/v1/status", None);
        assert_eq!(counts_on_tcp.status, 404);

        let dup = api(
            &cfg,
            "POST",
            "/v1/items",
            Some(
                json!({"source":"cli","external_id":"ext-1","title":"Once","body":""})
                    .to_string()
                    .as_bytes(),
            ),
        );
        assert_eq!(dup.status, 200);
        let first: Value = serde_json::from_slice(&dup.body).unwrap();
        let again = api(
            &cfg,
            "POST",
            "/v1/items",
            Some(
                json!({"source":"cli","external_id":"ext-1","title":"Once","body":"ignored"})
                    .to_string()
                    .as_bytes(),
            ),
        );
        let second: Value = serde_json::from_slice(&again.body).unwrap();
        assert_eq!(first["id"], second["id"]);

        let listed = api(&cfg, "GET", "/v1/items?state=inbox", None);
        let listed: Value = serde_json::from_slice(&listed.body).unwrap();
        assert!(listed["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == id));

        let show = api(&cfg, "GET", &format!("/v1/items/{id}"), None);
        let show: Value = serde_json::from_slice(&show.body).unwrap();
        assert_eq!(show["events"][0]["kind"], "create");
        assert_eq!(show["events"][0]["to_state"], "inbox");

        let status = api(&cfg, "GET", "/v1/status", None);
        let status: Value = serde_json::from_slice(&status.body).unwrap();
        assert!(status["counts"]["inbox"].as_i64().unwrap() >= 1);
        assert!(status.get("ok").is_none());
        assert_eq!(status["lease_cap"], LEASE_CAP);
        assert_eq!(status["socket"], cfg.socket_path.to_str().unwrap());

        {
            let conn = open(&cfg.db_path).unwrap();
            conn.execute(
                "UPDATE items SET state = 'awaiting_plan_review', summary = 'sum' WHERE id = ?1",
                params![id],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (id, item_id, title, acceptance, state, depth, ord, updated_at)
                 VALUES ('j_deadbeef', ?1, 'Leaf <x>', '', 'feedback_pending', 1, 0, '2026-11-25T00:00:00Z')",
                params![id],
            )
            .unwrap();
        }
        let plan = tcp(&cfg, "GET", "/queue/plan-review", None);
        let plan_html = String::from_utf8(plan.body).unwrap();
        assert_eq!(plan.status, 200);
        assert!(plan_html.contains("awaiting plan review"));
        assert!(plan_html.contains(&format!("/i/{id}/plan")));
        assert!(!plan_html.to_ascii_lowercase().contains("<button"));
        let feedback = tcp(&cfg, "GET", "/queue/feedback", None);
        let feedback_html = String::from_utf8(feedback.body).unwrap();
        assert!(feedback_html.contains("awaiting feedback"));
        assert!(feedback_html.contains(&format!("/i/{id}/jobs/j_deadbeef/review")));
        assert!(feedback_html.contains("Leaf &lt;x&gt;"));

        let empty = api(
            &cfg,
            "POST",
            "/v1/items",
            Some(br#"{"title":"","body":""}"#),
        );
        assert_eq!(empty.status, 400);

        let rel: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT card_relpath FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rel, format!("docs/{id}/card.html"));
        drop(running);
    }

    #[test]
    fn show_keeps_the_last_ten_events_newest_first() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        fs::create_dir_all(&cfg.data_dir).unwrap();
        let id = {
            let mut conn = open(&cfg.db_path).unwrap();
            let id = state::create_item(
                &mut conn,
                RawIntake {
                    source: "cli".into(),
                    external_id: None,
                    title: "Events".into(),
                    body: "body".into(),
                    repo: None,
                    target_branch: None,
                },
            )
            .unwrap();
            for n in 0..12 {
                conn.execute(
                    "INSERT INTO events (at, actor, item_id, kind, detail)
                     VALUES ('2026-11-25T00:00:00Z', 'human', ?1, 'note', ?2)",
                    params![id, format!("n{n}")],
                )
                .unwrap();
            }
            id
        };
        let (cfg, running) = {
            let running = Running::start(cfg.clone()).unwrap();
            (cfg, running)
        };
        let show = api(&cfg, "GET", &format!("/v1/items/{id}"), None);
        let show: Value = serde_json::from_slice(&show.body).unwrap();
        let events = show["events"].as_array().unwrap();
        assert_eq!(events.len(), 10);
        let ids: Vec<i64> = events
            .iter()
            .map(|event| event["id"].as_i64().unwrap())
            .collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(ids, sorted);
        assert_eq!(events[0]["detail"], "n11");
        assert!(events
            .iter()
            .all(|event| event["detail"] != "n0" && event["detail"] != "n1"));
        assert!(events.iter().all(|event| event["kind"] != "create"));
        drop(running);
    }

    fn loopback_only(port: u16) -> bool {
        let text = fs::read_to_string("/proc/net/tcp").unwrap();
        let suffix = format!(":{port:04X}");
        let mut found = false;
        for line in text.lines().skip(1) {
            let Some(local) = line.split_whitespace().nth(1) else {
                continue;
            };
            if local.ends_with(&suffix) {
                found = true;
                let ip = local.split(':').next().unwrap_or("");
                if ip != "0100007F" {
                    return false;
                }
            }
        }
        found
    }
}
