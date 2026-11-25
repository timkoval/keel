//! One process owns the database. Mutations stay on the Unix socket.
//! `127.0.0.1` serves GET document routes only.

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
use std::time::{Duration, Instant};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};

use crate::adapters::{claude, grok};
use crate::claim::{self, Target};
use crate::config::Config;
use crate::db::{open, Error, Result};
use crate::http::{self, Request, Response};
use crate::proc;
use crate::render::{
    self, CardView, PlanJobView, PlanSection, PlanView, PointView, QueueLink, QueueView,
};
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

pub(crate) struct LockGuard {
    file: File,
    pid: u32,
    start: String,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        // Clear the pid hint before the fd closes. A crash leaves it; the
        // flock, not the text, is what the next start trusts.
        if let Ok(Some((pid, start))) = read_lock_file(&mut self.file) {
            if pid == self.pid && start == self.start {
                let _ = self.file.set_len(0);
                let _ = self.file.sync_all();
            }
        }
    }
}

pub(crate) fn serve(config: &Config, shutdown: &AtomicBool) -> Result<()> {
    ensure_private_dir(&config.data_dir)?;
    // Held for the process lifetime. The socket is unlinked only while this
    // flock is ours, so a second runtime dir cannot open the same database.
    let _lock = acquire_lock(&config.lock_path)?;
    ensure_private_dir(&config.runtime_dir)?;
    let _ = fs::remove_file(&config.socket_path);
    let unix = bind_socket(&config.socket_path)?;
    let tcp = match TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], config.port))) {
        Ok(listener) => listener,
        Err(err) => {
            // This process created the listener. A failed bind before that
            // returns above and leaves the path alone.
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
    let db = open(&config.db_path)?;
    let mut db_perms = fs::metadata(&config.db_path)?.permissions();
    db_perms.set_mode(0o600);
    fs::set_permissions(&config.db_path, db_perms)?;
    let shared = Arc::new(Shared {
        db: Mutex::new(db),
        config: config.clone(),
    });
    // Before accept, so a restart cannot hand the worktree to a new claim
    // while the previous group is still alive. Failures leave the row leased.
    let _ = claim::reap(&shared.db, &shared.config.data_dir, true);
    let mut next_reap = Instant::now() + Duration::from_secs(30);
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
        if Instant::now() >= next_reap {
            next_reap = Instant::now() + Duration::from_secs(30);
            let shared = Arc::clone(&shared);
            thread::spawn(move || {
                let _ = claim::reap(&shared.db, &shared.config.data_dir, false);
            });
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
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let req = match http::read_request(&mut stream) {
        Ok(req) => req,
        Err(err) if quiet(&err) => return,
        Err(err) => {
            let _ = http::write_response(&mut stream, &http::error_response(400, &err));
            return;
        }
    };
    let response = route_unix(&req, &shared);
    let _ = http::write_response(&mut stream, &response);
}

fn handle_tcp(mut stream: TcpStream, shared: Arc<Shared>) {
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let req = match http::read_request(&mut stream) {
        Ok(req) => req,
        Err(err) if quiet(&err) => return,
        Err(err) => {
            let _ = http::write_response(&mut stream, &http::error_response(400, &err));
            return;
        }
    };
    let response = route_tcp(&req, &shared);
    let _ = http::write_response(&mut stream, &response);
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

fn route_unix(req: &Request, shared: &Arc<Shared>) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/v1/items") => create_response(shared, &req.body),
        ("GET", "/v1/items") => list_response(shared, http::query(&req.query, "state")),
        ("GET", "/v1/status") => status_response(shared),
        ("GET", "/v1/resolve") => resolve_response(shared, http::query(&req.query, "id")),
        ("POST", path) => match action_route(path) {
            Some((target, id, action)) => mutate_response(shared, req, target, id, action),
            None => missing(path),
        },
        ("GET", path) if path.starts_with("/v1/lease/") => {
            let id = &path["/v1/lease/".len()..];
            if id.is_empty() || id.contains('/') || !render::safe_id(id) {
                return missing(id);
            }
            lease_response(shared, id)
        }
        ("GET", path) if path.starts_with("/v1/items/") => {
            let id = &path["/v1/items/".len()..];
            if id.contains('/') || !render::safe_id(id) {
                return missing(id);
            }
            item_response(shared, id)
        }
        ("GET", _) => missing(&req.path),
        _ => method_not_allowed("GET, POST"),
    }
}

fn action_route(path: &str) -> Option<(Target, &str, &str)> {
    let mut parts = path.split('/');
    if parts.next() != Some("") || parts.next() != Some("v1") {
        return None;
    }
    let target = match parts.next() {
        Some("items") => Target::Item,
        Some("jobs") => Target::Job,
        _ => return None,
    };
    let id = parts.next()?;
    let action = parts.next()?;
    if parts.next().is_some() || !render::safe_id(id) {
        return None;
    }
    if !matches!(
        action,
        "claim"
            | "heartbeat"
            | "release"
            | "note"
            | "waive"
            | "approve"
            | "drop"
            | "submit"
            | "permit"
            | "accept"
            | "reject"
            | "merge"
    ) {
        return None;
    }
    Some((target, id, action))
}

fn mutate_response(
    shared: &Arc<Shared>,
    req: &Request,
    target: Target,
    id: &str,
    action: &str,
) -> Response {
    let surface_cli = http::header(&req.headers, "x-keel-surface") == Some("cli");
    match action {
        "claim" => {
            let input = match claim::parse_claim(&req.body) {
                Ok(input) => input,
                Err(err) => return fail(err),
            };
            let spawn = match claim::requests_spawn(&req.body) {
                Ok(spawn) => spawn,
                Err(err) => return fail(err),
            };
            if spawn && input.pid.is_some() {
                return fail(Error::Invalid("external claim is not spawned".into()));
            }
            let grok_job = input.adapter.as_deref() == Some("grok");
            let claude_job = input.adapter.as_deref() == Some("claude");
            if grok_job {
                if let Err(err) = grok::ensure_grok_on_path() {
                    return fail(err);
                }
            }
            if claude_job {
                if let Err(err) = claude::ensure_claude_on_path() {
                    return fail(err);
                }
            }
            match claim::claim_and_prepare(&shared.db, target, id, &input, &shared.config.data_dir)
            {
                Ok(prepared) => {
                    if grok_job {
                        let launch = grok::Launch {
                            target: prepared.target,
                            id: prepared.id.clone(),
                            role: prepared.claim.role,
                            token: prepared.claim.token.clone(),
                            worktree: prepared.worktree.clone(),
                            prompt_path: prepared.prompt_path.clone(),
                        };
                        let shared = Arc::clone(shared);
                        thread::spawn(move || {
                            let expected = launch
                                .worktree
                                .as_ref()
                                .and_then(|path| crate::git::rev_parse(path, "HEAD").ok());
                            let held = WorkerLaunch::from_grok(&launch);
                            let result =
                                grok::execute(&shared.db, &shared.config.data_dir, &launch);
                            finish_worker(
                                &shared,
                                &held,
                                expected.as_deref(),
                                result.map(|done| done.output),
                            );
                        });
                    } else if claude_job {
                        let launch = claude::Launch {
                            target: prepared.target,
                            id: prepared.id.clone(),
                            role: prepared.claim.role,
                            token: prepared.claim.token.clone(),
                            worktree: prepared.worktree.clone(),
                            prompt_path: prepared.prompt_path.clone(),
                        };
                        let shared = Arc::clone(shared);
                        thread::spawn(move || {
                            let expected = launch
                                .worktree
                                .as_ref()
                                .and_then(|path| crate::git::rev_parse(path, "HEAD").ok());
                            let held = WorkerLaunch::from_claude(&launch);
                            let result =
                                claude::execute(&shared.db, &shared.config.data_dir, &launch);
                            finish_worker(
                                &shared,
                                &held,
                                expected.as_deref(),
                                result.map(|done| done.output),
                            );
                        });
                    } else if spawn {
                        if let Err(failure) = spawn_and_attach(
                            shared,
                            &prepared.id,
                            &prepared.claim.token,
                            prepared.worktree.as_deref(),
                        ) {
                            return fail(abandon_failed_spawn(
                                &shared.db,
                                target,
                                id,
                                &shared.config.data_dir,
                                failure,
                            ));
                        }
                    }
                    Response::json(
                        200,
                        json!({
                            "id": prepared.id,
                            "kind": prepared.target.as_str(),
                            "role": prepared.claim.role.as_str(),
                            "claim_token": prepared.claim.token,
                            "worker_id": prepared.claim.worker_id,
                            "lease_until": prepared.claim.lease_until,
                            "worktree": prepared.worktree.as_ref().map(|path| path.display().to_string()),
                            "prompt_path": prepared.prompt_path.display().to_string(),
                            "diff_path": prepared.diff_path.as_ref().map(|path| path.display().to_string()),
                            "snapshot": snapshot_json(prepared.snapshot.as_ref()),
                        }),
                    )
                }
                Err(err) => fail(err),
            }
        }
        "heartbeat" => {
            let token = match claim::parse_heartbeat(&req.body) {
                Ok(token) => token,
                Err(err) => return fail(err),
            };
            match claim::heartbeat(&shared.db, target, id, &token) {
                Ok(lease_until) => Response::json(
                    200,
                    json!({
                        "id": id,
                        "kind": target.as_str(),
                        "lease_until": lease_until,
                    }),
                ),
                Err(err) => fail(err),
            }
        }
        "release" => {
            let release = match claim::parse_release(&req.body, surface_cli) {
                Ok(release) => release,
                Err(err) => return fail(err),
            };
            match claim::release(&shared.db, target, id, release, &shared.config.data_dir) {
                Ok(state) => Response::json(
                    200,
                    json!({
                        "id": id,
                        "kind": target.as_str(),
                        "state": state,
                    }),
                ),
                Err(err) => fail(err),
            }
        }
        "note" => item_action(target, action, || note_response(shared, id, req)),
        "waive" => item_action(target, action, || {
            waive_response(shared, id, req, surface_cli)
        }),
        "approve" => item_action(target, action, || {
            approve_response(shared, id, req, surface_cli)
        }),
        "drop" => item_action(target, action, || {
            drop_response(shared, id, req, surface_cli)
        }),
        "submit" => item_action(target, action, || submit_response(shared, id, req)),
        "permit" => permit_response(shared, target, id, req, surface_cli),
        "accept" => job_action(target, action, || accept_response(shared, id, surface_cli)),
        "reject" => job_action(target, action, || {
            reject_response(shared, id, req, surface_cli)
        }),
        "merge" => item_action(target, action, || {
            merge_response(shared, id, req, surface_cli)
        }),
        _ => missing(action),
    }
}

fn job_action(target: Target, action: &str, call: impl FnOnce() -> Response) -> Response {
    if target != Target::Job {
        return fail(Error::Invalid(format!("{action} is a job route")));
    }
    call()
}

fn item_action(target: Target, action: &str, call: impl FnOnce() -> Response) -> Response {
    if target != Target::Item {
        return fail(Error::Invalid(format!("{action} is an item route")));
    }
    call()
}

fn resolve_response(shared: &Shared, id: Option<&str>) -> Response {
    let Some(id) = id.filter(|id| !id.is_empty()) else {
        return fail(Error::Invalid("id is required".into()));
    };
    let conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return fail(err),
    };
    match claim::resolve_id(&conn, id) {
        Ok(resolved) => Response::json(
            200,
            json!({
                "id": resolved.id,
                "kind": resolved.target.as_str(),
            }),
        ),
        Err(err) => fail(err),
    }
}

/// The document port has no create route. Tailscale Serve would forward the whole port.
fn route_tcp(req: &Request, shared: &Shared) -> Response {
    if req.method != "GET" {
        return method_not_allowed("GET");
    }
    match req.path.as_str() {
        "/health" => Response::json(200, json!({ "ok": true })),
        "/queue/plan-review" => queue_response(shared, QueueKind::Plan),
        "/queue/feedback" => queue_response(shared, QueueKind::Feedback),
        other => match doc_route(other) {
            Some(Doc::Card(id)) => card_response(shared, id),
            Some(Doc::Plan { id, rev }) => plan_response(shared, id, rev),
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

fn spawn_and_attach(
    shared: &Shared,
    id: &str,
    token: &str,
    worktree: Option<&Path>,
) -> std::result::Result<(), proc::SpawnFailure> {
    let marker = match worktree {
        Some(path) => path.join(proc::GRANDCHILD_FILE),
        None => shared
            .config
            .data_dir
            .join("spawns")
            .join(id)
            .join(proc::GRANDCHILD_FILE),
    };
    let spawned = proc::spawn_marker(&marker)?;
    let mut conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return Err(proc::kill_unless_empty(spawned.pid, err)),
    };
    if let Err(err) = state::attach_process(
        &mut conn,
        id,
        token,
        spawned.pid,
        spawned.pgid,
        &spawned.starttime,
    ) {
        drop(conn);
        return Err(proc::kill_unless_empty(spawned.pid, err));
    }
    Ok(())
}

/// A live group stays leased. The row goes back to the queue only when the
/// group was never created or `empty_group` already saw it empty.
pub(crate) fn abandon_failed_spawn(
    db: &Mutex<Connection>,
    target: Target,
    id: &str,
    data_dir: &Path,
    failure: proc::SpawnFailure,
) -> Error {
    if !failure.group_live {
        let _ = claim::release(db, target, id, state::Release::human(), data_dir);
    }
    failure.error
}

fn accept_response(shared: &Shared, id: &str, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let accepted = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::accept_job(&mut conn, id) {
            Ok(()) => job_view(&conn, id),
            Err(err) => Err(err),
        }
    };
    match accepted {
        Ok((job_state, item_id, item_state)) => Response::json(
            200,
            json!({
                "id": id,
                "state": job_state,
                "item_id": item_id,
                "item_state": item_state,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn reject_response(shared: &Shared, id: &str, req: &Request, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let text = match required_string(&obj, "text") {
        Ok(text) => text,
        Err(err) => return fail(err),
    };
    let rejected = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::reject_job(&mut conn, id, &text) {
            Ok(()) => job_view(&conn, id),
            Err(err) => Err(err),
        }
    };
    match rejected {
        Ok((job_state, item_id, item_state)) => Response::json(
            200,
            json!({
                "id": id,
                "state": job_state,
                "item_id": item_id,
                "item_state": item_state,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn merge_response(shared: &Shared, id: &str, req: &Request, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object_or_empty(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let cont = match optional_bool(&obj, "continue") {
        Ok(cont) => cont,
        Err(err) => return fail(err),
    };
    let abort = match optional_bool(&obj, "abort") {
        Ok(abort) => abort,
        Err(err) => return fail(err),
    };
    if cont && abort {
        return fail(Error::Invalid("merge cannot continue and abort".into()));
    }
    let mode = if abort {
        crate::git::MergeMode::Abort
    } else if cont {
        crate::git::MergeMode::Continue
    } else {
        crate::git::MergeMode::Plain
    };
    // Git runs outside the SQLite mutex. The flock lives inside merge_local.
    match crate::git::merge_local(&shared.db, &shared.config.data_dir, id, mode) {
        Ok(report) => Response::json(
            200,
            json!({
                "id": report.id,
                "state": report.state,
                "merged_sha": report.merged_sha,
                "integrate_sha": report.integrate_sha,
                "conflict_kind": report.conflict_kind,
                "worktree": report.worktree.display().to_string(),
            }),
        ),
        Err(err) => fail(err),
    }
}

fn job_view(conn: &Connection, id: &str) -> Result<(String, String, String)> {
    conn.query_row(
        "SELECT j.state, i.id, i.state
         FROM jobs j JOIN items i ON i.id = j.item_id
         WHERE j.id = ?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}

fn permit_response(
    shared: &Shared,
    target: Target,
    id: &str,
    req: &Request,
    surface_cli: bool,
) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let choice = match required_string(&obj, "choice") {
        Ok(choice) if choice.trim().is_empty() => {
            return fail(Error::Invalid("missing permit choice".into()));
        }
        Ok(choice) => choice,
        Err(err) => return fail(err),
    };
    match grok::answer(&shared.db, target, id, &choice) {
        Ok(result) => Response::json(200, json!({ "id": id, "result": result })),
        Err(err) => fail(err),
    }
}

fn lease_response(shared: &Shared, id: &str) -> Response {
    let conn = match lock_db(&shared.db) {
        Ok(conn) => conn,
        Err(err) => return fail(err),
    };
    let resolved = match claim::resolve_id(&conn, id) {
        Ok(resolved) => resolved,
        Err(err) => return fail(err),
    };
    let mut body = match lease_row(&conn, resolved.target, &resolved.id) {
        Ok(body) => body,
        Err(err) => return fail(err),
    };
    drop(conn);
    if let Some(pending) = grok::pending_public(&resolved.id) {
        body["pending_permit"] = pending;
    }
    if let Some(object) = body.as_object_mut() {
        object.remove("claim_token");
        object.remove("secret");
    }
    Response::json(200, body)
}

fn lease_row(conn: &Connection, target: Target, id: &str) -> Result<Value> {
    match target {
        Target::Job => conn
            .query_row(
                "SELECT state, blocked_reason, pending_permit, claim_token IS NOT NULL, claim_worker
                 FROM jobs WHERE id = ?1",
                params![id],
                |row| {
                    let pending = row.get::<_, Option<String>>(2)?;
                    Ok(json!({
                        "id": id,
                        "kind": "job",
                        "state": row.get::<_, String>(0)?,
                        "blocked_reason": row.get::<_, Option<String>>(1)?,
                        "pending_permit": parse_permit(&pending),
                        "claimed": row.get::<_, i64>(3)? != 0,
                        "worker_id": row.get::<_, Option<String>>(4)?,
                    }))
                },
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string())),
        Target::Item => conn
            .query_row(
                "SELECT state, claim_token IS NOT NULL, claim_worker FROM items WHERE id = ?1",
                params![id],
                |row| {
                    Ok(json!({
                        "id": id,
                        "kind": "item",
                        "state": row.get::<_, String>(0)?,
                        "blocked_reason": Value::Null,
                        "pending_permit": Value::Null,
                        "claimed": row.get::<_, i64>(1)? != 0,
                        "worker_id": row.get::<_, Option<String>>(2)?,
                    }))
                },
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(id.to_string())),
    }
}

fn parse_permit(blob: &Option<String>) -> Value {
    blob.as_deref()
        .and_then(|text| serde_json::from_str(text).ok())
        .unwrap_or(Value::Null)
}

struct WorkerLaunch {
    adapter: &'static str,
    target: Target,
    id: String,
    role: state::Role,
    token: String,
    worktree: Option<PathBuf>,
}

impl WorkerLaunch {
    fn from_grok(launch: &grok::Launch) -> Self {
        Self {
            adapter: "grok",
            target: launch.target,
            id: launch.id.clone(),
            role: launch.role,
            token: launch.token.clone(),
            worktree: launch.worktree.clone(),
        }
    }

    fn from_claude(launch: &claude::Launch) -> Self {
        Self {
            adapter: "claude",
            target: launch.target,
            id: launch.id.clone(),
            role: launch.role,
            token: launch.token.clone(),
            worktree: launch.worktree.clone(),
        }
    }
}

fn finish_worker(
    shared: &Shared,
    launch: &WorkerLaunch,
    expected_head: Option<&str>,
    result: Result<Option<String>>,
) {
    let outcome = match result {
        Ok(output) => apply_outcome(shared, launch, expected_head, output.as_deref()),
        Err(err) => Err(err),
    };
    if let Err(err) = outcome {
        // A group that survived SIGKILL stays leased. fail_implement and a
        // human release would clear the row without signaling it.
        if grok::group_still_live(&err) {
            note_log(
                &shared.config.data_dir,
                &launch.id,
                &format!("{} process group is still live; lease kept", launch.adapter),
            );
            return;
        }
        let _ = abandon_worker(shared, launch, &err.to_string());
    }
}

fn apply_outcome(
    shared: &Shared,
    launch: &WorkerLaunch,
    expected_head: Option<&str>,
    output: Option<&str>,
) -> Result<()> {
    match launch.role {
        state::Role::Plan | state::Role::Revise => {
            let document = output.ok_or_else(|| {
                Error::Invalid(format!("{} did not write a result", launch.adapter))
            })?;
            let tree = match (launch.worktree.as_ref(), expected_head) {
                (Some(path), Some(expected)) => detached_tree(path, expected)?,
                _ => state::DetachedWorktree::unchanged(),
            };
            {
                let mut conn = lock_db(&shared.db)?;
                match launch.role {
                    state::Role::Plan => {
                        state::submit_plan(&mut conn, &launch.id, &launch.token, document, tree)?;
                    }
                    _ => {
                        state::submit_revision(
                            &mut conn,
                            &launch.id,
                            &launch.token,
                            document,
                            tree,
                        )?;
                    }
                }
            }
            if let Err(err) = publish_plan(shared, &launch.id) {
                note_log(
                    &shared.config.data_dir,
                    &launch.id,
                    &format!("plan html: {err}"),
                );
            }
            Ok(())
        }
        state::Role::Implement => {
            let facts = implement_facts(shared, launch)?;
            let mut conn = lock_db(&shared.db)?;
            if facts.tree.clean && facts.tree.on_branch && !facts.tree.head_is_base {
                state::submit_implement(
                    &mut conn,
                    &launch.id,
                    &launch.token,
                    &facts.head,
                    facts.tree,
                )?;
            } else {
                state::fail_implement(&mut conn, &launch.id, &launch.token, !facts.tree.clean)?;
            }
            Ok(())
        }
        state::Role::Review => {
            let document = output.ok_or_else(|| {
                Error::Invalid(format!("{} did not write a review", launch.adapter))
            })?;
            let expected = expected_head
                .ok_or_else(|| Error::Invalid("review worktree head was not recorded".into()))?;
            let path = launch
                .worktree
                .as_ref()
                .ok_or_else(|| Error::Invalid("review worktree is missing".into()))?;
            let tree = detached_tree(path, expected)?;
            let mut conn = lock_db(&shared.db)?;
            state::submit_review(&mut conn, &launch.id, &launch.token, document, tree)?;
            Ok(())
        }
    }
}

fn detached_tree(path: &Path, expected_head: &str) -> Result<state::DetachedWorktree> {
    let snap = crate::git::snapshot(path)?;
    Ok(state::DetachedWorktree {
        clean: snap.porcelain.is_empty(),
        head_unchanged: snap.head == expected_head,
    })
}

struct ImplementFacts {
    tree: state::ImplementWorktree,
    head: String,
}

fn implement_facts(shared: &Shared, launch: &WorkerLaunch) -> Result<ImplementFacts> {
    let (base, item_id, branch) = {
        let conn = lock_db(&shared.db)?;
        conn.query_row(
            "SELECT j.base_sha, j.item_id, j.branch FROM jobs j WHERE j.id = ?1",
            params![launch.id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )?
    };
    let path = launch
        .worktree
        .as_ref()
        .ok_or_else(|| Error::Invalid("implement worktree is missing".into()))?;
    let snap = crate::git::snapshot(path)?;
    let branch = branch.unwrap_or_else(|| format!("keel/{item_id}/{}", launch.id));
    let base = base.unwrap_or_default();
    Ok(ImplementFacts {
        head: snap.head.clone(),
        tree: state::ImplementWorktree {
            clean: snap.porcelain.is_empty(),
            on_branch: snap.branch == branch,
            head_is_base: snap.head == base,
        },
    })
}

fn abandon_worker(shared: &Shared, launch: &WorkerLaunch, reason: &str) -> Result<()> {
    note_log(
        &shared.config.data_dir,
        &launch.id,
        &format!("{}: {reason}", launch.adapter),
    );
    if reason.contains("process group is not empty") {
        return Err(Error::Conflict {
            state: "leased".into(),
            message: "process group is not empty".into(),
        });
    }
    if launch.role == state::Role::Implement {
        let dirty = launch
            .worktree
            .as_ref()
            .and_then(|path| crate::git::snapshot(path).ok())
            .map(|snap| !snap.porcelain.is_empty())
            .unwrap_or(false);
        let mut conn = lock_db(&shared.db)?;
        if state::fail_implement(&mut conn, &launch.id, &launch.token, dirty).is_ok() {
            return Ok(());
        }
    }
    claim::release(
        &shared.db,
        launch.target,
        &launch.id,
        state::Release::human(),
        &shared.config.data_dir,
    )?;
    Ok(())
}

fn note_log(data_dir: &Path, id: &str, line: &str) {
    if !render::safe_id(id) {
        return;
    }
    let path = data_dir.join("logs").join(format!("{id}.log"));
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = std::io::Write::write_all(&mut file, line.as_bytes());
        let _ = std::io::Write::write_all(&mut file, b"\n");
    }
}

fn snapshot_json(snapshot: Option<&crate::git::PrimarySnapshot>) -> Value {
    match snapshot {
        None => Value::Null,
        Some(snapshot) => json!({
            "head": snapshot.head,
            "branch": snapshot.branch,
            "porcelain": snapshot.porcelain,
        }),
    }
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
    // The row is already committed. A retry has no external_id, so a 500 here
    // would insert a second inbox item.
    let mut body = json!({
        "id": id,
        "card_url": shared.config.card_url(&id),
    });
    if let Err(err) = publish_card(shared, &id) {
        body["warning"] = json!(format!("card was not written: {err}"));
    }
    Response::json(200, body)
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
    if path.is_file() {
        // A committed card whose relpath update failed is still the record.
        let _ = set_card_relpath(shared, id);
    } else if let Err(err) = publish_card(shared, id) {
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
    // The file is durable. A failed column update is repaired on the next card read.
    if let Err(err) = set_card_relpath(shared, &card.id) {
        eprintln!("keel: card {} relpath not recorded: {err}", card.id);
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
    card.open_points = load_points(conn, id)?;
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
        "SELECT 'item', id, claim_worker, claim_role, claim_pid, claim_starttime, lease_until,
                claim_external
         FROM items WHERE claim_token IS NOT NULL
         UNION ALL
         SELECT 'job', id, claim_worker, claim_role, claim_pid, claim_starttime, lease_until,
                claim_external
         FROM jobs WHERE claim_token IS NOT NULL
         ORDER BY 1, 2",
    )?;
    let leases = lease_stmt.query_map([], |row| {
        let pid = row.get::<_, Option<i64>>(4)?;
        let external = row.get::<_, i64>(7)? != 0;
        let mut lease = json!({
            "kind": row.get::<_, String>(0)?,
            "id": row.get::<_, String>(1)?,
            "worker": row.get::<_, Option<String>>(2)?,
            "role": row.get::<_, Option<String>>(3)?,
            "pid": pid,
            "starttime": row.get::<_, Option<String>>(5)?,
            "lease_until": row.get::<_, Option<String>>(6)?,
        });
        // The crash window between insert and spawn. The reaper will not requeue it.
        if pid.is_none() && !external {
            lease["unsupervised"] = json!(true);
        }
        Ok(lease)
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

enum Doc<'a> {
    Card(&'a str),
    Plan { id: &'a str, rev: Option<i64> },
}

fn doc_route(path: &str) -> Option<Doc<'_>> {
    let rest = path.strip_prefix("/i/")?;
    let mut parts = rest.split('/');
    let id = parts.next()?;
    if !render::safe_id(id) {
        return None;
    }
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("card"), None, None, None) => Some(Doc::Card(id)),
        (Some("plan"), None, None, None) => Some(Doc::Plan { id, rev: None }),
        (Some("plan"), Some("r"), Some(rev), None) => {
            let rev = parse_rev(rev)?;
            Some(Doc::Plan { id, rev: Some(rev) })
        }
        _ => None,
    }
}

fn parse_rev(text: &str) -> Option<i64> {
    if text.is_empty() || text.len() > 9 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let rev = text.parse().ok()?;
    if rev < 1 {
        None
    } else {
        Some(rev)
    }
}

fn note_response(shared: &Shared, id: &str, req: &Request) -> Response {
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let text = match required_string(&obj, "text") {
        Ok(text) => text,
        Err(err) => return fail(err),
    };
    let noted = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::note(&mut conn, id, &text) {
            Ok(point_id) => match read_item_state(&conn, id) {
                Ok(state_name) => Ok((point_id, state_name)),
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        }
    };
    match noted {
        Ok((point_id, state_name)) => finish_plan(
            shared,
            id,
            json!({
                "id": id,
                "point_id": point_id,
                "state": state_name,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn waive_response(shared: &Shared, id: &str, req: &Request, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let point_id = match required_string(&obj, "point_id") {
        Ok(point_id) => point_id,
        Err(err) => return fail(err),
    };
    let waived = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::waive(&mut conn, id, &point_id) {
            Ok(()) => read_item_state(&conn, id),
            Err(err) => Err(err),
        }
    };
    match waived {
        Ok(state_name) => finish_plan(
            shared,
            id,
            json!({
                "id": id,
                "point_id": point_id,
                "state": state_name,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn approve_response(shared: &Shared, id: &str, req: &Request, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    // No git in this slice. The caller passes the sha `approve` pins.
    let base_sha = match required_string(&obj, "base_sha") {
        Ok(base_sha) => base_sha,
        Err(err) => return fail(err),
    };
    let approved = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::approve(&mut conn, id, &base_sha) {
            Ok(job_ids) => match read_item_state(&conn, id) {
                Ok(state_name) => Ok((job_ids, state_name)),
                Err(err) => Err(err),
            },
            Err(err) => Err(err),
        }
    };
    match approved {
        Ok((job_ids, state_name)) => finish_plan(
            shared,
            id,
            json!({
                "id": id,
                "state": state_name,
                "base_sha": base_sha,
                "job_ids": job_ids,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn drop_response(shared: &Shared, id: &str, req: &Request, surface_cli: bool) -> Response {
    if let Err(err) = require_cli(surface_cli) {
        return fail(err);
    }
    let obj = match json_object_or_empty(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let force = match optional_bool(&obj, "force") {
        Ok(force) => force,
        Err(err) => return fail(err),
    };
    // Non-force drops that will 409 must not signal. External pids are skipped
    // inside empty_spawned_groups. A null pid with no pgid is cleared as-is.
    if force {
        if let Err(err) = claim::empty_spawned_groups(&shared.db, id) {
            return fail(err);
        }
    }
    let dropped = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match state::drop_item(&mut conn, id, force) {
            Ok(()) => read_item_state(&conn, id),
            Err(err) => Err(err),
        }
    };
    match dropped {
        Ok(state_name) => Response::json(
            200,
            json!({
                "id": id,
                "state": state_name,
            }),
        ),
        Err(err) => fail(err),
    }
}

fn submit_response(shared: &Shared, id: &str, req: &Request) -> Response {
    let obj = match json_object(&req.body) {
        Ok(obj) => obj,
        Err(err) => return fail(err),
    };
    let token = match required_string(&obj, "claim_token") {
        Ok(token) if token.is_empty() => {
            return fail(Error::Invalid("claim_token is empty".into()));
        }
        Ok(token) => token,
        Err(err) => return fail(err),
    };
    let Some(document_value) = obj.get("document") else {
        return fail(Error::Invalid("document is required".into()));
    };
    let document = match document_text(document_value) {
        Ok(document) => document,
        Err(err) => return fail(err),
    };
    let submitted = {
        let mut conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        // Snapshot enforcement is a later submit. The claim returns one to the
        // caller and does not store it until a column exists.
        let tree = state::DetachedWorktree::unchanged();
        let result = match read_item_state(&conn, id) {
            Ok(state_name) => match state_name.as_str() {
                "planning" => state::submit_plan(&mut conn, id, &token, &document, tree),
                "revising" => state::submit_revision(&mut conn, id, &token, &document, tree),
                _ => Err(Error::Conflict {
                    state: state_name,
                    message: "submit requires planning or revising".into(),
                }),
            },
            Err(err) => Err(err),
        };
        match result {
            Ok(()) => read_plan_rev(&conn, id),
            Err(err) => Err(err),
        }
    };
    match submitted {
        Ok((state_name, rev)) => finish_plan(
            shared,
            id,
            json!({
                "id": id,
                "state": state_name,
                "plan_rev": rev,
                "plan_url": shared.config.plan_url(id),
            }),
        ),
        Err(err) => fail(err),
    }
}

fn finish_plan(shared: &Shared, id: &str, mut body: Value) -> Response {
    if let Err(err) = publish_plan(shared, id) {
        body["warning"] = json!(format!("plan was not written: {err}"));
    }
    Response::json(200, body)
}

fn plan_response(shared: &Shared, id: &str, rev: Option<i64>) -> Response {
    let current = {
        let conn = match lock_db(&shared.db) {
            Ok(conn) => conn,
            Err(err) => return fail(err),
        };
        match read_plan_rev(&conn, id) {
            Ok((_, rev)) => rev,
            Err(err) => return fail(err),
        }
    };
    // `plan_rev` 0 means the column has never been written.
    if current < 1 {
        return missing(&format!("{id}/plan"));
    }
    let rev = rev.unwrap_or(current);
    let path = plan_file(shared, id, rev);
    if rev == current {
        if path.is_file() {
            let _ = set_plan_relpath(shared, id, current);
        } else if let Err(err) = publish_plan(shared, id) {
            return fail(err);
        }
    }
    match fs::read_to_string(&path) {
        Ok(text) => Response::html(text),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => missing(&format!("plan r{rev}")),
        Err(err) => fail(err.into()),
    }
}

fn plan_file(shared: &Shared, id: &str, rev: i64) -> PathBuf {
    shared
        .config
        .docs_dir
        .join(id)
        .join("plan")
        .join(format!("r{rev}.html"))
}

/// Same path as the card: canonical file, then a best-effort artifact copy.
/// Only the current revision is published. Older `rN` files stay where submit wrote them.
fn publish_plan(shared: &Shared, id: &str) -> Result<()> {
    if !render::safe_id(id) {
        return Err(Error::NotFound(id.to_string()));
    }
    let plan = {
        let conn = lock_db(&shared.db)?;
        match load_plan(&conn, id, &shared.config)? {
            Some(plan) => plan,
            None => return Ok(()),
        }
    };
    let html = render::plan_page(&plan);
    let canonical = plan_file(shared, &plan.id, plan.plan_rev);
    // The database lock is not held across the disk write.
    write_atomic(&canonical, html.as_bytes())?;
    // The file is durable. A failed column update is repaired on the next plan read.
    if let Err(err) = set_plan_relpath(shared, &plan.id, plan.plan_rev) {
        eprintln!("keel: plan {} relpath not recorded: {err}", plan.id);
    }
    let published = shared
        .config
        .publish_root
        .join("keel")
        .join(&plan.id)
        .join("plan.html");
    // The artifact copy is best-effort. The canonical file is the record.
    if let Err(err) = write_atomic(&published, html.as_bytes()) {
        eprintln!("keel: publish plan {} failed: {err}", plan.id);
    }
    Ok(())
}

fn load_plan(conn: &Connection, id: &str, config: &Config) -> Result<Option<PlanView>> {
    let row = conn
        .query_row(
            "SELECT id, title, state, summary, plan_rev, plan_json, updated_at
             FROM items WHERE id = ?1",
            params![id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, String>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((item_id, title, state_name, _summary, plan_rev, plan_json, updated_at)) = row else {
        return Err(Error::NotFound(id.to_string()));
    };
    let Some(plan_json) = plan_json.filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    if plan_rev < 1 {
        return Ok(None);
    }
    let document = state::validate_plan_json(&plan_json)?;
    let db_jobs = load_job_views(conn, id)?;
    let jobs = if db_jobs.is_empty() {
        // No rows yet. Still list subjobs so plan review shows the tree.
        views_from_plan(&document.jobs)
    } else {
        db_jobs
    };
    let previous_url = if plan_rev > 1 {
        Some(config.plan_rev_url(id, plan_rev - 1))
    } else {
        None
    };
    Ok(Some(PlanView {
        id: item_id,
        title,
        state: state_name,
        summary: document.summary,
        plan_rev,
        updated_at,
        previous_url,
        open_points: load_points(conn, id)?,
        sections: sections_of(&plan_json),
        jobs,
    }))
}

fn sections_of(document: &str) -> Vec<PlanSection> {
    let Ok(value) = serde_json::from_str::<Value>(document) else {
        return Vec::new();
    };
    let Some(list) = value.get("sections").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut sections = Vec::new();
    for entry in list {
        let Some(obj) = entry.as_object() else {
            continue;
        };
        let heading = obj
            .get("heading")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let body = obj
            .get("body")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if heading.is_empty() && body.is_empty() {
            continue;
        }
        sections.push(PlanSection { heading, body });
    }
    sections
}

fn load_points(conn: &Connection, id: &str) -> Result<Vec<PointView>> {
    let mut stmt =
        conn.prepare("SELECT id, text, status FROM open_points WHERE item_id = ?1 ORDER BY id")?;
    let points = stmt.query_map(params![id], |row| {
        Ok(PointView {
            id: row.get(0)?,
            text: row.get(1)?,
            status: row.get(2)?,
        })
    })?;
    let mut out = Vec::new();
    for point in points {
        out.push(point?);
    }
    Ok(out)
}

fn views_from_plan(jobs: &[state::PlanJob]) -> Vec<PlanJobView> {
    let mut out = Vec::new();
    for job in jobs {
        out.push(PlanJobView {
            id: None,
            parent_id: None,
            title: job.title.clone(),
            acceptance: job.acceptance.clone(),
        });
        for child in &job.subjobs {
            out.push(PlanJobView {
                id: None,
                parent_id: None,
                title: child.title.clone(),
                acceptance: child.acceptance.clone(),
            });
        }
    }
    out
}

fn load_job_views(conn: &Connection, id: &str) -> Result<Vec<PlanJobView>> {
    let mut stmt = conn.prepare(
        "SELECT id, parent_job_id, title, acceptance
         FROM jobs WHERE item_id = ?1 ORDER BY ord, id",
    )?;
    let rows = stmt.query_map(params![id], |row| {
        Ok(PlanJobView {
            id: row.get(0)?,
            parent_id: row.get(1)?,
            title: row.get(2)?,
            acceptance: row.get(3)?,
        })
    })?;
    let mut jobs = Vec::new();
    for job in rows {
        jobs.push(job?);
    }
    Ok(jobs)
}

fn read_item_state(conn: &Connection, id: &str) -> Result<String> {
    conn.query_row(
        "SELECT state FROM items WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}

fn read_plan_rev(conn: &Connection, id: &str) -> Result<(String, i64)> {
    conn.query_row(
        "SELECT state, plan_rev FROM items WHERE id = ?1",
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(id.to_string()))
}

fn set_plan_relpath(shared: &Shared, id: &str, rev: i64) -> Result<()> {
    let rel = format!("docs/{id}/plan/r{rev}.html");
    let conn = lock_db(&shared.db)?;
    conn.execute(
        "UPDATE items SET plan_relpath = ?1 WHERE id = ?2 AND plan_relpath IS NOT ?1",
        params![rel, id],
    )?;
    Ok(())
}

fn require_cli(surface_cli: bool) -> Result<()> {
    if surface_cli {
        Ok(())
    } else {
        Err(Error::Invalid("this route is cli only".into()))
    }
}

fn json_object(body: &[u8]) -> Result<Map<String, Value>> {
    let value: Value =
        serde_json::from_slice(body).map_err(|err| Error::Invalid(format!("json: {err}")))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Invalid("expected a JSON object".into()))
}

fn json_object_or_empty(body: &[u8]) -> Result<Map<String, Value>> {
    if body.is_empty() {
        return Ok(Map::new());
    }
    json_object(body)
}

fn document_text(value: &Value) -> Result<String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Object(_) => {
            serde_json::to_string(value).map_err(|err| Error::Invalid(format!("document: {err}")))
        }
        _ => Err(Error::Invalid(
            "document must be a JSON object or a JSON string".into(),
        )),
    }
}

fn optional_bool(obj: &Map<String, Value>, key: &str) -> Result<bool> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(Error::Invalid(format!("{key} must be a boolean"))),
    }
}

fn set_card_relpath(shared: &Shared, id: &str) -> Result<()> {
    let rel = format!("docs/{id}/card.html");
    let conn = lock_db(&shared.db)?;
    conn.execute(
        "UPDATE items SET card_relpath = ?1 WHERE id = ?2 AND card_relpath IS NOT ?1",
        params![rel, id],
    )?;
    Ok(())
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

/// Exclusive flock on the sidecar next to `keel.db`. Never replace that inode:
/// a new file would not conflict with a lock still held on the old one.
pub(crate) fn acquire_lock(path: &Path) -> Result<LockGuard> {
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
        fs::create_dir_all(&cfg.data_dir).unwrap();
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
    fn held_flock_does_not_open_the_database_or_unlink_the_socket() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        fs::create_dir_all(&cfg.data_dir).unwrap();
        fs::create_dir_all(&cfg.runtime_dir).unwrap();
        let held = acquire_lock(&cfg.lock_path).unwrap();
        let listener = StdUnix::bind(&cfg.socket_path).unwrap();
        drop(listener);
        let err = match Running::start(cfg.clone()) {
            Err(err) => err,
            Ok(_running) => panic!("daemon started while the database lock was held"),
        };
        assert!(err.to_string().contains("already running"), "{err}");
        assert!(!cfg.db_path.exists());
        assert!(cfg.socket_path.exists());
        drop(held);
    }

    #[test]
    fn second_serve_loses_the_database_flock() {
        let _rm = Rm(scratch_dir());
        let cfg = isolated(&_rm.0, free_port());
        let running = Running::start(cfg.clone()).unwrap();
        let real = read_proc_starttime(std::process::id()).unwrap();
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        assert_eq!(text.trim(), format!("{} {real}", std::process::id()));
        let stop = AtomicBool::new(false);
        let again = serve(&cfg, &stop).expect_err("second daemon");
        assert!(again.to_string().contains("already running"), "{again}");
        let text = fs::read_to_string(&cfg.lock_path).unwrap();
        assert_eq!(text.trim(), format!("{} {real}", std::process::id()));
        assert!(cfg.socket_path.exists());
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
        let head = tcp(&cfg, "HEAD", "/health", None);
        assert_eq!(head.status, 405);
        assert!(String::from_utf8_lossy(&head.body).contains("method not allowed"));
        let head_unix = api(&cfg, "HEAD", "/v1/status", None);
        assert_eq!(head_unix.status, 405);
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
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET card_relpath = NULL WHERE id = ?1",
                params![id],
            )
            .unwrap();
        let repaired = tcp(&cfg, "GET", &format!("/i/{id}/card"), None);
        assert_eq!(repaired.status, 200);
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

    fn post(cfg: &Config, path: &str, body: Value) -> Response {
        api(cfg, "POST", path, Some(body.to_string().as_bytes()))
    }

    fn post_cli(cfg: &Config, path: &str, body: Value) -> Response {
        let mut stream = UnixStream::connect(&cfg.socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        http::exchange_with(
            &mut stream,
            "POST",
            path,
            Some(body.to_string().as_bytes()),
            &[("x-keel-surface", "cli")],
        )
        .unwrap()
    }

    #[test]
    fn claim_heartbeat_release_stay_off_the_document_port() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let created = post_item(&cfg, "Claim me", "body");
        let id = created["id"].as_str().unwrap().to_string();

        let claimed = post(
            &cfg,
            &format!("/v1/items/{id}/claim"),
            json!({"role": "plan", "adapter": "cli"}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let claimed: Value = serde_json::from_slice(&claimed.body).unwrap();
        let token = claimed["claim_token"].as_str().unwrap().to_string();
        // No repo, so plan does not create a worktree. The prompt is still written.
        assert!(claimed["worktree"].is_null());
        assert!(claimed["snapshot"].is_null());
        let prompt_path = claimed["prompt_path"].as_str().unwrap();
        assert!(Path::new(prompt_path).is_file());
        let pid: Option<i64> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pid FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pid.is_none());

        let expired = post(
            &cfg,
            &format!("/v1/items/{id}/release"),
            json!({"cause": "expiry", "pid_gone": true}),
        );
        assert_eq!(
            expired.status,
            409,
            "{}",
            String::from_utf8_lossy(&expired.body)
        );
        assert!(String::from_utf8_lossy(&expired.body).contains("null claim_pid"));
        let state: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "planning");

        let bare = post(&cfg, &format!("/v1/items/{id}/release"), json!({}));
        assert_eq!(bare.status, 400, "{}", String::from_utf8_lossy(&bare.body));
        assert!(String::from_utf8_lossy(&bare.body).contains("claim_token"));

        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET lease_until = '2000-01-01T00:00:00Z' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        let beat = post(
            &cfg,
            &format!("/v1/items/{id}/heartbeat"),
            json!({"claim_token": token}),
        );
        assert_eq!(beat.status, 200, "{}", String::from_utf8_lossy(&beat.body));
        let beat: Value = serde_json::from_slice(&beat.body).unwrap();
        assert_ne!(beat["lease_until"], "2000-01-01T00:00:00Z");

        let tcp_claim = tcp(
            &cfg,
            "POST",
            &format!("/v1/items/{id}/claim"),
            Some(br#"{"role":"plan"}"#),
        );
        assert_eq!(tcp_claim.status, 405);
        let tcp_release = tcp(
            &cfg,
            "POST",
            &format!("/v1/items/{id}/release"),
            Some(b"{}"),
        );
        assert_eq!(tcp_release.status, 405);
        let tcp_resolve = tcp(&cfg, "GET", &format!("/v1/resolve?id={id}"), None);
        assert_eq!(tcp_resolve.status, 404);

        let released = post_cli(&cfg, &format!("/v1/items/{id}/release"), json!({}));
        assert_eq!(
            released.status,
            200,
            "{}",
            String::from_utf8_lossy(&released.body)
        );
        let released: Value = serde_json::from_slice(&released.body).unwrap();
        assert_eq!(released["state"], "inbox");
        drop(running);
    }

    #[test]
    fn missing_token_does_not_expire_an_external_lease() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let created = post_item(&cfg, "External", "body");
        let id = created["id"].as_str().unwrap().to_string();
        let claimed = post(
            &cfg,
            &format!("/v1/items/{id}/claim"),
            json!({"role": "plan", "adapter": "mcp", "pid": std::process::id()}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let external: i64 = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_external FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(external, 1);
        let pgid: Option<i64> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pgid FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pgid.is_none());

        // The recorded pid is dead, so a real expiry would clear the row.
        // A body with no token must not take that path.
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET claim_pid = 2147483646, claim_starttime = '1' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        let bare = post(&cfg, &format!("/v1/items/{id}/release"), json!({}));
        assert_eq!(bare.status, 400, "{}", String::from_utf8_lossy(&bare.body));
        let state: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "planning");

        let expired = post(
            &cfg,
            &format!("/v1/items/{id}/release"),
            json!({"cause": "expiry"}),
        );
        assert_eq!(
            expired.status,
            200,
            "{}",
            String::from_utf8_lossy(&expired.body)
        );
        let state: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "inbox");
        drop(running);
    }

    #[test]
    fn fake_adapter_heartbeats_the_job_route() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let created = post_item(&cfg, "Implement", "body");
        let item_id = created["id"].as_str().unwrap().to_string();
        let repo = _rm.0.join("repo");
        let head = crate::git::init_repo(&repo);
        let before = head.clone();
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET state = 'awaiting_implementation', repo = ?1, target_branch = 'main', base_sha = ?2
                 WHERE id = ?3",
                params![repo.to_str().unwrap(), head, item_id],
            )
            .unwrap();
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO jobs (id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at)
                 VALUES ('j_impl', ?1, 'Leaf', '', 'queued', 1, 0, ?2, '2026-11-25T00:00:00Z')",
                params![item_id, head],
            )
            .unwrap();
        let claimed = post(
            &cfg,
            "/v1/jobs/j_impl/claim",
            json!({"role": "implement", "adapter": "fake", "worker_id": "fake:impl"}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let claimed: Value = serde_json::from_slice(&claimed.body).unwrap();
        let token = claimed["claim_token"].as_str().unwrap().to_string();
        assert_eq!(claimed["snapshot"]["head"], before);
        assert_eq!(crate::git::rev_parse(&repo, "HEAD").unwrap(), before);
        let worktree = claimed["worktree"].as_str().unwrap();
        assert!(Path::new(worktree).is_dir());
        assert!(claimed["prompt_path"]
            .as_str()
            .unwrap()
            .ends_with("j_impl.txt"));
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2000-01-01T00:00:00Z' WHERE id = 'j_impl'",
                [],
            )
            .unwrap();

        let adapter = claim::FakeAdapter {
            socket_path: cfg.socket_path.clone(),
            pause: Duration::from_millis(250),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            adapter.heartbeat(crate::state::Role::Implement, "j_impl", &token)
        });
        started_rx.recv().unwrap();
        thread::sleep(Duration::from_millis(40));
        let status_at = std::time::Instant::now();
        let status = api(&cfg, "GET", "/v1/status", None);
        assert_eq!(status.status, 200);
        assert!(
            status_at.elapsed() < Duration::from_millis(200),
            "status blocked for {:?}",
            status_at.elapsed()
        );
        let path = handle.join().unwrap().unwrap();
        assert_eq!(path, "/v1/jobs/j_impl/heartbeat");
        let lease: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT lease_until FROM jobs WHERE id = 'j_impl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(lease, "2000-01-01T00:00:00Z");
        let item_lease: Option<String> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT lease_until FROM items WHERE id = ?1",
                params![item_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(item_lease.is_none());

        let released = post_cli(&cfg, "/v1/jobs/j_impl/release", json!({}));
        assert_eq!(
            released.status,
            200,
            "{}",
            String::from_utf8_lossy(&released.body)
        );
        let released: Value = serde_json::from_slice(&released.body).unwrap();
        assert_eq!(released["state"], "queued");

        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE jobs SET state = 'ready_for_review', implementer_worker = 'fake:impl'
                 WHERE id = 'j_impl'",
                [],
            )
            .unwrap();
        let review = post(
            &cfg,
            "/v1/jobs/j_impl/claim",
            json!({"role": "review", "adapter": "fake", "worker_id": "fake:rev"}),
        );
        assert_eq!(
            review.status,
            200,
            "{}",
            String::from_utf8_lossy(&review.body)
        );
        let review: Value = serde_json::from_slice(&review.body).unwrap();
        let token = review["claim_token"].as_str().unwrap().to_string();
        let adapter = claim::FakeAdapter {
            socket_path: cfg.socket_path.clone(),
            pause: Duration::ZERO,
        };
        let path = adapter
            .heartbeat(crate::state::Role::Review, "j_impl", &token)
            .unwrap();
        assert_eq!(path, "/v1/jobs/j_impl/heartbeat");
        let released = post_cli(&cfg, "/v1/jobs/j_impl/release", json!({}));
        assert_eq!(
            released.status,
            200,
            "{}",
            String::from_utf8_lossy(&released.body)
        );
        let same = post(
            &cfg,
            "/v1/jobs/j_impl/claim",
            json!({"role": "review", "worker_id": "fake:impl"}),
        );
        assert_eq!(same.status, 409, "{}", String::from_utf8_lossy(&same.body));
        assert!(String::from_utf8_lossy(&same.body).contains("implementer"));
        drop(running);
    }

    #[test]
    fn resolve_asks_for_the_full_id_when_a_prefix_matches_both() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        // The socket is bound before the schema commit. Wait until the accept loop.
        assert_eq!(api(&cfg, "GET", "/v1/status", None).status, 200);
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO items (id, title, body, source, state, created_at, updated_at)
                 VALUES ('ab_item', 't', 'b', 'cli', 'inbox', '2026-11-25T00:00:00Z', '2026-11-25T00:00:00Z')",
                [],
            )
            .unwrap();
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO jobs (id, item_id, title, state, depth, ord, updated_at)
                 VALUES ('ab_job', 'ab_item', 't', 'queued', 1, 0, '2026-11-25T00:00:00Z')",
                [],
            )
            .unwrap();
        let both = api(&cfg, "GET", "/v1/resolve?id=ab", None);
        assert_eq!(both.status, 409, "{}", String::from_utf8_lossy(&both.body));
        assert!(String::from_utf8_lossy(&both.body).contains("full id"));
        let item = api(&cfg, "GET", "/v1/resolve?id=ab_item", None);
        assert_eq!(item.status, 200);
        let item: Value = serde_json::from_slice(&item.body).unwrap();
        assert_eq!(item["kind"], "item");
        assert_eq!(item["id"], "ab_item");
        let job = api(&cfg, "GET", "/v1/resolve?id=ab_job", None);
        let job: Value = serde_json::from_slice(&job.body).unwrap();
        assert_eq!(job["kind"], "job");
        let missing = api(&cfg, "GET", "/v1/resolve?id=nope", None);
        assert_eq!(missing.status, 404);
        drop(running);
    }

    fn tcp_with(
        cfg: &Config,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> Response {
        let mut stream = TcpStream::connect(("127.0.0.1", cfg.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        http::exchange_with(&mut stream, method, path, body, headers).unwrap()
    }

    fn claim_role(cfg: &Config, id: &str, role: &str) -> String {
        let claimed = post(
            cfg,
            &format!("/v1/items/{id}/claim"),
            json!({"role": role, "adapter": "cli", "worker_id": format!("cli:{role}")}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let claimed: Value = serde_json::from_slice(&claimed.body).unwrap();
        claimed["claim_token"].as_str().unwrap().to_string()
    }

    fn item_state_of(cfg: &Config, id: &str) -> String {
        open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn note_waive_revise_approve_and_drop() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let repo = _rm.0.join("repo");
        crate::git::init_repo(&repo);
        let repo_s = repo.to_str().unwrap();
        let created = api(
            &cfg,
            "POST",
            "/v1/items",
            Some(
                json!({
                    "source": "cli",
                    "title": "Pin <walk>",
                    "body": "body",
                    "repo": repo_s,
                    "target_branch": "main",
                })
                .to_string()
                .as_bytes(),
            ),
        );
        assert_eq!(
            created.status,
            200,
            "{}",
            String::from_utf8_lossy(&created.body)
        );
        let created: Value = serde_json::from_slice(&created.body).unwrap();
        let id = created["id"].as_str().unwrap().to_string();

        let early = tcp(&cfg, "GET", &format!("/i/{id}/plan"), None);
        assert_eq!(early.status, 404);

        let token = claim_role(&cfg, &id, "plan");
        let document = r#"{"summary":"Pin </script>","jobs":[{"title":"Walk","acceptance":"listed"}],"sections":[{"heading":"Scope <a>","body":"no twin"}]}"#;
        let submitted = post(
            &cfg,
            &format!("/v1/items/{id}/submit"),
            json!({"claim_token": token, "document": document}),
        );
        assert_eq!(
            submitted.status,
            200,
            "{}",
            String::from_utf8_lossy(&submitted.body)
        );
        let submitted: Value = serde_json::from_slice(&submitted.body).unwrap();
        assert_eq!(submitted["state"], "awaiting_plan_review");
        assert_eq!(submitted["plan_rev"], 1);
        assert_eq!(submitted["plan_url"], cfg.plan_url(&id));
        let stored: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT plan_json FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, document);
        let rel: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT plan_relpath FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rel, format!("docs/{id}/plan/r1.html"));

        let canonical = cfg.docs_dir.join(&id).join("plan").join("r1.html");
        let published = cfg.publish_root.join("keel").join(&id).join("plan.html");
        let html = fs::read_to_string(&canonical).unwrap();
        assert_eq!(html, fs::read_to_string(&published).unwrap());
        assert!(!cfg.docs_dir.join(&id).join("plan.md").exists());
        assert!(!published.with_extension("md").exists());
        assert_eq!(html.matches("</script>").count(), 1);
        assert!(html.contains("Pin &lt;/script&gt;"));
        assert!(html.contains("Scope &lt;a&gt;"));
        assert!(html.contains("no twin"));
        assert!(html.contains("script-src 'none'"));
        assert!(!html.to_ascii_lowercase().contains("<button"));
        assert!(!html.to_ascii_lowercase().contains("<form"));
        let doc = keel_doc(&html);
        assert_eq!(doc["keel"], 1);
        assert_eq!(doc["kind"], "plan");
        assert_eq!(doc["item_id"], id);
        assert!(doc["job_id"].is_null());
        assert_eq!(doc["rev"], 1);
        assert_eq!(doc["summary"], "Pin </script>");
        assert_eq!(doc["jobs"][0]["title"], "Walk");
        assert!(doc["jobs"][0]["id"].is_null());
        assert!(doc["diff"].is_null());

        fs::remove_file(&canonical).unwrap();
        let repaired = tcp(&cfg, "GET", &format!("/i/{id}/plan"), None);
        assert_eq!(repaired.status, 200);
        assert_eq!(repaired.body, fs::read(&canonical).unwrap());
        let old = tcp(&cfg, "GET", &format!("/i/{id}/plan/r/1"), None);
        assert_eq!(old.status, 200);

        let not_held = post(
            &cfg,
            &format!("/v1/items/{id}/submit"),
            json!({
                "claim_token": "stale",
                "document": r#"{"summary":"s","jobs":[{"title":"t","subjobs":[{"title":"child"}]}]}"#,
            }),
        );
        assert_eq!(
            not_held.status,
            409,
            "{}",
            String::from_utf8_lossy(&not_held.body)
        );
        assert!(String::from_utf8_lossy(&not_held.body).contains("planning"));
        assert_eq!(item_state_of(&cfg, &id), "awaiting_plan_review");

        let first = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "first"}),
        );
        assert_eq!(
            first.status,
            200,
            "{}",
            String::from_utf8_lossy(&first.body)
        );
        let first: Value = serde_json::from_slice(&first.body).unwrap();
        let first_id = first["point_id"].as_str().unwrap().to_string();
        assert_eq!(first["state"], "revision_requested");
        let second = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "second"}),
        );
        let second: Value = serde_json::from_slice(&second.body).unwrap();
        let second_id = second["point_id"].as_str().unwrap().to_string();
        let bare_waive = post(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": first_id}),
        );
        assert_eq!(bare_waive.status, 400);
        let waived = post_cli(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": first_id}),
        );
        assert_eq!(
            waived.status,
            200,
            "{}",
            String::from_utf8_lossy(&waived.body)
        );
        let waived: Value = serde_json::from_slice(&waived.body).unwrap();
        assert_eq!(waived["state"], "revision_requested");
        let last = post_cli(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": second_id}),
        );
        let last: Value = serde_json::from_slice(&last.body).unwrap();
        assert_eq!(last["state"], "awaiting_plan_review");

        let keep = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "keep"}),
        );
        let keep: Value = serde_json::from_slice(&keep.body).unwrap();
        let keep_id = keep["point_id"].as_str().unwrap().to_string();
        let mid_point = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "mid"}),
        );
        let mid_point: Value = serde_json::from_slice(&mid_point.body).unwrap();
        let mid_id = mid_point["point_id"].as_str().unwrap().to_string();
        let fix = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "fix"}),
        );
        let fix: Value = serde_json::from_slice(&fix.body).unwrap();
        let fix_id = fix["point_id"].as_str().unwrap().to_string();
        assert_eq!(item_state_of(&cfg, &id), "revision_requested");
        let token = claim_role(&cfg, &id, "revise");
        assert_eq!(item_state_of(&cfg, &id), "revising");
        let mid = post_cli(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": mid_id}),
        );
        let mid: Value = serde_json::from_slice(&mid.body).unwrap();
        assert_eq!(mid["state"], "revising");
        let missing_point = post(
            &cfg,
            &format!("/v1/items/{id}/submit"),
            json!({
                "claim_token": token,
                "document": r#"{"summary":"nope","jobs":[],"resolved_point_ids":["p_missing"]}"#,
            }),
        );
        assert_eq!(missing_point.status, 400);
        assert_eq!(item_state_of(&cfg, &id), "revising");
        let rev: i64 = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT plan_rev FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rev, 1);
        let revised = format!(
            r#"{{"summary":"revised","jobs":[{{"title":"Walk","acceptance":"listed"}}],"resolved_point_ids":["{fix_id}"]}}"#
        );
        let submitted = post(
            &cfg,
            &format!("/v1/items/{id}/submit"),
            json!({"claim_token": token, "document": revised}),
        );
        assert_eq!(
            submitted.status,
            200,
            "{}",
            String::from_utf8_lossy(&submitted.body)
        );
        let submitted: Value = serde_json::from_slice(&submitted.body).unwrap();
        assert_eq!(submitted["state"], "awaiting_plan_review");
        assert_eq!(submitted["plan_rev"], 2);
        let fix_status: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT status FROM open_points WHERE id = ?1",
                params![fix_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(fix_status, "resolved");
        let keep_status: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT status FROM open_points WHERE id = ?1",
                params![keep_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(keep_status, "open");
        let open_blocked = post_cli(
            &cfg,
            &format!("/v1/items/{id}/approve"),
            json!({"base_sha": "fixture-base"}),
        );
        assert_eq!(
            open_blocked.status,
            409,
            "{}",
            String::from_utf8_lossy(&open_blocked.body)
        );
        assert!(String::from_utf8_lossy(&open_blocked.body).contains("open"));
        let cleared_keep = post_cli(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": keep_id}),
        );
        let cleared_keep: Value = serde_json::from_slice(&cleared_keep.body).unwrap();
        assert_eq!(cleared_keep["state"], "awaiting_plan_review");
        let current = tcp(&cfg, "GET", &format!("/i/{id}/plan"), None);
        let current_html = String::from_utf8(current.body).unwrap();
        assert!(current_html.contains("revised"));
        assert!(current_html.contains("/plan/r/1"));
        let r1 = fs::read_to_string(cfg.docs_dir.join(&id).join("plan").join("r1.html")).unwrap();
        assert!(r1.contains("Pin &lt;/script&gt;"));
        assert!(!r1.contains("revised"));
        let token_after: Option<String> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(token_after.is_none());

        let block = post(
            &cfg,
            &format!("/v1/items/{id}/note"),
            json!({"text": "block"}),
        );
        let block: Value = serde_json::from_slice(&block.body).unwrap();
        let block_id = block["point_id"].as_str().unwrap().to_string();
        assert_eq!(block["state"], "revision_requested");
        let noted_html =
            fs::read_to_string(cfg.docs_dir.join(&id).join("plan").join("r2.html")).unwrap();
        assert!(noted_html.contains("block"));
        let blocked = post_cli(
            &cfg,
            &format!("/v1/items/{id}/approve"),
            json!({"base_sha": "fixture-base"}),
        );
        assert_eq!(
            blocked.status,
            409,
            "{}",
            String::from_utf8_lossy(&blocked.body)
        );
        let cleared = post_cli(
            &cfg,
            &format!("/v1/items/{id}/waive"),
            json!({"point_id": block_id}),
        );
        let cleared: Value = serde_json::from_slice(&cleared.body).unwrap();
        assert_eq!(cleared["state"], "awaiting_plan_review");
        let sneaky = post(
            &cfg,
            &format!("/v1/items/{id}/approve"),
            json!({"base_sha": "fixture-base"}),
        );
        assert_eq!(sneaky.status, 400);
        assert_eq!(item_state_of(&cfg, &id), "awaiting_plan_review");
        let approved = post_cli(
            &cfg,
            &format!("/v1/items/{id}/approve"),
            json!({"base_sha": "fixture-base"}),
        );
        assert_eq!(
            approved.status,
            200,
            "{}",
            String::from_utf8_lossy(&approved.body)
        );
        let approved: Value = serde_json::from_slice(&approved.body).unwrap();
        assert_eq!(approved["state"], "awaiting_implementation");
        assert_eq!(approved["base_sha"], "fixture-base");
        let job_id = approved["job_ids"][0].as_str().unwrap().to_string();
        assert!(approved["job_ids"].as_array().unwrap().len() == 1);
        let (job_title, job_base, job_state): (String, String, String) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT title, base_sha, state FROM jobs WHERE id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(job_title, "Walk");
        assert_eq!(job_base, "fixture-base");
        assert_eq!(job_state, "queued");
        let item_base: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT base_sha FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(item_base, "fixture-base");
        let planned = keel_doc(
            &fs::read_to_string(cfg.docs_dir.join(&id).join("plan").join("r2.html")).unwrap(),
        );
        assert_eq!(planned["jobs"][0]["id"], job_id);
        assert_eq!(planned["jobs"][0]["title"], "Walk");
        assert_eq!(planned["state"], "awaiting_implementation");

        let tcp_drop = tcp_with(
            &cfg,
            "POST",
            &format!("/v1/items/{id}/drop"),
            Some(br#"{"force":true}"#),
            &[("x-keel-surface", "cli")],
        );
        assert_eq!(tcp_drop.status, 405);
        assert_eq!(item_state_of(&cfg, &id), "awaiting_implementation");
        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{id}/drop"),
            json!({"force": false}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert_eq!(item_state_of(&cfg, &id), "dropped");
        let job_state: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM jobs WHERE id = ?1",
                params![job_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(job_state, "cancelled");

        let bare = post_item(&cfg, "No repo", "body");
        let bare_id = bare["id"].as_str().unwrap();
        let token = claim_role(&cfg, bare_id, "plan");
        let submitted = post(
            &cfg,
            &format!("/v1/items/{bare_id}/submit"),
            json!({"claim_token": token, "document": {"summary":"s","jobs":[{"title":"T","acceptance":""}]}}),
        );
        assert_eq!(
            submitted.status,
            200,
            "{}",
            String::from_utf8_lossy(&submitted.body)
        );
        let refused = post_cli(
            &cfg,
            &format!("/v1/items/{bare_id}/approve"),
            json!({"base_sha": "fixture-base"}),
        );
        assert_eq!(
            refused.status,
            409,
            "{}",
            String::from_utf8_lossy(&refused.body)
        );
        assert!(String::from_utf8_lossy(&refused.body).contains("repo"));

        let empty = api(
            &cfg,
            "POST",
            "/v1/items",
            Some(
                json!({
                    "source": "cli",
                    "title": "One leaf",
                    "body": "",
                    "repo": repo_s,
                    "target_branch": "main",
                })
                .to_string()
                .as_bytes(),
            ),
        );
        let empty: Value = serde_json::from_slice(&empty.body).unwrap();
        let empty_id = empty["id"].as_str().unwrap();
        let token = claim_role(&cfg, empty_id, "plan");
        let submitted = post(
            &cfg,
            &format!("/v1/items/{empty_id}/submit"),
            json!({"claim_token": token, "document": {"summary":"solo","jobs":[]}}),
        );
        assert_eq!(
            submitted.status,
            200,
            "{}",
            String::from_utf8_lossy(&submitted.body)
        );
        let approved = post_cli(
            &cfg,
            &format!("/v1/items/{empty_id}/approve"),
            json!({"base_sha": "fixture-empty"}),
        );
        assert_eq!(
            approved.status,
            200,
            "{}",
            String::from_utf8_lossy(&approved.body)
        );
        let approved: Value = serde_json::from_slice(&approved.body).unwrap();
        let leaf = approved["job_ids"][0].as_str().unwrap();
        let leaf_title: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT title FROM jobs WHERE id = ?1",
                params![leaf],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leaf_title, "One leaf");

        let nested_item = post_item(&cfg, "Nested", "body");
        let nested_id = nested_item["id"].as_str().unwrap();
        let token = claim_role(&cfg, nested_id, "plan");
        let rejected = post(
            &cfg,
            &format!("/v1/items/{nested_id}/submit"),
            json!({
                "claim_token": token,
                "document": r#"{"summary":"s","jobs":[{"title":"t","subjobs":[{"title":"child","subjobs":[{"title":"no"}]}]}]}"#,
            }),
        );
        assert_eq!(
            rejected.status,
            409,
            "{}",
            String::from_utf8_lossy(&rejected.body)
        );
        assert!(String::from_utf8_lossy(&rejected.body).contains("depth 3"));
        assert_eq!(item_state_of(&cfg, nested_id), "planning");
        assert!(!cfg.docs_dir.join(nested_id).join("plan").exists());
        let accepted = post(
            &cfg,
            &format!("/v1/items/{nested_id}/submit"),
            json!({
                "claim_token": token,
                "document": r#"{"summary":"s","jobs":[{"title":"t","subjobs":[{"title":"child"}]}]}"#,
            }),
        );
        assert_eq!(
            accepted.status,
            200,
            "{}",
            String::from_utf8_lossy(&accepted.body)
        );
        assert_eq!(item_state_of(&cfg, nested_id), "awaiting_plan_review");
        let html =
            fs::read_to_string(cfg.docs_dir.join(nested_id).join("plan").join("r1.html")).unwrap();
        assert!(html.contains("child"));

        let loose = post_item(&cfg, "Loose", "");
        let loose_id = loose["id"].as_str().unwrap().to_string();
        let noted = post(
            &cfg,
            &format!("/v1/items/{loose_id}/note"),
            json!({"text": "while inbox"}),
        );
        let noted: Value = serde_json::from_slice(&noted.body).unwrap();
        assert_eq!(noted["state"], "inbox");
        let bare_drop = post(
            &cfg,
            &format!("/v1/items/{loose_id}/drop"),
            json!({"force": false}),
        );
        assert_eq!(bare_drop.status, 400);
        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{loose_id}/drop"),
            json!({"force": false}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert_eq!(item_state_of(&cfg, &loose_id), "dropped");

        let held = post_item(&cfg, "Held", "");
        let held_id = held["id"].as_str().unwrap();
        let _token = claim_role(&cfg, held_id, "plan");
        assert_eq!(item_state_of(&cfg, held_id), "planning");
        let refused = post_cli(
            &cfg,
            &format!("/v1/items/{held_id}/drop"),
            json!({"force": false}),
        );
        assert_eq!(
            refused.status,
            409,
            "{}",
            String::from_utf8_lossy(&refused.body)
        );
        assert_eq!(item_state_of(&cfg, held_id), "planning");
        let forced = post_cli(
            &cfg,
            &format!("/v1/items/{held_id}/drop"),
            json!({"force": true}),
        );
        assert_eq!(
            forced.status,
            200,
            "{}",
            String::from_utf8_lossy(&forced.body)
        );
        assert_eq!(item_state_of(&cfg, held_id), "dropped");
        let pid: Option<i64> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pid FROM items WHERE id = ?1",
                params![held_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pid.is_none());
        let job_note = post(
            &cfg,
            &format!("/v1/jobs/{job_id}/note"),
            json!({"text": "nope"}),
        );
        assert_eq!(job_note.status, 400);
        drop(running);
    }

    #[test]
    fn drop_force_signals_spawned_groups_and_spares_the_rest() {
        use std::process::{Command, Stdio};

        fn alive(pid: i64) -> bool {
            let Ok(text) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return false;
            };
            let Some(end) = text.rfind(')') else {
                return false;
            };
            let state = text[end + 1..]
                .split_whitespace()
                .next()
                .and_then(|state| state.chars().next());
            matches!(state, Some('S') | Some('R') | Some('D'))
        }

        struct KillChild(Option<std::process::Child>);
        impl Drop for KillChild {
            fn drop(&mut self) {
                if let Some(child) = self.0.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }

        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();

        let spawned = post_item(&cfg, "Spawned", "body");
        let spawned_id = spawned["id"].as_str().unwrap().to_string();
        let claimed = post(
            &cfg,
            &format!("/v1/items/{spawned_id}/claim"),
            json!({"role": "plan", "adapter": "cli", "spawn": true}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let spawned_pid: i64 = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pid FROM items WHERE id = ?1",
                params![spawned_id],
                |row| row.get(0),
            )
            .unwrap();
        let _spawned_guard = crate::proc::GroupGuard {
            pgid: Some(spawned_pid),
            pid: Some(spawned_pid),
        };
        assert!(alive(spawned_pid));
        let refused = post_cli(
            &cfg,
            &format!("/v1/items/{spawned_id}/drop"),
            json!({"force": false}),
        );
        assert_eq!(
            refused.status,
            409,
            "{}",
            String::from_utf8_lossy(&refused.body)
        );
        assert!(alive(spawned_pid), "non-force drop signaled the group");

        let ext = post_item(&cfg, "External", "body");
        let ext_id = ext["id"].as_str().unwrap().to_string();
        let ext_child = Command::new("sleep")
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let ext_pid = i64::from(ext_child.id());
        let _ext = KillChild(Some(ext_child));
        let claimed = post(
            &cfg,
            &format!("/v1/items/{ext_id}/claim"),
            json!({"role": "plan", "adapter": "mcp", "pid": ext_pid}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{ext_id}/drop"),
            json!({"force": true}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert!(alive(ext_pid), "drop --force signaled an external pid");

        let reused = post_item(&cfg, "Reused", "body");
        let reused_id = reused["id"].as_str().unwrap().to_string();
        let claimed = post(
            &cfg,
            &format!("/v1/items/{reused_id}/claim"),
            json!({"role": "plan", "adapter": "cli", "spawn": true}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let reused_pid: i64 = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pid FROM items WHERE id = ?1",
                params![reused_id],
                |row| row.get(0),
            )
            .unwrap();
        let _reused_guard = crate::proc::GroupGuard {
            pgid: Some(reused_pid),
            pid: Some(reused_pid),
        };
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET claim_starttime = '1' WHERE id = ?1",
                params![reused_id],
            )
            .unwrap();
        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{reused_id}/drop"),
            json!({"force": true}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert!(alive(reused_pid), "drop --force signaled a reused pid");

        let bare = post_item(&cfg, "Null pid", "body");
        let bare_id = bare["id"].as_str().unwrap().to_string();
        let claimed = post(
            &cfg,
            &format!("/v1/items/{bare_id}/claim"),
            json!({"role": "plan", "adapter": "cli"}),
        );
        assert_eq!(
            claimed.status,
            200,
            "{}",
            String::from_utf8_lossy(&claimed.body)
        );
        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{bare_id}/drop"),
            json!({"force": true}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert_eq!(item_state_of(&cfg, &bare_id), "dropped");

        let dropped = post_cli(
            &cfg,
            &format!("/v1/items/{spawned_id}/drop"),
            json!({"force": true}),
        );
        assert_eq!(
            dropped.status,
            200,
            "{}",
            String::from_utf8_lossy(&dropped.body)
        );
        assert_eq!(item_state_of(&cfg, &spawned_id), "dropped");
        assert!(!alive(spawned_pid), "drop --force left the spawned group");
        drop(running);
    }

    fn keel_doc(html: &str) -> Value {
        let marker = "<script type=\"application/json\" id=\"keel-doc\">";
        let start = html.find(marker).unwrap() + marker.len();
        let end = html[start..].find("</script>").unwrap() + start;
        serde_json::from_str(&html[start..end]).unwrap()
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
