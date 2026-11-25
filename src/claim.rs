//! Claim, heartbeat, and release over the existing lease transitions.
//! The database mutex is held only for the transaction. No child is spawned here.

use std::fs::File;
use std::io::Read;
use std::sync::{Mutex, MutexGuard};

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};

use crate::daemon;
use crate::db::{Error, Result};
use crate::state::{self, Claim, ClaimSpec, Release, ReleaseCause, Role};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    Item,
    Job,
}

impl Target {
    pub fn as_str(self) -> &'static str {
        match self {
            Target::Item => "item",
            Target::Job => "job",
        }
    }
}

pub struct ClaimInput {
    pub role: Role,
    pub adapter: Option<String>,
    pub worker_id: Option<String>,
    pub pid: Option<i64>,
}

pub struct Prepared {
    pub target: Target,
    pub id: String,
    pub claim: Claim,
}

pub struct Resolved {
    pub target: Target,
    pub id: String,
}

/// Test double. It has no child and no database handle.
/// Implement and review heartbeats go to the job route; plan and revise to the item route.
#[cfg(test)]
pub struct FakeAdapter {
    pub socket_path: std::path::PathBuf,
    pub pause: std::time::Duration,
}

#[cfg(test)]
impl FakeAdapter {
    pub fn heartbeat(&self, role: Role, id: &str, token: &str) -> Result<String> {
        // The pause is the stand-in for `run`. It must not sit on the database mutex.
        if !self.pause.is_zero() {
            std::thread::sleep(self.pause);
        }
        let path = heartbeat_path(role, id);
        let body = serde_json::json!({ "claim_token": token }).to_string();
        let mut stream = std::os::unix::net::UnixStream::connect(&self.socket_path)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(3)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(3)))?;
        let response = crate::http::exchange(&mut stream, "POST", &path, Some(body.as_bytes()))?;
        if !(200..300).contains(&response.status) {
            let message = String::from_utf8_lossy(&response.body);
            return Err(Error::Invalid(format!(
                "heartbeat {} {message}",
                response.status
            )));
        }
        Ok(path)
    }
}

/// Implement and review slide the job row. An item heartbeat would leave that lease alone.
#[cfg(test)]
pub fn heartbeat_path(role: Role, id: &str) -> String {
    match role {
        Role::Implement | Role::Review => format!("/v1/jobs/{id}/heartbeat"),
        Role::Plan | Role::Revise => format!("/v1/items/{id}/heartbeat"),
    }
}

/// Inserts the lease and drops the mutex before returning.
/// The caller runs the adapter, or the fake one, only after this returns.
pub fn claim_and_prepare(
    db: &Mutex<Connection>,
    target: Target,
    id: &str,
    input: &ClaimInput,
) -> Result<Prepared> {
    let mut conn = lock_db(db)?;
    // Starttime is read while this process holds the writer mutex, then the
    // claim transaction commits and the guard ends before return.
    let spec = build_spec(input)?;
    let claim = match target {
        Target::Item => state::claim_item(&mut conn, id, spec)?,
        Target::Job => state::claim_job(&mut conn, id, spec)?,
    };
    drop(conn);
    Ok(Prepared {
        target,
        id: id.to_string(),
        claim,
    })
}

pub fn heartbeat(db: &Mutex<Connection>, target: Target, id: &str, token: &str) -> Result<String> {
    let mut conn = lock_db(db)?;
    match target {
        Target::Item => state::heartbeat_item(&mut conn, id, token)?,
        Target::Job => state::heartbeat_job(&mut conn, id, token)?,
    }
    let until = read_text(&conn, target, id, "lease_until", "lease_until is missing")?;
    drop(conn);
    Ok(until)
}

pub fn release(
    db: &Mutex<Connection>,
    target: Target,
    id: &str,
    mut release: Release,
) -> Result<String> {
    let mut conn = lock_db(db)?;
    // The body cannot claim a live pid is gone. A null pid stays not-gone,
    // so expiry still refuses the pre-spawn window.
    release.pid_gone = pid_is_gone(&conn, target, id)?;
    match target {
        Target::Item => state::release_item(&mut conn, id, release)?,
        Target::Job => state::release_job(&mut conn, id, release)?,
    }
    let state_name = read_text(&conn, target, id, "state", "state is missing")?;
    drop(conn);
    Ok(state_name)
}

/// Dozens of ids. This is a primary-key filter, not a scan to upgrade, until
/// the table leaves the low hundreds and prefix lookup shows up in `status`.
pub fn resolve_id(conn: &Connection, prefix: &str) -> Result<Resolved> {
    if prefix.is_empty() || !crate::render::safe_id(prefix) {
        return Err(Error::Invalid(
            "id must be letters, digits, '_' or '-'".into(),
        ));
    }
    let items = matching_ids(conn, Target::Item, prefix)?;
    let jobs = matching_ids(conn, Target::Job, prefix)?;
    match (items.len(), jobs.len()) {
        (1, 0) => Ok(Resolved {
            target: Target::Item,
            id: items.into_iter().next().unwrap(),
        }),
        (0, 1) => Ok(Resolved {
            target: Target::Job,
            id: jobs.into_iter().next().unwrap(),
        }),
        (0, 0) => Err(Error::NotFound(prefix.to_string())),
        _ => Err(Error::Conflict {
            state: "ambiguous".into(),
            message: "id prefix matches more than one row; pass the full id".into(),
        }),
    }
}

pub fn parse_claim(body: &[u8]) -> Result<ClaimInput> {
    let obj = object(body)?;
    let role = parse_role(&required_string(&obj, "role")?)?;
    let adapter = optional_string(&obj, "adapter")?;
    let worker_id = optional_string(&obj, "worker_id")?;
    let pid = optional_i64(&obj, "pid")?;
    Ok(ClaimInput {
        role,
        adapter,
        worker_id,
        pid,
    })
}

pub fn parse_role(value: &str) -> Result<Role> {
    match value {
        "plan" => Ok(Role::Plan),
        "revise" => Ok(Role::Revise),
        "implement" => Ok(Role::Implement),
        "review" => Ok(Role::Review),
        other => Err(Error::Invalid(format!(
            "role must be plan, revise, implement, or review, not {other}"
        ))),
    }
}

pub fn parse_heartbeat(body: &[u8]) -> Result<String> {
    let obj = object(body)?;
    match required_string(&obj, "claim_token")? {
        token if token.is_empty() => Err(Error::Invalid("claim_token is empty".into())),
        token => Ok(token),
    }
}

/// A missing token is not expiry. Expiry is only `cause=expiry`.
/// The CLI may omit the token; anything else must send one.
pub fn parse_release(body: &[u8], surface_cli: bool) -> Result<Release> {
    let obj = if body.is_empty() {
        Map::new()
    } else {
        object(body)?
    };
    let cause = match optional_string(&obj, "cause")?.as_deref() {
        None | Some("release") => ReleaseCause::Human,
        Some("expiry") => ReleaseCause::Expiry,
        Some(other) => {
            return Err(Error::Invalid(format!(
                "cause must be release or expiry, not {other}"
            )))
        }
    };
    let force = optional_bool(&obj, "force")?;
    let claim_token = match optional_string(&obj, "claim_token")? {
        Some(token) if token.is_empty() => {
            return Err(Error::Invalid("claim_token is empty".into()))
        }
        other => other,
    };
    if cause == ReleaseCause::Expiry {
        if force {
            return Err(Error::Invalid("expiry has no --force".into()));
        }
        return Ok(Release {
            cause,
            force: false,
            claim_token,
            pid_gone: false,
        });
    }
    if claim_token.is_some() {
        return Ok(Release {
            cause: ReleaseCause::Human,
            force,
            claim_token,
            pid_gone: false,
        });
    }
    if surface_cli {
        return Ok(Release {
            cause: ReleaseCause::Human,
            force,
            claim_token: None,
            pid_gone: false,
        });
    }
    Err(Error::Invalid(
        "claim_token is required unless the caller is the CLI".into(),
    ))
}

fn build_spec(input: &ClaimInput) -> Result<ClaimSpec> {
    let worker_id = worker_id(input)?;
    let Some(pid) = input.pid else {
        return Ok(ClaimSpec::daemon(worker_id, input.role));
    };
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return Err(Error::Invalid("pid is not alive".into()));
    }
    let starttime = daemon::read_proc_starttime(pid as u32)
        .ok_or_else(|| Error::Invalid("pid is not alive".into()))?;
    Ok(ClaimSpec {
        worker_id,
        role: input.role,
        external: true,
        pid: Some(pid),
        pgid: None,
        starttime: Some(starttime),
    })
}

fn worker_id(input: &ClaimInput) -> Result<String> {
    if let Some(id) = input.worker_id.as_deref().filter(|id| !id.is_empty()) {
        if id.len() > 80 || id.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(Error::Invalid("worker_id is not a token".into()));
        }
        return Ok(id.to_string());
    }
    let adapter = input.adapter.as_deref().unwrap_or("cli");
    let ok = !adapter.is_empty()
        && adapter.len() <= 32
        && adapter
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(Error::Invalid("adapter must be a short ascii name".into()));
    }
    Ok(format!("{adapter}:{}", random_hex(16)?))
}

fn pid_is_gone(conn: &Connection, target: Target, id: &str) -> Result<bool> {
    let sql = match target {
        Target::Item => "SELECT claim_pid, claim_starttime FROM items WHERE id = ?1",
        Target::Job => "SELECT claim_pid, claim_starttime FROM jobs WHERE id = ?1",
    };
    let row = conn
        .query_row(sql, params![id], |row| {
            Ok((
                row.get::<_, Option<i64>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })
        .optional()?;
    let Some((pid, starttime)) = row else {
        return Ok(false);
    };
    // Null pid is the unsupervised window. It is not "gone", and expiry must not clear it.
    let Some(pid) = pid else {
        return Ok(false);
    };
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return Ok(true);
    }
    match daemon::read_proc_starttime(pid as u32) {
        None => Ok(true),
        Some(live) => Ok(starttime.as_deref() != Some(live.as_str())),
    }
}

fn read_text(
    conn: &Connection,
    target: Target,
    id: &str,
    column: &str,
    missing: &str,
) -> Result<String> {
    let sql = match (target, column) {
        (Target::Item, "state") => "SELECT state FROM items WHERE id = ?1",
        (Target::Item, "lease_until") => "SELECT lease_until FROM items WHERE id = ?1",
        (Target::Job, "state") => "SELECT state FROM jobs WHERE id = ?1",
        (Target::Job, "lease_until") => "SELECT lease_until FROM jobs WHERE id = ?1",
        _ => return Err(Error::Invalid(missing.into())),
    };
    let value: Option<String> = conn
        .query_row(sql, params![id], |row| row.get(0))
        .optional()?
        .flatten();
    value.ok_or_else(|| Error::Invalid(missing.into()))
}

fn matching_ids(conn: &Connection, target: Target, prefix: &str) -> Result<Vec<String>> {
    let sql = match target {
        Target::Item => {
            "SELECT id FROM items WHERE id = ?1 OR substr(id, 1, length(?1)) = ?1 ORDER BY id"
        }
        Target::Job => {
            "SELECT id FROM jobs WHERE id = ?1 OR substr(id, 1, length(?1)) = ?1 ORDER BY id"
        }
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![prefix], |row| row.get(0))?;
    let mut ids = Vec::new();
    for id in rows {
        ids.push(id?);
    }
    Ok(ids)
}

fn object(body: &[u8]) -> Result<Map<String, Value>> {
    let value: Value =
        serde_json::from_slice(body).map_err(|err| Error::Invalid(format!("json: {err}")))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| Error::Invalid("expected a JSON object".into()))
}

fn required_string(obj: &Map<String, Value>, key: &str) -> Result<String> {
    match obj.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(Error::Invalid(format!("{key} is required"))),
    }
}

fn optional_string(obj: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::Invalid(format!("{key} must be a string"))),
    }
}

fn optional_bool(obj: &Map<String, Value>, key: &str) -> Result<bool> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(Error::Invalid(format!("{key} must be a boolean"))),
    }
}

fn optional_i64(obj: &Map<String, Value>, key: &str) -> Result<Option<i64>> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .map(Some)
            .ok_or_else(|| Error::Invalid(format!("{key} must be an integer"))),
        Some(_) => Err(Error::Invalid(format!("{key} must be an integer"))),
    }
}

fn random_hex(nbytes: usize) -> Result<String> {
    let mut buf = vec![0u8; nbytes];
    File::open("/dev/urandom")?.read_exact(&mut buf)?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(nbytes * 2);
    for byte in buf {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    Ok(out)
}

fn lock_db(db: &Mutex<Connection>) -> Result<MutexGuard<'_, Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_in_memory;
    use crate::state::RawIntake;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    fn intake() -> RawIntake {
        RawIntake {
            source: "cli".into(),
            external_id: None,
            title: "Pin the walk".into(),
            body: "body".into(),
            repo: Some(std::path::PathBuf::from("/tmp/repo")),
            target_branch: Some("main".into()),
        }
    }

    fn plan_input() -> ClaimInput {
        ClaimInput {
            role: Role::Plan,
            adapter: Some("cli".into()),
            worker_id: None,
            pid: None,
        }
    }

    #[test]
    fn heartbeat_route_follows_the_role() {
        assert_eq!(heartbeat_path(Role::Plan, "k_1"), "/v1/items/k_1/heartbeat");
        assert_eq!(
            heartbeat_path(Role::Revise, "k_1"),
            "/v1/items/k_1/heartbeat"
        );
        assert_eq!(
            heartbeat_path(Role::Implement, "j_1"),
            "/v1/jobs/j_1/heartbeat"
        );
        assert_eq!(
            heartbeat_path(Role::Review, "j_1"),
            "/v1/jobs/j_1/heartbeat"
        );
    }

    #[test]
    fn claim_and_prepare_drops_the_mutex_before_return() {
        let db = Arc::new(Mutex::new(open_in_memory().unwrap()));
        let id = {
            let mut conn = db.lock().unwrap();
            state::create_item(&mut conn, intake()).unwrap()
        };
        let hold = db.lock().unwrap();
        let db2 = Arc::clone(&db);
        let id2 = id.clone();
        let started = Arc::new(Barrier::new(2));
        let started2 = Arc::clone(&started);
        let handle = thread::spawn(move || {
            started2.wait();
            claim_and_prepare(&db2, Target::Item, &id2, &plan_input())
        });
        started.wait();
        thread::sleep(Duration::from_millis(80));
        assert!(
            !handle.is_finished(),
            "claim finished while the mutex was still held"
        );
        drop(hold);
        let prepared = handle.join().unwrap().unwrap();
        assert!(db.try_lock().is_ok());
        assert_eq!(prepared.target, Target::Item);
        assert!(prepared.claim.token.len() >= 32);
        let pid: Option<i64> = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT claim_pid FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pid.is_none());
    }

    #[test]
    fn dead_pid_does_not_insert_and_null_pid_does_not_expire() {
        let db = Mutex::new(open_in_memory().unwrap());
        let id = {
            let mut conn = db.lock().unwrap();
            state::create_item(&mut conn, intake()).unwrap()
        };
        let mut dead = plan_input();
        dead.pid = Some(2_147_483_646);
        let err = match claim_and_prepare(&db, Target::Item, &id, &dead) {
            Err(err) => err,
            Ok(_) => panic!("dead pid was claimed"),
        };
        assert!(err.to_string().contains("not alive"), "{err}");
        let token: Option<String> = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT claim_token FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(token.is_none());

        claim_and_prepare(&db, Target::Item, &id, &plan_input()).unwrap();
        let err = release(&db, Target::Item, &id, Release::expiry(true)).unwrap_err();
        assert!(err.to_string().contains("null claim_pid"), "{err}");
        let state_name: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state_name, "planning");

        // Non-external unsupervised window. Human release clears it without a token.
        release(&db, Target::Item, &id, Release::human()).unwrap();
        let state_name: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state_name, "inbox");
    }

    #[test]
    fn a_live_external_pid_is_not_expired_or_released_without_force() {
        let db = Mutex::new(open_in_memory().unwrap());
        let id = {
            let mut conn = db.lock().unwrap();
            state::create_item(&mut conn, intake()).unwrap()
        };
        let mut input = plan_input();
        input.adapter = Some("mcp".into());
        input.pid = Some(i64::from(std::process::id()));
        let prepared = claim_and_prepare(&db, Target::Item, &id, &input).unwrap();
        let external: i64 = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT claim_external FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(external, 1);
        let err = release(&db, Target::Item, &id, Release::expiry(true)).unwrap_err();
        assert!(err.to_string().contains("still live"), "{err}");
        let err = release(&db, Target::Item, &id, Release::human()).unwrap_err();
        assert!(err.to_string().contains("force"), "{err}");
        let mut mcp = Release::human();
        mcp.claim_token = Some(prepared.claim.token);
        let err = release(&db, Target::Item, &id, mcp).unwrap_err();
        assert!(err.to_string().contains("still live"), "{err}");
        let mut forced = Release::human();
        forced.force = true;
        release(&db, Target::Item, &id, forced).unwrap();
    }

    #[test]
    fn missing_token_is_not_an_expiry_cause() {
        let human = parse_release(b"{}", true).unwrap();
        assert_eq!(human.cause, ReleaseCause::Human);
        assert!(human.claim_token.is_none());
        assert!(!human.pid_gone);
        let err = parse_release(b"{}", false).unwrap_err();
        assert!(err.to_string().contains("claim_token"), "{err}");
        let expiry = parse_release(br#"{"cause":"expiry","pid_gone":true}"#, false).unwrap();
        assert_eq!(expiry.cause, ReleaseCause::Expiry);
        assert!(!expiry.pid_gone);
        assert!(parse_release(br#"{"cause":"expiry","force":true}"#, true).is_err());
    }
}
