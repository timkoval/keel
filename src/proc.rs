//! Daemon-spawned children get their own process group (`pgid == pid`).
//! Signals go to `-pgid` so a grandchild in the worktree dies with the leader.
//! A live pid whose starttime does not match was reused; it is not this child
//! and must not be signaled.

use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::daemon::read_proc_starttime;
use crate::db::{Error, Result};

/// Linux errno and signals. These values are the kernel ABI, not libc headers.
const ESRCH: i32 = 3;
const SIGTERM: i32 = 15;
const SIGKILL: i32 = 9;
/// `WNOHANG` so a live leader does not block the reaper.
const WNOHANG: i32 = 1;

/// Name of the file the test grandchild creates inside the worktree.
pub(crate) const GRANDCHILD_FILE: &str = "keel-grandchild";

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
    fn getpgrp() -> i32;
    fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
}

/// Facts the supervisor needs. The row update stays in `state`.
pub(crate) struct ProcLease {
    pub external: bool,
    pub pid: Option<i64>,
    pub pgid: Option<i64>,
    pub starttime: Option<String>,
}

pub(crate) struct Spawned {
    pub pid: i64,
    pub pgid: i64,
    pub starttime: String,
}

enum Leader {
    /// `/proc` starttime still matches the row.
    Same,
    /// Nothing is in `/proc` for this pid.
    Dead,
    /// A different process now owns the number.
    Reused,
}

/// Reaper predicate. A null pid is the unsupervised window and is never expired.
/// An external pid is expired only when that pid is gone or was reused, and the
/// caller must not signal it. A spawned leader whose starttime does not match
/// is not expired here: signaling it would hit the wrong process.
pub(crate) fn should_expire(lease: &ProcLease, startup: bool, expired: bool) -> bool {
    let Some(pid) = lease.pid else {
        return false;
    };
    if lease.external {
        return !matches!(
            leader(pid, lease.starttime.as_deref().unwrap_or("")),
            Leader::Same
        );
    }
    if lease.pgid.is_none_or(|pgid| pgid <= 1) {
        return false;
    }
    if matches!(
        leader(pid, lease.starttime.as_deref().unwrap_or("")),
        Leader::Reused
    ) {
        return false;
    }
    startup || expired
}

pub(crate) fn leader_reused(lease: &ProcLease) -> bool {
    if lease.external {
        return false;
    }
    let Some(pid) = lease.pid else {
        return false;
    };
    matches!(
        leader(pid, lease.starttime.as_deref().unwrap_or("")),
        Leader::Reused
    )
}

/// `true` when the caller may set `pid_gone`. External rows are never signaled.
/// A reused leader is not this child: no SIGTERM, SIGKILL, or `kill(-pgid, 0)`.
/// A group we still own is `true` only when `kill(-pgid, 0)` is `ESRCH`.
pub(crate) fn prepare_gone(lease: &ProcLease) -> Result<bool> {
    let Some(pid) = lease.pid else {
        return Ok(false);
    };
    if lease.external {
        return Ok(record_gone(pid, lease.starttime.as_deref()));
    }
    let Some(pgid) = lease.pgid else {
        // No group was stored. Do not fall back to `kill(pid)`: that leaves
        // grandchildren, and a missing pgid is not proof the group is empty.
        return Ok(false);
    };
    match leader(pid, lease.starttime.as_deref().unwrap_or("")) {
        Leader::Reused => Ok(true),
        Leader::Same | Leader::Dead => {
            if !group_empty(pgid)? {
                empty_group(pgid, pid)?;
            } else {
                reap_leader(pid);
            }
            group_empty(pgid)
        }
    }
}

/// Why a spawn did not finish attaching. `group_live` means a process group
/// was created and `empty_group` did not see it become empty, so the caller
/// must not return the row to the queue.
#[derive(Debug)]
pub(crate) struct SpawnFailure {
    pub error: Error,
    pub group_live: bool,
}

impl SpawnFailure {
    fn idle(error: Error) -> Self {
        Self {
            error,
            group_live: false,
        }
    }
}

/// Test child: leader pgid equals pid, grandchild writes `marker` and both sleep
/// until the group is killed. Not an adapter and not a model binary.
pub(crate) fn spawn_marker(marker: &Path) -> std::result::Result<Spawned, SpawnFailure> {
    // Non-interactive bash does not enable job control, so the background
    // grandchild stays in the leader's group. `process_group(0)` makes that
    // group id the leader pid.
    spawn_script(
        marker,
        r#"
marker=$1
(
  echo "$BASHPID" > "$marker"
  exec sleep 300
) &
exec sleep 300
"#,
    )
}

/// Same group as [`spawn_marker`], but both bash processes ignore `SIGTERM`.
/// `empty_group` still has to `SIGKILL` before the row can return to the queue.
#[cfg(test)]
fn spawn_ignore_term(marker: &Path) -> std::result::Result<Spawned, SpawnFailure> {
    spawn_script(
        marker,
        r#"
trap '' TERM
marker=$1
(
  trap '' TERM
  echo "$BASHPID" > "$marker"
  while true; do
    sleep 300
  done
) &
while true; do
  sleep 300
done
"#,
    )
}

fn spawn_script(marker: &Path, script: &str) -> std::result::Result<Spawned, SpawnFailure> {
    if let Some(parent) = marker.parent() {
        fs::create_dir_all(parent).map_err(|err| SpawnFailure::idle(err.into()))?;
    }
    let _ = fs::remove_file(marker);
    let mut cmd = Command::new("bash");
    cmd.arg("-c")
        .arg(script)
        .arg("keel-child")
        .arg(marker)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.process_group(0);
    let child = cmd.spawn().map_err(|err| SpawnFailure::idle(err.into()))?;
    let pid = i64::from(child.id());
    // Dropping `Child` does not wait. The leader stays our child so a later
    // `waitpid` can collect the zombie; otherwise `kill(-pgid, 0)` never
    // returns ESRCH.
    drop(child);
    let spawned = (|| -> Result<Spawned> {
        let starttime = read_proc_starttime(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned child has no starttime".into()))?;
        let pgid = read_pgrp(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned child has no pgid".into()))?;
        if pgid != pid {
            return Err(Error::Invalid("spawned pgid does not equal pid".into()));
        }
        Ok(Spawned {
            pid,
            pgid,
            starttime,
        })
    })();
    match spawned {
        Ok(spawned) => Ok(spawned),
        Err(err) => Err(kill_unless_empty(pid, err)),
    }
}

/// After the child exists, the row may be released only when this returns
/// `group_live == false`. Any `empty_group` error means the group is still live.
pub(crate) fn kill_unless_empty(pid: i64, error: Error) -> SpawnFailure {
    let pgid = if pid > 0 && pid <= i64::from(u32::MAX) {
        read_pgrp(pid as u32).unwrap_or(pid)
    } else {
        pid
    };
    match empty_group(pgid, pid) {
        Ok(()) => SpawnFailure {
            error,
            group_live: false,
        },
        Err(kill_err) => SpawnFailure {
            error: kill_err,
            group_live: true,
        },
    }
}

pub(crate) fn group_empty(pgid: i64) -> Result<bool> {
    let pgid = checked_pgid(pgid)?;
    signal(pgid, 0)
}

/// `SIGTERM`, then `SIGKILL` if the group is still there after two seconds.
/// Returns only when `kill(-pgid, 0)` is `ESRCH`, or the group survived `SIGKILL`.
pub(crate) fn empty_group(pgid: i64, leader: i64) -> Result<()> {
    let pgid = checked_pgid(pgid)?;
    if signal(pgid, 0)? {
        reap_leader(leader);
        return Ok(());
    }
    signal(pgid, SIGTERM)?;
    if wait_empty(pgid, leader, Duration::from_secs(2))? {
        return Ok(());
    }
    signal(pgid, SIGKILL)?;
    if wait_empty(pgid, leader, Duration::from_secs(2))? {
        return Ok(());
    }
    Err(Error::Conflict {
        state: "leased".into(),
        message: "process group is not empty".into(),
    })
}

fn wait_empty(pgid: i32, leader: i64, budget: Duration) -> Result<bool> {
    let deadline = Instant::now() + budget;
    loop {
        reap_leader(leader);
        if signal(pgid, 0)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// `true` when the kernel reports no process in the group (`ESRCH`).
fn signal(pgid: i32, sig: i32) -> Result<bool> {
    let target = pgid
        .checked_neg()
        .ok_or_else(|| Error::Invalid("refusing to signal an out-of-range process group".into()))?;
    let rc = unsafe { kill(target, sig) };
    if rc == 0 {
        return Ok(false);
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(ESRCH) {
        return Ok(true);
    }
    Err(Error::Io(err))
}

fn checked_pgid(pgid: i64) -> Result<i32> {
    if pgid <= 1 || pgid > i64::from(i32::MAX) {
        return Err(Error::Invalid("pgid is not a process group".into()));
    }
    let own = unsafe { getpgrp() };
    // `kill(-our_pgrp)` would terminate the daemon and the test harness.
    if pgid as i32 == own || own <= 1 {
        return Err(Error::Invalid(
            "refusing to signal the caller's process group".into(),
        ));
    }
    Ok(pgid as i32)
}

fn reap_leader(pid: i64) {
    if pid <= 1 || pid > i64::from(i32::MAX) {
        return;
    }
    let mut status = 0;
    unsafe {
        waitpid(pid as i32, &mut status, WNOHANG);
    }
}

fn leader(pid: i64, starttime: &str) -> Leader {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return Leader::Dead;
    }
    match read_proc_starttime(pid as u32) {
        None => Leader::Dead,
        Some(live) if live == starttime => Leader::Same,
        Some(_) => Leader::Reused,
    }
}

fn record_gone(pid: i64, starttime: Option<&str>) -> bool {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return true;
    }
    match read_proc_starttime(pid as u32) {
        None => true,
        Some(live) => starttime != Some(live.as_str()),
    }
}

fn read_pgrp(pid: u32) -> Option<i64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let end = text.rfind(')')?;
    let fields: Vec<&str> = text[end + 1..].split_whitespace().collect();
    // `/proc/pid/stat` field 5, the third field after `comm`.
    fields.get(2)?.parse().ok()
}

#[cfg(test)]
pub(crate) struct GroupGuard {
    pub pgid: Option<i64>,
    pub pid: Option<i64>,
}

#[cfg(test)]
impl Drop for GroupGuard {
    fn drop(&mut self) {
        if let Some(pgid) = self.pgid {
            let _ = empty_group(pgid, self.pid.unwrap_or(pgid));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::claim::{self, ClaimInput, Target};
    use crate::db::open_in_memory;
    use crate::state::{self, RawIntake, Release};
    use rusqlite::params;
    use std::os::unix::process::CommandExt;
    use std::sync::{Arc, Mutex};

    fn scratch() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "keel-proc-{}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn proc_state(pid: i64) -> Option<char> {
        let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let end = text.rfind(')')?;
        text[end + 1..]
            .split_whitespace()
            .next()
            .and_then(|state| state.chars().next())
    }

    /// Sleeping or running. A zombie means we signaled and have not reaped.
    fn still_running(pid: i64) -> bool {
        matches!(proc_state(pid), Some('S') | Some('R') | Some('D'))
    }

    fn wait_marker(path: &Path) -> bool {
        for _ in 0..100 {
            if path.is_file() && fs::metadata(path).map(|m| m.len() > 0).unwrap_or(false) {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    struct JobSetup {
        db: Mutex<rusqlite::Connection>,
        root: std::path::PathBuf,
        item: String,
        worktree: std::path::PathBuf,
        token: String,
    }

    fn claimed_job() -> JobSetup {
        let root = scratch();
        let repo = root.join("repo");
        let base = crate::git::init_repo(&repo);
        let db = Mutex::new(open_in_memory().unwrap());
        let item = {
            let mut conn = db.lock().unwrap();
            state::create_item(
                &mut conn,
                RawIntake {
                    source: "cli".into(),
                    external_id: None,
                    title: "Supervise".into(),
                    body: "body".into(),
                    repo: Some(repo),
                    target_branch: Some("main".into()),
                },
            )
            .unwrap()
        };
        {
            let conn = db.lock().unwrap();
            conn.execute(
                "UPDATE items SET state = 'awaiting_implementation', base_sha = ?1 WHERE id = ?2",
                params![base, item],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at
                 ) VALUES (
                    'j_child', ?1, 'Leaf', '', 'queued', 1, 0, ?2, '2026-11-25T00:00:00Z'
                 )",
                params![item, base],
            )
            .unwrap();
        }
        let prepared = claim::claim_and_prepare(
            &db,
            Target::Job,
            "j_child",
            &ClaimInput {
                role: state::Role::Implement,
                adapter: Some("fake".into()),
                worker_id: Some("fake:impl".into()),
                pid: None,
            },
            &root,
        )
        .unwrap();
        let worktree = prepared.worktree.expect("worktree");
        JobSetup {
            db,
            root,
            item,
            worktree,
            token: prepared.claim.token,
        }
    }

    fn attach(setup: &JobSetup, spawned: &Spawned) {
        let mut conn = setup.db.lock().unwrap();
        state::attach_process(
            &mut conn,
            "j_child",
            &setup.token,
            spawned.pid,
            spawned.pgid,
            &spawned.starttime,
        )
        .unwrap();
    }

    #[test]
    fn grandchild_is_dead_and_the_group_is_empty_before_requeue() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        assert_eq!(spawned.pid, spawned.pgid);
        assert_eq!(
            read_proc_starttime(spawned.pid as u32).as_deref(),
            Some(spawned.starttime.as_str())
        );
        attach(&setup, &spawned);
        assert!(wait_marker(&marker), "grandchild did not write {marker:?}");
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
        assert_ne!(gpid, spawned.pid);
        assert_eq!(read_pgrp(gpid as u32), Some(spawned.pgid));
        assert!(still_running(gpid));
        assert!(!group_empty(spawned.pgid).unwrap());
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");

        let state_name = claim::release(
            &setup.db,
            Target::Job,
            "j_child",
            Release::human(),
            &setup.root,
        )
        .unwrap();
        assert_eq!(state_name, "queued");
        assert!(marker.is_file(), "grandchild file missing after release");
        assert_eq!(
            fs::read_to_string(&marker)
                .unwrap()
                .trim()
                .parse::<i64>()
                .unwrap(),
            gpid
        );
        assert!(group_empty(spawned.pgid).unwrap());
        assert!(proc_state(gpid).is_none(), "grandchild still in /proc");
        assert!(proc_state(spawned.pid).is_none(), "leader still in /proc");
        let _ = setup.item;
    }

    #[test]
    fn ignored_sigterm_is_sigkilled_before_the_row_returns() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_ignore_term(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker), "grandchild did not write {marker:?}");
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
        assert_ne!(gpid, spawned.pid);
        assert_eq!(read_pgrp(gpid as u32), Some(spawned.pgid));
        // The group is still ours after SIGTERM. Requeue before SIGKILL would
        // leave this child in the worktree.
        signal(spawned.pgid as i32, SIGTERM).unwrap();
        thread::sleep(Duration::from_millis(200));
        assert!(still_running(spawned.pid), "leader exited on SIGTERM");
        assert!(still_running(gpid), "grandchild exited on SIGTERM");
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2000-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        claim::reap(&setup.db, &setup.root, false).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "queued");
        assert!(group_empty(spawned.pgid).unwrap());
        assert!(proc_state(gpid).is_none(), "grandchild still in /proc");
        assert!(proc_state(spawned.pid).is_none(), "leader still in /proc");
    }

    #[test]
    fn reaper_requeues_an_expired_group_only_after_it_is_empty() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker));
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2099-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        claim::reap(&setup.db, &setup.root, false).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");
        assert!(still_running(gpid));

        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2000-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        claim::reap(&setup.db, &setup.root, false).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "queued");
        assert!(marker.is_file());
        assert!(group_empty(spawned.pgid).unwrap());
        assert!(proc_state(gpid).is_none());
    }

    #[test]
    fn null_pid_is_not_requeued_and_a_live_external_pid_is_not_signaled() {
        let db = Mutex::new(open_in_memory().unwrap());
        let root = scratch();
        let id = {
            let mut conn = db.lock().unwrap();
            state::create_item(
                &mut conn,
                RawIntake {
                    source: "cli".into(),
                    external_id: None,
                    title: "Window".into(),
                    body: "body".into(),
                    repo: None,
                    target_branch: None,
                },
            )
            .unwrap()
        };
        claim::claim_and_prepare(
            &db,
            Target::Item,
            &id,
            &ClaimInput {
                role: state::Role::Plan,
                adapter: Some("fake".into()),
                worker_id: None,
                pid: None,
            },
            &root,
        )
        .unwrap();
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET lease_until = '2000-01-01T00:00:00Z' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        claim::reap(&db, &root, true).unwrap();
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
        let err = claim::release(&db, Target::Item, &id, Release::expiry(true), &root).unwrap_err();
        assert!(err.to_string().contains("null claim_pid"), "{err}");

        let sleeper = Command::new("sleep")
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = i64::from(sleeper.id());
        struct KillChild(Option<std::process::Child>);
        impl Drop for KillChild {
            fn drop(&mut self) {
                if let Some(child) = self.0.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
        let _kill = KillChild(Some(sleeper));
        // The unsupervised row is still claimed. Release it, then claim external.
        claim::release(&db, Target::Item, &id, Release::human(), &root).unwrap();
        claim::claim_and_prepare(
            &db,
            Target::Item,
            &id,
            &ClaimInput {
                role: state::Role::Plan,
                adapter: Some("mcp".into()),
                worker_id: Some("mcp:ext".into()),
                pid: Some(pid),
            },
            &root,
        )
        .unwrap();
        let pgid: Option<i64> = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT claim_pgid FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pgid.is_none());
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET lease_until = '2000-01-01T00:00:00Z' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        claim::reap(&db, &root, true).unwrap();
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
        assert!(still_running(pid));
        let err = claim::release(&db, Target::Item, &id, Release::human(), &root).unwrap_err();
        assert!(err.to_string().contains("force"), "{err}");
        assert!(still_running(pid));

        // The recorded pid is gone. Requeue without signaling the live sleeper,
        // which is no longer the claim.
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET claim_pid = 2147483646, claim_starttime = '1' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        claim::reap(&db, &root, false).unwrap();
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
        assert!(still_running(pid));
    }

    #[test]
    fn reused_leader_starttime_is_not_signaled_or_requeued() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker));
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET claim_starttime = '1', lease_until = '2000-01-01T00:00:00Z'
                 WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        claim::reap(&setup.db, &setup.root, true).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");
        assert!(still_running(spawned.pid));
        assert!(still_running(gpid));
        // Human release clears the row. The live group is not our child, so it
        // stays in /proc and is not probed with kill(-pgid, 0).
        let state_name = claim::release(
            &setup.db,
            Target::Job,
            "j_child",
            Release::human(),
            &setup.root,
        )
        .unwrap();
        assert_eq!(state_name, "queued");
        assert!(still_running(spawned.pid));
        assert!(still_running(gpid));
        assert!(!group_empty(spawned.pgid).unwrap());
    }

    #[test]
    fn reused_unrelated_group_leader_is_released_without_a_signal() {
        let setup = claimed_job();
        let mut cmd = Command::new("sleep");
        cmd.arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let child = cmd.spawn().unwrap();
        let pid = i64::from(child.id());
        let _keep = ChildHold(Some(child));
        let pgid = read_pgrp(pid as u32).unwrap();
        assert_eq!(pgid, pid);
        assert!(still_running(pid));
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET claim_pid = ?1, claim_pgid = ?1, claim_starttime = '1',
                        lease_until = '2000-01-01T00:00:00Z'
                 WHERE id = 'j_child'",
                params![pid],
            )
            .unwrap();
        claim::reap(&setup.db, &setup.root, true).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");
        assert!(still_running(pid));
        let state_name = claim::release(
            &setup.db,
            Target::Job,
            "j_child",
            Release::human(),
            &setup.root,
        )
        .unwrap();
        assert_eq!(state_name, "queued");
        assert!(still_running(pid), "unrelated group leader left /proc");
        assert!(!group_empty(pgid).unwrap());
    }

    #[test]
    fn reap_rereads_before_signal_and_does_not_take_a_moved_row() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker));
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2000-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        let slot = std::sync::Arc::new(Mutex::new(None::<std::process::Child>));
        let hook_slot = std::sync::Arc::clone(&slot);
        let _clear = ClearHook;
        claim::set_reap_before_read(Some(Box::new(move |conn| {
            let mut cmd = Command::new("sleep");
            cmd.arg("300")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(0);
            let child = cmd.spawn().unwrap();
            let pid = i64::from(child.id());
            *hook_slot.lock().unwrap() = Some(child);
            let start = crate::daemon::read_proc_starttime(pid as u32).unwrap();
            conn.execute(
                "UPDATE jobs SET claim_pid = ?1, claim_pgid = ?1, claim_starttime = ?2,
                        lease_until = '2099-01-01T00:00:00Z'
                 WHERE id = 'j_child'",
                params![pid, start],
            )
            .unwrap();
        })));
        let _slot_guard = SlotGuard(Arc::clone(&slot));
        claim::reap(&setup.db, &setup.root, false).unwrap();
        let (state_name, claim_pid): (String, i64) = setup
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state, claim_pid FROM jobs WHERE id = 'j_child'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(state_name, "implementing");
        let sleeper_pid = i64::from(slot.lock().unwrap().as_ref().unwrap().id());
        assert_eq!(claim_pid, sleeper_pid);
        assert!(
            still_running(spawned.pid),
            "stale reap killed the old group"
        );
        assert!(
            still_running(sleeper_pid),
            "stale reap killed the new group"
        );
    }

    #[test]
    fn reap_does_not_kill_a_lease_renewed_after_the_snapshot() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker));
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();
        setup
            .db
            .lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET lease_until = '2000-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        let _clear = ClearHook;
        claim::set_reap_before_read(Some(Box::new(|conn| {
            conn.execute(
                "UPDATE jobs SET lease_until = '2099-01-01T00:00:00Z' WHERE id = 'j_child'",
                [],
            )
            .unwrap();
        })));
        claim::reap(&setup.db, &setup.root, false).unwrap();
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");
        assert!(still_running(spawned.pid));
        assert!(still_running(gpid));
    }

    #[test]
    fn kill_unless_empty_keeps_a_refused_group_and_reports_a_dead_one() {
        let refused = kill_unless_empty(1, Error::Invalid("attach failed".into()));
        assert!(refused.group_live);
        assert!(
            refused.error.to_string().contains("process group")
                || refused.error.to_string().contains("refusing"),
            "{}",
            refused.error
        );

        let marker = scratch().join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        let cleared = kill_unless_empty(spawned.pid, Error::Invalid("attach failed".into()));
        assert!(!cleared.group_live);
        assert!(
            cleared.error.to_string().contains("attach failed"),
            "{}",
            cleared.error
        );
        assert!(proc_state(spawned.pid).is_none());
    }

    #[test]
    fn live_group_after_attach_failure_is_not_released() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        let err = crate::daemon::abandon_failed_spawn(
            &setup.db,
            Target::Job,
            "j_child",
            &setup.root,
            SpawnFailure {
                error: Error::Invalid("attach failed".into()),
                group_live: true,
            },
        );
        assert!(err.to_string().contains("attach failed"), "{err}");
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "implementing");
        assert!(still_running(spawned.pid));
        assert!(!group_empty(spawned.pgid).unwrap());

        let released = crate::daemon::abandon_failed_spawn(
            &setup.db,
            Target::Job,
            "j_child",
            &setup.root,
            SpawnFailure {
                error: Error::Invalid("never started".into()),
                group_live: false,
            },
        );
        assert!(released.to_string().contains("never started"), "{released}");
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_child'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(state_name, "queued");
        assert!(still_running(spawned.pid));
    }

    #[test]
    fn empty_spawned_groups_signals_ours_and_spares_external_and_reused() {
        let setup = claimed_job();
        let marker = setup.worktree.join(GRANDCHILD_FILE);
        let spawned = spawn_marker(&marker).unwrap();
        let _guard = GroupGuard {
            pgid: Some(spawned.pgid),
            pid: Some(spawned.pid),
        };
        attach(&setup, &spawned);
        assert!(wait_marker(&marker));
        let gpid: i64 = fs::read_to_string(&marker).unwrap().trim().parse().unwrap();

        let mut ext = Command::new("sleep");
        ext.arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let ext_child = ext.spawn().unwrap();
        let ext_pid = i64::from(ext_child.id());
        let _ext = ChildHold(Some(ext_child));
        let ext_start = crate::daemon::read_proc_starttime(ext_pid as u32).unwrap();

        let mut reused = Command::new("sleep");
        reused
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let reused_child = reused.spawn().unwrap();
        let reused_pid = i64::from(reused_child.id());
        let _reused = ChildHold(Some(reused_child));
        assert_eq!(read_pgrp(reused_pid as u32), Some(reused_pid));

        {
            let conn = setup.db.lock().unwrap();
            conn.execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at,
                    claim_token, claim_external, claim_pid, claim_starttime
                 ) VALUES (
                    'j_ext', ?1, 'Ext', '', 'implementing', 1, 1, 'abc', '2026-11-25T00:00:00Z',
                    'tok-ext', 1, ?2, ?3
                 )",
                params![setup.item, ext_pid, ext_start],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at,
                    claim_token, claim_external, claim_pid, claim_pgid, claim_starttime
                 ) VALUES (
                    'j_reused', ?1, 'Reused', '', 'implementing', 1, 2, 'abc', '2026-11-25T00:00:00Z',
                    'tok-re', 0, ?2, ?2, '1'
                 )",
                params![setup.item, reused_pid],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at,
                    claim_token, claim_external
                 ) VALUES (
                    'j_null', ?1, 'Null', '', 'implementing', 1, 3, 'abc', '2026-11-25T00:00:00Z',
                    'tok-null', 0
                 )",
                params![setup.item],
            )
            .unwrap();
        }

        claim::empty_spawned_groups(&setup.db, &setup.item).unwrap();
        assert!(
            proc_state(spawned.pid).is_none(),
            "spawned leader still live"
        );
        assert!(proc_state(gpid).is_none(), "grandchild still live");
        assert!(group_empty(spawned.pgid).unwrap());
        assert!(still_running(ext_pid), "external pid was signaled");
        assert!(still_running(reused_pid), "reused pid was signaled");
        assert!(!group_empty(reused_pid).unwrap());

        {
            let mut conn = setup.db.lock().unwrap();
            state::drop_item(&mut conn, &setup.item, true).unwrap();
        }
        let state_name: String = setup
            .db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![setup.item],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state_name, "dropped");
        assert!(still_running(ext_pid));
        assert!(still_running(reused_pid));
    }

    struct ChildHold(Option<std::process::Child>);

    impl Drop for ChildHold {
        fn drop(&mut self) {
            if let Some(child) = self.0.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    struct ClearHook;

    impl Drop for ClearHook {
        fn drop(&mut self) {
            claim::set_reap_before_read(None);
        }
    }

    struct SlotGuard(Arc<Mutex<Option<std::process::Child>>>);

    impl Drop for SlotGuard {
        fn drop(&mut self) {
            if let Some(mut child) = self.0.lock().unwrap().take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}
