//! Item and job edges. Callers pass git and process facts in; this module only
//! writes the row. A conflict error carries the current state (HTTP 409).

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::db::{Error, Result};

const BODY_MAX: usize = 32 * 1024;
const TITLE_MAX_CHARS: usize = 120;
const DOC_MAX: usize = 256 * 1024;
const MAX_LEASES: i64 = 4;

const CLEAR_CLAIM_COLS: &str = "\
claim_worker = NULL, claim_token = NULL, claim_role = NULL, claim_external = 0, \
claim_pid = NULL, claim_pgid = NULL, claim_starttime = NULL, lease_until = NULL";

const CLEAR_JOB_LEASE_COLS: &str = "\
blocked_reason = NULL, pending_permit = NULL, \
claim_worker = NULL, claim_token = NULL, claim_role = NULL, claim_external = 0, \
claim_pid = NULL, claim_pgid = NULL, claim_starttime = NULL, lease_until = NULL";

const SET_CLAIM_COLS: &str = "\
claim_worker = ?3, claim_token = ?4, claim_role = ?5, claim_external = ?6, \
claim_pid = ?7, claim_pgid = ?8, claim_starttime = ?9, lease_until = ?10";

const HUMAN: &str = "human";
const DISPATCHER: &str = "dispatcher";
const WORKER: &str = "worker";
const REAPER: &str = "reaper";
const DAEMON: &str = "daemon";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ItemState {
    Inbox,
    Planning,
    AwaitingPlanReview,
    RevisionRequested,
    Revising,
    AwaitingImplementation,
    Implementing,
    AwaitingMerge,
    MergeConflict,
    Merged,
    Dropped,
}

impl ItemState {
    fn as_str(self) -> &'static str {
        match self {
            ItemState::Inbox => "inbox",
            ItemState::Planning => "planning",
            ItemState::AwaitingPlanReview => "awaiting_plan_review",
            ItemState::RevisionRequested => "revision_requested",
            ItemState::Revising => "revising",
            ItemState::AwaitingImplementation => "awaiting_implementation",
            ItemState::Implementing => "implementing",
            ItemState::AwaitingMerge => "awaiting_merge",
            ItemState::MergeConflict => "merge_conflict",
            ItemState::Merged => "merged",
            ItemState::Dropped => "dropped",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "inbox" => ItemState::Inbox,
            "planning" => ItemState::Planning,
            "awaiting_plan_review" => ItemState::AwaitingPlanReview,
            "revision_requested" => ItemState::RevisionRequested,
            "revising" => ItemState::Revising,
            "awaiting_implementation" => ItemState::AwaitingImplementation,
            "implementing" => ItemState::Implementing,
            "awaiting_merge" => ItemState::AwaitingMerge,
            "merge_conflict" => ItemState::MergeConflict,
            "merged" => ItemState::Merged,
            "dropped" => ItemState::Dropped,
            other => {
                return Err(Error::Invalid(format!("unknown item state {other}")));
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobState {
    Queued,
    Implementing,
    ReadyForReview,
    InReview,
    FeedbackPending,
    Accepted,
    Cancelled,
}

impl JobState {
    fn as_str(self) -> &'static str {
        match self {
            JobState::Queued => "queued",
            JobState::Implementing => "implementing",
            JobState::ReadyForReview => "ready_for_review",
            JobState::InReview => "in_review",
            JobState::FeedbackPending => "feedback_pending",
            JobState::Accepted => "accepted",
            JobState::Cancelled => "cancelled",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        Ok(match value {
            "queued" => JobState::Queued,
            "implementing" => JobState::Implementing,
            "ready_for_review" => JobState::ReadyForReview,
            "in_review" => JobState::InReview,
            "feedback_pending" => JobState::FeedbackPending,
            "accepted" => JobState::Accepted,
            "cancelled" => JobState::Cancelled,
            other => {
                return Err(Error::Invalid(format!("unknown job state {other}")));
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Plan,
    Revise,
    Implement,
    Review,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Plan => "plan",
            Role::Revise => "revise",
            Role::Implement => "implement",
            Role::Review => "review",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseCause {
    Human,
    Expiry,
}

/// One release or expiry. The pid fact, the optional MCP token, and the row
/// update run in the same immediate transaction.
#[derive(Clone, Debug)]
pub struct Release {
    pub cause: ReleaseCause,
    /// Human `keel release --force` for a live external pid. MCP has no force.
    pub force: bool,
    /// MCP release sends the token. A human release may omit it.
    pub claim_token: Option<String>,
    /// Caller observed the recorded pid is dead, or its starttime no longer matches.
    /// For a spawned child this is `kill(-pgid, 0) == ESRCH`.
    pub pid_gone: bool,
}

impl Release {
    pub fn human() -> Self {
        Self {
            cause: ReleaseCause::Human,
            force: false,
            claim_token: None,
            pid_gone: false,
        }
    }

    pub fn expiry(pid_gone: bool) -> Self {
        Self {
            cause: ReleaseCause::Expiry,
            force: false,
            claim_token: None,
            pid_gone,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictKind {
    Integrate,
    Primary,
}

impl ConflictKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ConflictKind::Integrate => "integrate",
            ConflictKind::Primary => "primary",
        }
    }
}

/// One captured request. Callers normalize into this; they do not write SQLite.
#[derive(Debug, Clone)]
pub struct RawIntake {
    pub source: String,
    pub external_id: Option<String>,
    pub title: String,
    pub body: String,
    pub repo: Option<PathBuf>,
    pub target_branch: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClaimSpec {
    pub worker_id: String,
    pub role: Role,
    /// `true` when an MCP client owns the pid. The daemon does not signal it.
    pub external: bool,
    pub pid: Option<i64>,
    pub pgid: Option<i64>,
    pub starttime: Option<String>,
}

impl ClaimSpec {
    pub fn daemon(worker_id: impl Into<String>, role: Role) -> Self {
        Self {
            worker_id: worker_id.into(),
            role,
            external: false,
            pid: None,
            pgid: None,
            starttime: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    pub token: String,
    pub worker_id: String,
    pub role: Role,
    pub lease_until: String,
}

/// Detached plan, revise, or review worktree, as observed by the caller.
#[derive(Debug, Clone, Copy)]
pub struct DetachedWorktree {
    pub clean: bool,
    pub head_unchanged: bool,
}

impl DetachedWorktree {
    pub fn unchanged() -> Self {
        Self {
            clean: true,
            head_unchanged: true,
        }
    }
}

/// Implement worktree facts. `head_is_base` means HEAD still equals `base_sha`.
#[derive(Debug, Clone, Copy)]
pub struct ImplementWorktree {
    pub clean: bool,
    pub on_branch: bool,
    pub head_is_base: bool,
}

impl ImplementWorktree {
    pub fn ready() -> Self {
        Self {
            clean: true,
            on_branch: true,
            head_is_base: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanJob {
    pub title: String,
    pub acceptance: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanDocument {
    pub summary: String,
    pub jobs: Vec<PlanJob>,
    pub resolved_point_ids: Vec<String>,
}

struct Item {
    title: String,
    state: ItemState,
    repo: Option<String>,
    target_branch: Option<String>,
    plan_json: Option<String>,
    claim_token: Option<String>,
    claim_external: bool,
    claim_pid: Option<i64>,
}

struct Job {
    item_id: String,
    state: JobState,
    claim_token: Option<String>,
    claim_external: bool,
    claim_pid: Option<i64>,
    implementer_worker: Option<String>,
    blocked_reason: Option<String>,
}

pub fn create_item(conn: &mut Connection, raw: RawIntake) -> Result<String> {
    let title = normalize_title(&raw.title)?;
    if raw.body.len() > BODY_MAX {
        return Err(Error::Invalid("body exceeds 32 KiB".into()));
    }
    if raw.source.is_empty() {
        return Err(Error::Invalid("source is empty".into()));
    }
    if raw.external_id.as_deref().is_some_and(str::is_empty) {
        return Err(Error::Invalid("external_id is empty".into()));
    }
    let repo = match &raw.repo {
        Some(path) => Some(path_to_utf8(path)?),
        None => None,
    };
    let target_branch = raw
        .target_branch
        .as_deref()
        .map(str::trim)
        .filter(|branch| !branch.is_empty())
        .map(str::to_string);
    with_tx(conn, |tx| {
        if let Some(external_id) = &raw.external_id {
            if let Some(existing) = find_external(tx, &raw.source, external_id)? {
                return Ok(existing);
            }
        }
        let id = new_id("k")?;
        let now = sql_now(tx)?;
        tx.execute(
            "INSERT INTO items (
                id, title, body, source, external_id, repo, target_branch,
                state, plan_rev, summary, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'inbox', 0, '', ?8, ?8)",
            params![
                id,
                title,
                raw.body,
                raw.source,
                raw.external_id,
                repo,
                target_branch,
                now
            ],
        )?;
        event(
            tx,
            &now,
            HUMAN,
            Some(&id),
            None,
            "create",
            None,
            Some(ItemState::Inbox.as_str()),
            None,
        )?;
        Ok(id)
    })
}

pub fn claim_item(conn: &mut Connection, item_id: &str, spec: ClaimSpec) -> Result<Claim> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        ensure_can_claim(tx, item.claim_token.as_deref(), None, item.state.as_str())?;
        let next = match (spec.role, item.state) {
            (Role::Plan, ItemState::Inbox) => ItemState::Planning,
            (Role::Revise, ItemState::RevisionRequested) => ItemState::Revising,
            (Role::Plan | Role::Revise, _) => {
                return Err(conflict(item.state, "claim role does not match item state"));
            }
            (Role::Implement | Role::Review, _) => {
                return Err(Error::Invalid(
                    "item claim role must be plan or revise".into(),
                ));
            }
        };
        let now = sql_now(tx)?;
        let claim = write_item_claim(tx, item_id, &spec, next, &now)?;
        event(
            tx,
            &now,
            DISPATCHER,
            Some(item_id),
            None,
            "claim",
            Some(item.state.as_str()),
            Some(next.as_str()),
            Some(spec.role.as_str()),
        )?;
        Ok(claim)
    })
}

pub fn claim_job(conn: &mut Connection, job_id: &str, spec: ClaimSpec) -> Result<Claim> {
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        let item = load_item(tx, &job.item_id)?;
        if is_container(tx, job_id)? {
            return Err(conflict(job.state, "container jobs are not claimed"));
        }
        // Dirty and a held token are the reasons a later claim is refused,
        // including while the row is still `implementing`.
        ensure_can_claim(
            tx,
            job.claim_token.as_deref(),
            job.blocked_reason.as_deref(),
            job.state.as_str(),
        )?;
        let next = match spec.role {
            Role::Implement => {
                if job.state != JobState::Queued {
                    return Err(conflict(job.state, "implement claim requires queued"));
                }
                match item.state {
                    ItemState::AwaitingImplementation | ItemState::Implementing => {}
                    _ => {
                        return Err(conflict(
                            item.state,
                            "item is not ready for an implement claim",
                        ));
                    }
                }
                JobState::Implementing
            }
            Role::Review => {
                if job.state != JobState::ReadyForReview {
                    return Err(conflict(
                        job.state,
                        "review claim requires ready_for_review",
                    ));
                }
                if item.state != ItemState::Implementing {
                    return Err(conflict(item.state, "item is not implementing"));
                }
                match &job.implementer_worker {
                    Some(implementer) if implementer == &spec.worker_id => {
                        return Err(conflict(
                            job.state,
                            "reviewer worker must differ from implementer_worker",
                        ));
                    }
                    Some(_) => {}
                    None => {
                        return Err(conflict(job.state, "implementer_worker is unset"));
                    }
                }
                JobState::InReview
            }
            Role::Plan | Role::Revise => {
                return Err(Error::Invalid(
                    "job claim role must be implement or review".into(),
                ));
            }
        };
        let now = sql_now(tx)?;
        let claim = insert_claim_fields(tx, &spec)?;
        let implementer = if spec.role == Role::Implement {
            Some(spec.worker_id.as_str())
        } else {
            None
        };
        let reviewer = if spec.role == Role::Review {
            Some(spec.worker_id.as_str())
        } else {
            None
        };
        let sql = format!(
            "UPDATE jobs SET
                state = ?1,
                updated_at = ?2,
                {SET_CLAIM_COLS},
                blocked_reason = NULL,
                pending_permit = NULL,
                implementer_worker = COALESCE(?11, implementer_worker),
                reviewer_worker = COALESCE(?12, reviewer_worker)
             WHERE id = ?13"
        );
        tx.execute(
            &sql,
            params![
                next.as_str(),
                now,
                claim.worker_id,
                claim.token,
                spec.role.as_str(),
                if spec.external { 1 } else { 0 },
                spec.pid,
                if spec.external { None } else { spec.pgid },
                spec.starttime,
                claim.lease_until,
                implementer,
                reviewer,
                job_id,
            ],
        )?;
        if spec.role == Role::Implement && item.state == ItemState::AwaitingImplementation {
            set_item_state(tx, &job.item_id, ItemState::Implementing, &now)?;
            event(
                tx,
                &now,
                DAEMON,
                Some(&job.item_id),
                Some(job_id),
                "implementing",
                Some(item.state.as_str()),
                Some(ItemState::Implementing.as_str()),
                None,
            )?;
        }
        event(
            tx,
            &now,
            DISPATCHER,
            Some(&job.item_id),
            Some(job_id),
            "claim",
            Some(job.state.as_str()),
            Some(next.as_str()),
            Some(spec.role.as_str()),
        )?;
        Ok(claim)
    })
}

pub fn release_item(conn: &mut Connection, item_id: &str, release: Release) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        let held = held_lease(
            item.claim_token.as_deref(),
            item.claim_external,
            item.claim_pid,
        )
        .ok_or_else(|| conflict(item.state, "item is not claimed"))?;
        let (actor, kind) = authorize_release(&held, &release, item.state)?;
        let open = open_point_count(tx, item_id)?;
        let next = match item.state {
            ItemState::Planning => ItemState::Inbox,
            ItemState::Revising if open > 0 => ItemState::RevisionRequested,
            ItemState::Revising => ItemState::AwaitingPlanReview,
            _ => {
                return Err(conflict(
                    item.state,
                    "item cannot be released from this state",
                ))
            }
        };
        let now = sql_now(tx)?;
        clear_item_claim(tx, item_id, next, &now, Some(held.token.as_str()))?;
        event(
            tx,
            &now,
            actor,
            Some(item_id),
            None,
            kind,
            Some(item.state.as_str()),
            Some(next.as_str()),
            None,
        )?;
        Ok(())
    })
}

pub fn release_job(conn: &mut Connection, job_id: &str, release: Release) -> Result<()> {
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        let held = held_lease(
            job.claim_token.as_deref(),
            job.claim_external,
            job.claim_pid,
        )
        .ok_or_else(|| conflict(job.state, "job is not claimed"))?;
        let (actor, kind) = authorize_release(&held, &release, job.state)?;
        let next = match job.state {
            JobState::Implementing => JobState::Queued,
            JobState::InReview => JobState::ReadyForReview,
            _ => {
                return Err(conflict(
                    job.state,
                    "job cannot be released from this state",
                ))
            }
        };
        let now = sql_now(tx)?;
        clear_job_claim(tx, job_id, next, &now, Some(held.token.as_str()))?;
        event(
            tx,
            &now,
            actor,
            Some(&job.item_id),
            Some(job_id),
            kind,
            Some(job.state.as_str()),
            Some(next.as_str()),
            None,
        )?;
        Ok(())
    })
}

/// Fills pid, pgid, and starttime on the unsupervised non-external row.
/// Does not rotate the token or slide `lease_until`.
pub fn attach_process(
    conn: &mut Connection,
    id: &str,
    claim_token: &str,
    pid: i64,
    pgid: i64,
    starttime: &str,
) -> Result<()> {
    if pid <= 0 || pgid <= 0 || pid != pgid || starttime.is_empty() {
        return Err(Error::Invalid(
            "pid and pgid must be equal and positive, with starttime".into(),
        ));
    }
    with_tx(conn, |tx| {
        let item_rows = tx.execute(
            "UPDATE items SET claim_pid = ?1, claim_pgid = ?2, claim_starttime = ?3
             WHERE id = ?4 AND claim_token = ?5 AND claim_external = 0 AND claim_pid IS NULL",
            params![pid, pgid, starttime, id, claim_token],
        )?;
        let job_rows = tx.execute(
            "UPDATE jobs SET claim_pid = ?1, claim_pgid = ?2, claim_starttime = ?3
             WHERE id = ?4 AND claim_token = ?5 AND claim_external = 0 AND claim_pid IS NULL",
            params![pid, pgid, starttime, id, claim_token],
        )?;
        if item_rows + job_rows == 1 {
            return Ok(());
        }
        if item_rows + job_rows > 1 {
            return Err(conflict(id, "id matches both an item and a job"));
        }
        match load_item(tx, id) {
            Ok(item) => {
                return Err(attach_miss(
                    item.state,
                    item.claim_token.as_deref(),
                    item.claim_external,
                    item.claim_pid,
                    claim_token,
                ));
            }
            Err(Error::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        match load_job(tx, id) {
            Ok(job) => {
                return Err(attach_miss(
                    job.state,
                    job.claim_token.as_deref(),
                    job.claim_external,
                    job.claim_pid,
                    claim_token,
                ));
            }
            Err(Error::NotFound(_)) => {}
            Err(err) => return Err(err),
        }
        Err(Error::NotFound(id.to_string()))
    })
}

pub fn heartbeat_item(conn: &mut Connection, item_id: &str, claim_token: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let updated = tx.execute(
            "UPDATE items
             SET lease_until = strftime('%Y-%m-%dT%H:%M:%SZ','now','+900 seconds')
             WHERE id = ?1 AND claim_token = ?2",
            params![item_id, claim_token],
        )?;
        if updated != 1 {
            let item = load_item(tx, item_id)?;
            return Err(conflict(item.state, "stale or missing claim_token"));
        }
        Ok(())
    })
}

pub fn heartbeat_job(conn: &mut Connection, job_id: &str, claim_token: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let updated = tx.execute(
            "UPDATE jobs
             SET lease_until = strftime('%Y-%m-%dT%H:%M:%SZ','now','+900 seconds')
             WHERE id = ?1 AND claim_token = ?2",
            params![job_id, claim_token],
        )?;
        if updated != 1 {
            let job = load_job(tx, job_id)?;
            return Err(conflict(job.state, "stale or missing claim_token"));
        }
        Ok(())
    })
}

pub fn submit_plan(
    conn: &mut Connection,
    item_id: &str,
    claim_token: &str,
    document: &str,
    tree: DetachedWorktree,
) -> Result<()> {
    submit_item_document(
        conn,
        item_id,
        claim_token,
        document,
        tree,
        ItemState::Planning,
        "submit_plan",
    )
}

pub fn submit_revision(
    conn: &mut Connection,
    item_id: &str,
    claim_token: &str,
    document: &str,
    tree: DetachedWorktree,
) -> Result<()> {
    submit_item_document(
        conn,
        item_id,
        claim_token,
        document,
        tree,
        ItemState::Revising,
        "submit_revision",
    )
}

fn submit_item_document(
    conn: &mut Connection,
    item_id: &str,
    claim_token: &str,
    document: &str,
    tree: DetachedWorktree,
    expected: ItemState,
    kind: &'static str,
) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != expected {
            return Err(conflict(
                item.state,
                format!("submit requires {}", expected.as_str()),
            ));
        }
        match_token(item.claim_token.as_deref(), claim_token, item.state)?;
        check_detached(tree, item.state)?;
        let plan = validate_plan_json(document).map_err(|err| subjobs_conflict(item.state, err))?;
        let now = sql_now(tx)?;
        resolve_points(tx, item_id, &plan.resolved_point_ids, &now)?;
        let next = ItemState::AwaitingPlanReview;
        let sql = format!(
            "UPDATE items SET
                state = ?1,
                plan_rev = plan_rev + 1,
                summary = ?2,
                plan_json = ?3,
                updated_at = ?4,
                {CLEAR_CLAIM_COLS}
             WHERE id = ?5 AND claim_token = ?6"
        );
        let updated = tx.execute(
            &sql,
            params![
                next.as_str(),
                plan.summary,
                document,
                now,
                item_id,
                claim_token
            ],
        )?;
        if updated != 1 {
            return Err(conflict(item.state, "stale or missing claim_token"));
        }
        event(
            tx,
            &now,
            WORKER,
            Some(item_id),
            None,
            kind,
            Some(item.state.as_str()),
            Some(next.as_str()),
            None,
        )?;
        Ok(())
    })
}

/// Inserts one open point. Moves `awaiting_plan_review` to `revision_requested`.
/// Any other non-terminal state keeps its state and still records the point.
pub fn note(conn: &mut Connection, item_id: &str, text: &str) -> Result<String> {
    let text = text.trim();
    if text.is_empty() {
        return Err(Error::Invalid("note text is empty".into()));
    }
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if matches!(item.state, ItemState::Merged | ItemState::Dropped) {
            return Err(conflict(item.state, "cannot note a terminal item"));
        }
        let now = sql_now(tx)?;
        let id = new_id("p")?;
        tx.execute(
            "INSERT INTO open_points (id, item_id, text, status, created_at)
             VALUES (?1, ?2, ?3, 'open', ?4)",
            params![id, item_id, text, now],
        )?;
        let next = if item.state == ItemState::AwaitingPlanReview {
            ItemState::RevisionRequested
        } else {
            item.state
        };
        if next != item.state {
            set_item_state(tx, item_id, next, &now)?;
        } else {
            tx.execute(
                "UPDATE items SET updated_at = ?1 WHERE id = ?2",
                params![now, item_id],
            )?;
        }
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "note",
            Some(item.state.as_str()),
            Some(next.as_str()),
            Some(&id),
        )?;
        Ok(id)
    })
}

pub fn waive(conn: &mut Connection, item_id: &str, point_id: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if matches!(item.state, ItemState::Merged | ItemState::Dropped) {
            return Err(conflict(item.state, "cannot waive a terminal item"));
        }
        let status: Option<String> = tx
            .query_row(
                "SELECT status FROM open_points WHERE id = ?1 AND item_id = ?2",
                params![point_id, item_id],
                |row| row.get(0),
            )
            .optional()?;
        match status.as_deref() {
            Some("open") => {}
            Some(_) => return Err(conflict(item.state, "point is not open")),
            None => return Err(Error::NotFound(point_id.to_string())),
        }
        let now = sql_now(tx)?;
        tx.execute(
            "UPDATE open_points SET status = 'waived', resolved_at = ?1 WHERE id = ?2",
            params![now, point_id],
        )?;
        let open = open_point_count(tx, item_id)?;
        let next = if open == 0
            && item.state == ItemState::RevisionRequested
            && item.claim_token.is_none()
        {
            ItemState::AwaitingPlanReview
        } else {
            item.state
        };
        if next != item.state {
            set_item_state(tx, item_id, next, &now)?;
        } else {
            tx.execute(
                "UPDATE items SET updated_at = ?1 WHERE id = ?2",
                params![now, item_id],
            )?;
        }
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "waive",
            Some(item.state.as_str()),
            Some(next.as_str()),
            Some(point_id),
        )?;
        Ok(())
    })
}

/// Pins `base_sha` and inserts one leaf per plan job.
/// An empty `jobs` array becomes one leaf titled with the item title.
/// `base_sha` is resolved by the caller; this function does not run git.
pub fn approve(conn: &mut Connection, item_id: &str, base_sha: &str) -> Result<Vec<String>> {
    if base_sha.is_empty() {
        return Err(Error::Invalid("base_sha is empty".into()));
    }
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != ItemState::AwaitingPlanReview {
            return Err(conflict(
                item.state,
                "approve requires awaiting_plan_review",
            ));
        }
        if open_point_count(tx, item_id)? > 0 {
            return Err(conflict(item.state, "approve blocked by an open point"));
        }
        if item.repo.as_deref().unwrap_or("").is_empty()
            || item.target_branch.as_deref().unwrap_or("").is_empty()
        {
            return Err(conflict(item.state, "repo and target_branch must be set"));
        }
        let Some(plan_json) = item.plan_json.as_deref() else {
            return Err(conflict(item.state, "plan_json is missing"));
        };
        let plan =
            validate_plan_json(plan_json).map_err(|err| subjobs_conflict(item.state, err))?;
        let leaves: Vec<PlanJob> = if plan.jobs.is_empty() {
            vec![PlanJob {
                title: item.title.clone(),
                acceptance: String::new(),
            }]
        } else {
            plan.jobs
        };
        let now = sql_now(tx)?;
        let mut ids = Vec::with_capacity(leaves.len());
        for (ord, leaf) in leaves.iter().enumerate() {
            let job_id = new_id("j")?;
            tx.execute(
                "INSERT INTO jobs (
                    id, item_id, parent_job_id, title, acceptance, state, depth, ord,
                    base_sha, updated_at
                 ) VALUES (?1, ?2, NULL, ?3, ?4, 'queued', 1, ?5, ?6, ?7)",
                params![
                    job_id,
                    item_id,
                    leaf.title,
                    leaf.acceptance,
                    ord as i64,
                    base_sha,
                    now
                ],
            )?;
            ids.push(job_id);
        }
        tx.execute(
            "UPDATE items SET
                state = 'awaiting_implementation',
                base_sha = ?1,
                updated_at = ?2
             WHERE id = ?3",
            params![base_sha, now, item_id],
        )?;
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "approve",
            Some(item.state.as_str()),
            Some(ItemState::AwaitingImplementation.as_str()),
            Some(base_sha),
        )?;
        Ok(ids)
    })
}

pub fn submit_implement(
    conn: &mut Connection,
    job_id: &str,
    claim_token: &str,
    diff_ref: &str,
    tree: ImplementWorktree,
) -> Result<()> {
    if diff_ref.is_empty() {
        return Err(Error::Invalid("diff_ref is empty".into()));
    }
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::Implementing {
            return Err(conflict(
                job.state,
                "implement submit requires implementing",
            ));
        }
        match_token(job.claim_token.as_deref(), claim_token, job.state)?;
        if !tree.clean {
            return Err(conflict(job.state, "worktree is dirty"));
        }
        if !tree.on_branch {
            return Err(conflict(job.state, "HEAD is not on the job branch"));
        }
        if tree.head_is_base {
            return Err(conflict(job.state, "HEAD equals base_sha"));
        }
        let now = sql_now(tx)?;
        let next = JobState::ReadyForReview;
        let sql = format!(
            "UPDATE jobs SET
                state = ?1,
                diff_ref = ?2,
                updated_at = ?3,
                {CLEAR_JOB_LEASE_COLS}
             WHERE id = ?4 AND claim_token = ?5"
        );
        let updated = tx.execute(
            &sql,
            params![next.as_str(), diff_ref, now, job_id, claim_token],
        )?;
        if updated != 1 {
            return Err(conflict(job.state, "stale or missing claim_token"));
        }
        event(
            tx,
            &now,
            WORKER,
            Some(&job.item_id),
            Some(job_id),
            "submit_implement",
            Some(job.state.as_str()),
            Some(next.as_str()),
            Some(diff_ref),
        )?;
        Ok(())
    })
}

pub fn fail_implement(
    conn: &mut Connection,
    job_id: &str,
    claim_token: &str,
    dirty: bool,
) -> Result<()> {
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::Implementing {
            return Err(conflict(job.state, "failure requires implementing"));
        }
        match_token(job.claim_token.as_deref(), claim_token, job.state)?;
        let now = sql_now(tx)?;
        if dirty {
            let sql = format!(
                "UPDATE jobs SET
                    updated_at = ?1,
                    blocked_reason = 'dirty',
                    pending_permit = NULL,
                    {CLEAR_CLAIM_COLS}
                 WHERE id = ?2 AND claim_token = ?3"
            );
            let updated = tx.execute(&sql, params![now, job_id, claim_token])?;
            if updated != 1 {
                return Err(conflict(job.state, "stale or missing claim_token"));
            }
            event(
                tx,
                &now,
                WORKER,
                Some(&job.item_id),
                Some(job_id),
                "fail_dirty",
                Some(job.state.as_str()),
                Some(job.state.as_str()),
                Some("dirty"),
            )?;
        } else {
            clear_job_claim(tx, job_id, JobState::Queued, &now, Some(claim_token))?;
            event(
                tx,
                &now,
                WORKER,
                Some(&job.item_id),
                Some(job_id),
                "fail_clean",
                Some(job.state.as_str()),
                Some(JobState::Queued.as_str()),
                None,
            )?;
        }
        Ok(())
    })
}

pub fn retry_job(conn: &mut Connection, job_id: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::Implementing
            || job.claim_token.is_some()
            || job.blocked_reason.as_deref() != Some("dirty")
        {
            return Err(conflict(
                job.state,
                "retry requires implementing, unclaimed, blocked_reason=dirty",
            ));
        }
        let now = sql_now(tx)?;
        clear_job_claim(tx, job_id, JobState::Queued, &now, None)?;
        event(
            tx,
            &now,
            HUMAN,
            Some(&job.item_id),
            Some(job_id),
            "retry",
            Some(job.state.as_str()),
            Some(JobState::Queued.as_str()),
            None,
        )?;
        Ok(())
    })
}

pub fn submit_review(
    conn: &mut Connection,
    job_id: &str,
    claim_token: &str,
    review_json: &str,
    tree: DetachedWorktree,
) -> Result<()> {
    if review_json.len() > DOC_MAX {
        return Err(Error::Invalid("document exceeds 256 KiB".into()));
    }
    let review_value: serde_json::Value = serde_json::from_str(review_json)
        .map_err(|err| Error::Invalid(format!("review is not JSON: {err}")))?;
    if review_value.as_object().is_none() {
        return Err(Error::Invalid(
            "review document must be a JSON object".into(),
        ));
    }
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::InReview {
            return Err(conflict(job.state, "review submit requires in_review"));
        }
        match_token(job.claim_token.as_deref(), claim_token, job.state)?;
        check_detached(tree, job.state)?;
        let now = sql_now(tx)?;
        let next = JobState::FeedbackPending;
        let sql = format!(
            "UPDATE jobs SET
                state = ?1,
                review_rev = review_rev + 1,
                review_json = ?2,
                updated_at = ?3,
                {CLEAR_JOB_LEASE_COLS}
             WHERE id = ?4 AND claim_token = ?5"
        );
        let updated = tx.execute(
            &sql,
            params![next.as_str(), review_json, now, job_id, claim_token],
        )?;
        if updated != 1 {
            return Err(conflict(job.state, "stale or missing claim_token"));
        }
        event(
            tx,
            &now,
            WORKER,
            Some(&job.item_id),
            Some(job_id),
            "submit_review",
            Some(job.state.as_str()),
            Some(next.as_str()),
            None,
        )?;
        Ok(())
    })
}

pub fn accept_job(conn: &mut Connection, job_id: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::FeedbackPending {
            return Err(conflict(job.state, "accept requires feedback_pending"));
        }
        let now = sql_now(tx)?;
        set_job_state(tx, job_id, JobState::Accepted, &now)?;
        event(
            tx,
            &now,
            HUMAN,
            Some(&job.item_id),
            Some(job_id),
            "accept",
            Some(job.state.as_str()),
            Some(JobState::Accepted.as_str()),
            None,
        )?;
        promote_containers(tx, job_id, &now)?;
        promote_item_if_all_accepted(tx, &job.item_id, &now)?;
        Ok(())
    })
}

pub fn reject_job(conn: &mut Connection, job_id: &str, text: &str) -> Result<()> {
    let text = text.trim();
    if text.is_empty() {
        return Err(Error::Invalid("reject text is empty".into()));
    }
    with_tx(conn, |tx| {
        let job = load_job(tx, job_id)?;
        if job.state != JobState::FeedbackPending {
            return Err(conflict(job.state, "reject requires feedback_pending"));
        }
        let now = sql_now(tx)?;
        let note_id = new_id("n")?;
        tx.execute(
            "INSERT INTO job_notes (id, job_id, text, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![note_id, job_id, text, now],
        )?;
        set_job_state(tx, job_id, JobState::Queued, &now)?;
        event(
            tx,
            &now,
            HUMAN,
            Some(&job.item_id),
            Some(job_id),
            "reject",
            Some(job.state.as_str()),
            Some(JobState::Queued.as_str()),
            Some(&note_id),
        )?;
        Ok(())
    })
}

pub fn drop_item(conn: &mut Connection, item_id: &str, force: bool) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if matches!(item.state, ItemState::Merged | ItemState::Dropped) {
            return Err(conflict(item.state, "terminal items cannot be dropped"));
        }
        let jobs = job_flags(tx, item_id)?;
        let needs_force = item.claim_token.is_some()
            || jobs.iter().any(|job| {
                job.claimed || matches!(job.state, JobState::Implementing | JobState::InReview)
            })
            || matches!(
                item.state,
                ItemState::Planning | ItemState::Revising | ItemState::Implementing
            );
        if needs_force && !force {
            return Err(conflict(item.state, "drop requires --force"));
        }
        let now = sql_now(tx)?;
        for job in &jobs {
            if job.state == JobState::Cancelled {
                continue;
            }
            clear_job_claim(tx, &job.id, JobState::Cancelled, &now, None)?;
            event(
                tx,
                &now,
                HUMAN,
                Some(item_id),
                Some(&job.id),
                "cancel",
                Some(job.state.as_str()),
                Some(JobState::Cancelled.as_str()),
                None,
            )?;
        }
        clear_item_claim(tx, item_id, ItemState::Dropped, &now, None)?;
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "drop",
            Some(item.state.as_str()),
            Some(ItemState::Dropped.as_str()),
            if force { Some("force") } else { None },
        )?;
        Ok(())
    })
}

pub fn merge_item(conn: &mut Connection, item_id: &str, merged_sha: &str) -> Result<()> {
    if merged_sha.is_empty() {
        return Err(Error::Invalid("merged_sha is empty".into()));
    }
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != ItemState::AwaitingMerge {
            return Err(conflict(item.state, "merge requires awaiting_merge"));
        }
        let now = sql_now(tx)?;
        tx.execute(
            "UPDATE items SET
                state = 'merged',
                merged_sha = ?1,
                conflict_kind = NULL,
                updated_at = ?2
             WHERE id = ?3",
            params![merged_sha, now, item_id],
        )?;
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "merge",
            Some(item.state.as_str()),
            Some(ItemState::Merged.as_str()),
            Some(merged_sha),
        )?;
        Ok(())
    })
}

pub fn mark_merge_conflict(conn: &mut Connection, item_id: &str, kind: ConflictKind) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != ItemState::AwaitingMerge {
            return Err(conflict(
                item.state,
                "merge conflict requires awaiting_merge",
            ));
        }
        let now = sql_now(tx)?;
        tx.execute(
            "UPDATE items SET state = 'merge_conflict', conflict_kind = ?1, updated_at = ?2
             WHERE id = ?3",
            params![kind.as_str(), now, item_id],
        )?;
        event(
            tx,
            &now,
            DAEMON,
            Some(item_id),
            None,
            "merge_conflict",
            Some(item.state.as_str()),
            Some(ItemState::MergeConflict.as_str()),
            Some(kind.as_str()),
        )?;
        Ok(())
    })
}

pub fn merge_continue(conn: &mut Connection, item_id: &str, merged_sha: &str) -> Result<()> {
    if merged_sha.is_empty() {
        return Err(Error::Invalid("merged_sha is empty".into()));
    }
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != ItemState::MergeConflict {
            return Err(conflict(
                item.state,
                "merge --continue requires merge_conflict",
            ));
        }
        let now = sql_now(tx)?;
        tx.execute(
            "UPDATE items SET
                state = 'merged',
                merged_sha = ?1,
                conflict_kind = NULL,
                updated_at = ?2
             WHERE id = ?3",
            params![merged_sha, now, item_id],
        )?;
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "merge_continue",
            Some(item.state.as_str()),
            Some(ItemState::Merged.as_str()),
            Some(merged_sha),
        )?;
        Ok(())
    })
}

pub fn merge_abort(conn: &mut Connection, item_id: &str) -> Result<()> {
    with_tx(conn, |tx| {
        let item = load_item(tx, item_id)?;
        if item.state != ItemState::MergeConflict {
            return Err(conflict(
                item.state,
                "merge --abort requires merge_conflict",
            ));
        }
        let now = sql_now(tx)?;
        tx.execute(
            "UPDATE items SET
                state = 'awaiting_merge',
                integrate_sha = NULL,
                integrate_ord = NULL,
                conflict_kind = NULL,
                updated_at = ?1
             WHERE id = ?2",
            params![now, item_id],
        )?;
        event(
            tx,
            &now,
            HUMAN,
            Some(item_id),
            None,
            "merge_abort",
            Some(item.state.as_str()),
            Some(ItemState::AwaitingMerge.as_str()),
            None,
        )?;
        Ok(())
    })
}

pub fn validate_plan_json(document: &str) -> Result<PlanDocument> {
    if document.len() > DOC_MAX {
        return Err(Error::Invalid("document exceeds 256 KiB".into()));
    }
    let value: serde_json::Value = serde_json::from_str(document)
        .map_err(|err| Error::Invalid(format!("plan is not JSON: {err}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| Error::Invalid("plan must be a JSON object".into()))?;
    let summary = match object.get("summary") {
        None => String::new(),
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(_) => return Err(Error::Invalid("summary must be a string".into())),
    };
    let jobs_value = object.get("jobs");
    let jobs = match jobs_value {
        None => Vec::new(),
        Some(serde_json::Value::Array(entries)) => {
            let mut jobs = Vec::with_capacity(entries.len());
            for entry in entries {
                let job = entry
                    .as_object()
                    .ok_or_else(|| Error::Invalid("job must be an object".into()))?;
                match job.get("subjobs") {
                    None => {}
                    Some(serde_json::Value::Array(nested)) if nested.is_empty() => {}
                    Some(serde_json::Value::Array(_)) => {
                        return Err(Error::Invalid(
                            "plan jobs contain a non-empty subjobs array".into(),
                        ));
                    }
                    Some(_) => return Err(Error::Invalid("subjobs must be an array".into())),
                }
                let title = job
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .filter(|title| !title.is_empty())
                    .ok_or_else(|| Error::Invalid("job title is required".into()))?;
                let acceptance = match job.get("acceptance") {
                    None => String::new(),
                    Some(serde_json::Value::String(text)) => text.clone(),
                    Some(_) => {
                        return Err(Error::Invalid("acceptance must be a string".into()));
                    }
                };
                jobs.push(PlanJob {
                    title: title.to_string(),
                    acceptance,
                });
            }
            jobs
        }
        Some(_) => return Err(Error::Invalid("jobs must be an array".into())),
    };
    let mut resolved_point_ids = Vec::new();
    if let Some(ids) = object.get("resolved_point_ids") {
        let list = ids
            .as_array()
            .ok_or_else(|| Error::Invalid("resolved_point_ids must be an array".into()))?;
        for id in list {
            let id = id
                .as_str()
                .ok_or_else(|| Error::Invalid("resolved point id must be a string".into()))?;
            if resolved_point_ids.iter().any(|existing| existing == id) {
                return Err(Error::Invalid(format!("duplicate resolved point {id}")));
            }
            resolved_point_ids.push(id.to_string());
        }
    }
    Ok(PlanDocument {
        summary,
        jobs,
        resolved_point_ids,
    })
}

fn subjobs_conflict(state: impl AsState, err: Error) -> Error {
    match err {
        Error::Invalid(message) if message.contains("non-empty subjobs") => {
            conflict(state, message)
        }
        other => other,
    }
}

fn resolve_points(tx: &Transaction, item_id: &str, ids: &[String], now: &str) -> Result<()> {
    for id in ids {
        let updated = tx.execute(
            "UPDATE open_points
             SET status = 'resolved', resolved_at = ?1
             WHERE id = ?2 AND item_id = ?3 AND status = 'open'",
            params![now, id, item_id],
        )?;
        if updated != 1 {
            return Err(Error::Invalid(format!("resolved point {id} is not open")));
        }
    }
    Ok(())
}

struct JobFlag {
    id: String,
    state: JobState,
    claimed: bool,
}

fn job_flags(tx: &Transaction, item_id: &str) -> Result<Vec<JobFlag>> {
    let mut stmt = tx.prepare(
        "SELECT id, state, claim_token IS NOT NULL FROM jobs WHERE item_id = ?1 ORDER BY ord",
    )?;
    let rows = stmt.query_map(params![item_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    let mut jobs = Vec::new();
    for row in rows {
        let (id, state, claimed) = row?;
        jobs.push(JobFlag {
            id,
            state: JobState::parse(&state)?,
            claimed: claimed != 0,
        });
    }
    Ok(jobs)
}

fn promote_containers(tx: &Transaction, job_id: &str, now: &str) -> Result<()> {
    // Product depth is 2. The cap only stops a corrupt parent cycle.
    let mut current = job_id.to_string();
    for _ in 0..8 {
        let parent: Option<String> = tx
            .query_row(
                "SELECT parent_job_id FROM jobs WHERE id = ?1",
                params![current],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let Some(parent) = parent else {
            return Ok(());
        };
        let pending: i64 = tx.query_row(
            "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1 AND state != 'accepted'",
            params![parent],
            |row| row.get(0),
        )?;
        if pending != 0 {
            return Ok(());
        }
        let parent_job = load_job(tx, &parent)?;
        if parent_job.state != JobState::Queued {
            return Ok(());
        }
        clear_job_claim(tx, &parent, JobState::Accepted, now, None)?;
        event(
            tx,
            now,
            DAEMON,
            Some(&parent_job.item_id),
            Some(&parent),
            "accept",
            Some(parent_job.state.as_str()),
            Some(JobState::Accepted.as_str()),
            Some("container"),
        )?;
        current = parent;
    }
    Ok(())
}

fn promote_item_if_all_accepted(tx: &Transaction, item_id: &str, now: &str) -> Result<()> {
    let total: i64 = tx.query_row(
        "SELECT COUNT(*) FROM jobs WHERE item_id = ?1",
        params![item_id],
        |row| row.get(0),
    )?;
    let unaccepted: i64 = tx.query_row(
        "SELECT COUNT(*) FROM jobs WHERE item_id = ?1 AND state != 'accepted'",
        params![item_id],
        |row| row.get(0),
    )?;
    if total == 0 || unaccepted != 0 {
        return Ok(());
    }
    let item = load_item(tx, item_id)?;
    if item.state != ItemState::Implementing {
        return Ok(());
    }
    set_item_state(tx, item_id, ItemState::AwaitingMerge, now)?;
    event(
        tx,
        now,
        DAEMON,
        Some(item_id),
        None,
        "awaiting_merge",
        Some(item.state.as_str()),
        Some(ItemState::AwaitingMerge.as_str()),
        None,
    )?;
    Ok(())
}

fn ensure_can_claim(
    tx: &Transaction,
    existing_token: Option<&str>,
    blocked_reason: Option<&str>,
    state: &str,
) -> Result<()> {
    if existing_token.is_some() {
        // Includes the pre-spawn window where claim_pid is still null.
        return Err(conflict(state, "claim still held"));
    }
    if blocked_reason == Some("dirty") {
        return Err(conflict(state, "blocked_reason=dirty; retry first"));
    }
    let leases = lease_count(tx)?;
    if leases >= MAX_LEASES {
        return Err(conflict(state, "lease cap is 4"));
    }
    Ok(())
}

fn lease_count(tx: &Transaction) -> Result<i64> {
    let count: i64 = tx.query_row(
        "SELECT
            (SELECT COUNT(*) FROM items WHERE claim_token IS NOT NULL) +
            (SELECT COUNT(*) FROM jobs WHERE claim_token IS NOT NULL)",
        [],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn is_container(tx: &Transaction, job_id: &str) -> Result<bool> {
    let children: i64 = tx.query_row(
        "SELECT COUNT(*) FROM jobs WHERE parent_job_id = ?1",
        params![job_id],
        |row| row.get(0),
    )?;
    Ok(children > 0)
}

fn write_item_claim(
    tx: &Transaction,
    item_id: &str,
    spec: &ClaimSpec,
    next: ItemState,
    now: &str,
) -> Result<Claim> {
    let claim = insert_claim_fields(tx, spec)?;
    let sql =
        format!("UPDATE items SET state = ?1, updated_at = ?2, {SET_CLAIM_COLS} WHERE id = ?11");
    tx.execute(
        &sql,
        params![
            next.as_str(),
            now,
            claim.worker_id,
            claim.token,
            spec.role.as_str(),
            if spec.external { 1 } else { 0 },
            spec.pid,
            if spec.external { None } else { spec.pgid },
            spec.starttime,
            claim.lease_until,
            item_id,
        ],
    )?;
    Ok(claim)
}

fn insert_claim_fields(tx: &Transaction, spec: &ClaimSpec) -> Result<Claim> {
    check_claim_spec(spec)?;
    Ok(Claim {
        token: new_token()?,
        worker_id: spec.worker_id.clone(),
        role: spec.role,
        lease_until: sql_lease_until(tx)?,
    })
}

fn check_claim_spec(spec: &ClaimSpec) -> Result<()> {
    if spec.worker_id.is_empty() {
        return Err(Error::Invalid("worker_id is empty".into()));
    }
    if spec.external {
        if spec.pid.is_none_or(|pid| pid <= 0) {
            return Err(Error::Invalid(
                "external claim requires a positive pid".into(),
            ));
        }
        if spec.starttime.as_deref().unwrap_or("").is_empty() {
            return Err(Error::Invalid("external claim requires starttime".into()));
        }
        return Ok(());
    }
    let start_empty = spec.starttime.as_deref().unwrap_or("").is_empty();
    let unsupervised = spec.pid.is_none() && spec.pgid.is_none() && start_empty;
    let attached = matches!(
        (spec.pid, spec.pgid),
        (Some(pid), Some(pgid)) if pid > 0 && pid == pgid
    ) && !start_empty;
    if unsupervised || attached {
        return Ok(());
    }
    Err(Error::Invalid(
        "spawned claim must leave pid, pgid, and starttime unset, or set pid = pgid > 0 with starttime"
            .into(),
    ))
}

fn match_token(held: Option<&str>, given: &str, state: impl AsState) -> Result<()> {
    match held {
        Some(token) if token == given => Ok(()),
        _ => Err(conflict(state, "stale or missing claim_token")),
    }
}

fn check_detached(tree: DetachedWorktree, state: impl AsState) -> Result<()> {
    if !tree.clean {
        return Err(conflict(state, "detached worktree is dirty"));
    }
    if !tree.head_unchanged {
        return Err(conflict(state, "detached worktree HEAD moved"));
    }
    Ok(())
}

struct HeldLease {
    token: String,
    external: bool,
    pid: Option<i64>,
}

fn held_lease(token: Option<&str>, external: bool, pid: Option<i64>) -> Option<HeldLease> {
    token.map(|token| HeldLease {
        token: token.to_string(),
        external,
        pid,
    })
}

fn authorize_release(
    held: &HeldLease,
    release: &Release,
    state: impl AsState,
) -> Result<(&'static str, &'static str)> {
    if let Some(token) = &release.claim_token {
        if token != &held.token {
            return Err(conflict(state, "stale or missing claim_token"));
        }
    }
    match release.cause {
        ReleaseCause::Expiry => {
            if release.force {
                return Err(Error::Invalid("expiry has no --force".into()));
            }
            // A null pid is the pre-spawn window. Only a human release clears it.
            if held.pid.is_none() {
                return Err(conflict(state, "null claim_pid is not expired"));
            }
            if !release.pid_gone {
                return Err(conflict(state, "lease pid is still live"));
            }
            Ok((REAPER, "expire"))
        }
        ReleaseCause::Human if release.claim_token.is_some() => {
            if release.force {
                return Err(Error::Invalid("MCP release has no --force".into()));
            }
            if held.pid.is_some() && !release.pid_gone {
                return Err(conflict(state, "lease pid is still live"));
            }
            Ok(("mcp", "release"))
        }
        ReleaseCause::Human => {
            if held.external && !release.pid_gone && !release.force {
                return Err(conflict(
                    state,
                    "external pid still matches; release needs --force",
                ));
            }
            Ok((HUMAN, "release"))
        }
    }
}

fn attach_miss(
    state: impl AsState,
    token: Option<&str>,
    external: bool,
    pid: Option<i64>,
    claim_token: &str,
) -> Error {
    if token != Some(claim_token) {
        return conflict(state, "stale or missing claim_token");
    }
    if external {
        return conflict(state, "external lease is not attached");
    }
    if pid.is_some() {
        return conflict(state, "claim_pid is already set");
    }
    conflict(state, "claim is not the unsupervised window")
}

fn clear_item_claim(
    tx: &Transaction,
    item_id: &str,
    state: ItemState,
    now: &str,
    token: Option<&str>,
) -> Result<()> {
    let sql = if token.is_some() {
        format!(
            "UPDATE items SET state = ?1, updated_at = ?2, {CLEAR_CLAIM_COLS}
             WHERE id = ?3 AND claim_token = ?4"
        )
    } else {
        format!("UPDATE items SET state = ?1, updated_at = ?2, {CLEAR_CLAIM_COLS} WHERE id = ?3")
    };
    let updated = if let Some(token) = token {
        tx.execute(&sql, params![state.as_str(), now, item_id, token])?
    } else {
        tx.execute(&sql, params![state.as_str(), now, item_id])?
    };
    if updated != 1 {
        return Err(conflict(state, "stale or missing claim_token"));
    }
    Ok(())
}

fn clear_job_claim(
    tx: &Transaction,
    job_id: &str,
    state: JobState,
    now: &str,
    token: Option<&str>,
) -> Result<()> {
    let sql = if token.is_some() {
        format!(
            "UPDATE jobs SET state = ?1, updated_at = ?2, {CLEAR_JOB_LEASE_COLS}
             WHERE id = ?3 AND claim_token = ?4"
        )
    } else {
        format!("UPDATE jobs SET state = ?1, updated_at = ?2, {CLEAR_JOB_LEASE_COLS} WHERE id = ?3")
    };
    let updated = if let Some(token) = token {
        tx.execute(&sql, params![state.as_str(), now, job_id, token])?
    } else {
        tx.execute(&sql, params![state.as_str(), now, job_id])?
    };
    if updated != 1 {
        return Err(conflict(state, "stale or missing claim_token"));
    }
    Ok(())
}

fn set_item_state(tx: &Transaction, item_id: &str, state: ItemState, now: &str) -> Result<()> {
    tx.execute(
        "UPDATE items SET state = ?1, updated_at = ?2 WHERE id = ?3",
        params![state.as_str(), now, item_id],
    )?;
    Ok(())
}

fn set_job_state(tx: &Transaction, job_id: &str, state: JobState, now: &str) -> Result<()> {
    tx.execute(
        "UPDATE jobs SET state = ?1, updated_at = ?2 WHERE id = ?3",
        params![state.as_str(), now, job_id],
    )?;
    Ok(())
}

fn open_point_count(tx: &Transaction, item_id: &str) -> Result<i64> {
    let count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM open_points WHERE item_id = ?1 AND status = 'open'",
        params![item_id],
        |row| row.get(0),
    )?;
    Ok(count)
}

fn find_external(tx: &Transaction, source: &str, external_id: &str) -> Result<Option<String>> {
    let id = tx
        .query_row(
            "SELECT id FROM items WHERE source = ?1 AND external_id = ?2",
            params![source, external_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(id)
}

fn load_item(tx: &Transaction, item_id: &str) -> Result<Item> {
    let row = tx
        .query_row(
            "SELECT title, state, repo, target_branch, plan_json, claim_token,
                    claim_external, claim_pid
             FROM items WHERE id = ?1",
            params![item_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                ))
            },
        )
        .optional()?;
    let Some((
        title,
        state,
        repo,
        target_branch,
        plan_json,
        claim_token,
        claim_external,
        claim_pid,
    )) = row
    else {
        return Err(Error::NotFound(item_id.to_string()));
    };
    Ok(Item {
        title,
        state: ItemState::parse(&state)?,
        repo,
        target_branch,
        plan_json,
        claim_token,
        claim_external: claim_external != 0,
        claim_pid,
    })
}

fn load_job(tx: &Transaction, job_id: &str) -> Result<Job> {
    let row = tx
        .query_row(
            "SELECT item_id, state, claim_token, claim_external, claim_pid,
                    implementer_worker, blocked_reason
             FROM jobs WHERE id = ?1",
            params![job_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        item_id,
        state,
        claim_token,
        claim_external,
        claim_pid,
        implementer_worker,
        blocked_reason,
    )) = row
    else {
        return Err(Error::NotFound(job_id.to_string()));
    };
    Ok(Job {
        item_id,
        state: JobState::parse(&state)?,
        claim_token,
        claim_external: claim_external != 0,
        claim_pid,
        implementer_worker,
        blocked_reason,
    })
}

#[allow(clippy::too_many_arguments)]
fn event(
    tx: &Transaction,
    now: &str,
    actor: &str,
    item_id: Option<&str>,
    job_id: Option<&str>,
    kind: &str,
    from_state: Option<&str>,
    to_state: Option<&str>,
    detail: Option<&str>,
) -> Result<()> {
    tx.execute(
        "INSERT INTO events (at, actor, item_id, job_id, kind, from_state, to_state, detail)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![now, actor, item_id, job_id, kind, from_state, to_state, detail],
    )?;
    Ok(())
}

fn with_tx<T>(
    conn: &mut Connection,
    body: impl FnOnce(&Transaction<'_>) -> Result<T>,
) -> Result<T> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let value = body(&tx)?;
    tx.commit()?;
    Ok(value)
}

trait AsState {
    fn as_state(&self) -> String;
}

impl AsState for ItemState {
    fn as_state(&self) -> String {
        self.as_str().to_string()
    }
}

impl AsState for JobState {
    fn as_state(&self) -> String {
        self.as_str().to_string()
    }
}

impl AsState for &str {
    fn as_state(&self) -> String {
        (*self).to_string()
    }
}

fn conflict(state: impl AsState, message: impl Into<String>) -> Error {
    Error::Conflict {
        state: state.as_state(),
        message: message.into(),
    }
}

fn sql_now(tx: &Transaction) -> Result<String> {
    let now = tx.query_row("SELECT strftime('%Y-%m-%dT%H:%M:%SZ','now')", [], |row| {
        row.get(0)
    })?;
    Ok(now)
}

fn sql_lease_until(tx: &Transaction) -> Result<String> {
    let until = tx.query_row(
        "SELECT strftime('%Y-%m-%dT%H:%M:%SZ','now','+900 seconds')",
        [],
        |row| row.get(0),
    )?;
    Ok(until)
}

fn normalize_title(raw: &str) -> Result<String> {
    let line = raw.lines().next().unwrap_or("");
    let title = line.trim();
    if title.is_empty() {
        return Err(Error::Invalid("title is empty".into()));
    }
    if title.chars().count() > TITLE_MAX_CHARS {
        return Err(Error::Invalid("title exceeds 120 chars".into()));
    }
    Ok(title.to_string())
}

fn path_to_utf8(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_string)
        .ok_or_else(|| Error::Invalid("repo path is not utf-8".into()))
}

fn new_id(prefix: &str) -> Result<String> {
    let mut bytes = [0u8; 16];
    fill_random(&mut bytes)?;
    let mut id = String::with_capacity(prefix.len() + 1 + 32);
    id.push_str(prefix);
    id.push('_');
    push_hex(&mut id, &bytes);
    Ok(id)
}

fn new_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes)?;
    let mut token = String::with_capacity(64);
    push_hex(&mut token, &bytes);
    Ok(token)
}

fn fill_random(buf: &mut [u8]) -> Result<()> {
    let mut file = std::fs::File::open("/dev/urandom")?;
    std::io::Read::read_exact(&mut file, buf)?;
    Ok(())
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_in_memory;
    use rusqlite::params;

    const PLAN: &str = r#"{"summary":"One paragraph.","jobs":[{"title":"Pin the walk","acceptance":"doctor warns"}]}"#;
    const REVIEW: &str = r#"{"verdict":"comments"}"#;

    fn mem() -> Connection {
        open_in_memory().unwrap()
    }

    fn intake(repo: bool) -> RawIntake {
        RawIntake {
            source: "cli".into(),
            external_id: None,
            title: "Pin the walk".into(),
            body: "Say what happens at 400 files".into(),
            repo: repo.then(|| PathBuf::from("/tmp/repo")),
            target_branch: repo.then(|| "main".into()),
        }
    }

    fn item(conn: &mut Connection) -> String {
        create_item(conn, intake(true)).unwrap()
    }

    fn item_state(conn: &mut Connection, id: &str) -> String {
        conn.query_row(
            "SELECT state FROM items WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn job_state(conn: &mut Connection, id: &str) -> String {
        conn.query_row("SELECT state FROM jobs WHERE id = ?1", params![id], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn claim_token_item(conn: &mut Connection, id: &str) -> Option<String> {
        conn.query_row(
            "SELECT claim_token FROM items WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn claim_token_job(conn: &mut Connection, id: &str) -> Option<String> {
        conn.query_row(
            "SELECT claim_token FROM jobs WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn blocked(conn: &mut Connection, id: &str) -> Option<String> {
        conn.query_row(
            "SELECT blocked_reason FROM jobs WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn point_status(conn: &mut Connection, id: &str) -> String {
        conn.query_row(
            "SELECT status FROM open_points WHERE id = ?1",
            params![id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn conflict_of(err: Result<()>) -> (String, String) {
        match err {
            Err(Error::Conflict { state, message }) => (state, message),
            other => panic!("expected conflict, got {other:?}"),
        }
    }

    fn to_plan_review(conn: &mut Connection) -> String {
        let id = item(conn);
        let claim = claim_item(conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        submit_plan(conn, &id, &claim.token, PLAN, DetachedWorktree::unchanged()).unwrap();
        id
    }

    fn to_approved(conn: &mut Connection) -> (String, String) {
        let id = to_plan_review(conn);
        let jobs = approve(conn, &id, "abc123").unwrap();
        assert_eq!(jobs.len(), 1);
        (id, jobs[0].clone())
    }

    fn to_implementing(conn: &mut Connection) -> (String, String, Claim) {
        let (id, job) = to_approved(conn);
        let claim = claim_job(conn, &job, ClaimSpec::daemon("grok:impl", Role::Implement)).unwrap();
        (id, job, claim)
    }

    fn to_ready(conn: &mut Connection) -> (String, String) {
        let (id, job, claim) = to_implementing(conn);
        submit_implement(
            conn,
            &job,
            &claim.token,
            "def456",
            ImplementWorktree::ready(),
        )
        .unwrap();
        (id, job)
    }

    fn to_feedback(conn: &mut Connection) -> (String, String) {
        let (id, job) = to_ready(conn);
        let claim = claim_job(conn, &job, ClaimSpec::daemon("claude:rev", Role::Review)).unwrap();
        submit_review(
            conn,
            &job,
            &claim.token,
            REVIEW,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        (id, job)
    }

    fn to_awaiting_merge(conn: &mut Connection) -> (String, String) {
        let (id, job) = to_feedback(conn);
        accept_job(conn, &job).unwrap();
        (id, job)
    }

    #[test]
    fn create_lands_in_inbox_and_is_idempotent_per_external_id() {
        let mut conn = mem();
        let id = item(&mut conn);
        assert_eq!(item_state(&mut conn, &id), "inbox");
        let mut again = intake(true);
        again.external_id = None;
        let second = create_item(&mut conn, again).unwrap();
        assert_ne!(id, second);

        let mut keyed = intake(false);
        keyed.external_id = Some("ext-1".into());
        keyed.source = "cli".into();
        let first = create_item(&mut conn, keyed.clone()).unwrap();
        let repeat = create_item(&mut conn, keyed).unwrap();
        assert_eq!(first, repeat);

        let kinds: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT kind FROM events WHERE item_id = ?1 ORDER BY id")
                .unwrap();
            stmt.query_map(params![id], |row| row.get(0))
                .unwrap()
                .map(|row| row.unwrap())
                .collect()
        };
        assert_eq!(kinds, vec!["create".to_string()]);
    }

    #[test]
    fn intake_rejects_oversize_title_and_body() {
        let mut conn = mem();
        let mut raw = intake(true);
        raw.title = "x".repeat(121);
        assert!(matches!(
            create_item(&mut conn, raw),
            Err(Error::Invalid(_))
        ));
        let mut raw = intake(true);
        raw.title = "  first line only  \nand more".into();
        let id = create_item(&mut conn, raw).unwrap();
        let title: String = conn
            .query_row(
                "SELECT title FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(title, "first line only");
        let mut raw = intake(true);
        raw.body = "y".repeat(BODY_MAX + 1);
        assert!(matches!(
            create_item(&mut conn, raw),
            Err(Error::Invalid(_))
        ));
    }

    #[test]
    fn claim_plan_and_release_or_expiry_return_to_inbox() {
        let mut conn = mem();
        let id = item(&mut conn);
        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        assert_eq!(claim.role, Role::Plan);
        assert_eq!(claim.token.len(), 64);
        assert!(!claim.lease_until.is_empty());
        assert_eq!(item_state(&mut conn, &id), "planning");
        assert!(claim_token_item(&mut conn, &id).is_some());
        let (state, message) = conflict_of(
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:other", Role::Plan)).map(|_| ()),
        );
        assert_eq!(state, "planning");
        assert!(message.contains("claim"), "{message}");

        release_item(&mut conn, &id, Release::human()).unwrap();
        assert_eq!(item_state(&mut conn, &id), "inbox");
        assert!(claim_token_item(&mut conn, &id).is_none());

        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        assert!(!claim.token.is_empty());
        let (state, message) = conflict_of(release_item(&mut conn, &id, Release::expiry(true)));
        assert_eq!(state, "planning");
        assert!(message.contains("null claim_pid"), "{message}");
        assert_eq!(item_state(&mut conn, &id), "planning");
        release_item(&mut conn, &id, Release::human()).unwrap();
        assert_eq!(item_state(&mut conn, &id), "inbox");
        let (state, _) = conflict_of(release_item(&mut conn, &id, Release::human()));
        assert_eq!(state, "inbox");
    }

    #[test]
    fn submit_plan_stores_json_and_rejects_subjobs_and_a_dirty_tree() {
        let mut conn = mem();
        let id = item(&mut conn);
        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        let dirty = DetachedWorktree {
            clean: false,
            head_unchanged: true,
        };
        let (state, _) = conflict_of(submit_plan(&mut conn, &id, &claim.token, PLAN, dirty));
        assert_eq!(state, "planning");
        assert_eq!(item_state(&mut conn, &id), "planning");

        let moved = DetachedWorktree {
            clean: true,
            head_unchanged: false,
        };
        let (state, _) = conflict_of(submit_plan(&mut conn, &id, &claim.token, PLAN, moved));
        assert_eq!(state, "planning");

        let (state, _) = conflict_of(submit_plan(
            &mut conn,
            &id,
            "not-the-token",
            PLAN,
            DetachedWorktree::unchanged(),
        ));
        assert_eq!(state, "planning");

        let nested = r#"{"summary":"s","jobs":[{"title":"t","subjobs":[{"title":"child"}]}]}"#;
        let err = submit_plan(
            &mut conn,
            &id,
            &claim.token,
            nested,
            DetachedWorktree::unchanged(),
        )
        .unwrap_err();
        match err {
            Error::Conflict { state, message } => {
                assert_eq!(state, "planning");
                assert!(message.contains("subjobs"), "{message}");
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        assert_eq!(item_state(&mut conn, &id), "planning");

        let empty_sub = r#"{"summary":"s","jobs":[{"title":"t","acceptance":"a","subjobs":[]}]}"#;
        submit_plan(
            &mut conn,
            &id,
            &claim.token,
            empty_sub,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");
        let stored: String = conn
            .query_row(
                "SELECT plan_json FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, empty_sub);
        assert!(claim_token_item(&mut conn, &id).is_none());
        let rev: i64 = conn
            .query_row(
                "SELECT plan_rev FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rev, 1);
    }

    #[test]
    fn validate_plan_json_rejects_non_empty_subjobs() {
        let err = validate_plan_json(
            r#"{"jobs":[{"title":"a","subjobs":[{"title":"b","subjobs":[{"title":"c"}]}]}]}"#,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
        let doc = validate_plan_json(r#"{"summary":"s","jobs":[]}"#).unwrap();
        assert!(doc.jobs.is_empty());
        assert_eq!(doc.summary, "s");
        let err = validate_plan_json(r#"{"jobs":[{}]}"#).unwrap_err();
        assert!(matches!(err, Error::Invalid(_)));
        assert!(validate_plan_json(&"x".repeat(DOC_MAX + 1)).is_err());
    }

    #[test]
    fn note_and_waive_last_point_returns_without_a_revise() {
        let mut conn = mem();
        let id = to_plan_review(&mut conn);
        let first = note(&mut conn, &id, "Say what happens at 400 files").unwrap();
        assert_eq!(item_state(&mut conn, &id), "revision_requested");
        let second = note(&mut conn, &id, "Name the walk order").unwrap();
        assert_eq!(item_state(&mut conn, &id), "revision_requested");

        waive(&mut conn, &id, &first).unwrap();
        assert_eq!(point_status(&mut conn, &first), "waived");
        assert_eq!(item_state(&mut conn, &id), "revision_requested");

        waive(&mut conn, &id, &second).unwrap();
        assert_eq!(point_status(&mut conn, &second), "waived");
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");
        let (state, _) = conflict_of(
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).map(|_| ()),
        );
        assert_eq!(state, "awaiting_plan_review");
    }

    #[test]
    fn waive_while_revising_does_not_change_item_state() {
        let mut conn = mem();
        let id = to_plan_review(&mut conn);
        let point = note(&mut conn, &id, "one point").unwrap();
        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        assert_eq!(item_state(&mut conn, &id), "revising");
        waive(&mut conn, &id, &point).unwrap();
        assert_eq!(item_state(&mut conn, &id), "revising");
        assert_eq!(point_status(&mut conn, &point), "waived");
        assert!(claim_token_item(&mut conn, &id).is_some());
        let _ = claim;
    }

    #[test]
    fn revise_submit_always_returns_to_plan_review() {
        let mut conn = mem();
        let id = to_plan_review(&mut conn);
        let point = note(&mut conn, &id, "fix the cap").unwrap();
        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        let document = format!(
            r#"{{"summary":"revised","jobs":[{{"title":"Pin the walk","acceptance":"ok"}}],"resolved_point_ids":["{point}"]}}"#
        );
        submit_revision(
            &mut conn,
            &id,
            &claim.token,
            &document,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");
        assert_eq!(point_status(&mut conn, &point), "resolved");
        assert!(claim_token_item(&mut conn, &id).is_none());
    }

    #[test]
    fn release_and_expiry_while_revising_follow_open_points() {
        let mut conn = mem();
        let id = to_plan_review(&mut conn);
        let point = note(&mut conn, &id, "still open").unwrap();
        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        assert!(!claim.token.is_empty());
        release_item(&mut conn, &id, Release::human()).unwrap();
        assert_eq!(item_state(&mut conn, &id), "revision_requested");
        assert_eq!(point_status(&mut conn, &point), "open");
        assert!(claim_token_item(&mut conn, &id).is_none());

        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        let (state, _) = conflict_of(release_item(&mut conn, &id, Release::expiry(false)));
        assert_eq!(state, "revising");
        attach_process(&mut conn, &id, &claim.token, 40, 40, "start").unwrap();
        let (state, _) = conflict_of(release_item(&mut conn, &id, Release::expiry(false)));
        assert_eq!(state, "revising");
        release_item(&mut conn, &id, Release::expiry(true)).unwrap();
        assert_eq!(item_state(&mut conn, &id), "revision_requested");

        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        waive(&mut conn, &id, &point).unwrap();
        assert_eq!(item_state(&mut conn, &id), "revising");
        release_item(&mut conn, &id, Release::human()).unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");
        let _ = claim;

        let point = note(&mut conn, &id, "again").unwrap();
        let claim =
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        waive(&mut conn, &id, &point).unwrap();
        attach_process(&mut conn, &id, &claim.token, 41, 41, "start").unwrap();
        release_item(&mut conn, &id, Release::expiry(true)).unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");
    }

    #[test]
    fn approve_inserts_a_leaf_and_refuses_skips_open_points_and_subjobs() {
        let mut conn = mem();
        let bare = create_item(&mut conn, intake(false)).unwrap();
        let (state, _) = conflict_of(approve(&mut conn, &bare, "abc").map(|_| ()));
        assert_eq!(state, "inbox");

        let id = to_plan_review(&mut conn);
        conn.execute(
            "INSERT INTO open_points (id, item_id, text, status, created_at)
             VALUES ('p_manual', ?1, 'hidden', 'open', '2026-11-25T00:00:00Z')",
            params![id],
        )
        .unwrap();
        let (state, message) = conflict_of(approve(&mut conn, &id, "abc").map(|_| ()));
        assert_eq!(state, "awaiting_plan_review");
        assert!(message.contains("open"), "{message}");
        conn.execute(
            "UPDATE open_points SET status = 'waived' WHERE id = 'p_manual'",
            [],
        )
        .unwrap();

        let no_repo = create_item(&mut conn, intake(false)).unwrap();
        let claim = claim_item(
            &mut conn,
            &no_repo,
            ClaimSpec::daemon("grok:plan", Role::Plan),
        )
        .unwrap();
        submit_plan(
            &mut conn,
            &no_repo,
            &claim.token,
            PLAN,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        let (state, message) = conflict_of(approve(&mut conn, &no_repo, "abc").map(|_| ()));
        assert_eq!(state, "awaiting_plan_review");
        assert!(message.contains("repo"), "{message}");

        conn.execute(
            "UPDATE items SET plan_json = ?1 WHERE id = ?2",
            params![
                r#"{"jobs":[{"title":"t","subjobs":[{"title":"nested"}]}]}"#,
                id
            ],
        )
        .unwrap();
        let err = approve(&mut conn, &id, "abc").unwrap_err();
        assert!(matches!(err, Error::Conflict { .. }));
        assert_eq!(item_state(&mut conn, &id), "awaiting_plan_review");

        conn.execute(
            "UPDATE items SET plan_json = ?1 WHERE id = ?2",
            params![PLAN, id],
        )
        .unwrap();
        let jobs = approve(&mut conn, &id, "base-sha").unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(item_state(&mut conn, &id), "awaiting_implementation");
        assert_eq!(job_state(&mut conn, &jobs[0]), "queued");
        let (ord, base, depth, title): (i64, String, i64, String) = conn
            .query_row(
                "SELECT ord, base_sha, depth, title FROM jobs WHERE id = ?1",
                params![jobs[0]],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(ord, 0);
        assert_eq!(base, "base-sha");
        assert_eq!(depth, 1);
        assert_eq!(title, "Pin the walk");
        let item_base: String = conn
            .query_row(
                "SELECT base_sha FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(item_base, "base-sha");
    }

    #[test]
    fn empty_plan_jobs_become_one_leaf_titled_with_the_item() {
        let mut conn = mem();
        let id = item(&mut conn);
        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        submit_plan(
            &mut conn,
            &id,
            &claim.token,
            r#"{"summary":"no jobs","jobs":[]}"#,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        let jobs = approve(&mut conn, &id, "base").unwrap();
        let title: String = conn
            .query_row(
                "SELECT title FROM jobs WHERE id = ?1",
                params![jobs[0]],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(title, "Pin the walk");
    }

    #[test]
    fn two_leaves_claim_and_accept_move_the_item_once_each() {
        let mut conn = mem();
        let id = item(&mut conn);
        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        submit_plan(
            &mut conn,
            &id,
            &claim.token,
            r#"{"summary":"two","jobs":[{"title":"a","acceptance":""},{"title":"b","acceptance":"yes"}]}"#,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        let jobs = approve(&mut conn, &id, "base").unwrap();
        assert_eq!(jobs.len(), 2);
        let first = claim_job(
            &mut conn,
            &jobs[0],
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        assert_eq!(item_state(&mut conn, &id), "implementing");
        assert_eq!(job_state(&mut conn, &jobs[0]), "implementing");
        let second = claim_job(
            &mut conn,
            &jobs[1],
            ClaimSpec::daemon("grok:impl2", Role::Implement),
        )
        .unwrap();
        assert_eq!(item_state(&mut conn, &id), "implementing");
        submit_implement(
            &mut conn,
            &jobs[0],
            &first.token,
            "sha-a",
            ImplementWorktree::ready(),
        )
        .unwrap();
        submit_implement(
            &mut conn,
            &jobs[1],
            &second.token,
            "sha-b",
            ImplementWorktree::ready(),
        )
        .unwrap();
        let review_a = claim_job(
            &mut conn,
            &jobs[0],
            ClaimSpec::daemon("claude:r1", Role::Review),
        )
        .unwrap();
        let review_b = claim_job(
            &mut conn,
            &jobs[1],
            ClaimSpec::daemon("claude:r2", Role::Review),
        )
        .unwrap();
        submit_review(
            &mut conn,
            &jobs[0],
            &review_a.token,
            REVIEW,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        submit_review(
            &mut conn,
            &jobs[1],
            &review_b.token,
            REVIEW,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        accept_job(&mut conn, &jobs[0]).unwrap();
        assert_eq!(item_state(&mut conn, &id), "implementing");
        accept_job(&mut conn, &jobs[1]).unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_merge");
        assert_eq!(job_state(&mut conn, &jobs[0]), "accepted");
        assert_eq!(job_state(&mut conn, &jobs[1]), "accepted");
    }

    #[test]
    fn implement_submit_release_expiry_and_clean_failure_return_to_queued() {
        let mut conn = mem();
        let (id, job, claim) = to_implementing(&mut conn);
        assert_eq!(item_state(&mut conn, &id), "implementing");
        let dirty = ImplementWorktree {
            clean: false,
            on_branch: true,
            head_is_base: false,
        };
        let (state, _) = conflict_of(submit_implement(
            &mut conn,
            &job,
            &claim.token,
            "sha",
            dirty,
        ));
        assert_eq!(state, "implementing");
        assert_eq!(job_state(&mut conn, &job), "implementing");
        let off_branch = ImplementWorktree {
            clean: true,
            on_branch: false,
            head_is_base: false,
        };
        assert!(submit_implement(&mut conn, &job, &claim.token, "sha", off_branch).is_err());
        let same_base = ImplementWorktree {
            clean: true,
            on_branch: true,
            head_is_base: true,
        };
        assert!(submit_implement(&mut conn, &job, &claim.token, "sha", same_base).is_err());
        assert_eq!(job_state(&mut conn, &job), "implementing");

        release_job(&mut conn, &job, Release::human()).unwrap();
        assert_eq!(job_state(&mut conn, &job), "queued");
        assert!(claim_token_job(&mut conn, &job).is_none());

        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        assert!(release_job(&mut conn, &job, Release::expiry(true)).is_err());
        attach_process(&mut conn, &job, &claim.token, 50, 50, "start").unwrap();
        assert!(release_job(&mut conn, &job, Release::expiry(false)).is_err());
        release_job(&mut conn, &job, Release::expiry(true)).unwrap();
        assert_eq!(job_state(&mut conn, &job), "queued");

        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        fail_implement(&mut conn, &job, &claim.token, false).unwrap();
        assert_eq!(job_state(&mut conn, &job), "queued");
        assert!(blocked(&mut conn, &job).is_none());

        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        submit_implement(
            &mut conn,
            &job,
            &claim.token,
            "head-sha",
            ImplementWorktree::ready(),
        )
        .unwrap();
        assert_eq!(job_state(&mut conn, &job), "ready_for_review");
        let diff: String = conn
            .query_row(
                "SELECT diff_ref FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(diff, "head-sha");
        assert!(claim_token_job(&mut conn, &job).is_none());
    }

    #[test]
    fn dirty_failure_blocks_claim_until_retry() {
        let mut conn = mem();
        let (_id, job, claim) = to_implementing(&mut conn);
        fail_implement(&mut conn, &job, &claim.token, true).unwrap();
        assert_eq!(job_state(&mut conn, &job), "implementing");
        assert!(claim_token_job(&mut conn, &job).is_none());
        assert_eq!(blocked(&mut conn, &job).as_deref(), Some("dirty"));
        let (state, message) = conflict_of(
            claim_job(
                &mut conn,
                &job,
                ClaimSpec::daemon("grok:impl", Role::Implement),
            )
            .map(|_| ()),
        );
        assert_eq!(state, "implementing");
        assert!(message.contains("dirty"), "{message}");
        retry_job(&mut conn, &job).unwrap();
        assert_eq!(job_state(&mut conn, &job), "queued");
        assert!(blocked(&mut conn, &job).is_none());
        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        assert_eq!(job_state(&mut conn, &job), "implementing");
        assert!(!claim.token.is_empty());
    }

    #[test]
    fn retry_refuses_the_wrong_shape() {
        let mut conn = mem();
        let (_id, job, claim) = to_implementing(&mut conn);
        let (state, _) = conflict_of(retry_job(&mut conn, &job));
        assert_eq!(state, "implementing");
        fail_implement(&mut conn, &job, &claim.token, false).unwrap();
        let (state, _) = conflict_of(retry_job(&mut conn, &job));
        assert_eq!(state, "queued");
    }

    #[test]
    fn review_claim_requires_a_different_worker_and_submit_checks_the_tree() {
        let mut conn = mem();
        let (_id, job) = to_ready(&mut conn);
        let (state, message) = conflict_of(
            claim_job(
                &mut conn,
                &job,
                ClaimSpec::daemon("grok:impl", Role::Review),
            )
            .map(|_| ()),
        );
        assert_eq!(state, "ready_for_review");
        assert!(message.contains("implementer"), "{message}");

        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:other", Role::Review),
        )
        .unwrap();
        assert_eq!(job_state(&mut conn, &job), "in_review");
        let dirty = DetachedWorktree {
            clean: false,
            head_unchanged: true,
        };
        let (state, _) = conflict_of(submit_review(&mut conn, &job, &claim.token, REVIEW, dirty));
        assert_eq!(state, "in_review");
        assert_eq!(job_state(&mut conn, &job), "in_review");
        let moved = DetachedWorktree {
            clean: true,
            head_unchanged: false,
        };
        assert!(submit_review(&mut conn, &job, &claim.token, REVIEW, moved).is_err());
        submit_review(
            &mut conn,
            &job,
            &claim.token,
            REVIEW,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        assert_eq!(job_state(&mut conn, &job), "feedback_pending");
        assert!(claim_token_job(&mut conn, &job).is_none());
        let reviewer: String = conn
            .query_row(
                "SELECT reviewer_worker FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reviewer, "grok:other");
    }

    #[test]
    fn release_and_expiry_during_review_return_to_ready() {
        let mut conn = mem();
        let (_id, job) = to_ready(&mut conn);
        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("claude:rev", Role::Review),
        )
        .unwrap();
        assert!(!claim.token.is_empty());
        release_job(&mut conn, &job, Release::human()).unwrap();
        assert_eq!(job_state(&mut conn, &job), "ready_for_review");
        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("claude:rev", Role::Review),
        )
        .unwrap();
        assert!(!claim.token.is_empty());
        assert!(release_job(&mut conn, &job, Release::expiry(false)).is_err());
        attach_process(&mut conn, &job, &claim.token, 60, 60, "start").unwrap();
        release_job(&mut conn, &job, Release::expiry(true)).unwrap();
        assert_eq!(job_state(&mut conn, &job), "ready_for_review");
        assert!(claim_token_job(&mut conn, &job).is_none());
    }

    #[test]
    fn accept_and_reject_only_from_feedback_pending() {
        let mut conn = mem();
        let (id, job) = to_feedback(&mut conn);
        reject_job(&mut conn, &job, "needs a test").unwrap();
        assert_eq!(job_state(&mut conn, &job), "queued");
        assert_eq!(item_state(&mut conn, &id), "implementing");
        let note: String = conn
            .query_row(
                "SELECT text FROM job_notes WHERE job_id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(note, "needs a test");
        let branch: Option<String> = conn
            .query_row(
                "SELECT branch FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert!(branch.is_none());
        let (state, _) = conflict_of(accept_job(&mut conn, &job));
        assert_eq!(state, "queued");

        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        submit_implement(
            &mut conn,
            &job,
            &claim.token,
            "sha2",
            ImplementWorktree::ready(),
        )
        .unwrap();
        let claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("claude:rev", Role::Review),
        )
        .unwrap();
        submit_review(
            &mut conn,
            &job,
            &claim.token,
            REVIEW,
            DetachedWorktree::unchanged(),
        )
        .unwrap();
        accept_job(&mut conn, &job).unwrap();
        assert_eq!(job_state(&mut conn, &job), "accepted");
        assert_eq!(item_state(&mut conn, &id), "awaiting_merge");
        let (state, _) = conflict_of(reject_job(&mut conn, &job, "too late"));
        assert_eq!(state, "accepted");
    }

    #[test]
    fn container_propagates_when_the_last_child_is_accepted() {
        let mut conn = mem();
        let id = item(&mut conn);
        conn.execute(
            "UPDATE items SET state = 'implementing', base_sha = 'base' WHERE id = ?1",
            params![id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO jobs (
                id, item_id, parent_job_id, title, acceptance, state, depth, ord, updated_at
             ) VALUES
             ('j_container', ?1, NULL, 'container', '', 'queued', 1, 0, '2026-11-25T00:00:00Z'),
             ('j_child_a', ?1, 'j_container', 'a', '', 'accepted', 2, 0, '2026-11-25T00:00:00Z'),
             ('j_child_b', ?1, 'j_container', 'b', '', 'feedback_pending', 2, 1, '2026-11-25T00:00:00Z'),
             ('j_leaf', ?1, NULL, 'leaf', '', 'queued', 1, 1, '2026-11-25T00:00:00Z')",
            params![id],
        )
        .unwrap();
        let (state, message) = conflict_of(
            claim_job(
                &mut conn,
                "j_container",
                ClaimSpec::daemon("grok:impl", Role::Implement),
            )
            .map(|_| ()),
        );
        assert_eq!(state, "queued");
        assert!(message.contains("container"), "{message}");
        accept_job(&mut conn, "j_child_b").unwrap();
        assert_eq!(job_state(&mut conn, "j_child_b"), "accepted");
        assert_eq!(job_state(&mut conn, "j_container"), "accepted");
        assert_eq!(job_state(&mut conn, "j_leaf"), "queued");
        assert_eq!(item_state(&mut conn, &id), "implementing");

        conn.execute(
            "UPDATE jobs SET state = 'feedback_pending' WHERE id = 'j_leaf'",
            [],
        )
        .unwrap();
        accept_job(&mut conn, "j_leaf").unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_merge");
    }

    #[test]
    fn fifth_live_lease_is_refused_until_one_is_released() {
        let mut conn = mem();
        let mut ids = Vec::new();
        for _ in 0..4 {
            let id = item(&mut conn);
            claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
            ids.push(id);
        }
        let extra = item(&mut conn);
        let (state, message) = conflict_of(
            claim_item(
                &mut conn,
                &extra,
                ClaimSpec::daemon("grok:plan", Role::Plan),
            )
            .map(|_| ()),
        );
        assert_eq!(state, "inbox");
        assert!(message.contains("cap"), "{message}");
        release_item(&mut conn, &ids[0], Release::human()).unwrap();
        claim_item(
            &mut conn,
            &extra,
            ClaimSpec::daemon("grok:plan", Role::Plan),
        )
        .unwrap();
        assert_eq!(item_state(&mut conn, &extra), "planning");
    }

    #[test]
    fn external_claim_without_a_pid_is_rejected() {
        let mut conn = mem();
        let id = item(&mut conn);
        let mut spec = ClaimSpec::daemon("mcp:1", Role::Plan);
        spec.external = true;
        spec.pid = None;
        assert!(matches!(
            claim_item(&mut conn, &id, spec),
            Err(Error::Invalid(_))
        ));
        let mut spec = ClaimSpec::daemon("mcp:1", Role::Plan);
        spec.external = true;
        spec.pid = Some(42);
        spec.pgid = Some(99);
        assert!(matches!(
            claim_item(&mut conn, &id, spec),
            Err(Error::Invalid(_))
        ));
        let mut spec = ClaimSpec::daemon("mcp:1", Role::Plan);
        spec.external = true;
        spec.pid = Some(42);
        spec.starttime = Some("22".into());
        let claim = claim_item(&mut conn, &id, spec).unwrap();
        assert!(!claim.token.is_empty());
        let pgid: Option<i64> = conn
            .query_row(
                "SELECT claim_pgid FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(pgid.is_none());
        let external: i64 = conn
            .query_row(
                "SELECT claim_external FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(external, 1);
        let (state, message) = conflict_of(release_item(&mut conn, &id, Release::human()));
        assert_eq!(state, "planning");
        assert!(message.contains("force"), "{message}");
        let (state, _) = conflict_of(release_item(&mut conn, &id, Release::expiry(false)));
        assert_eq!(state, "planning");
        let mut mcp = Release::human();
        mcp.claim_token = Some(claim.token.clone());
        let (state, _) = conflict_of(release_item(&mut conn, &id, mcp));
        assert_eq!(state, "planning");
        let mut mcp_gone = Release::human();
        mcp_gone.claim_token = Some("nope".into());
        mcp_gone.pid_gone = true;
        let (state, message) = conflict_of(release_item(&mut conn, &id, mcp_gone));
        assert_eq!(state, "planning");
        assert!(message.contains("token"), "{message}");
        let mut mcp_ok = Release::human();
        mcp_ok.claim_token = Some(claim.token.clone());
        mcp_ok.pid_gone = true;
        release_item(&mut conn, &id, mcp_ok).unwrap();
        assert_eq!(item_state(&mut conn, &id), "inbox");

        let id = item(&mut conn);
        let mut spec = ClaimSpec::daemon("mcp:2", Role::Plan);
        spec.external = true;
        spec.pid = Some(7);
        spec.starttime = Some("22".into());
        claim_item(&mut conn, &id, spec).unwrap();
        let mut forced = Release::human();
        forced.force = true;
        release_item(&mut conn, &id, forced).unwrap();
        assert_eq!(item_state(&mut conn, &id), "inbox");
    }

    #[test]
    fn attach_process_fills_the_unsupervised_window_and_heartbeat_is_per_row() {
        let mut conn = mem();
        let id = item(&mut conn);
        let claim = claim_item(&mut conn, &id, ClaimSpec::daemon("grok:plan", Role::Plan)).unwrap();
        let other = item(&mut conn);
        assert!(matches!(
            claim_item(
                &mut conn,
                &other,
                ClaimSpec {
                    worker_id: "grok:bad".into(),
                    role: Role::Plan,
                    external: false,
                    pid: Some(3),
                    pgid: None,
                    starttime: None,
                },
            ),
            Err(Error::Invalid(_))
        ));
        assert!(attach_process(&mut conn, &id, "stale", 8, 8, "t").is_err());
        attach_process(&mut conn, &id, &claim.token, 8, 8, "t").unwrap();
        assert!(attach_process(&mut conn, &id, &claim.token, 8, 8, "t").is_err());
        let (pid, pgid, start): (i64, i64, String) = conn
            .query_row(
                "SELECT claim_pid, claim_pgid, claim_starttime FROM items WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((pid, pgid, start), (8, 8, "t".into()));
        conn.execute(
            "UPDATE items SET lease_until = '2000-01-01T00:00:00Z' WHERE id = ?1",
            params![id],
        )
        .unwrap();
        assert!(heartbeat_item(&mut conn, &id, "nope").is_err());
        heartbeat_item(&mut conn, &id, &claim.token).unwrap();
        let after: String = conn
            .query_row(
                "SELECT lease_until FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(after, "2000-01-01T00:00:00Z");

        release_item(&mut conn, &id, Release::expiry(true)).unwrap();
        let (item_id, job) = to_approved(&mut conn);
        conn.execute(
            "UPDATE jobs SET pending_permit = '{\"id\":1}' WHERE id = ?1",
            params![job],
        )
        .unwrap();
        let job_claim = claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("grok:impl", Role::Implement),
        )
        .unwrap();
        let permit: Option<String> = conn
            .query_row(
                "SELECT pending_permit FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert!(permit.is_none());
        conn.execute(
            "UPDATE jobs SET pending_permit = '{\"id\":1}', lease_until = '2000-01-01T00:00:00Z' WHERE id = ?1",
            params![job],
        )
        .unwrap();
        let item_lease: Option<String> = conn
            .query_row(
                "SELECT lease_until FROM items WHERE id = ?1",
                params![item_id],
                |row| row.get(0),
            )
            .unwrap();
        heartbeat_job(&mut conn, &job, &job_claim.token).unwrap();
        let job_lease: String = conn
            .query_row(
                "SELECT lease_until FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert_ne!(job_lease, "2000-01-01T00:00:00Z");
        let item_lease_after: Option<String> = conn
            .query_row(
                "SELECT lease_until FROM items WHERE id = ?1",
                params![item_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(item_lease, item_lease_after);
        fail_implement(&mut conn, &job, &job_claim.token, true).unwrap();
        let permit: Option<String> = conn
            .query_row(
                "SELECT pending_permit FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert!(permit.is_none());
    }

    #[test]
    fn plain_drop_cancels_jobs_and_force_is_required_while_claimed() {
        let mut conn = mem();
        let inbox = item(&mut conn);
        drop_item(&mut conn, &inbox, false).unwrap();
        assert_eq!(item_state(&mut conn, &inbox), "dropped");
        let (state, _) = conflict_of(drop_item(&mut conn, &inbox, true));
        assert_eq!(state, "dropped");

        let planned = item(&mut conn);
        claim_item(
            &mut conn,
            &planned,
            ClaimSpec::daemon("grok:plan", Role::Plan),
        )
        .unwrap();
        let (state, message) = conflict_of(drop_item(&mut conn, &planned, false));
        assert_eq!(state, "planning");
        assert!(message.contains("force"), "{message}");
        drop_item(&mut conn, &planned, true).unwrap();
        assert_eq!(item_state(&mut conn, &planned), "dropped");
        assert!(claim_token_item(&mut conn, &planned).is_none());

        for make in [
            DropAt::PlanReview,
            DropAt::Revision,
            DropAt::Approved,
            DropAt::Merge,
            DropAt::Conflict,
        ] {
            let mut conn = mem();
            let id = make.setup(&mut conn);
            drop_item(&mut conn, &id, false).unwrap();
            assert_eq!(item_state(&mut conn, &id), "dropped", "{make:?}");
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum DropAt {
        PlanReview,
        Revision,
        Approved,
        Merge,
        Conflict,
    }

    impl DropAt {
        fn setup(self, conn: &mut Connection) -> String {
            match self {
                DropAt::PlanReview => to_plan_review(conn),
                DropAt::Revision => {
                    let id = to_plan_review(conn);
                    note(conn, &id, "a point").unwrap();
                    id
                }
                DropAt::Approved => to_approved(conn).0,
                DropAt::Merge => to_awaiting_merge(conn).0,
                DropAt::Conflict => {
                    let (id, _) = to_awaiting_merge(conn);
                    mark_merge_conflict(conn, &id, ConflictKind::Integrate).unwrap();
                    id
                }
            }
        }
    }

    #[test]
    fn force_drop_cancels_implementing_and_in_review_jobs() {
        let mut conn = mem();
        let (id, job, _claim) = to_implementing(&mut conn);
        let (state, _) = conflict_of(drop_item(&mut conn, &id, false));
        assert_eq!(state, "implementing");
        drop_item(&mut conn, &id, true).unwrap();
        assert_eq!(item_state(&mut conn, &id), "dropped");
        assert_eq!(job_state(&mut conn, &job), "cancelled");
        assert!(claim_token_job(&mut conn, &job).is_none());

        let mut conn = mem();
        let id = to_plan_review(&mut conn);
        let point = note(&mut conn, &id, "point").unwrap();
        assert!(!point.is_empty());
        claim_item(&mut conn, &id, ClaimSpec::daemon("grok:rev", Role::Revise)).unwrap();
        assert!(drop_item(&mut conn, &id, false).is_err());
        drop_item(&mut conn, &id, true).unwrap();
        assert_eq!(item_state(&mut conn, &id), "dropped");

        let mut conn = mem();
        let (id, job) = to_ready(&mut conn);
        assert!(drop_item(&mut conn, &id, false).is_err());
        drop_item(&mut conn, &id, true).unwrap();
        assert_eq!(job_state(&mut conn, &job), "cancelled");

        let mut conn = mem();
        let (id, job) = to_ready(&mut conn);
        claim_job(
            &mut conn,
            &job,
            ClaimSpec::daemon("claude:rev", Role::Review),
        )
        .unwrap();
        assert_eq!(job_state(&mut conn, &job), "in_review");
        let (state, _) = conflict_of(drop_item(&mut conn, &id, false));
        assert_eq!(state, "implementing");
        drop_item(&mut conn, &id, true).unwrap();
        assert_eq!(job_state(&mut conn, &job), "cancelled");
        assert_eq!(item_state(&mut conn, &id), "dropped");

        let mut conn = mem();
        let (id, job) = to_feedback(&mut conn);
        assert!(drop_item(&mut conn, &id, false).is_err());
        drop_item(&mut conn, &id, true).unwrap();
        assert_eq!(job_state(&mut conn, &job), "cancelled");

        let mut conn = mem();
        let (id, job) = to_awaiting_merge(&mut conn);
        drop_item(&mut conn, &id, false).unwrap();
        assert_eq!(job_state(&mut conn, &job), "cancelled");
        assert_eq!(item_state(&mut conn, &id), "dropped");
    }

    #[test]
    fn approved_queued_job_is_cancelled_by_a_plain_drop() {
        let mut conn = mem();
        let (id, job) = to_approved(&mut conn);
        drop_item(&mut conn, &id, false).unwrap();
        assert_eq!(job_state(&mut conn, &job), "cancelled");
        assert_eq!(item_state(&mut conn, &id), "dropped");
    }

    #[test]
    fn merge_conflict_continue_and_abort() {
        let mut conn = mem();
        let (id, _job) = to_awaiting_merge(&mut conn);
        let (state, _) = conflict_of(merge_abort(&mut conn, &id));
        assert_eq!(state, "awaiting_merge");
        let (state, _) = conflict_of(merge_continue(&mut conn, &id, "nope"));
        assert_eq!(state, "awaiting_merge");
        drop(conn);

        let mut conn = mem();
        let (id, job) = to_awaiting_merge(&mut conn);
        merge_item(&mut conn, &id, "merged-sha").unwrap();
        assert_eq!(item_state(&mut conn, &id), "merged");
        let sha: String = conn
            .query_row(
                "SELECT merged_sha FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sha, "merged-sha");
        let (state, _) = conflict_of(drop_item(&mut conn, &id, true));
        assert_eq!(state, "merged");
        let (state, _) = conflict_of(merge_abort(&mut conn, &id));
        assert_eq!(state, "merged");
        assert_eq!(job_state(&mut conn, &job), "accepted");

        let mut conn = mem();
        let (id, job) = to_awaiting_merge(&mut conn);
        conn.execute(
            "UPDATE items SET integrate_sha = 'int-sha', integrate_ord = 0 WHERE id = ?1",
            params![id],
        )
        .unwrap();
        mark_merge_conflict(&mut conn, &id, ConflictKind::Primary).unwrap();
        assert_eq!(item_state(&mut conn, &id), "merge_conflict");
        let (kind, integrate, ord): (String, String, i64) = conn
            .query_row(
                "SELECT conflict_kind, integrate_sha, integrate_ord FROM items WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(kind, "primary");
        assert_eq!(integrate, "int-sha");
        assert_eq!(ord, 0);
        let (state, _) = conflict_of(merge_item(&mut conn, &id, "no"));
        assert_eq!(state, "merge_conflict");

        merge_abort(&mut conn, &id).unwrap();
        assert_eq!(item_state(&mut conn, &id), "awaiting_merge");
        let (integrate, ord, kind, merged): (Option<String>, Option<i64>, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT integrate_sha, integrate_ord, conflict_kind, merged_sha FROM items WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert!(integrate.is_none());
        assert!(ord.is_none());
        assert!(kind.is_none());
        assert!(merged.is_none());
        assert_eq!(job_state(&mut conn, &job), "accepted");
        let branch: Option<String> = conn
            .query_row(
                "SELECT branch FROM jobs WHERE id = ?1",
                params![job],
                |row| row.get(0),
            )
            .unwrap();
        assert!(branch.is_none());

        let mut conn = mem();
        let (id, _) = to_awaiting_merge(&mut conn);
        mark_merge_conflict(&mut conn, &id, ConflictKind::Integrate).unwrap();
        merge_continue(&mut conn, &id, "continued").unwrap();
        assert_eq!(item_state(&mut conn, &id), "merged");
        let sha: String = conn
            .query_row(
                "SELECT merged_sha FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sha, "continued");
        let kind: Option<String> = conn
            .query_row(
                "SELECT conflict_kind FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(kind.is_none());
    }

    #[test]
    fn happy_path_event_kinds() {
        let mut conn = mem();
        let (id, _) = to_awaiting_merge(&mut conn);
        merge_item(&mut conn, &id, "sha").unwrap();
        let kinds: Vec<String> = {
            let mut stmt = conn
                .prepare(
                    "SELECT kind FROM events WHERE item_id = ?1 AND job_id IS NULL ORDER BY id",
                )
                .unwrap();
            stmt.query_map(params![id], |row| row.get(0))
                .unwrap()
                .map(|row| row.unwrap())
                .collect()
        };
        assert_eq!(
            kinds,
            vec![
                "create",
                "claim",
                "submit_plan",
                "approve",
                "awaiting_merge",
                "merge",
            ]
        );
    }
}
