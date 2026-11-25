//! Read-only checks, plus a daily copy of `keel.db` beside itself.
//! The only write is a missing current plan or review HTML file when the
//! JSON column is still there. Git is inspected and not updated. Restoring
//! `keel.db.bak` is not this command.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{params, Connection};
use serde_json::Value;

use crate::config::Config;
use crate::daemon;
use crate::db::{self, Error, Result};
use crate::git;
use crate::render::{self, ReviewView};

const DAY: Duration = Duration::from_secs(24 * 60 * 60);
const WALK_LIMIT: usize = 400;
const MAX_DEPTH: u32 = 10;
const SKIP_DIRS: &[&str] = &[
    "node_modules",
    "target",
    ".git",
    ".gradle",
    "__pycache__",
    ".venv",
];
const PREVIEW_EXT: &[&str] = &[
    "html", "htm", "svg", "png", "jpg", "jpeg", "gif", "webp", "md", "txt", "pdf",
];

/// `keel.db.bak` next to the database. Never under `agent-artifacts`.
pub(crate) fn backup_if_stale(conn: &Connection, db_path: &Path) -> Result<()> {
    let bak = backup_path(db_path)?;
    if !backup_is_stale(&bak) {
        return Ok(());
    }
    if let Some(parent) = bak.parent() {
        fs::create_dir_all(parent)?;
    }
    conn.backup(rusqlite::MAIN_DB, &bak, None)?;
    let mut perms = fs::metadata(&bak)?.permissions();
    perms.set_mode(0o600);
    fs::set_permissions(&bak, perms)?;
    Ok(())
}

pub(crate) fn run(db: &Mutex<Connection>, config: &Config) -> Result<Vec<String>> {
    let snap = {
        let conn = lock_db(db)?;
        snapshot(&conn, config)?
    };
    let mut lines = Vec::new();
    if snap.version == db::USER_VERSION {
        lines.push(format!("user_version: {}", snap.version));
    } else {
        lines.push(format!(
            "warn user_version: {} expected {}",
            snap.version,
            db::USER_VERSION
        ));
    }

    let (html, published, updates) = documents(config, &snap);
    if html.is_empty() {
        lines.push("html: ok".into());
    } else {
        lines.extend(html);
    }
    if published.is_empty() {
        lines.push("published: ok".into());
    } else {
        lines.extend(published);
    }
    remember_relpath(db, &updates)?;

    let mut worktrees = worktree_lines(&config.data_dir, &snap.items, &snap.jobs);
    if worktrees.is_empty() {
        lines.push("worktrees: ok".into());
    } else {
        worktrees.sort();
        lines.extend(worktrees);
    }

    let mut leases = lease_lines(&snap.items, &snap.jobs);
    if leases.is_empty() {
        lines.push("leases: ok".into());
    } else {
        leases.sort();
        lines.extend(leases);
    }

    let (files, keel) = walk_previewable(&config.publish_root);
    let marker = if files >= WALK_LIMIT { "warn " } else { "" };
    lines.push(format!(
        "{marker}artifacts: {files} files, {keel} under keel/, limit {WALK_LIMIT}"
    ));

    let git_notes = git_lines(&snap.items);
    if git_notes.is_empty() {
        lines.push("git: ok".into());
    } else {
        lines.extend(git_notes);
    }
    Ok(lines)
}

fn backup_path(db_path: &Path) -> Result<PathBuf> {
    let bak = db_path.with_file_name("keel.db.bak");
    // The artifact walk already publishes HTML here. A database copy would
    // crowd that walk and is not how a lost queue is restored.
    if bak
        .components()
        .any(|component| component.as_os_str() == "agent-artifacts")
    {
        return Err(Error::Invalid(
            "refusing to copy keel.db into agent-artifacts".into(),
        ));
    }
    Ok(bak)
}

fn backup_is_stale(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return true;
    };
    let Ok(modified) = meta.modified() else {
        return true;
    };
    match modified.elapsed() {
        Ok(age) => age >= DAY,
        // A future mtime is not older than a day.
        Err(_) => false,
    }
}

struct Snap {
    version: i32,
    items: Vec<ItemFact>,
    jobs: Vec<JobFact>,
}

struct ItemFact {
    id: String,
    state: String,
    repo: Option<String>,
    merged_sha: Option<String>,
    plan_rev: i64,
    plan: PlanDoc,
    claim_pid: Option<i64>,
    claim_starttime: Option<String>,
}

enum PlanDoc {
    Absent,
    Ready(render::PlanView),
    Failed(String),
}

struct JobFact {
    id: String,
    item_id: String,
    state: String,
    review_rev: i64,
    review: ReviewDoc,
    worktree: Option<PathBuf>,
    claim_pid: Option<i64>,
    claim_starttime: Option<String>,
}

enum ReviewDoc {
    Absent,
    Ready(ReviewView),
    Failed(String),
}

struct ItemRow {
    id: String,
    state: String,
    repo: Option<String>,
    merged_sha: Option<String>,
    plan_rev: i64,
    plan_json: Option<String>,
    claim_pid: Option<i64>,
    claim_starttime: Option<String>,
}

struct JobRow {
    id: String,
    item_id: String,
    title: String,
    state: String,
    review_rev: i64,
    review_json: Option<String>,
    base_sha: Option<String>,
    diff_ref: Option<String>,
    updated_at: String,
    worktree: Option<String>,
    claim_pid: Option<i64>,
    claim_starttime: Option<String>,
}

fn snapshot(conn: &Connection, config: &Config) -> Result<Snap> {
    let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let mut item_rows = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, state, repo, merged_sha, plan_rev, plan_json, claim_pid, claim_starttime
             FROM items ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ItemRow {
                id: row.get(0)?,
                state: row.get(1)?,
                repo: row.get(2)?,
                merged_sha: row.get(3)?,
                plan_rev: row.get(4)?,
                plan_json: row.get(5)?,
                claim_pid: row.get(6)?,
                claim_starttime: row.get(7)?,
            })
        })?;
        for row in rows {
            item_rows.push(row?);
        }
    }
    let mut items = Vec::with_capacity(item_rows.len());
    for row in item_rows {
        let plan = plan_doc(
            conn,
            config,
            &row.id,
            row.plan_rev,
            row.plan_json.as_deref(),
        );
        items.push(ItemFact {
            id: row.id,
            state: row.state,
            repo: row.repo,
            merged_sha: row.merged_sha,
            plan_rev: row.plan_rev,
            plan,
            claim_pid: row.claim_pid,
            claim_starttime: row.claim_starttime,
        });
    }

    let mut job_rows = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT id, item_id, title, state, review_rev, review_json, base_sha, diff_ref,
                    updated_at, worktree, claim_pid, claim_starttime
             FROM jobs ORDER BY id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(JobRow {
                id: row.get(0)?,
                item_id: row.get(1)?,
                title: row.get(2)?,
                state: row.get(3)?,
                review_rev: row.get(4)?,
                review_json: row.get(5)?,
                base_sha: row.get(6)?,
                diff_ref: row.get(7)?,
                updated_at: row.get(8)?,
                worktree: row.get(9)?,
                claim_pid: row.get(10)?,
                claim_starttime: row.get(11)?,
            })
        })?;
        for row in rows {
            job_rows.push(row?);
        }
    }
    let mut jobs = Vec::with_capacity(job_rows.len());
    for row in job_rows {
        let review = review_doc(&row);
        jobs.push(JobFact {
            id: row.id,
            item_id: row.item_id,
            state: row.state,
            review_rev: row.review_rev,
            review,
            worktree: row.worktree.map(PathBuf::from),
            claim_pid: row.claim_pid,
            claim_starttime: row.claim_starttime,
        });
    }
    Ok(Snap {
        version,
        items,
        jobs,
    })
}

fn plan_doc(conn: &Connection, config: &Config, id: &str, rev: i64, json: Option<&str>) -> PlanDoc {
    if json.map(|text| text.is_empty()).unwrap_or(true) {
        return PlanDoc::Absent;
    }
    if rev < 1 {
        return PlanDoc::Failed("plan_rev is 0".into());
    }
    match daemon::load_plan(conn, id, config) {
        Ok(Some(view)) => PlanDoc::Ready(view),
        Ok(None) => PlanDoc::Failed("plan_json did not render".into()),
        Err(err) => PlanDoc::Failed(err.to_string()),
    }
}

fn review_doc(row: &JobRow) -> ReviewDoc {
    let Some(json) = row.review_json.as_deref().filter(|text| !text.is_empty()) else {
        return ReviewDoc::Absent;
    };
    if row.review_rev < 1 {
        return ReviewDoc::Failed("review_rev is 0".into());
    }
    let value: Value = match serde_json::from_str(json) {
        Ok(value) => value,
        Err(err) => return ReviewDoc::Failed(err.to_string()),
    };
    if !value.is_object() {
        return ReviewDoc::Failed("review document must be a JSON object".into());
    }
    let verdict = value
        .get("verdict")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let summary = value
        .get("summary")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let stat = value
        .get("diff")
        .and_then(|diff| diff.get("stat"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    ReviewDoc::Ready(ReviewView {
        item_id: row.item_id.clone(),
        job_id: row.id.clone(),
        title: row.title.clone(),
        state: row.state.clone(),
        summary,
        rev: row.review_rev,
        updated_at: row.updated_at.clone(),
        verdict,
        base: row.base_sha.clone().unwrap_or_default(),
        head: row.diff_ref.clone().unwrap_or_default(),
        stat,
    })
}

struct RelPath {
    plan: bool,
    id: String,
    rel: String,
}

fn documents(config: &Config, snap: &Snap) -> (Vec<String>, Vec<String>, Vec<RelPath>) {
    let mut html = Vec::new();
    let mut published = Vec::new();
    let mut updates = Vec::new();
    for item in &snap.items {
        if !render::safe_id(&item.id) {
            html.push(format!(
                "warn html: card {} id is not a path segment",
                item.id
            ));
            continue;
        }
        let card = config.docs_dir.join(&item.id).join("card.html");
        match fs::read_to_string(&card) {
            Ok(text) => {
                if let Some(line) = embedded_mismatch("card", &item.id, &text, "item_id") {
                    html.push(line);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                html.push(format!("warn html: card {} missing", item.id));
            }
            Err(err) => html.push(format!("warn html: card {} {err}", item.id)),
        }
        if let Some(line) = published_line(
            "card",
            &item.id,
            &card,
            &config
                .publish_root
                .join("keel")
                .join(&item.id)
                .join("card.html"),
        ) {
            published.push(line);
        }

        let plan_path = config
            .docs_dir
            .join(&item.id)
            .join("plan")
            .join(format!("r{}.html", item.plan_rev.max(1)));
        match &item.plan {
            PlanDoc::Absent if item.plan_rev < 1 => {}
            PlanDoc::Absent => match fs::read_to_string(&plan_path) {
                Ok(text) => {
                    if let Some(line) = embedded_mismatch("plan", &item.id, &text, "item_id") {
                        html.push(line);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    html.push(format!("warn html: plan {} missing", item.id));
                }
                Err(err) => html.push(format!("warn html: plan {} {err}", item.id)),
            },
            PlanDoc::Failed(err) => {
                if !plan_path.is_file() {
                    html.push(format!(
                        "warn html: plan {} re-render failed: {err}",
                        item.id
                    ));
                } else if let Ok(text) = fs::read_to_string(&plan_path) {
                    if let Some(line) = embedded_mismatch("plan", &item.id, &text, "item_id") {
                        html.push(line);
                    }
                }
            }
            PlanDoc::Ready(view) => {
                let path = config
                    .docs_dir
                    .join(&item.id)
                    .join("plan")
                    .join(format!("r{}.html", view.plan_rev));
                if path.is_file() {
                    if let Ok(text) = fs::read_to_string(&path) {
                        if let Some(line) = embedded_mismatch("plan", &item.id, &text, "item_id") {
                            html.push(line);
                        }
                    }
                } else {
                    let page = render::plan_page(view);
                    match daemon::write_atomic(&path, page.as_bytes()) {
                        Ok(()) => {
                            html.push(format!("fixed html: plan {} re-rendered", item.id));
                            updates.push(RelPath {
                                plan: true,
                                id: item.id.clone(),
                                rel: format!("docs/{}/plan/r{}.html", item.id, view.plan_rev),
                            });
                        }
                        Err(err) => html.push(format!(
                            "warn html: plan {} re-render failed: {err}",
                            item.id
                        )),
                    }
                }
                if let Some(line) = published_line(
                    "plan",
                    &item.id,
                    &path,
                    &config
                        .publish_root
                        .join("keel")
                        .join(&item.id)
                        .join("plan.html"),
                ) {
                    published.push(line);
                }
            }
        }
        if !matches!(item.plan, PlanDoc::Ready(_)) {
            if let Some(line) = published_line(
                "plan",
                &item.id,
                &plan_path,
                &config
                    .publish_root
                    .join("keel")
                    .join(&item.id)
                    .join("plan.html"),
            ) {
                published.push(line);
            }
        }
    }
    for job in &snap.jobs {
        if !render::safe_id(&job.id) || !render::safe_id(&job.item_id) {
            html.push(format!(
                "warn html: review {} id is not a path segment",
                job.id
            ));
            continue;
        }
        let rev = match &job.review {
            ReviewDoc::Ready(view) => view.rev,
            _ => job.review_rev.max(1),
        };
        let path = config
            .docs_dir
            .join(&job.item_id)
            .join("jobs")
            .join(&job.id)
            .join("review")
            .join(format!("r{rev}.html"));
        let published_path = config
            .publish_root
            .join("keel")
            .join(&job.item_id)
            .join(format!("review-{}.html", job.id));
        match &job.review {
            ReviewDoc::Absent if job.review_rev < 1 => {}
            ReviewDoc::Absent => note_existing("review", &job.id, &path, "job_id", &mut html),
            ReviewDoc::Failed(err) => {
                if path.is_file() {
                    note_existing("review", &job.id, &path, "job_id", &mut html);
                } else {
                    html.push(format!(
                        "warn html: review {} re-render failed: {err}",
                        job.id
                    ));
                }
            }
            ReviewDoc::Ready(view) => {
                if path.is_file() {
                    note_existing("review", &job.id, &path, "job_id", &mut html);
                } else {
                    let page = render::review_page(view);
                    match daemon::write_atomic(&path, page.as_bytes()) {
                        Ok(()) => {
                            html.push(format!("fixed html: review {} re-rendered", job.id));
                            updates.push(RelPath {
                                plan: false,
                                id: job.id.clone(),
                                rel: format!(
                                    "docs/{}/jobs/{}/review/r{}.html",
                                    job.item_id, job.id, view.rev
                                ),
                            });
                        }
                        Err(err) => html.push(format!(
                            "warn html: review {} re-render failed: {err}",
                            job.id
                        )),
                    }
                }
            }
        }
        if job.review_rev >= 1 || !matches!(job.review, ReviewDoc::Absent) {
            if let Some(line) = published_line("review", &job.id, &path, &published_path) {
                published.push(line);
            }
        }
    }
    (html, published, updates)
}

fn note_existing(kind: &str, id: &str, path: &Path, field: &str, html: &mut Vec<String>) {
    match fs::read_to_string(path) {
        Ok(text) => {
            if let Some(line) = embedded_mismatch(kind, id, &text, field) {
                html.push(line);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            html.push(format!("warn html: {kind} {id} missing"));
        }
        Err(err) => html.push(format!("warn html: {kind} {id} {err}")),
    }
}

fn embedded_mismatch(kind: &str, id: &str, html: &str, field: &str) -> Option<String> {
    let Some(doc) = embedded_doc(html) else {
        return Some(format!("warn html: {kind} {id} embedded id missing"));
    };
    let found = doc.get(field).and_then(Value::as_str).unwrap_or("");
    if found == id {
        None
    } else {
        Some(format!(
            "warn html: {kind} {id} embedded id {found} != {id}"
        ))
    }
}

fn embedded_doc(html: &str) -> Option<Value> {
    let marker = "<script type=\"application/json\" id=\"keel-doc\">";
    let start = html.find(marker)? + marker.len();
    let end = html[start..].find("</script>")? + start;
    serde_json::from_str(html[start..end].trim()).ok()
}

fn published_line(kind: &str, id: &str, canonical: &Path, published: &Path) -> Option<String> {
    if !published.is_file() {
        return None;
    }
    if !canonical.is_file() {
        return Some(format!(
            "warn published: {kind} {id} copy exists without canonical"
        ));
    }
    let canon = match sha256_file(canonical) {
        Ok(hash) => hash,
        Err(err) => return Some(format!("warn published: {kind} {id} {err}")),
    };
    let copy = match sha256_file(published) {
        Ok(hash) => hash,
        Err(err) => return Some(format!("warn published: {kind} {id} {err}")),
    };
    if canon == copy {
        Some(format!("published: {kind} {id} {canon}"))
    } else {
        Some(format!(
            "warn published: {kind} {id} sha256 {canon} != {copy}"
        ))
    }
}

fn remember_relpath(db: &Mutex<Connection>, updates: &[RelPath]) -> Result<()> {
    if updates.is_empty() {
        return Ok(());
    }
    let conn = lock_db(db)?;
    for update in updates {
        if update.plan {
            conn.execute(
                "UPDATE items SET plan_relpath = ?1 WHERE id = ?2",
                params![update.rel, update.id],
            )?;
        } else {
            conn.execute(
                "UPDATE jobs SET review_relpath = ?1 WHERE id = ?2",
                params![update.rel, update.id],
            )?;
        }
    }
    Ok(())
}

fn worktree_lines(data_dir: &Path, items: &[ItemFact], jobs: &[JobFact]) -> Vec<String> {
    let mut lines = Vec::new();
    let states: BTreeMap<&str, &str> = items
        .iter()
        .map(|item| (item.id.as_str(), item.state.as_str()))
        .collect();
    let by_id: BTreeMap<&str, &JobFact> = jobs.iter().map(|job| (job.id.as_str(), job)).collect();
    let root = data_dir.join("worktrees");
    if let Ok(item_dirs) = fs::read_dir(&root) {
        for item_dir in item_dirs.flatten() {
            if !item_dir
                .file_type()
                .map(|kind| kind.is_dir())
                .unwrap_or(false)
            {
                continue;
            }
            let item_id = item_dir.file_name().to_string_lossy().into_owned();
            let dropped = states.get(item_id.as_str()).copied() == Some("dropped");
            let Ok(slots) = fs::read_dir(item_dir.path()) else {
                continue;
            };
            for slot in slots.flatten() {
                if !slot.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                    continue;
                }
                let name = slot.file_name().to_string_lossy().into_owned();
                let path = slot.path();
                if dropped {
                    lines.push(format!(
                        "warn worktree: {} item {item_id} dropped",
                        path.display()
                    ));
                    continue;
                }
                let job_id = name.strip_suffix("-review").unwrap_or(&name);
                if let Some(job) = by_id.get(job_id) {
                    if job.item_id == item_id && job.state == "cancelled" {
                        lines.push(format!(
                            "warn worktree: {} job {job_id} cancelled",
                            path.display()
                        ));
                    }
                }
            }
        }
    }
    for job in jobs {
        if job.state != "cancelled" {
            continue;
        }
        let Some(path) = job.worktree.as_ref().filter(|path| path.is_dir()) else {
            continue;
        };
        let shown = path.display().to_string();
        if lines.iter().any(|line| line.contains(&shown)) {
            continue;
        }
        lines.push(format!("warn worktree: {shown} job {} cancelled", job.id));
    }
    lines
}

/// Compare the stored starttime to `/proc`. A mismatch is a report: this does
/// not signal the pid, including when the number was reused.
fn lease_lines(items: &[ItemFact], jobs: &[JobFact]) -> Vec<String> {
    let mut lines = Vec::new();
    for item in items {
        if let Some(line) = lease_mismatch(
            "item",
            &item.id,
            item.claim_pid,
            item.claim_starttime.as_deref(),
        ) {
            lines.push(line);
        }
    }
    for job in jobs {
        if let Some(line) = lease_mismatch(
            "job",
            &job.id,
            job.claim_pid,
            job.claim_starttime.as_deref(),
        ) {
            lines.push(line);
        }
    }
    lines
}

fn lease_mismatch(kind: &str, id: &str, pid: Option<i64>, start: Option<&str>) -> Option<String> {
    let pid = pid?;
    let stored = start.unwrap_or("missing");
    let live = if (1..=i64::from(u32::MAX)).contains(&pid) {
        daemon::read_proc_starttime(pid as u32)
    } else {
        None
    };
    match live {
        Some(live) if live == stored => None,
        Some(live) => Some(format!(
            "warn lease: {kind} {id} pid {pid} starttime {stored} != {live}"
        )),
        None => Some(format!(
            "warn lease: {kind} {id} pid {pid} starttime {stored} != dead"
        )),
    }
}

/// Same filters as grokback-bridge `list_previewable`: extension, skip dirs,
/// depth 10, first 400 files in directory order. The count is how many of
/// those files sit under `keel/`.
fn walk_previewable(root: &Path) -> (usize, usize) {
    let mut files = 0usize;
    let mut keel = 0usize;
    if root.is_dir() {
        walk_dir(root, 0, root, &mut files, &mut keel);
    }
    (files, keel)
}

fn walk_dir(dir: &Path, dir_depth: u32, root: &Path, files: &mut usize, keel: &mut usize) {
    if *files >= WALK_LIMIT || dir_depth >= MAX_DEPTH {
        return;
    }
    let Ok(read) = fs::read_dir(dir) else {
        return;
    };
    let entries: Vec<_> = read.flatten().collect();
    for entry in entries {
        if *files >= WALK_LIMIT {
            return;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if SKIP_DIRS.iter().any(|skip| name == *skip) {
            continue;
        }
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let child_depth = dir_depth + 1;
        if child_depth > MAX_DEPTH {
            continue;
        }
        if kind.is_dir() {
            walk_dir(&entry.path(), child_depth, root, files, keel);
        } else if kind.is_file() && previewable(&entry.path()) {
            if under_keel(root, &entry.path()) {
                *keel += 1;
            }
            *files += 1;
        }
    }
}

fn previewable(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| PREVIEW_EXT.iter().any(|ok| ext.eq_ignore_ascii_case(ok)))
}

fn under_keel(root: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(root) else {
        return false;
    };
    let mut parts = rel.components();
    let Some(first) = parts.next() else {
        return false;
    };
    first.as_os_str() == "keel" && parts.next().is_some()
}

/// Report only. `merged_sha` that is not an ancestor of primary `HEAD`, and a
/// first-parent Keel merge commit this database does not store. No checkout,
/// reset, or merge.
fn git_lines(items: &[ItemFact]) -> Vec<String> {
    let mut lines = Vec::new();
    let mut repos: BTreeMap<String, Vec<(String, Option<String>)>> = BTreeMap::new();
    for item in items {
        let Some(repo) = item.repo.as_ref().filter(|repo| !repo.is_empty()) else {
            if let Some(sha) = item.merged_sha.as_ref().filter(|sha| !sha.is_empty()) {
                lines.push(format!(
                    "warn git: item {} merged_sha {sha} has no primary",
                    item.id
                ));
            }
            continue;
        };
        repos
            .entry(repo.clone())
            .or_default()
            .push((item.id.clone(), item.merged_sha.clone()));
    }
    for (repo, rows) in repos {
        let path = Path::new(&repo);
        if !path.is_dir() {
            lines.push(format!("warn git: {repo} is not a checkout"));
            continue;
        }
        let mut known = Vec::new();
        for (id, merged) in &rows {
            let Some(sha) = merged.as_ref().filter(|sha| !sha.is_empty()) else {
                continue;
            };
            known.push(sha.clone());
            match git::is_ancestor(path, sha, "HEAD") {
                Ok(true) => {}
                Ok(false) => lines.push(format!(
                    "warn git: item {id} merged_sha {sha} is not an ancestor of primary HEAD"
                )),
                Err(err) => lines.push(format!(
                    "warn git: item {id} merged_sha {sha} is not an ancestor of primary HEAD ({err})"
                )),
            }
        }
        match git::first_parent_merges(path) {
            Ok(merges) => {
                for (sha, subject) in merges {
                    if subject.contains("keel/") && !known.iter().any(|stored| stored == &sha) {
                        lines.push(format!(
                            "warn git: {sha} on {repo} is a keel merge the database does not know about"
                        ));
                    }
                }
            }
            Err(err) => lines.push(format!("warn git: {repo} {err}")),
        }
    }
    lines
}

fn sha256_file(path: &Path) -> Result<String> {
    let bytes = fs::read(path)?;
    Ok(hex(&sha256(&bytes)))
}

fn sha256(message: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];
    fn rotr(value: u32, bits: u32) -> u32 {
        value.rotate_right(bits)
    }
    let mut h = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (message.len() as u64).wrapping_mul(8);
    let mut data = Vec::with_capacity(message.len() + 72);
    data.extend_from_slice(message);
    data.push(0x80);
    while data.len() % 64 != 56 {
        data.push(0);
    }
    data.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in data.chunks(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
            let s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];
        for i in 0..64 {
            let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            let ch = (e & f) ^ (!e & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn lock_db(db: &Mutex<Connection>) -> Result<MutexGuard<'_, Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_in_memory;
    use crate::render::CardView;
    use crate::state::{self, RawIntake};
    use std::net::TcpStream;
    use std::os::unix::net::UnixStream;
    use std::process::{Command, Stdio};
    use std::time::SystemTime;

    struct Rm(PathBuf);
    impl Drop for Rm {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scratch() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "keel-doctor-{}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn fresh() -> (Rm, Config, Mutex<Connection>) {
        let root = scratch();
        let cfg = crate::config::isolated(&root, 7431);
        fs::create_dir_all(&cfg.data_dir).unwrap();
        let db = Mutex::new(open_in_memory().unwrap());
        (Rm(root), cfg, db)
    }

    fn add_item(db: &Mutex<Connection>, repo: Option<PathBuf>) -> String {
        let mut conn = db.lock().unwrap();
        state::create_item(
            &mut conn,
            RawIntake {
                source: "cli".into(),
                external_id: None,
                title: "Doctor".into(),
                body: "body".into(),
                repo,
                target_branch: Some("main".into()),
            },
        )
        .unwrap()
    }

    fn write_card(cfg: &Config, id: &str) {
        let html = render::card_page(&CardView {
            id: id.into(),
            title: "Doctor".into(),
            body: "body".into(),
            source: "cli".into(),
            repo: None,
            target_branch: Some("main".into()),
            state: "inbox".into(),
            summary: "".into(),
            plan_rev: 0,
            created_at: "2026-11-25T00:00:00Z".into(),
            updated_at: "2026-11-25T00:00:00Z".into(),
            open_points: Vec::new(),
        });
        let path = cfg.docs_dir.join(id).join("card.html");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, html).unwrap();
    }

    #[test]
    fn sha256_matches_the_empty_and_abc_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn backup_is_daily_and_not_in_the_artifact_root() {
        let _rm = Rm(scratch());
        let db_path = _rm.0.join("data").join("keel.db");
        fs::create_dir_all(db_path.parent().unwrap()).unwrap();
        let conn = crate::db::open(&db_path).unwrap();
        backup_if_stale(&conn, &db_path).unwrap();
        let bak = db_path.with_file_name("keel.db.bak");
        assert!(bak.is_file());
        assert!(bak.starts_with(_rm.0.join("data")));
        assert!(bak
            .components()
            .all(|component| component.as_os_str() != "agent-artifacts"));
        {
            let bak_conn = Connection::open(&bak).unwrap();
            bak_conn.pragma_update(None, "user_version", 7).unwrap();
        }
        backup_if_stale(&conn, &db_path).unwrap();
        let version: i32 = Connection::open(&bak)
            .unwrap()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 7, "fresh backup was copied again");
        {
            let file = fs::OpenOptions::new().write(true).open(&bak).unwrap();
            file.set_modified(SystemTime::now() - Duration::from_secs(3 * 24 * 60 * 60))
                .unwrap();
        }
        backup_if_stale(&conn, &db_path).unwrap();
        let version: i32 = Connection::open(&bak)
            .unwrap()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, db::USER_VERSION);
        let refused = Path::new("/tmp/agent-artifacts/keel.db");
        let err = backup_path(refused).unwrap_err();
        assert!(err.to_string().contains("agent-artifacts"), "{err}");
    }

    #[test]
    fn rerenders_missing_html_and_does_not_fix_anything_else() {
        let (_rm, cfg, db) = fresh();
        let id = add_item(&db, None);
        write_card(&cfg, &id);
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET state = 'awaiting_plan_review', plan_rev = 1, summary = 'Sum',
                        plan_json = '{\"summary\":\"Sum\",\"jobs\":[]}'
                 WHERE id = ?1",
                params![id],
            )
            .unwrap();
        db.lock()
            .unwrap()
            .execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, base_sha, diff_ref,
                    review_rev, review_json, updated_at
                 ) VALUES (
                    'j_rev', ?1, 'Leaf', '', 'feedback_pending', 1, 0, 'abc', 'def',
                    1, '{\"verdict\":\"comments\",\"summary\":\"ok\"}', '2026-11-25T00:00:00Z'
                 )",
                params![id],
            )
            .unwrap();
        let lines = run(&db, &cfg).unwrap();
        assert!(
            lines
                .iter()
                .any(|line| line == &format!("fixed html: plan {id} re-rendered")),
            "{lines:?}"
        );
        assert!(lines
            .iter()
            .any(|line| line == "fixed html: review j_rev re-rendered"));
        let plan = fs::read_to_string(cfg.docs_dir.join(&id).join("plan").join("r1.html")).unwrap();
        let review = fs::read_to_string(
            cfg.docs_dir
                .join(&id)
                .join("jobs")
                .join("j_rev")
                .join("review")
                .join("r1.html"),
        )
        .unwrap();
        assert_eq!(embedded_doc(&plan).unwrap()["item_id"], id);
        assert_eq!(embedded_doc(&review).unwrap()["job_id"], "j_rev");
        assert!(!cfg
            .publish_root
            .join("keel")
            .join(&id)
            .join("plan.html")
            .exists());
        assert!(!cfg
            .publish_root
            .join("keel")
            .join(&id)
            .join("review-j_rev.html")
            .exists());
        let again = run(&db, &cfg).unwrap();
        assert!(
            again.iter().all(|line| !line.contains("re-rendered")),
            "{again:?}"
        );

        let plan_path = cfg.docs_dir.join(&id).join("plan").join("r1.html");
        let wrong = plan.replace(&id, "k_other");
        fs::write(&plan_path, &wrong).unwrap();
        let published = cfg.publish_root.join("keel").join(&id).join("plan.html");
        fs::create_dir_all(published.parent().unwrap()).unwrap();
        fs::write(&published, b"not the canonical bytes").unwrap();
        let lines = run(&db, &cfg).unwrap();
        assert!(
            lines
                .iter()
                .any(|line| line.contains("embedded id") && line.contains("k_other")),
            "{lines:?}"
        );
        assert!(lines
            .iter()
            .any(|line| line.starts_with("warn published: plan") && line.contains("!=")));
        assert_eq!(fs::read_to_string(&plan_path).unwrap(), wrong);
        assert_eq!(fs::read(&published).unwrap(), b"not the canonical bytes");
    }

    #[test]
    fn reports_orphans_and_starttime_without_killing() {
        let (_rm, cfg, db) = fresh();
        let dropped = add_item(&db, None);
        let live = add_item(&db, None);
        write_card(&cfg, &dropped);
        write_card(&cfg, &live);
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET state = 'dropped' WHERE id = ?1",
                params![dropped],
            )
            .unwrap();
        db.lock()
            .unwrap()
            .execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, updated_at
                 ) VALUES ('j_dead', ?1, 'Gone', '', 'cancelled', 1, 0, '2026-11-25T00:00:00Z'),
                          ('j_live', ?1, 'Stay', '', 'queued', 1, 1, '2026-11-25T00:00:00Z')",
                params![live],
            )
            .unwrap();
        let dropped_tree = cfg.data_dir.join("worktrees").join(&dropped).join("plan");
        let dead_tree = cfg.data_dir.join("worktrees").join(&live).join("j_dead");
        let live_tree = cfg.data_dir.join("worktrees").join(&live).join("j_live");
        fs::create_dir_all(&dropped_tree).unwrap();
        fs::create_dir_all(&dead_tree).unwrap();
        fs::create_dir_all(&live_tree).unwrap();
        let sleeper = Command::new("sleep")
            .arg("300")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let pid = i64::from(sleeper.id());
        struct Kill(Option<std::process::Child>);
        impl Drop for Kill {
            fn drop(&mut self) {
                if let Some(child) = self.0.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
        let _kill = Kill(Some(sleeper));
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET claim_pid = ?1, claim_starttime = '1', claim_role = 'plan',
                        claim_token = 't' WHERE id = ?2",
                params![pid, live],
            )
            .unwrap();
        db.lock()
            .unwrap()
            .execute(
                "UPDATE jobs SET claim_pid = 2147483646, claim_starttime = '9' WHERE id = 'j_live'",
                [],
            )
            .unwrap();
        let lines = run(&db, &cfg).unwrap();
        let joined = lines.join("\n");
        assert!(
            joined.contains(&format!("item {dropped} dropped")),
            "{joined}"
        );
        assert!(joined.contains("job j_dead cancelled"), "{joined}");
        assert!(!joined.contains("job j_live cancelled"), "{joined}");
        assert!(
            joined.contains(&format!("pid {pid} starttime 1 !=")),
            "{joined}"
        );
        assert!(
            joined.contains("pid 2147483646 starttime 9 != dead"),
            "{joined}"
        );
        assert!(Path::new(&format!("/proc/{pid}")).is_dir());
        assert!(dropped_tree.is_dir());
        assert!(dead_tree.is_dir());
        assert!(live_tree.is_dir());
        let start = daemon::read_proc_starttime(pid as u32).unwrap();
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET claim_starttime = ?1 WHERE id = ?2",
                params![start, live],
            )
            .unwrap();
        let lines = run(&db, &cfg).unwrap();
        let joined = lines.join("\n");
        assert!(!joined.contains(&format!("pid {pid}")), "{joined}");
        assert!(Path::new(&format!("/proc/{pid}")).is_dir());
    }

    #[test]
    fn git_disagreement_is_reported_and_head_stays() {
        let (_rm, cfg, db) = fresh();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let id = add_item(&db, Some(repo.clone()));
        write_card(&cfg, &id);
        git(&repo, &["checkout", "-b", &format!("keel/{id}/integrate")]);
        fs::write(repo.join("leaf"), "x\n").unwrap();
        git(&repo, &["add", "leaf"]);
        git(&repo, &["commit", "-m", "leaf"]);
        git(&repo, &["checkout", "main"]);
        git(
            &repo,
            &[
                "merge",
                "--no-ff",
                "--no-edit",
                &format!("keel/{id}/integrate"),
            ],
        );
        let keel_sha = crate::git::rev_parse(&repo, "HEAD").unwrap();
        git(&repo, &["checkout", "-b", "side"]);
        fs::write(repo.join("side"), "y\n").unwrap();
        git(&repo, &["add", "side"]);
        git(&repo, &["commit", "-m", "side"]);
        git(&repo, &["checkout", "main"]);
        git(&repo, &["merge", "--no-ff", "--no-edit", "side"]);
        let side_sha = crate::git::rev_parse(&repo, "HEAD").unwrap();
        assert_ne!(keel_sha, side_sha);
        let lines = run(&db, &cfg).unwrap();
        let joined = lines.join("\n");
        assert!(
            joined.contains(&format!("{keel_sha} on {} is a keel merge", repo.display())),
            "{joined}"
        );
        assert!(!joined.contains(&side_sha), "{joined}");
        db.lock()
            .unwrap()
            .execute(
                "UPDATE items SET state = 'merged', merged_sha = ?1 WHERE id = ?2",
                params![keel_sha, id],
            )
            .unwrap();
        let lines = run(&db, &cfg).unwrap();
        assert!(lines.iter().any(|line| line == "git: ok"), "{lines:?}");
        git(&repo, &["reset", "--hard", &base]);
        let head_before = crate::git::rev_parse(&repo, "HEAD").unwrap();
        assert_eq!(head_before, base);
        let lines = run(&db, &cfg).unwrap();
        let joined = lines.join("\n");
        assert!(
            joined.contains(&format!(
                "item {id} merged_sha {keel_sha} is not an ancestor of primary HEAD"
            )),
            "{joined}"
        );
        assert_eq!(crate::git::rev_parse(&repo, "HEAD").unwrap(), head_before);
        let stored: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT merged_sha FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, keel_sha);
    }

    #[test]
    fn artifact_walk_counts_keel_and_warns_at_400() {
        let (_rm, cfg, db) = fresh();
        fs::create_dir_all(cfg.publish_root.join("other")).unwrap();
        fs::create_dir_all(cfg.publish_root.join("keel").join("k")).unwrap();
        fs::create_dir_all(cfg.publish_root.join("target")).unwrap();
        fs::write(cfg.publish_root.join("other").join("a.html"), b"a").unwrap();
        fs::write(cfg.publish_root.join("other").join("b.txt"), b"b").unwrap();
        fs::write(
            cfg.publish_root.join("keel").join("k").join("card.html"),
            b"c",
        )
        .unwrap();
        fs::write(cfg.publish_root.join("target").join("skip.html"), b"no").unwrap();
        fs::write(cfg.publish_root.join("note.bin"), b"no").unwrap();
        let lines = run(&db, &cfg).unwrap();
        assert!(
            lines
                .iter()
                .any(|line| line == "artifacts: 3 files, 1 under keel/, limit 400"),
            "{lines:?}"
        );
        for n in 0..400 {
            fs::write(
                cfg.publish_root.join("keel").join(format!("{n}.html")),
                b"x",
            )
            .unwrap();
        }
        let lines = run(&db, &cfg).unwrap();
        assert!(
            lines
                .iter()
                .any(|line| line.starts_with("warn artifacts: 400 files,")
                    && line.contains("under keel/")),
            "{lines:?}"
        );
    }

    #[test]
    fn doctor_route_and_startup_backup_leave_git_alone() {
        let _rm = crate::daemon::Rm(crate::daemon::scratch_dir());
        let (cfg, running) = crate::daemon::boot_at(&_rm.0).unwrap();
        let mut stream = UnixStream::connect(&cfg.socket_path).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let response = crate::http::exchange(&mut stream, "GET", "/v1/doctor", None).unwrap();
        // The socket accepts only after the startup copy. Connect alone can
        // succeed while that copy is still ahead of the accept loop.
        let bak = cfg.db_path.with_file_name("keel.db.bak");
        assert!(bak.is_file(), "startup did not copy keel.db");
        assert!(bak.starts_with(&cfg.data_dir));
        assert!(!bak.starts_with(&cfg.publish_root));
        assert!(bak
            .components()
            .all(|component| component.as_os_str() != "agent-artifacts"));
        assert_eq!(
            response.status,
            200,
            "{}",
            String::from_utf8_lossy(&response.body)
        );
        let body: Value = serde_json::from_slice(&response.body).unwrap();
        let lines: Vec<String> = body["lines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|line| line.as_str().unwrap().to_string())
            .collect();
        assert!(
            lines.iter().any(|line| line == "user_version: 1"),
            "{lines:?}"
        );
        assert!(lines.iter().any(|line| line == "git: ok"), "{lines:?}");
        let mut tcp = TcpStream::connect(("127.0.0.1", cfg.port)).unwrap();
        let tcp_response = crate::http::exchange(&mut tcp, "GET", "/v1/doctor", None).unwrap();
        assert_eq!(tcp_response.status, 404);
        drop(running);
    }

    fn git(repo: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env("GIT_AUTHOR_NAME", "keel")
            .env("GIT_AUTHOR_EMAIL", "keel@example.com")
            .env("GIT_COMMITTER_NAME", "keel")
            .env("GIT_COMMITTER_EMAIL", "keel@example.com")
            .status()
            .unwrap();
        assert!(status.success(), "{args:?} {status}");
    }
}
