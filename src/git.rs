//! Git invocations for the claim path and the human merge.
//! Plan, implement, and review never commit the primary. The only primary
//! commit is `keel merge`, and the only primary reset is `git merge --abort`
//! after that merge conflicts. Callers pass the repo; this module does not pick one.

use std::ffi::OsStr;
use std::fs::File;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard};

use rusqlite::Connection;

use crate::db::{Error, Result};
use crate::state::{self, ConflictKind};

/// Review diffs kept on disk. The prompt carries `--stat` when this truncates.
pub(crate) const DIFF_CAP: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrimarySnapshot {
    pub head: String,
    pub branch: String,
    pub porcelain: String,
}

pub(crate) fn snapshot(repo: &Path) -> Result<PrimarySnapshot> {
    let head = rev_parse(repo, "HEAD")?;
    let branch = git_text(repo, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let porcelain = git_text(repo, &["status", "--porcelain"])?;
    Ok(PrimarySnapshot {
        head,
        branch: branch.trim_end().to_string(),
        porcelain,
    })
}

pub(crate) fn rev_parse(repo: &Path, rev: &str) -> Result<String> {
    check_rev(rev)?;
    let text = git_text(repo, &["rev-parse", "--verify", rev])?;
    let sha = text.trim();
    if sha.len() != 40 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Invalid(format!(
            "rev {rev} did not resolve to a commit"
        )));
    }
    Ok(sha.to_string())
}

pub(crate) fn add_detached(repo: &Path, dest: &Path, rev: &str) -> Result<()> {
    check_rev(rev)?;
    let dest = path_arg(dest)?;
    git_ok(repo, &["worktree", "add", "--detach", dest, rev])?;
    Ok(())
}

pub(crate) fn add_branch(repo: &Path, dest: &Path, branch: &str, rev: &str) -> Result<()> {
    check_rev(branch)?;
    check_rev(rev)?;
    let dest = path_arg(dest)?;
    git_ok(repo, &["worktree", "add", "-b", branch, dest, rev])?;
    Ok(())
}

/// Attach a checkout to a branch that already exists. Does not create or delete it.
pub(crate) fn add_existing_branch(repo: &Path, dest: &Path, branch: &str) -> Result<()> {
    check_rev(branch)?;
    let dest = path_arg(dest)?;
    git_ok(repo, &["worktree", "add", dest, branch])?;
    Ok(())
}

pub(crate) fn branch_exists(repo: &Path, branch: &str) -> Result<bool> {
    check_rev(branch)?;
    let spec = format!("refs/heads/{branch}");
    let output = git_cmd(repo, &["show-ref", "--verify", "--quiet", &spec])?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(git_failure(
            &["show-ref", "--verify", "--quiet", &spec],
            &output,
        )),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LinkedCheckout {
    pub head: String,
    /// `HEAD` when the checkout is detached.
    pub branch: String,
}

/// `Ok(None)` when `dest` is absent. An existing path that is not this repo's
/// checkout is an error: the caller must not delete it to force `worktree add`.
pub(crate) fn linked_checkout(repo: &Path, dest: &Path) -> Result<Option<LinkedCheckout>> {
    if !dest.exists() {
        return Ok(None);
    }
    let dest_common = git_text(
        dest,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )
    .map_err(|_| {
        Error::Invalid(format!(
            "worktree path {} exists and is not a checkout of this repo",
            dest.display()
        ))
    })?;
    let repo_common = git_text(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    if !same_dir(dest_common.trim(), repo_common.trim())? {
        return Err(Error::Invalid(format!(
            "worktree path {} belongs to a different repository",
            dest.display()
        )));
    }
    let head = rev_parse(dest, "HEAD")?;
    let branch = git_text(dest, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    Ok(Some(LinkedCheckout {
        head,
        branch: branch.trim_end().to_string(),
    }))
}

fn same_dir(left: &str, right: &str) -> Result<bool> {
    let left = std::fs::canonicalize(left)?;
    let right = std::fs::canonicalize(right)?;
    Ok(left == right)
}

pub(crate) fn remove_worktree(repo: &Path, dest: &Path) -> Result<()> {
    // A release of a claim that never created the slot is still success.
    if !dest.exists() {
        return Ok(());
    }
    let dest = path_arg(dest)?;
    git_ok(repo, &["worktree", "remove", "--force", dest])?;
    Ok(())
}

pub(crate) fn delete_branch(repo: &Path, branch: &str) -> Result<()> {
    check_rev(branch)?;
    git_ok(repo, &["branch", "-D", branch])?;
    Ok(())
}

/// `git diff` exits 1 when the trees differ. That is a successful read.
pub(crate) fn diff_bytes(repo: &Path, base: &str, other: &str) -> Result<Vec<u8>> {
    check_rev(base)?;
    check_rev(other)?;
    git_diff(repo, &["diff", "--no-color", base, other])
}

pub(crate) fn diff_stat(repo: &Path, base: &str, other: &str) -> Result<String> {
    check_rev(base)?;
    check_rev(other)?;
    let bytes = git_diff(repo, &["diff", "--no-color", "--stat", base, other])?;
    String::from_utf8(bytes).map_err(|_| Error::Invalid("git diff --stat is not utf-8".into()))
}

fn check_rev(rev: &str) -> Result<()> {
    let ok = !rev.is_empty()
        && rev.len() <= 200
        && !rev.starts_with('-')
        && !rev.contains("..")
        && !rev.contains('@')
        && rev
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '/' || c == '_' || c == '-' || c == '.');
    if !ok {
        return Err(Error::Invalid(format!("ref {rev:?} is not a safe git rev")));
    }
    Ok(())
}

fn path_arg(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::Invalid("path is not utf-8".into()))
}

fn git_text(repo: &Path, args: &[&str]) -> Result<String> {
    let output = git_ok(repo, args)?;
    String::from_utf8(output.stdout).map_err(|_| Error::Invalid("git output is not utf-8".into()))
}

fn git_ok(repo: &Path, args: &[&str]) -> Result<Output> {
    let output = git_cmd(repo, args)?;
    if output.status.success() {
        return Ok(output);
    }
    Err(git_failure(args, &output))
}

fn git_diff(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = git_cmd(repo, args)?;
    match output.status.code() {
        Some(0 | 1) => Ok(output.stdout),
        _ => Err(git_failure(args, &output)),
    }
}

fn git_failure(args: &[&str], output: &Output) -> Error {
    let err = String::from_utf8_lossy(&output.stderr);
    let err = err.trim();
    Error::Invalid(format!("git {}: {err}", args.join(" ")))
}

fn git_cmd(repo: &Path, args: &[&str]) -> Result<Output> {
    let mut cmd = Command::new("git");
    // Hooks run before the primary snapshot. A post-checkout hook could move
    // that checkout. Color would also spend the review diff cap on ANSI.
    cmd.arg("--no-pager")
        .arg("-c")
        .arg("core.hooksPath=/dev/null")
        .arg("-c")
        .arg("color.ui=false")
        // A merge must not block the daemon on an editor.
        .arg("-c")
        .arg("core.editor=true")
        .arg("-C")
        .arg(repo);
    cmd.args(args.iter().copied().map(OsStr::new));
    // A parent GIT_DIR would make `-C` operate on the wrong repository.
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
    ] {
        cmd.env_remove(key);
    }
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    cmd.stdin(Stdio::null());
    cmd.output().map_err(Error::from)
}

const LOCK_EX: i32 = 2;
const LOCK_UN: i32 = 8;

extern "C" {
    fn flock(fd: i32, op: i32) -> i32;
}

/// Exclusive lock on the primary's common git dir for the whole merge.
/// Two items can share a checkout. The SQLite mutex only covers the row write.
struct GitLock {
    file: File,
}

impl Drop for GitLock {
    fn drop(&mut self) {
        unsafe { flock(self.file.as_raw_fd(), LOCK_UN) };
    }
}

pub(crate) enum MergeMode {
    Plain,
    Continue,
    Abort,
}

pub(crate) struct MergeReport {
    pub id: String,
    pub state: String,
    pub merged_sha: Option<String>,
    pub integrate_sha: Option<String>,
    pub conflict_kind: Option<String>,
    pub worktree: PathBuf,
}

enum MergeResult {
    Clean,
    Conflict,
}

pub(crate) fn merge_local(
    db: &Mutex<Connection>,
    data_dir: &Path,
    item_id: &str,
    mode: MergeMode,
) -> Result<MergeReport> {
    if !crate::render::safe_id(item_id) {
        return Err(Error::Invalid("id is not a path segment".into()));
    }
    let (facts, _) = load_facts(db, item_id)?;
    gate(&facts.state, &mode)?;
    let locked = facts.repo.clone();
    // Held until this function returns, including the conflict paths.
    let _lock = lock_common(&locked)?;
    let (mut facts, leaves) = load_facts(db, item_id)?;
    gate(&facts.state, &mode)?;
    if facts.repo != locked {
        return Err(Error::Invalid("repo changed; retry merge".into()));
    }
    let branch = integrate_branch(item_id)?;
    let worktree = data_dir.join("worktrees").join(item_id).join("integrate");
    match mode {
        MergeMode::Abort => throw_integrate(db, item_id, &facts, &branch, &worktree)?,
        MergeMode::Continue => {
            continue_integrate(db, item_id, &mut facts, &branch, &worktree)?;
            primary_phase(db, item_id, &facts, &branch, &worktree)?;
        }
        MergeMode::Plain => {
            if !resume_leaves(db, item_id, &mut facts, &leaves, &branch, &worktree)? {
                primary_phase(db, item_id, &facts, &branch, &worktree)?;
            }
        }
    }
    report(db, item_id, worktree)
}

fn throw_integrate(
    db: &Mutex<Connection>,
    item_id: &str,
    facts: &state::MergeFacts,
    branch: &str,
    worktree: &Path,
) -> Result<()> {
    // Reset the integrate checkout only. The primary is not a worktree we move.
    if let Some(found) = linked_checkout(&facts.repo, worktree)? {
        if found.branch != branch {
            return Err(Error::Invalid(format!(
                "integrate worktree is on {}, not {branch}",
                found.branch
            )));
        }
        git_ok(worktree, &["reset", "--hard", &facts.base_sha])?;
        let head = rev_parse(worktree, "HEAD")?;
        if head != facts.base_sha {
            return Err(Error::Invalid(
                "integrate worktree did not reset to base_sha".into(),
            ));
        }
    } else if branch_exists(&facts.repo, branch)? {
        let spec = format!("refs/heads/{branch}");
        git_ok(&facts.repo, &["update-ref", &spec, &facts.base_sha])?;
    }
    let mut conn = lock_db(db)?;
    state::merge_abort(&mut conn, item_id)
}

fn continue_integrate(
    db: &Mutex<Connection>,
    item_id: &str,
    facts: &mut state::MergeFacts,
    branch: &str,
    worktree: &Path,
) -> Result<()> {
    let Some(found) = linked_checkout(&facts.repo, worktree)? else {
        return Err(Error::Invalid(format!(
            "integrate worktree is missing: {}",
            worktree.display()
        )));
    };
    if found.branch != branch {
        return Err(Error::Invalid(format!(
            "integrate worktree is on {}, not {branch}",
            found.branch
        )));
    }
    if unmerged(worktree)? {
        return Err(Error::Invalid(format!(
            "unmerged paths in {}",
            worktree.display()
        )));
    }
    if merge_head(worktree)? {
        // The index is resolved. Finish that merge; do not require the human to commit.
        git_ok(worktree, &["commit", "--no-edit"])?;
        let head = rev_parse(worktree, "HEAD")?;
        store_integrate(db, item_id, facts, &head, None)?;
        return Ok(());
    }
    let porcelain = git_text(worktree, &["status", "--porcelain"])?;
    if !porcelain.trim().is_empty() {
        return Err(Error::Invalid("integrate worktree is dirty".into()));
    }
    let anchor = facts
        .integrate_sha
        .clone()
        .unwrap_or_else(|| facts.base_sha.clone());
    let head = rev_parse(worktree, "HEAD")?;
    if head == anchor {
        // The human aborted the leaf merge in place. `--continue` would retry it forever.
        if facts.conflict_kind.as_deref() == Some("integrate") {
            return Err(Error::Invalid(
                "integrate HEAD did not move; run keel merge --abort".into(),
            ));
        }
        return Ok(());
    }
    if !is_ancestor(worktree, &anchor, "HEAD")? {
        return Err(Error::Invalid(
            "integrate HEAD is not a descendant of integrate_sha".into(),
        ));
    }
    store_integrate(db, item_id, facts, &head, None)
}

/// `Ok(true)` when a leaf conflict is now the human's to finish.
fn resume_leaves(
    db: &Mutex<Connection>,
    item_id: &str,
    facts: &mut state::MergeFacts,
    leaves: &[state::MergeLeaf],
    branch: &str,
    worktree: &Path,
) -> Result<bool> {
    if leaves.is_empty() && facts.integrate_sha.is_none() {
        return Err(Error::Invalid("no accepted leaves to merge".into()));
    }
    let pending: Vec<&state::MergeLeaf> = leaves
        .iter()
        .filter(|leaf| {
            facts
                .integrate_ord
                .map(|done| leaf.ord > done)
                .unwrap_or(true)
        })
        .collect();
    for leaf in &pending {
        if leaf.branch.as_deref().unwrap_or("").is_empty() {
            return Err(Error::Invalid(format!("job {} has no branch", leaf.id)));
        }
    }
    let expected = facts
        .integrate_sha
        .clone()
        .unwrap_or_else(|| facts.base_sha.clone());
    ensure_integrate(&facts.repo, worktree, branch, &expected)?;
    if merge_head(worktree)? || unmerged(worktree)? {
        // A crash between the conflict and the row write. Do not start another merge.
        mark_conflict(db, item_id, ConflictKind::Integrate)?;
        return Ok(true);
    }
    let porcelain = git_text(worktree, &["status", "--porcelain"])?;
    if !porcelain.trim().is_empty() {
        return Err(Error::Invalid("integrate worktree is dirty".into()));
    }
    for leaf in pending {
        let leaf_branch = leaf.branch.as_deref().unwrap_or("");
        match merge_ref(worktree, leaf_branch, true)? {
            MergeResult::Clean => {
                let head = rev_parse(worktree, "HEAD")?;
                // Record before the next leaf so a crash cannot merge this ord twice.
                store_integrate(db, item_id, facts, &head, Some(leaf.ord))?;
            }
            MergeResult::Conflict => {
                mark_conflict(db, item_id, ConflictKind::Integrate)?;
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn primary_phase(
    db: &Mutex<Connection>,
    item_id: &str,
    facts: &state::MergeFacts,
    branch: &str,
    worktree: &Path,
) -> Result<()> {
    let repo = &facts.repo;
    // One retry. A second primary conflict stops; this command does not loop.
    for attempt in 0..2 {
        if let Some(reason) = primary_block(repo, &facts.target_branch, &facts.base_sha)? {
            return Err(Error::Invalid(reason));
        }
        if !integrate_ready(worktree)? {
            return Err(Error::Invalid("integrate worktree is not clean".into()));
        }
        match merge_ref(repo, branch, true)? {
            MergeResult::Clean => {
                commit_merged(db, repo, item_id)?;
                return Ok(());
            }
            MergeResult::Conflict => {
                // The primary must be clean before we touch integrate.
                git_ok(repo, &["merge", "--abort"])?;
                if !primary_clean(repo)? {
                    return Err(Error::Invalid(
                        "primary checkout was not clean after merge --abort".into(),
                    ));
                }
                if attempt == 1 {
                    mark_conflict(db, item_id, ConflictKind::Primary)?;
                    return Ok(());
                }
                let head = rev_parse(repo, "HEAD")?;
                match merge_ref(worktree, &head, false)? {
                    MergeResult::Clean => {
                        let sha = rev_parse(worktree, "HEAD")?;
                        store_sha(db, item_id, &sha, None)?;
                    }
                    MergeResult::Conflict => {
                        mark_conflict(db, item_id, ConflictKind::Primary)?;
                        return Ok(());
                    }
                }
            }
        }
    }
    Ok(())
}

fn commit_merged(db: &Mutex<Connection>, repo: &Path, item_id: &str) -> Result<()> {
    let sha = rev_parse(repo, "HEAD")?;
    let jobs = {
        let mut conn = lock_db(db)?;
        let (facts, _) = state::merge_facts(&mut conn, item_id)?;
        let jobs = state::job_checkouts(&conn, item_id)?;
        // The row is merged before any worktree is removed. Deleting a checkout
        // is not what makes the merge true.
        if facts.state == "merge_conflict" {
            state::merge_continue(&mut conn, item_id, &sha)?;
        } else {
            state::merge_item(&mut conn, item_id, &sha)?;
        }
        jobs
    };
    for job in jobs {
        if let Some(path) = job.worktree.as_ref().filter(|path| path.exists()) {
            remove_worktree(repo, path)?;
        }
        if let Some(branch) = job.branch.as_deref().filter(|branch| !branch.is_empty()) {
            if branch_exists(repo, branch)? {
                delete_branch(repo, branch)?;
            }
        }
    }
    Ok(())
}

fn ensure_integrate(repo: &Path, worktree: &Path, branch: &str, expected: &str) -> Result<()> {
    if let Some(found) = linked_checkout(repo, worktree)? {
        if found.branch != branch {
            return Err(Error::Invalid(format!(
                "integrate worktree is on {}, not {branch}",
                found.branch
            )));
        }
        // A plain merge does not adopt a commit the human made on the side.
        if found.head != expected {
            return Err(Error::Invalid(format!(
                "integrate HEAD {} does not match {expected}",
                found.head
            )));
        }
        return Ok(());
    }
    if branch_exists(repo, branch)? {
        let head = rev_parse(repo, branch)?;
        if head != expected {
            return Err(Error::Invalid(format!(
                "integrate ref {head} does not match {expected}"
            )));
        }
        add_existing_branch(repo, worktree, branch)?;
        return Ok(());
    }
    if let Some(parent) = worktree.parent() {
        std::fs::create_dir_all(parent)?;
    }
    add_branch(repo, worktree, branch, expected)
}

fn primary_block(repo: &Path, target_branch: &str, base_sha: &str) -> Result<Option<String>> {
    let snap = snapshot(repo)?;
    if !snap.porcelain.trim().is_empty() {
        return Ok(Some("primary checkout is dirty".into()));
    }
    if snap.branch != target_branch {
        return Ok(Some(format!(
            "primary branch is {}, not {target_branch}",
            snap.branch
        )));
    }
    if !is_ancestor(repo, base_sha, "HEAD")? {
        return Ok(Some("base_sha is not an ancestor of primary HEAD".into()));
    }
    Ok(None)
}

fn primary_clean(repo: &Path) -> Result<bool> {
    if merge_head(repo)? || unmerged(repo)? {
        return Ok(false);
    }
    let porcelain = git_text(repo, &["status", "--porcelain"])?;
    Ok(porcelain.trim().is_empty())
}

fn integrate_ready(worktree: &Path) -> Result<bool> {
    if !worktree.exists() || merge_head(worktree)? || unmerged(worktree)? {
        return Ok(false);
    }
    let porcelain = git_text(worktree, &["status", "--porcelain"])?;
    Ok(porcelain.trim().is_empty())
}

fn merge_ref(repo: &Path, rev: &str, no_ff: bool) -> Result<MergeResult> {
    check_rev(rev)?;
    let mut args = vec!["merge", "--no-edit"];
    if no_ff {
        args.push("--no-ff");
    }
    args.push(rev);
    let output = git_cmd(repo, &args)?;
    if output.status.success() {
        return Ok(MergeResult::Clean);
    }
    // A content conflict leaves MERGE_HEAD. Anything else must not look like one.
    if merge_head(repo)? || unmerged(repo)? {
        return Ok(MergeResult::Conflict);
    }
    Err(git_failure(&args, &output))
}

fn store_integrate(
    db: &Mutex<Connection>,
    item_id: &str,
    facts: &mut state::MergeFacts,
    sha: &str,
    ord: Option<i64>,
) -> Result<()> {
    store_sha(db, item_id, sha, ord)?;
    facts.integrate_sha = Some(sha.to_string());
    if let Some(ord) = ord {
        facts.integrate_ord = Some(ord);
    }
    Ok(())
}

fn store_sha(db: &Mutex<Connection>, item_id: &str, sha: &str, ord: Option<i64>) -> Result<()> {
    let mut conn = lock_db(db)?;
    state::record_integrate(&mut conn, item_id, sha, ord)
}

fn mark_conflict(db: &Mutex<Connection>, item_id: &str, kind: ConflictKind) -> Result<()> {
    let mut conn = lock_db(db)?;
    state::mark_merge_conflict(&mut conn, item_id, kind)
}

fn report(db: &Mutex<Connection>, item_id: &str, worktree: PathBuf) -> Result<MergeReport> {
    let mut conn = lock_db(db)?;
    let (facts, _) = state::merge_facts(&mut conn, item_id)?;
    Ok(MergeReport {
        id: item_id.to_string(),
        state: facts.state,
        merged_sha: facts.merged_sha,
        integrate_sha: facts.integrate_sha,
        conflict_kind: facts.conflict_kind,
        worktree,
    })
}

fn load_facts(
    db: &Mutex<Connection>,
    item_id: &str,
) -> Result<(state::MergeFacts, Vec<state::MergeLeaf>)> {
    let mut conn = lock_db(db)?;
    state::merge_facts(&mut conn, item_id)
}

fn gate(state: &str, mode: &MergeMode) -> Result<()> {
    let message = match mode {
        MergeMode::Plain if state != "awaiting_merge" => "merge requires awaiting_merge",
        MergeMode::Continue if state != "merge_conflict" => {
            "merge --continue requires merge_conflict"
        }
        MergeMode::Abort if state != "merge_conflict" => "merge --abort requires merge_conflict",
        _ => return Ok(()),
    };
    Err(Error::Conflict {
        state: state.to_string(),
        message: message.to_string(),
    })
}

fn integrate_branch(item_id: &str) -> Result<String> {
    let branch = format!("keel/{item_id}/integrate");
    check_rev(&branch)?;
    Ok(branch)
}

fn lock_common(repo: &Path) -> Result<GitLock> {
    let dir = git_text(
        repo,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    let dir = dir.trim();
    if dir.is_empty() {
        return Err(Error::Invalid("git common dir is empty".into()));
    }
    let file = File::open(dir)?;
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX) };
    if rc != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(GitLock { file })
}

fn lock_db(db: &Mutex<Connection>) -> Result<MutexGuard<'_, Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

fn is_ancestor(repo: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    check_rev(ancestor)?;
    check_rev(descendant)?;
    let output = git_cmd(repo, &["merge-base", "--is-ancestor", ancestor, descendant])?;
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(git_failure(
            &["merge-base", "--is-ancestor", ancestor, descendant],
            &output,
        )),
    }
}

fn unmerged(repo: &Path) -> Result<bool> {
    let text = git_text(repo, &["ls-files", "--unmerged"])?;
    Ok(!text.trim().is_empty())
}

fn merge_head(repo: &Path) -> Result<bool> {
    let path = git_text(repo, &["rev-parse", "--git-path", "MERGE_HEAD"])?;
    let path = path.trim();
    if path.is_empty() {
        return Ok(false);
    }
    let full = Path::new(path);
    let full = if full.is_absolute() {
        full.to_path_buf()
    } else {
        repo.join(full)
    };
    Ok(full.exists())
}

#[cfg(test)]
pub(crate) fn init_repo(path: &Path) -> String {
    std::fs::create_dir_all(path).unwrap();
    git_ok(path, &["init", "-b", "main"]).unwrap();
    git_ok(path, &["config", "user.email", "keel@example.com"]).unwrap();
    git_ok(path, &["config", "user.name", "keel"]).unwrap();
    std::fs::write(path.join("README"), "one\n").unwrap();
    git_ok(path, &["add", "README"]).unwrap();
    git_ok(path, &["commit", "-m", "init"]).unwrap();
    rev_parse(path, "HEAD").unwrap()
}

#[cfg(test)]
pub(crate) fn commit_file(repo: &Path, name: &str, bytes: &[u8]) -> String {
    std::fs::write(repo.join(name), bytes).unwrap();
    git_ok(repo, &["add", "--", name]).unwrap();
    git_ok(repo, &["commit", "-m", name]).unwrap();
    rev_parse(repo, "HEAD").unwrap()
}
