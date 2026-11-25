//! Git invocations for the claim path. The primary checkout is never checked out,
//! committed, or reset here. Callers pass the repo; this module does not pick one.

use std::ffi::OsStr;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use crate::db::{Error, Result};

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
