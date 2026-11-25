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

pub(crate) fn remove_worktree(repo: &Path, dest: &Path) -> Result<()> {
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
    git_diff(repo, &["diff", base, other])
}

pub(crate) fn diff_stat(repo: &Path, base: &str, other: &str) -> Result<String> {
    check_rev(base)?;
    check_rev(other)?;
    let bytes = git_diff(repo, &["diff", "--stat", base, other])?;
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
    cmd.arg("--no-pager").arg("-C").arg(repo);
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
