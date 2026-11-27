//! Direct `claude` in the worktree. No `herdr` binary and no `HERDR_ENV`.
//! `--allowedTools` is Claude's own tool gate, not an OS sandbox.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::claim::{self, Target};
use crate::daemon::read_proc_starttime;
use crate::db::{Error, Result};
use crate::proc;
use crate::state::{self, Role};

const HEARTBEAT_EVERY: Duration = Duration::from_secs(60);
const COMPLETION_MARKER: &str = "__TASK_COMPLETE__";

const IMPLEMENT_TOOLS: &[&str] = &[
    "Read",
    "Write",
    "Edit",
    "Bash(git status *)",
    "Bash(git diff *)",
    "Bash(git add *)",
    "Bash(git commit *)",
    "Bash(git log *)",
];
const READ_ONLY_TOOLS: &[&str] = &["Read"];

#[derive(Clone)]
pub(crate) struct Launch {
    pub target: Target,
    pub id: String,
    pub role: Role,
    pub token: String,
    pub worktree: Option<PathBuf>,
    pub prompt_path: PathBuf,
}

pub(crate) struct RunDone {
    pub output: Option<String>,
}

pub(crate) fn ensure_claude(path: &str) -> Result<PathBuf> {
    for dir in path.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = Path::new(dir).join("claude");
        // execvp skips a file the caller cannot execute. A non-executable
        // earlier on PATH must not hide a later one, or spawn fails with
        // EACCES only after the lease exists.
        if can_exec(&candidate) {
            return Ok(candidate);
        }
    }
    Err(Error::Invalid("claude is not on PATH".into()))
}

/// Same permission check as `execvp`: owner, group, or other execute bit.
/// Root may execute when any execute bit is set. No bit means nobody can.
fn can_exec(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    let mode = meta.permissions().mode();
    if mode & 0o111 == 0 {
        return false;
    }
    let uid = unsafe { geteuid() };
    if uid == 0 {
        return true;
    }
    if meta.uid() == uid {
        return mode & 0o100 != 0;
    }
    if meta.gid() == unsafe { getegid() } || supplementary_groups().contains(&meta.gid()) {
        return mode & 0o010 != 0;
    }
    mode & 0o001 != 0
}

fn supplementary_groups() -> Vec<u32> {
    let n = unsafe { getgroups(0, std::ptr::null_mut()) };
    if n <= 0 {
        return Vec::new();
    }
    let mut groups = vec![0u32; n as usize];
    let got = unsafe { getgroups(n, groups.as_mut_ptr()) };
    if got < 0 {
        return Vec::new();
    }
    groups.truncate(got as usize);
    groups
}

extern "C" {
    fn geteuid() -> u32;
    fn getegid() -> u32;
    fn getgroups(size: i32, list: *mut u32) -> i32;
}

pub(crate) fn ensure_claude_on_path() -> Result<PathBuf> {
    ensure_claude(&std::env::var("PATH").unwrap_or_default())
}

pub(crate) fn execute(
    db: &Mutex<rusqlite::Connection>,
    data_dir: &Path,
    launch: &Launch,
) -> Result<RunDone> {
    if !crate::render::safe_id(&launch.id) {
        return Err(Error::Invalid("id is not a path segment".into()));
    }
    // data_dir holds keel.db and log secrets. Plan and revise of an item with
    // no repo have no worktree; do not use that directory as cwd or target_repo.
    let Some(cwd) = launch.worktree.clone() else {
        return Err(Error::Invalid(
            "refusing to run claude without a worktree".into(),
        ));
    };
    let bin = ensure_claude_on_path()?;
    let scope = load_scope(db, launch)?;
    if let Some(repo) = scope.repo.as_deref() {
        if same_path(Path::new(repo), &cwd) {
            return Err(Error::Invalid(
                "refusing to point claude at the primary checkout".into(),
            ));
        }
    }
    let mode = mode_from_plan(launch.role, scope.plan_json.as_deref())?;
    // The column is the implementer's gate. A review run uses `plan` on the
    // command line and must not replace `auto` before review HTML can show it.
    if launch.role == Role::Implement && launch.target == Target::Job {
        let mut conn = lock_db(db)?;
        state::set_job_permission_mode(&mut conn, &launch.id, &launch.token, &mode)?;
    }
    let _ = fs::create_dir_all(data_dir.join("results"));
    let output_path = data_dir.join("results").join(format!("{}.json", launch.id));
    let bundle_path = data_dir.join("bundles").join(format!("{}.md", launch.id));
    let prompt = fs::read_to_string(&launch.prompt_path).unwrap_or_default();
    let commands = validation_commands(scope.validation_json.as_deref())?;
    let text = render_bundle(&BundleSpec {
        task: &task_name(&scope.item_id, scope.job_id.as_deref(), launch.role),
        description: &scope.title,
        target_repo: cwd
            .to_str()
            .ok_or_else(|| Error::Invalid("worktree path is not utf-8".into()))?,
        branch: &bundle_branch(&scope.item_id, scope.job_id.as_deref(), launch.role),
        permission_mode: &mode,
        allow_subagents: launch.role == Role::Implement,
        allowed_tools: tools_for(launch.role),
        owned_files: owned_for(launch.role),
        prohibited_actions: actions_for(launch.role),
        commands: &commands,
        prompt: &prompt,
    });
    write_private(&bundle_path, text.as_bytes())?;
    let args = claude_argv(
        &mode,
        tools_for(launch.role),
        &exec_message(&bundle_path, &output_path),
    );
    let log_path = data_dir.join("logs").join(format!("{}.log", launch.id));
    let spawned = spawn_grouped(&bin, &args, &cwd, &log_path)?;
    let mut held = ChildGuard {
        child: Some(spawned.child),
        pid: spawned.pid,
        pgid: spawned.pgid,
        settled: false,
    };
    let recorded = {
        let mut conn = lock_db(db)?;
        state::attach_process(
            &mut conn,
            &launch.id,
            &launch.token,
            spawned.pid,
            spawned.pgid,
            &spawned.starttime,
        )
    };
    if let Err(err) = recorded {
        return stop_held(&mut held, err);
    }
    let mut child = held
        .child
        .take()
        .ok_or_else(|| Error::Invalid("claude child is missing".into()))?;
    let stop = Arc::new(AtomicBool::new(false));
    let status = thread::scope(|scope| {
        let flag = Arc::clone(&stop);
        let id = launch.id.clone();
        let token = launch.token.clone();
        scope.spawn(move || heartbeat_loop(db, launch.target, id, token, flag));
        let status = child.wait();
        stop.store(true, Ordering::SeqCst);
        status
    })?;
    match held.reap() {
        Err(live) => return Err(live),
        Ok(()) => {}
    }
    if !status.success() {
        return Err(Error::Invalid(format!("claude exited ({status})")));
    }
    Ok(RunDone {
        output: read_output(&output_path)?,
    })
}

/// `plan` for plan, revise, and review. Implement is `auto` unless the item's
/// plan JSON names `plan`, `auto`, or `acceptEdits`. `always-approve` is refused.
pub(crate) fn mode_from_plan(role: Role, plan_json: Option<&str>) -> Result<String> {
    let override_mode = item_permission_override(plan_json)?;
    match role {
        Role::Plan | Role::Revise | Role::Review => Ok("plan".into()),
        Role::Implement => match override_mode.as_deref() {
            None => Ok("auto".into()),
            Some(mode) if matches!(mode, "plan" | "auto" | "acceptEdits") => Ok(mode.into()),
            Some(mode) => Err(Error::Invalid(format!("refusing permission_mode {mode}"))),
        },
    }
}

pub(crate) fn tools_for(role: Role) -> &'static [&'static str] {
    match role {
        Role::Implement => IMPLEMENT_TOOLS,
        Role::Plan | Role::Revise | Role::Review => READ_ONLY_TOOLS,
    }
}

fn owned_for(role: Role) -> &'static [&'static str] {
    match role {
        Role::Implement => &["."],
        Role::Plan | Role::Revise | Role::Review => &[],
    }
}

fn actions_for(role: Role) -> &'static [&'static str] {
    match role {
        Role::Implement => &["push", "deploy"],
        Role::Plan | Role::Revise | Role::Review => &["push", "deploy", "commit"],
    }
}

fn task_name(item_id: &str, job_id: Option<&str>, role: Role) -> String {
    format!(
        "keel-{}-{}-{}",
        item_id,
        job_id.unwrap_or("none"),
        role.as_str()
    )
}

fn bundle_branch(item_id: &str, job_id: Option<&str>, role: Role) -> String {
    match role {
        Role::Implement | Role::Review => {
            format!("keel/{item_id}/{}", job_id.unwrap_or("none"))
        }
        // The template's default is `main`. Plan and revise are detached and
        // must not be told to check that out.
        Role::Plan | Role::Revise => "detached".into(),
    }
}

pub(crate) fn claude_argv(mode: &str, tools: &[&str], message: &str) -> Vec<String> {
    let mut args = vec!["--permission-mode".into(), mode.into()];
    for tool in tools {
        args.push("--allowedTools".into());
        args.push((*tool).to_string());
    }
    args.push("--message".into());
    args.push(message.to_string());
    args
}

fn exec_message(bundle: &Path, output: &Path) -> String {
    format!(
        "Read {} and do exactly that scope. Write the Keel result JSON to {}. Do not git checkout and do not git push.",
        bundle.display(),
        output.display()
    )
}

struct BundleSpec<'a> {
    task: &'a str,
    description: &'a str,
    target_repo: &'a str,
    branch: &'a str,
    permission_mode: &'a str,
    allow_subagents: bool,
    allowed_tools: &'a [&'a str],
    owned_files: &'a [&'a str],
    prohibited_actions: &'a [&'a str],
    commands: &'a [String],
    prompt: &'a str,
}

fn render_bundle(spec: &BundleSpec) -> String {
    let mut out = String::from("---\n");
    push_str(&mut out, "task", spec.task);
    push_str(&mut out, "description", spec.description);
    push_str(&mut out, "target_repo", spec.target_repo);
    push_str(&mut out, "branch", spec.branch);
    push_str(&mut out, "permission_mode", spec.permission_mode);
    out.push_str("allow_subagents: ");
    out.push_str(if spec.allow_subagents {
        "true"
    } else {
        "false"
    });
    out.push('\n');
    push_list(&mut out, "allowed_tools", spec.allowed_tools, 0);
    push_list(&mut out, "owned_files", spec.owned_files, 0);
    out.push_str("prohibited_files: []\n");
    push_list(&mut out, "prohibited_actions", spec.prohibited_actions, 0);
    out.push_str("validation:\n");
    if spec.commands.is_empty() {
        out.push_str("  commands: []\n");
    } else {
        let refs: Vec<&str> = spec.commands.iter().map(String::as_str).collect();
        push_list(&mut out, "commands", &refs, 2);
    }
    push_str(&mut out, "completion_marker", COMPLETION_MARKER);
    out.push_str("review_required: true\n");
    out.push_str("---\n");
    if !spec.prompt.is_empty() {
        out.push_str(spec.prompt);
        if !spec.prompt.ends_with('\n') {
            out.push('\n');
        }
    }
    out
}

fn push_str(out: &mut String, key: &str, value: &str) {
    out.push_str(key);
    out.push_str(": ");
    out.push_str(&yaml_quote(value));
    out.push('\n');
}

fn push_list(out: &mut String, key: &str, values: &[&str], indent: usize) {
    let pad = " ".repeat(indent);
    if values.is_empty() {
        out.push_str(&pad);
        out.push_str(key);
        out.push_str(": []\n");
        return;
    }
    out.push_str(&pad);
    out.push_str(key);
    out.push_str(":\n");
    for value in values {
        out.push_str(&pad);
        out.push_str("  - ");
        out.push_str(&yaml_quote(value));
        out.push('\n');
    }
}

fn yaml_quote(text: &str) -> String {
    let mut out = String::from("\"");
    for ch in text.chars() {
        match ch {
            '\\' | '"' => {
                out.push('\\');
                out.push(ch);
            }
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn item_permission_override(plan_json: Option<&str>) -> Result<Option<String>> {
    let Some(text) = plan_json.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    let value: Value =
        serde_json::from_str(text).map_err(|err| Error::Invalid(format!("plan_json: {err}")))?;
    match value.get("permission_mode") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(mode)) => Ok(Some(mode.clone())),
        Some(_) => Err(Error::Invalid("permission_mode must be a string".into())),
    }
}

pub(crate) fn validation_commands(text: Option<&str>) -> Result<Vec<String>> {
    let Some(text) = text.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(Vec::new());
    };
    let value: Value = serde_json::from_str(text)
        .map_err(|err| Error::Invalid(format!("validation_json: {err}")))?;
    let list = value
        .as_array()
        .ok_or_else(|| Error::Invalid("validation_json must be an array of commands".into()))?;
    let mut commands = Vec::with_capacity(list.len());
    for entry in list {
        let command = entry
            .as_str()
            .ok_or_else(|| Error::Invalid("validation command must be a string".into()))?;
        if command.is_empty() {
            return Err(Error::Invalid("validation command is empty".into()));
        }
        commands.push(command.to_string());
    }
    Ok(commands)
}

struct Scope {
    item_id: String,
    job_id: Option<String>,
    title: String,
    validation_json: Option<String>,
    plan_json: Option<String>,
    repo: Option<String>,
}

fn load_scope(db: &Mutex<rusqlite::Connection>, launch: &Launch) -> Result<Scope> {
    let conn = lock_db(db)?;
    match launch.target {
        Target::Job => conn.query_row(
            "SELECT i.id, i.title, i.validation_json, i.plan_json, i.repo
             FROM jobs j JOIN items i ON i.id = j.item_id
             WHERE j.id = ?1",
            rusqlite::params![launch.id],
            |row| {
                Ok(Scope {
                    item_id: row.get(0)?,
                    job_id: Some(launch.id.clone()),
                    title: row.get(1)?,
                    validation_json: row.get(2)?,
                    plan_json: row.get(3)?,
                    repo: row.get(4)?,
                })
            },
        ),
        Target::Item => conn.query_row(
            "SELECT id, title, validation_json, plan_json, repo FROM items WHERE id = ?1",
            rusqlite::params![launch.id],
            |row| {
                Ok(Scope {
                    item_id: row.get(0)?,
                    job_id: None,
                    title: row.get(1)?,
                    validation_json: row.get(2)?,
                    plan_json: row.get(3)?,
                    repo: row.get(4)?,
                })
            },
        ),
    }
    .map_err(|err| match err {
        rusqlite::Error::QueryReturnedNoRows => Error::NotFound(launch.id.clone()),
        other => other.into(),
    })
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

struct SpawnedProc {
    child: Child,
    pid: i64,
    pgid: i64,
    starttime: String,
}

struct ChildGuard {
    child: Option<Child>,
    pid: i64,
    pgid: i64,
    settled: bool,
}

impl ChildGuard {
    fn reap(&mut self) -> Result<()> {
        if self.settled && self.child.is_none() {
            return Ok(());
        }
        let result = proc::empty_group(self.pgid, self.pid);
        self.settled = true;
        if result.is_ok() {
            if let Some(mut child) = self.child.take() {
                let _ = child.wait();
            }
        }
        result
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.settled {
            let _ = self.reap();
        }
    }
}

fn stop_held<T>(held: &mut ChildGuard, err: Error) -> Result<T> {
    match held.reap() {
        Err(live) => Err(live),
        Ok(()) => Err(err),
    }
}

fn spawn_grouped(
    program: &Path,
    args: &[String],
    cwd: &Path,
    log_path: &Path,
) -> Result<SpawnedProc> {
    if let Some(parent) = log_path.parent() {
        ensure_private_dir(parent)?;
    }
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(log_path)?;
    let err = log.try_clone()?;
    let mut cmd = Command::new(program);
    cmd.args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .process_group(0);
    // Herdr's launcher refuses to start unless this is set. This exec is not Herdr.
    cmd.env_remove("HERDR_ENV");
    // A parent GIT_DIR would commit outside the worktree the bundle names.
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
    let mut child = cmd.spawn()?;
    let pid = i64::from(child.id());
    let spawned = (|| -> Result<(String, i64)> {
        let starttime = read_proc_starttime(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned claude has no starttime".into()))?;
        let pgid = read_pgrp(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned claude has no pgid".into()))?;
        if pgid != pid {
            return Err(Error::Invalid(
                "spawned claude pgid does not equal pid".into(),
            ));
        }
        Ok((starttime, pgid))
    })();
    match spawned {
        Ok((starttime, pgid)) => Ok(SpawnedProc {
            child,
            pid,
            pgid,
            starttime,
        }),
        Err(err) => match proc::empty_group(pid, pid) {
            Ok(()) => {
                let _ = child.wait();
                Err(err)
            }
            Err(live) => Err(live),
        },
    }
}

fn read_pgrp(pid: u32) -> Option<i64> {
    crate::psinfo::read_pgrp(pid)
}

fn heartbeat_loop(
    db: &Mutex<rusqlite::Connection>,
    target: Target,
    id: String,
    token: String,
    stop: Arc<AtomicBool>,
) {
    let mut next = Instant::now() + HEARTBEAT_EVERY;
    while !stop.load(Ordering::SeqCst) {
        if Instant::now() >= next {
            let _ = claim::heartbeat(db, target, &id, &token);
            next = Instant::now() + HEARTBEAT_EVERY;
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn read_output(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(path)?;
    if text.len() > 256 * 1024 {
        return Err(Error::Invalid("output exceeds 256 KiB".into()));
    }
    if text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(text))
}

fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)?;
    Ok(())
}

fn lock_db(
    db: &Mutex<rusqlite::Connection>,
) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::dispatch;
    use crate::daemon::{boot_at, scratch_dir, Rm};
    use crate::db::open;
    use rusqlite::params;
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::Mutex as StdMutex;

    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    #[test]
    fn argv_is_permission_mode_then_each_tool_then_message() {
        let message = exec_message(Path::new("/tmp/bundle.md"), Path::new("/tmp/out.json"));
        let args = claude_argv("auto", IMPLEMENT_TOOLS, &message);
        assert_eq!(args[0], "--permission-mode");
        assert_eq!(args[1], "auto");
        assert_eq!(args[args.len() - 2], "--message");
        assert_eq!(args[args.len() - 1], message);
        let mut tools = Vec::new();
        let mut i = 2;
        while i + 1 < args.len() && args[i] == "--allowedTools" {
            tools.push(args[i + 1].as_str());
            i += 2;
        }
        assert_eq!(tools, IMPLEMENT_TOOLS);
        assert!(args
            .iter()
            .all(|arg| arg != "herdr" && !arg.contains("HERDR")));
        assert!(tools
            .iter()
            .all(|tool| *tool != "Bash" && !tool.contains("push")));
        let plan = claude_argv("plan", tools_for(Role::Review), "read");
        assert_eq!(
            plan,
            vec![
                "--permission-mode",
                "plan",
                "--allowedTools",
                "Read",
                "--message",
                "read",
            ]
        );
        assert_eq!(tools_for(Role::Plan), READ_ONLY_TOOLS);
        assert_eq!(tools_for(Role::Revise), READ_ONLY_TOOLS);
        assert_eq!(actions_for(Role::Implement), ["push", "deploy"]);
        assert_eq!(actions_for(Role::Plan), ["push", "deploy", "commit"]);
    }

    #[test]
    fn bundle_fields_are_the_table_and_not_the_template_defaults() {
        let commands = vec!["cargo test -q".into()];
        let text = render_bundle(&BundleSpec {
            task: "keel-k_1-j_1-implement",
            description: "Pin \"the\" walk",
            target_repo: "/tmp/wt",
            branch: "keel/k_1/j_1",
            permission_mode: "auto",
            allow_subagents: true,
            allowed_tools: IMPLEMENT_TOOLS,
            owned_files: &["."],
            prohibited_actions: actions_for(Role::Implement),
            commands: &commands,
            prompt: "Role: implement\nDiff file: /tmp/j.diff\n",
        });
        assert!(text.starts_with("---\n"));
        assert!(text.contains("task: \"keel-k_1-j_1-implement\"\n"));
        assert!(text.contains("description: \"Pin \\\"the\\\" walk\"\n"));
        assert!(text.contains("target_repo: \"/tmp/wt\"\n"));
        assert!(text.contains("branch: \"keel/k_1/j_1\"\n"));
        assert!(!text.contains("branch: \"main\""));
        assert!(text.contains("permission_mode: \"auto\"\n"));
        assert!(text.contains("allow_subagents: true\n"));
        assert!(text.contains("  - \"Bash(git commit *)\"\n"));
        assert!(text.contains("owned_files:\n  - \".\"\n"));
        assert!(text.contains("prohibited_files: []\n"));
        assert!(text.contains("prohibited_actions:\n  - \"push\"\n  - \"deploy\"\n"));
        assert!(!text.contains("\"commit\""));
        assert!(text.contains("validation:\n  commands:\n    - \"cargo test -q\"\n"));
        assert!(text.contains("completion_marker: \"__TASK_COMPLETE__\"\n"));
        assert!(text.contains("review_required: true\n"));
        assert!(text.contains("\n---\nRole: implement\nDiff file: /tmp/j.diff\n"));
        for banned in [
            "devenv",
            "Cargo.toml",
            "flake.nix",
            "flake.lock",
            "herdr",
            "HERDR",
            "diff_check",
            "require_tests",
        ] {
            assert!(!text.contains(banned), "{banned} leaked into {text}");
        }
        let plan = render_bundle(&BundleSpec {
            task: "keel-k_1-none-plan",
            description: "Plan",
            target_repo: "/tmp/plan-wt",
            branch: "detached",
            permission_mode: "plan",
            allow_subagents: false,
            allowed_tools: READ_ONLY_TOOLS,
            owned_files: &[],
            prohibited_actions: actions_for(Role::Plan),
            commands: &[],
            prompt: "",
        });
        assert!(plan.contains("branch: \"detached\"\n"));
        assert!(plan.contains("allow_subagents: false\n"));
        assert!(plan.contains("allowed_tools:\n  - \"Read\"\n"));
        assert!(plan.contains("owned_files: []\n"));
        assert!(plan.contains("  - \"commit\"\n"));
        assert!(plan.contains("  commands: []\n"));
        assert!(!plan.contains("Bash"));
    }

    #[test]
    fn implement_mode_is_auto_unless_the_item_overrides_it() {
        assert_eq!(mode_from_plan(Role::Implement, None).unwrap(), "auto");
        assert_eq!(
            mode_from_plan(Role::Implement, Some(r#"{"summary":"s"}"#)).unwrap(),
            "auto"
        );
        assert_eq!(
            mode_from_plan(
                Role::Implement,
                Some(r#"{"summary":"s","permission_mode":"acceptEdits"}"#)
            )
            .unwrap(),
            "acceptEdits"
        );
        assert_eq!(
            mode_from_plan(Role::Review, Some(r#"{"permission_mode":"auto"}"#)).unwrap(),
            "plan"
        );
        assert_eq!(mode_from_plan(Role::Plan, None).unwrap(), "plan");
        assert_eq!(mode_from_plan(Role::Revise, None).unwrap(), "plan");
        let refused = mode_from_plan(
            Role::Implement,
            Some(r#"{"permission_mode":"always-approve"}"#),
        )
        .unwrap_err();
        assert!(refused.to_string().contains("refusing"), "{refused}");
        assert!(validation_commands(None).unwrap().is_empty());
        assert_eq!(
            validation_commands(Some("[\"cargo test\"]")).unwrap(),
            vec!["cargo test".to_string()]
        );
        assert!(validation_commands(Some("\"devenv shell -- cargo test\""))
            .unwrap_err()
            .to_string()
            .contains("array"));
    }

    #[test]
    fn missing_claude_is_a_clear_error() {
        let err = ensure_claude("/no/such/bin").unwrap_err();
        assert!(err.to_string().contains("claude is not on PATH"), "{err}");
    }

    #[test]
    fn non_executable_claude_does_not_shadow_a_later_one() {
        let dir = std::env::temp_dir().join(format!("keel-claude-exec-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let earlier = dir.join("earlier");
        let later = dir.join("later");
        fs::create_dir_all(&earlier).unwrap();
        fs::create_dir_all(&later).unwrap();
        let shadow = earlier.join("claude");
        fs::write(&shadow, b"not executable\n").unwrap();
        let mut plain = fs::metadata(&shadow).unwrap().permissions();
        plain.set_mode(0o644);
        fs::set_permissions(&shadow, plain).unwrap();
        let real = later.join("claude");
        fs::write(&real, b"#!/bin/sh\nexit 0\n").unwrap();
        let mut exec = fs::metadata(&real).unwrap().permissions();
        exec.set_mode(0o755);
        fs::set_permissions(&real, exec).unwrap();
        let path = format!("{}:{}", earlier.display(), later.display());
        assert_eq!(ensure_claude(&path).unwrap(), real);

        let owner_only = dir.join("owner");
        fs::create_dir_all(&owner_only).unwrap();
        let owned = owner_only.join("claude");
        fs::write(&owned, b"#!/bin/sh\n").unwrap();
        let mut owner_exec = fs::metadata(&owned).unwrap().permissions();
        owner_exec.set_mode(0o100);
        fs::set_permissions(&owned, owner_exec).unwrap();
        assert_eq!(ensure_claude(owner_only.to_str().unwrap()).unwrap(), owned);

        // We own the file, so the other-execute bit does not make it runnable.
        // Root is allowed to exec any file that has an execute bit.
        if unsafe { geteuid() } != 0 {
            let mut other_only = fs::metadata(&owned).unwrap().permissions();
            other_only.set_mode(0o001);
            fs::set_permissions(&owned, other_only).unwrap();
            let err = ensure_claude(owner_only.to_str().unwrap()).unwrap_err();
            assert!(err.to_string().contains("claude is not on PATH"), "{err}");
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn plan_without_a_worktree_is_refused_before_spawn() {
        let db = Mutex::new(rusqlite::Connection::open_in_memory().unwrap());
        let refused = |role| {
            execute(
                &db,
                Path::new("/tmp/keel-data-must-not-be-cwd"),
                &Launch {
                    target: Target::Item,
                    id: "item".into(),
                    role,
                    token: "tok".into(),
                    worktree: None,
                    prompt_path: PathBuf::from("/tmp/keel-no-such-prompt"),
                },
            )
        };
        for role in [Role::Plan, Role::Revise] {
            match refused(role) {
                Err(err) => assert!(err.to_string().contains("without a worktree"), "{err}"),
                Ok(_) => panic!("claude ran with no worktree"),
            }
        }
    }

    #[test]
    fn spawned_child_is_its_own_process_group() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let dir = std::env::temp_dir().join(format!("keel-claude-pg-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("child.log");
        let sleep = ensure_tool("sleep");
        let spawned = spawn_grouped(&sleep, &["30".into()], &dir, &log).unwrap();
        let pid = spawned.pid;
        let pgid = spawned.pgid;
        let mut guard = ChildGuard {
            child: Some(spawned.child),
            pid,
            pgid,
            settled: false,
        };
        assert_eq!(pid, pgid);
        assert_eq!(read_pgrp(pid as u32), Some(pgid));
        guard.reap().unwrap();
        assert!(proc::group_empty(pgid).unwrap());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn fake_claude_plans_implements_reviews_and_fails_clean_or_dirty() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let _saved = EnvSaved::capture();
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let repo = _rm.0.join("repo");
        let head = crate::git::init_repo(&repo);
        let bin = _rm.0.join("bin");
        let script = bin.join("claude");
        write_script(&script);
        prepend_path(&bin);
        // Prove the child does not inherit Herdr's gate even when the parent has it.
        std::env::set_var("HERDR_ENV", "1");
        std::env::set_var("KEEL_CLAUDE_FAKE", "plan");
        let plan_trace = _rm.0.join("plan.trace");
        std::env::set_var("KEEL_CLAUDE_TRACE", &plan_trace);

        dispatch(
            &cfg,
            &[
                "add".into(),
                "--repo".into(),
                repo.to_str().unwrap().into(),
                "--branch".into(),
                "main".into(),
                "--body".into(),
                "untrusted body".into(),
                "Claude the walk".into(),
            ],
        )
        .unwrap();
        let item: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        dispatch(
            &cfg,
            &[
                "run".into(),
                item.clone(),
                "--role".into(),
                "plan".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_item(&cfg, &item, "awaiting_plan_review");
        let plan_wt = cfg.data_dir.join("worktrees").join(&item).join("plan");
        let plan_bundle =
            fs::read_to_string(cfg.data_dir.join("bundles").join(format!("{item}.md"))).unwrap();
        let plan_seen = read_trace(&plan_trace);
        assert_eq!(plan_seen.herdr, "unset");
        assert_eq!(canon(Path::new(&plan_seen.arg0)), canon(&script));
        assert!(script.is_file());
        assert!(fs::read_to_string(&script)
            .unwrap()
            .starts_with("#!/bin/sh\n"));
        assert_eq!(canon(Path::new(&plan_seen.cwd)), canon(&plan_wt));
        assert_eq!(plan_seen.mode, "plan");
        assert_eq!(plan_seen.tools, vec!["Read".to_string()]);
        assert!(
            plan_seen.message.contains("git checkout"),
            "{:?}",
            plan_seen.message
        );
        assert!(
            plan_seen.message.contains("git push"),
            "{:?}",
            plan_seen.message
        );
        assert!(plan_seen.message.contains(&plan_bundle_path(&cfg, &item)));
        assert!(!plan_seen.args.iter().any(|arg| arg.contains("herdr")));
        assert!(plan_bundle.contains(&format!("target_repo: \"{}\"\n", plan_wt.display())));
        assert!(!plan_bundle.contains(&format!("target_repo: \"{}\"", repo.display())));
        assert!(plan_bundle.contains("branch: \"detached\"\n"));
        assert!(!plan_bundle.contains("branch: \"main\""));
        assert!(plan_bundle.contains("permission_mode: \"plan\"\n"));
        assert!(plan_bundle.contains("allow_subagents: false\n"));
        assert!(plan_bundle.contains("owned_files: []\n"));
        assert!(plan_bundle.contains("prohibited_files: []\n"));
        assert!(plan_bundle.contains("  commands: []\n"));
        assert!(plan_bundle.contains("untrusted body"));
        for banned in [
            "devenv",
            "Cargo.toml",
            "flake.nix",
            "flake.lock",
            "herdr",
            "HERDR",
        ] {
            assert!(!plan_bundle.contains(banned), "{banned}");
        }
        assert_primary(&repo, &head);

        let base = crate::git::rev_parse(&repo, "HEAD").unwrap();
        dispatch(
            &cfg,
            &[
                "approve".into(),
                item.clone(),
                "--base".into(),
                base.clone(),
            ],
        )
        .unwrap();
        let jobs = job_ids(&cfg, &item);
        assert_eq!(jobs.len(), 3, "{jobs:?}");
        let walk = jobs.iter().find(|job| job.title == "Walk").unwrap();
        let clean = jobs.iter().find(|job| job.title == "FailClean").unwrap();
        let dirty = jobs.iter().find(|job| job.title == "FailDirty").unwrap();

        std::env::set_var("KEEL_CLAUDE_FAKE", "implement");
        let impl_trace = _rm.0.join("impl.trace");
        std::env::set_var("KEEL_CLAUDE_TRACE", &impl_trace);
        dispatch(
            &cfg,
            &[
                "run".into(),
                walk.id.clone(),
                "--role".into(),
                "implement".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_job(&cfg, &walk.id, "ready_for_review");
        let impl_wt = cfg.data_dir.join("worktrees").join(&item).join(&walk.id);
        let impl_seen = read_trace(&impl_trace);
        let impl_bundle =
            fs::read_to_string(cfg.data_dir.join("bundles").join(format!("{}.md", walk.id)))
                .unwrap();
        let branch = format!("keel/{item}/{}", walk.id);
        assert_eq!(impl_seen.herdr, "unset");
        assert_eq!(canon(Path::new(&impl_seen.arg0)), canon(&script));
        assert_eq!(canon(Path::new(&impl_seen.cwd)), canon(&impl_wt));
        assert_eq!(impl_seen.mode, "auto");
        assert_eq!(
            impl_seen.tools,
            IMPLEMENT_TOOLS
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>()
        );
        assert!(impl_bundle.contains(&format!("target_repo: \"{}\"\n", impl_wt.display())));
        assert!(impl_bundle.contains(&format!("branch: \"{branch}\"\n")));
        assert!(!impl_bundle.contains("branch: \"main\""));
        assert!(impl_bundle.contains("allow_subagents: true\n"));
        assert!(impl_bundle.contains("owned_files:\n  - \".\"\n"));
        assert!(impl_bundle.contains("  commands: []\n"));
        assert!(!impl_bundle.contains("\"commit\""));
        assert!(!impl_bundle.contains("devenv"));
        let tree = crate::git::snapshot(&impl_wt).unwrap();
        assert_eq!(tree.branch, branch);
        assert_ne!(tree.head, base);
        assert!(tree.porcelain.is_empty());
        assert_eq!(job_mode(&cfg, &walk.id).as_deref(), Some("auto"));
        assert_primary(&repo, &head);

        std::env::set_var("KEEL_CLAUDE_FAKE", "fail");
        let fail_trace = _rm.0.join("fail.trace");
        std::env::set_var("KEEL_CLAUDE_TRACE", &fail_trace);
        dispatch(
            &cfg,
            &[
                "run".into(),
                clean.id.clone(),
                "--role".into(),
                "implement".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_trace(&fail_trace);
        wait_job_settled(&cfg, &clean.id, "queued", None);
        let clean_wt = cfg.data_dir.join("worktrees").join(&item).join(&clean.id);
        let clean_tree = crate::git::snapshot(&clean_wt).unwrap();
        assert!(clean_tree.porcelain.is_empty());
        assert_eq!(clean_tree.head, base);
        let (claim, blocked): (Option<String>, Option<String>) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token, blocked_reason FROM jobs WHERE id = ?1",
                params![clean.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(claim.is_none());
        assert!(blocked.is_none());
        assert_eq!(read_trace(&fail_trace).herdr, "unset");
        assert_primary(&repo, &head);

        std::env::set_var("KEEL_CLAUDE_FAKE", "dirty");
        let dirty_trace = _rm.0.join("dirty.trace");
        std::env::set_var("KEEL_CLAUDE_TRACE", &dirty_trace);
        dispatch(
            &cfg,
            &[
                "run".into(),
                dirty.id.clone(),
                "--role".into(),
                "implement".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_trace(&dirty_trace);
        wait_job_settled(&cfg, &dirty.id, "implementing", Some("dirty"));
        let dirty_wt = cfg.data_dir.join("worktrees").join(&item).join(&dirty.id);
        let dirty_tree = crate::git::snapshot(&dirty_wt).unwrap();
        assert!(!dirty_tree.porcelain.is_empty());
        assert!(dirty_wt.join("dirty-on-purpose.txt").is_file());
        let (claim, blocked): (Option<String>, Option<String>) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token, blocked_reason FROM jobs WHERE id = ?1",
                params![dirty.id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(claim.is_none());
        assert_eq!(blocked.as_deref(), Some("dirty"));
        assert_eq!(read_trace(&dirty_trace).mode, "auto");
        assert_primary(&repo, &head);

        std::env::set_var("KEEL_CLAUDE_FAKE", "review");
        let review_trace = _rm.0.join("review.trace");
        std::env::set_var("KEEL_CLAUDE_TRACE", &review_trace);
        dispatch(
            &cfg,
            &[
                "run".into(),
                walk.id.clone(),
                "--role".into(),
                "review".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_job(&cfg, &walk.id, "feedback_pending");
        let review_wt = cfg
            .data_dir
            .join("worktrees")
            .join(&item)
            .join(format!("{}-review", walk.id));
        let review_seen = read_trace(&review_trace);
        let review_bundle =
            fs::read_to_string(cfg.data_dir.join("bundles").join(format!("{}.md", walk.id)))
                .unwrap();
        let prompt = fs::read_to_string(
            cfg.data_dir
                .join("prompts")
                .join(format!("{}.txt", walk.id)),
        )
        .unwrap();
        assert_eq!(review_seen.herdr, "unset");
        assert_eq!(canon(Path::new(&review_seen.cwd)), canon(&review_wt));
        assert_eq!(review_seen.mode, "plan");
        assert_eq!(review_seen.tools, vec!["Read".to_string()]);
        assert!(review_bundle.contains(&format!("target_repo: \"{}\"\n", review_wt.display())));
        assert!(review_bundle.contains(&format!("branch: \"{branch}\"\n")));
        assert!(!review_bundle.contains("branch: \"main\""));
        assert!(review_bundle.contains("permission_mode: \"plan\"\n"));
        assert!(prompt.contains("Diff file: "));
        assert!(!prompt.contains("Diff file: unset"));
        let diff_line = prompt
            .lines()
            .find(|line| line.starts_with("Diff file: "))
            .unwrap()
            .to_string();
        assert!(
            review_bundle.contains(&diff_line),
            "{diff_line} not in bundle"
        );
        let diff_path = diff_line.trim_start_matches("Diff file: ");
        assert!(Path::new(diff_path).is_file(), "{diff_path}");
        assert_ne!(canon(Path::new(diff_path)), canon(&repo));
        let review_tree = crate::git::snapshot(&review_wt).unwrap();
        assert_eq!(review_tree.branch, "HEAD");
        assert_eq!(review_tree.head, base);
        assert!(review_tree.porcelain.is_empty());
        // Review's argv mode is plan. The stored mode stays the implementer's.
        assert_eq!(job_mode(&cfg, &walk.id).as_deref(), Some("auto"));
        let review_json: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT review_json FROM jobs WHERE id = ?1",
                params![walk.id],
                |row| row.get(0),
            )
            .unwrap();
        let review_value: Value = serde_json::from_str(&review_json).unwrap();
        assert!(review_value.as_object().is_some());
        assert_primary(&repo, &head);
        drop(running);
    }

    #[test]
    fn item_override_is_what_implement_stores() {
        let _env = ENV_LOCK.lock().unwrap_or_else(|err| err.into_inner());
        let _saved = EnvSaved::capture();
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let repo = _rm.0.join("repo");
        crate::git::init_repo(&repo);
        let bin = _rm.0.join("bin");
        write_script(&bin.join("claude"));
        prepend_path(&bin);
        std::env::set_var("HERDR_ENV", "1");
        std::env::set_var("KEEL_CLAUDE_FAKE", "plan-override");
        std::env::set_var("KEEL_CLAUDE_TRACE", _rm.0.join("ovr-plan.trace"));
        dispatch(
            &cfg,
            &[
                "add".into(),
                "--repo".into(),
                repo.to_str().unwrap().into(),
                "--branch".into(),
                "main".into(),
                "Override".into(),
            ],
        )
        .unwrap();
        let item: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        dispatch(
            &cfg,
            &[
                "run".into(),
                item.clone(),
                "--role".into(),
                "plan".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_item(&cfg, &item, "awaiting_plan_review");
        let base = crate::git::rev_parse(&repo, "HEAD").unwrap();
        dispatch(
            &cfg,
            &["approve".into(), item.clone(), "--base".into(), base],
        )
        .unwrap();
        let jobs = job_ids(&cfg, &item);
        assert_eq!(jobs.len(), 1);
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET validation_json = ?1 WHERE id = ?2",
                params!["[\"echo ok\"]", item],
            )
            .unwrap();
        std::env::set_var("KEEL_CLAUDE_FAKE", "implement");
        std::env::set_var("KEEL_CLAUDE_TRACE", _rm.0.join("ovr-impl.trace"));
        dispatch(
            &cfg,
            &[
                "run".into(),
                jobs[0].id.clone(),
                "--role".into(),
                "implement".into(),
                "--adapter".into(),
                "claude".into(),
            ],
        )
        .unwrap();
        wait_job(&cfg, &jobs[0].id, "ready_for_review");
        assert_eq!(job_mode(&cfg, &jobs[0].id).as_deref(), Some("acceptEdits"));
        let bundle = fs::read_to_string(
            cfg.data_dir
                .join("bundles")
                .join(format!("{}.md", jobs[0].id)),
        )
        .unwrap();
        assert!(bundle.contains("permission_mode: \"acceptEdits\"\n"));
        assert!(bundle.contains("    - \"echo ok\"\n"));
        assert!(!bundle.contains("devenv"));
        assert_eq!(
            read_trace(&_rm.0.join("ovr-impl.trace")).mode,
            "acceptEdits"
        );
        drop(running);
    }

    struct EnvSaved {
        path: Option<String>,
        herdr: Option<String>,
        fake: Option<String>,
        trace: Option<String>,
    }

    impl EnvSaved {
        fn capture() -> Self {
            Self {
                path: std::env::var("PATH").ok(),
                herdr: std::env::var("HERDR_ENV").ok(),
                fake: std::env::var("KEEL_CLAUDE_FAKE").ok(),
                trace: std::env::var("KEEL_CLAUDE_TRACE").ok(),
            }
        }
    }

    impl Drop for EnvSaved {
        fn drop(&mut self) {
            restore("PATH", self.path.as_deref());
            restore("HERDR_ENV", self.herdr.as_deref());
            restore("KEEL_CLAUDE_FAKE", self.fake.as_deref());
            restore("KEEL_CLAUDE_TRACE", self.trace.as_deref());
        }
    }

    fn restore(key: &str, value: Option<&str>) {
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
    }

    fn prepend_path(bin: &Path) {
        let mut path = bin.display().to_string();
        if let Ok(old) = std::env::var("PATH") {
            if !old.is_empty() {
                path.push(':');
                path.push_str(&old);
            }
        }
        std::env::set_var("PATH", path);
    }

    fn write_script(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o755)
            .open(path)
            .unwrap();
        file.write_all(FAKE_CLAUDE.as_bytes()).unwrap();
    }

    struct Seen {
        cwd: String,
        herdr: String,
        arg0: String,
        mode: String,
        tools: Vec<String>,
        message: String,
        args: Vec<String>,
    }

    fn read_trace(path: &Path) -> Seen {
        let text = fs::read_to_string(path)
            .unwrap_or_else(|err| panic!("trace {}: {err}", path.display()));
        let mut cwd = String::new();
        let mut herdr = String::new();
        let mut arg0 = String::new();
        let mut args = Vec::new();
        let mut in_args = false;
        for line in text.lines() {
            if line == "begin-args" {
                in_args = true;
                continue;
            }
            if line == "end-args" {
                break;
            }
            if in_args {
                args.push(line.to_string());
                continue;
            }
            if let Some(value) = line.strip_prefix("cwd=") {
                cwd = value.to_string();
            } else if let Some(value) = line.strip_prefix("HERDR_ENV=") {
                herdr = value.to_string();
            } else if let Some(value) = line.strip_prefix("arg0=") {
                arg0 = value.to_string();
            }
        }
        let mut mode = String::new();
        let mut tools = Vec::new();
        let mut message = String::new();
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--permission-mode" => {
                    mode = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                "--allowedTools" => {
                    tools.push(args.get(i + 1).cloned().unwrap_or_default());
                    i += 2;
                }
                "--message" => {
                    message = args.get(i + 1).cloned().unwrap_or_default();
                    i += 2;
                }
                _ => i += 1,
            }
        }
        Seen {
            cwd,
            herdr,
            arg0,
            mode,
            tools,
            message,
            args,
        }
    }

    fn canon(path: &Path) -> PathBuf {
        fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
    }

    fn plan_bundle_path(cfg: &crate::config::Config, item: &str) -> String {
        cfg.data_dir
            .join("bundles")
            .join(format!("{item}.md"))
            .display()
            .to_string()
    }

    fn assert_primary(repo: &Path, head: &str) {
        let snap = crate::git::snapshot(repo).unwrap();
        assert_eq!(snap.head, head);
        assert_eq!(snap.branch, "main");
        assert!(snap.porcelain.is_empty(), "{}", snap.porcelain);
    }

    #[derive(Debug)]
    struct NamedJob {
        id: String,
        title: String,
    }

    fn job_ids(cfg: &crate::config::Config, item: &str) -> Vec<NamedJob> {
        let conn = open(&cfg.db_path).unwrap();
        let mut stmt = conn
            .prepare("SELECT id, title FROM jobs WHERE item_id = ?1 ORDER BY ord")
            .unwrap();
        stmt.query_map(params![item], |row| {
            Ok(NamedJob {
                id: row.get(0)?,
                title: row.get(1)?,
            })
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
    }

    fn job_mode(cfg: &crate::config::Config, id: &str) -> Option<String> {
        open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT permission_mode FROM jobs WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn wait_item(cfg: &crate::config::Config, id: &str, want: &str) {
        let state = wait_column(cfg, "SELECT state FROM items WHERE id = ?1", id, want);
        assert_eq!(state, want, "item {id} log:\n{}", log_of(cfg, id));
    }

    fn wait_job(cfg: &crate::config::Config, id: &str, want: &str) {
        let state = wait_column(cfg, "SELECT state FROM jobs WHERE id = ?1", id, want);
        assert_eq!(state, want, "job {id} log:\n{}", log_of(cfg, id));
    }

    /// `queued` and `implementing` are also the pre-run states, so wait until
    /// the child has finished and the failure transition has cleared the claim.
    fn wait_job_settled(
        cfg: &crate::config::Config,
        id: &str,
        state_want: &str,
        blocked: Option<&str>,
    ) {
        let mut last = String::new();
        for _ in 0..200 {
            let (state, reason, claim): (String, Option<String>, Option<String>) =
                open(&cfg.db_path)
                    .unwrap()
                    .query_row(
                        "SELECT state, blocked_reason, claim_token FROM jobs WHERE id = ?1",
                        params![id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
            last = format!("{state} blocked={reason:?} claim={claim:?}");
            let reason_ok = blocked == reason.as_deref();
            if state == state_want && claim.is_none() && reason_ok {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("job {id} stayed {last}\n{}", log_of(cfg, id));
    }

    fn wait_trace(path: &Path) {
        for _ in 0..200 {
            if path.is_file() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("trace {} was not written", path.display());
    }

    fn wait_column(cfg: &crate::config::Config, sql: &str, id: &str, want: &str) -> String {
        let mut last = String::new();
        for _ in 0..200 {
            last = open(&cfg.db_path)
                .unwrap()
                .query_row(sql, params![id], |row| row.get(0))
                .unwrap();
            if last == want {
                return last;
            }
            thread::sleep(Duration::from_millis(50));
        }
        last
    }

    fn log_of(cfg: &crate::config::Config, id: &str) -> String {
        fs::read_to_string(cfg.data_dir.join("logs").join(format!("{id}.log"))).unwrap_or_default()
    }

    fn ensure_tool(name: &str) -> PathBuf {
        for dir in std::env::var("PATH").unwrap_or_default().split(':') {
            let candidate = Path::new(dir).join(name);
            if candidate.is_file() {
                return candidate;
            }
        }
        PathBuf::from(name)
    }

    const FAKE_CLAUDE: &str = r#"#!/bin/sh
set -eu
trace=${KEEL_CLAUDE_TRACE:?}
{
  printf 'cwd=%s\n' "$(pwd)"
  if [ -n "${HERDR_ENV+x}" ]; then
    printf 'HERDR_ENV=%s\n' "$HERDR_ENV"
  else
    printf 'HERDR_ENV=unset\n'
  fi
  printf 'arg0=%s\n' "$0"
  printf 'begin-args\n'
  for arg in "$@"; do
    printf '%s\n' "$arg"
  done
  printf 'end-args\n'
} > "$trace"
mode=${KEEL_CLAUDE_FAKE:-plan}
msg=
prev=
for arg in "$@"; do
  if [ "$prev" = "--message" ]; then
    msg=$arg
  fi
  prev=$arg
done
case "$mode" in
  fail)
    exit 1
    ;;
  dirty)
    printf 'dirt\n' > dirty-on-purpose.txt
    exit 1
    ;;
  implement)
    printf 'claude\n' > CLAUDE_OK
    git add -- CLAUDE_OK
    git commit -m 'claude implement'
    ;;
esac
prefix='Write the Keel result JSON to '
suffix='. Do not git checkout and do not git push.'
case "$msg" in
  *"$prefix"*) ;;
  *)
    echo 'missing output path' >&2
    exit 3
    ;;
esac
out=${msg#*"$prefix"}
out=${out%"$suffix"}
case "$mode" in
  review)
    printf '%s\n' '{"verdict":"comments","summary":"read the diff"}' > "$out"
    ;;
  plan-override)
    printf '%s\n' '{"summary":"override","permission_mode":"acceptEdits","jobs":[{"title":"One","acceptance":"done"}]}' > "$out"
    ;;
  *)
    printf '%s\n' '{"summary":"from claude","jobs":[{"title":"Walk","acceptance":"listed"},{"title":"FailClean","acceptance":"clean"},{"title":"FailDirty","acceptance":"dirty"}],"resolved_point_ids":[]}' > "$out"
    ;;
esac
exit 0
"#;
}
