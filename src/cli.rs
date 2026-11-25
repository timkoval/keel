//! Human commands. Every read and the one write go through the socket.
//! A missing or refused socket starts `keel serve` once; it does not open SQLite.

use std::env;
use std::fs::{self, File};
use std::io::{self, IsTerminal, Read};
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::config::{self, Config};
use crate::daemon::{self, ITEM_STATES};
use crate::db::{Error, Result};
use crate::http;

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let config = config::load()?;
    dispatch(&config, &args)
}

pub(crate) fn dispatch(config: &Config, args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        None => {
            eprintln!("{}", usage());
            Err(Error::Invalid("missing command".into()))
        }
        Some("-h" | "--help" | "help") => {
            println!("{}", usage());
            Ok(())
        }
        Some("serve") => {
            if args.len() != 1 {
                return Err(Error::Invalid("serve takes no arguments".into()));
            }
            let stop = std::sync::atomic::AtomicBool::new(false);
            daemon::serve(config, &stop)
        }
        Some("add") => add(config, &args[1..]),
        Some("inbox") => {
            no_args("inbox", args)?;
            list(config, Some("inbox"))
        }
        Some("list") => list(config, parse_state(&args[1..])?),
        Some("show") => {
            let id = one_id("show", args)?;
            let item = get_item(config, id)?;
            print!("{}", render_show(&item));
            Ok(())
        }
        Some("url") => {
            let id = one_id("url", args)?;
            let item = get_item(config, id)?;
            println!("{}", item["card_url"].as_str().unwrap_or(""));
            Ok(())
        }
        Some("open") => {
            let id = one_id("open", args)?;
            let item = get_item(config, id)?;
            let url = item["card_url"].as_str().unwrap_or("").to_string();
            println!("{url}");
            let status = Command::new("xdg-open").arg(&url).status()?;
            if !status.success() {
                return Err(Error::Invalid(format!("xdg-open exited ({status})")));
            }
            Ok(())
        }
        Some("status") => {
            no_args("status", args)?;
            let status = daemon_json(config, "GET", "/v1/status", None)?;
            print!("{}", render_status(&status));
            Ok(())
        }
        Some("claim") => claim_cmd(config, &args[1..]),
        Some("run") => run_cmd(config, &args[1..]),
        Some("heartbeat") => heartbeat_cmd(config, &args[1..]),
        Some("release") => release_cmd(config, &args[1..]),
        Some("note") => note_cmd(config, &args[1..]),
        Some("waive") => waive_cmd(config, &args[1..]),
        Some("submit") => submit_cmd(config, &args[1..]),
        Some("approve") => approve_cmd(config, &args[1..]),
        Some("drop") => drop_cmd(config, &args[1..]),
        Some(other) => {
            eprintln!("{}", usage());
            Err(Error::Invalid(format!("unknown command {other}")))
        }
    }
}

fn usage() -> &'static str {
    "\
usage:
  keel serve
  keel status
  keel add [--repo PATH] [--branch NAME] [--body TEXT] TITLE
  keel inbox
  keel list [--state STATE]
  keel show ID
  keel url ID
  keel open ID
  keel claim ID --role plan|revise|implement|review
  keel run ID --role plan|revise|implement|review [--adapter fake]
  keel heartbeat ID --token TOKEN
  keel release ID [--force]
  keel note ID TEXT
  keel waive ID POINT
  keel submit ID --token TOKEN [--file PATH]
  keel approve ID --base SHA
  keel drop ID [--force]"
}

fn no_args(cmd: &str, args: &[String]) -> Result<()> {
    if args.len() != 1 {
        return Err(Error::Invalid(format!("{cmd} takes no arguments")));
    }
    Ok(())
}

fn one_id<'a>(cmd: &str, args: &'a [String]) -> Result<&'a str> {
    if args.len() != 2 {
        return Err(Error::Invalid(format!("{cmd} takes an item id")));
    }
    Ok(&args[1])
}

fn parse_state(args: &[String]) -> Result<Option<&str>> {
    if args.is_empty() {
        return Ok(None);
    }
    if args.len() == 2 && args[0] == "--state" {
        return Ok(Some(args[1].as_str()));
    }
    if args.len() == 1 {
        if let Some(value) = args[0].strip_prefix("--state=") {
            if value.is_empty() {
                return Err(Error::Invalid("missing state".into()));
            }
            return Ok(Some(value));
        }
    }
    Err(Error::Invalid("usage: keel list [--state STATE]".into()))
}

fn add(config: &Config, args: &[String]) -> Result<()> {
    let mut repo = None;
    let mut branch = None;
    let mut body = None;
    let mut title = None;
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--repo" | "--branch" | "--body" => {
                let flag = arg.as_str();
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| Error::Invalid(format!("missing value for {flag}")))?;
                match flag {
                    "--repo" => repo = Some(value.clone()),
                    "--branch" => branch = Some(value.clone()),
                    _ => body = Some(value.clone()),
                }
            }
            s if s.starts_with("--repo=") => repo = Some(s["--repo=".len()..].to_string()),
            s if s.starts_with("--branch=") => branch = Some(s["--branch=".len()..].to_string()),
            s if s.starts_with("--body=") => body = Some(s["--body=".len()..].to_string()),
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if title.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                title = Some(s.to_string());
            }
        }
        i += 1;
    }
    let title = title.ok_or_else(|| Error::Invalid("missing title".into()))?;
    let body = match body {
        Some(body) => body,
        None if io::stdin().is_terminal() => String::new(),
        None => read_stdin_body()?,
    };
    let payload = json!({
        "source": "cli",
        "title": title,
        "body": body,
        "repo": repo,
        "target_branch": branch,
    });
    let created = daemon_json(
        config,
        "POST",
        "/v1/items",
        Some(payload.to_string().into_bytes()),
    )?;
    println!("{}", created["id"].as_str().unwrap_or(""));
    println!("{}", created["card_url"].as_str().unwrap_or(""));
    if let Some(warning) = created["warning"].as_str() {
        eprintln!("warning: {warning}");
    }
    Ok(())
}

fn read_stdin_body() -> Result<String> {
    let mut buf = Vec::new();
    let mut stdin = io::stdin();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stdin.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        if buf.len() + n > 32 * 1024 {
            return Err(Error::Invalid("body exceeds 32 KiB".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8(buf).map_err(|_| Error::Invalid("body is not utf-8".into()))
}

fn claim_cmd(config: &Config, args: &[String]) -> Result<()> {
    let (id, role, adapter, worker) = parse_role_flags(args)?;
    let role = crate::claim::parse_role(&role)?;
    let resolved = resolve(config, &id)?;
    let kind = resolved["kind"].as_str().unwrap_or("");
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = claim_path(role, kind, full_id)?;
    let mut payload = json!({
        "role": role.as_str(),
        "adapter": adapter.unwrap_or_else(|| "cli".into()),
    });
    if let Some(worker) = worker {
        payload["worker_id"] = json!(worker);
    }
    let claimed = daemon_json(
        config,
        "POST",
        &path,
        Some(payload.to_string().into_bytes()),
    )?;
    println!("{}", claimed["id"].as_str().unwrap_or(full_id));
    println!("{}", claimed["claim_token"].as_str().unwrap_or(""));
    Ok(())
}

fn run_cmd(config: &Config, args: &[String]) -> Result<()> {
    let (id, role, adapter, worker) = parse_role_flags(args)?;
    let adapter = adapter.unwrap_or_else(|| "fake".to_string());
    // No model binary. The fake adapter asks the daemon to spawn a test child
    // in its own process group, then heartbeats once.
    if adapter != "fake" {
        return Err(Error::Invalid(
            "only the fake adapter is available; it does not spawn a model".into(),
        ));
    }
    let role = crate::claim::parse_role(&role)?;
    let resolved = resolve(config, &id)?;
    let kind = resolved["kind"].as_str().unwrap_or("");
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = claim_path(role, kind, full_id)?;
    let mut payload = json!({
        "role": role.as_str(),
        "adapter": adapter,
        "spawn": true,
    });
    if let Some(worker) = worker {
        payload["worker_id"] = json!(worker);
    }
    let claimed = daemon_json(
        config,
        "POST",
        &path,
        Some(payload.to_string().into_bytes()),
    )?;
    let token = claimed["claim_token"]
        .as_str()
        .filter(|token| !token.is_empty())
        .ok_or_else(|| Error::Invalid("claim_token is missing".into()))?;
    let claimed_id = claimed["id"].as_str().unwrap_or(full_id);
    let until = crate::claim::fake_heartbeat(&config.socket_path, role, claimed_id, token)?;
    println!("{claimed_id}");
    println!("{token}");
    println!("{}", claimed["worktree"].as_str().unwrap_or("-"));
    println!("{}", claimed["prompt_path"].as_str().unwrap_or(""));
    println!("{}", claimed["snapshot"]["head"].as_str().unwrap_or("-"));
    println!("{until}");
    Ok(())
}

fn parse_role_flags(args: &[String]) -> Result<(String, String, Option<String>, Option<String>)> {
    let mut id = None;
    let mut role = None;
    let mut adapter = None;
    let mut worker = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--role" | "--adapter" | "--worker" => {
                let flag = arg;
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| Error::Invalid(format!("missing value for {flag}")))?;
                match flag {
                    "--role" => role = Some(value.clone()),
                    "--adapter" => adapter = Some(value.clone()),
                    _ => worker = Some(value.clone()),
                }
            }
            s if s.starts_with("--role=") => role = Some(s["--role=".len()..].to_string()),
            s if s.starts_with("--adapter=") => adapter = Some(s["--adapter=".len()..].to_string()),
            s if s.starts_with("--worker=") => worker = Some(s["--worker=".len()..].to_string()),
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
        i += 1;
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let role = role.ok_or_else(|| Error::Invalid("missing --role".into()))?;
    Ok((id, role, adapter, worker))
}

fn claim_path(role: crate::state::Role, kind: &str, full_id: &str) -> Result<String> {
    let matches_kind = matches!(
        (role, kind),
        (
            crate::state::Role::Plan | crate::state::Role::Revise,
            "item"
        ) | (
            crate::state::Role::Implement | crate::state::Role::Review,
            "job"
        )
    );
    if !matches_kind || full_id.is_empty() {
        return Err(Error::Invalid(format!(
            "role {} does not match {kind} {full_id}",
            role.as_str()
        )));
    }
    match kind {
        "item" => Ok(format!("/v1/items/{full_id}/claim")),
        "job" => Ok(format!("/v1/jobs/{full_id}/claim")),
        _ => Err(Error::Invalid(format!("unknown kind {kind}"))),
    }
}

fn heartbeat_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut token = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--token" => {
                i += 1;
                token = Some(
                    args.get(i)
                        .ok_or_else(|| Error::Invalid("missing value for --token".into()))?
                        .clone(),
                );
            }
            s if s.starts_with("--token=") => token = Some(s["--token=".len()..].to_string()),
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
        i += 1;
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let token = token.ok_or_else(|| Error::Invalid("missing --token".into()))?;
    if token.is_empty() {
        return Err(Error::Invalid("claim_token is empty".into()));
    }
    let resolved = resolve(config, &id)?;
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = match resolved["kind"].as_str() {
        Some("item") => format!("/v1/items/{full_id}/heartbeat"),
        Some("job") => format!("/v1/jobs/{full_id}/heartbeat"),
        other => {
            return Err(Error::Invalid(format!(
                "unknown kind {}",
                other.unwrap_or("")
            )))
        }
    };
    let body = json!({ "claim_token": token });
    let beat = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!("{}", beat["lease_until"].as_str().unwrap_or(""));
    Ok(())
}

fn release_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut force = false;
    for arg in args {
        match arg.as_str() {
            "--force" => force = true,
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let resolved = resolve(config, &id)?;
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = match resolved["kind"].as_str() {
        Some("item") => format!("/v1/items/{full_id}/release"),
        Some("job") => format!("/v1/jobs/{full_id}/release"),
        other => {
            return Err(Error::Invalid(format!(
                "unknown kind {}",
                other.unwrap_or("")
            )))
        }
    };
    // The CLI omits the token. The socket still records a human cause, not expiry.
    let body = json!({ "force": force });
    let released = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {}",
        released["id"].as_str().unwrap_or(full_id),
        released["state"].as_str().unwrap_or("")
    );
    Ok(())
}

fn note_cmd(config: &Config, args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(Error::Invalid("usage: keel note ID TEXT".into()));
    }
    let path = item_action_path(config, &args[0], "note")?;
    let body = json!({ "text": args[1] });
    let noted = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!("{}", noted["point_id"].as_str().unwrap_or(""));
    println!("{}", noted["state"].as_str().unwrap_or(""));
    warn(&noted);
    Ok(())
}

fn waive_cmd(config: &Config, args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(Error::Invalid("usage: keel waive ID POINT".into()));
    }
    let path = item_action_path(config, &args[0], "waive")?;
    let body = json!({ "point_id": args[1] });
    let waived = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {}",
        waived["id"].as_str().unwrap_or(""),
        waived["state"].as_str().unwrap_or("")
    );
    warn(&waived);
    Ok(())
}

fn submit_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut token = None;
    let mut file = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--token" | "--file" => {
                let flag = arg;
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| Error::Invalid(format!("missing value for {flag}")))?;
                match flag {
                    "--token" => token = Some(value.clone()),
                    _ => file = Some(value.clone()),
                }
            }
            s if s.starts_with("--token=") => token = Some(s["--token=".len()..].to_string()),
            s if s.starts_with("--file=") => file = Some(s["--file=".len()..].to_string()),
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
        i += 1;
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let token = token.ok_or_else(|| Error::Invalid("missing --token".into()))?;
    if token.is_empty() {
        return Err(Error::Invalid("claim_token is empty".into()));
    }
    let document = read_document(file.as_deref())?;
    let path = item_action_path(config, &id, "submit")?;
    let body = json!({ "claim_token": token, "document": document });
    let submitted = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {}",
        submitted["id"].as_str().unwrap_or(""),
        submitted["state"].as_str().unwrap_or("")
    );
    println!("{}", submitted["plan_url"].as_str().unwrap_or(""));
    warn(&submitted);
    Ok(())
}

fn approve_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut base = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--base" => {
                i += 1;
                base = Some(
                    args.get(i)
                        .ok_or_else(|| Error::Invalid("missing value for --base".into()))?
                        .clone(),
                );
            }
            s if s.starts_with("--base=") => base = Some(s["--base=".len()..].to_string()),
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
        i += 1;
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let base = base.ok_or_else(|| Error::Invalid("missing --base".into()))?;
    if base.is_empty() {
        return Err(Error::Invalid("base_sha is empty".into()));
    }
    let path = item_action_path(config, &id, "approve")?;
    let body = json!({ "base_sha": base });
    let approved = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {} {}",
        approved["id"].as_str().unwrap_or(""),
        approved["state"].as_str().unwrap_or(""),
        approved["base_sha"].as_str().unwrap_or("")
    );
    if let Some(jobs) = approved.get("job_ids").and_then(Value::as_array) {
        for job in jobs {
            if let Some(job_id) = job.as_str() {
                println!("{job_id}");
            }
        }
    }
    warn(&approved);
    Ok(())
}

fn drop_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut force = false;
    for arg in args {
        match arg.as_str() {
            "--force" => force = true,
            s if s.starts_with('-') => {
                return Err(Error::Invalid(format!("unknown flag {s}")));
            }
            s => {
                if id.is_some() {
                    return Err(Error::Invalid("unexpected extra argument".into()));
                }
                id = Some(s.to_string());
            }
        }
    }
    let id = id.ok_or_else(|| Error::Invalid("missing id".into()))?;
    let path = item_action_path(config, &id, "drop")?;
    let body = json!({ "force": force });
    let dropped = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {}",
        dropped["id"].as_str().unwrap_or(""),
        dropped["state"].as_str().unwrap_or("")
    );
    Ok(())
}

fn item_action_path(config: &Config, id: &str, action: &str) -> Result<String> {
    let resolved = resolve(config, id)?;
    if resolved["kind"].as_str() != Some("item") {
        return Err(Error::Invalid(format!("{action} takes an item id")));
    }
    let full_id = resolved["id"].as_str().unwrap_or("");
    if full_id.is_empty() {
        return Err(Error::NotFound(id.to_string()));
    }
    Ok(format!("/v1/items/{full_id}/{action}"))
}

fn read_document(path: Option<&str>) -> Result<String> {
    let text = match path {
        Some(path) => fs::read_to_string(path)?,
        None if io::stdin().is_terminal() => {
            return Err(Error::Invalid("missing document".into()));
        }
        None => read_stdin_capped(256 * 1024)?,
    };
    if text.len() > 256 * 1024 {
        return Err(Error::Invalid("document exceeds 256 KiB".into()));
    }
    Ok(text)
}

fn read_stdin_capped(max: usize) -> Result<String> {
    let mut buf = Vec::new();
    let mut stdin = io::stdin();
    let mut chunk = [0u8; 4096];
    loop {
        let n = stdin.read(&mut chunk)?;
        if n == 0 {
            break;
        }
        if buf.len() + n > max {
            return Err(Error::Invalid("document exceeds 256 KiB".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8(buf).map_err(|_| Error::Invalid("document is not utf-8".into()))
}

fn warn(value: &Value) {
    if let Some(warning) = value.get("warning").and_then(Value::as_str) {
        eprintln!("warning: {warning}");
    }
}

fn resolve(config: &Config, id: &str) -> Result<Value> {
    if !crate::render::safe_id(id) {
        return Err(Error::NotFound(id.to_string()));
    }
    daemon_json(
        config,
        "GET",
        &format!("/v1/resolve?id={}", url_encode(id)),
        None,
    )
}

fn list(config: &Config, state: Option<&str>) -> Result<()> {
    let path = match state {
        Some(state) => format!("/v1/items?state={}", url_encode(state)),
        None => "/v1/items".into(),
    };
    let body = daemon_json(config, "GET", &path, None)?;
    print!("{}", render_list(&body));
    Ok(())
}

fn get_item(config: &Config, id: &str) -> Result<Value> {
    if !crate::render::safe_id(id) {
        return Err(Error::NotFound(id.to_string()));
    }
    daemon_json(config, "GET", &format!("/v1/items/{id}"), None)
}

fn daemon_json(config: &Config, method: &str, path: &str, body: Option<Vec<u8>>) -> Result<Value> {
    let mut stream = connect(config)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let response = http::exchange_with(
        &mut stream,
        method,
        path,
        body.as_deref(),
        &[("x-keel-surface", "cli")],
    )?;
    if !(200..300).contains(&response.status) {
        let message = String::from_utf8_lossy(&response.body);
        return Err(Error::Invalid(format!(
            "daemon {} {}",
            response.status, message
        )));
    }
    serde_json::from_slice(&response.body).map_err(|err| Error::Invalid(format!("json: {err}")))
}

fn connect(config: &Config) -> Result<UnixStream> {
    match UnixStream::connect(&config.socket_path) {
        Ok(stream) => Ok(stream),
        Err(err) if is_stale(&err) => start_and_connect(config),
        Err(err) => Err(err.into()),
    }
}

fn is_stale(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
    )
}

fn start_and_connect(config: &Config) -> Result<UnixStream> {
    fs::create_dir_all(&config.data_dir)?;
    // Same flock the daemon holds. Busy means the owner is alive, even when
    // the pid text is missing. Release before spawn so the child can take it.
    let lock = daemon::acquire_lock(&config.lock_path)?;
    let _ = fs::remove_file(&config.socket_path);
    drop(lock);
    let exe = env::current_exe()?;
    fs::create_dir_all(&config.runtime_dir)?;
    // Null stdout, and a file for stderr. Inheriting the CLI pipes would keep
    // `output()` blocked, and a wait thread would keep the CLI process alive.
    let log_path = config.runtime_dir.join("serve.err");
    let log = File::create(&log_path)?;
    let mut cmd = Command::new(exe);
    cmd.arg("serve")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log);
    // A new session so SIGINT on the CLI does not kill the daemon it just started.
    unsafe {
        cmd.pre_exec(|| {
            if setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd.spawn()?;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(stream) = UnixStream::connect(&config.socket_path) {
            // Dropping Child does not kill the daemon. It is reparented when
            // this process exits.
            drop(child);
            return Ok(stream);
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                if let Ok(stream) = UnixStream::connect(&config.socket_path) {
                    return Ok(stream);
                }
                let detail = fs::read_to_string(&log_path).unwrap_or_default();
                return Err(Error::Invalid(format!(
                    "keel serve exited ({status}) {detail}"
                )));
            }
            Ok(None) => {}
            Err(err) => return Err(err.into()),
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let detail = fs::read_to_string(&log_path).unwrap_or_default();
            return Err(Error::Invalid(format!(
                "timed out starting keel serve {detail}"
            )));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

extern "C" {
    fn setsid() -> i32;
}

fn url_encode(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

pub(crate) fn render_show(item: &Value) -> String {
    let mut out = String::new();
    push_line(&mut out, "id", text(item, "id"));
    push_line(&mut out, "state", text(item, "state"));
    push_line(&mut out, "source", text(item, "source"));
    push_line(&mut out, "title", text(item, "title"));
    let repo = dash(item.get("repo"));
    let branch = dash(item.get("target_branch"));
    push_line(&mut out, "repo", &repo);
    push_line(&mut out, "branch", &branch);
    push_line(&mut out, "created", text(item, "created_at"));
    push_line(&mut out, "updated", text(item, "updated_at"));
    out.push('\n');
    out.push_str(text(item, "body"));
    out.push_str("\n\nevents:\n");
    if let Some(events) = item.get("events").and_then(Value::as_array) {
        for event in events {
            out.push_str(&format!(
                "{} {} {} {} {} {} {}\n",
                event.get("id").and_then(Value::as_i64).unwrap_or(0),
                text(event, "at"),
                text(event, "actor"),
                text(event, "kind"),
                dash(event.get("from_state")),
                dash(event.get("to_state")),
                dash(event.get("detail")),
            ));
        }
    }
    out
}

pub(crate) fn render_list(body: &Value) -> String {
    let mut out = String::new();
    let Some(items) = body.get("items").and_then(Value::as_array) else {
        return out;
    };
    for item in items {
        out.push_str(&format!(
            "{} {} {}\n",
            text(item, "id"),
            text(item, "state"),
            text(item, "title")
        ));
    }
    out
}

pub(crate) fn render_status(status: &Value) -> String {
    let mut out = String::new();
    out.push_str(&format!("profile: {}\n", text(status, "profile")));
    out.push_str(&format!(
        "pid: {}\n",
        status.get("pid").and_then(Value::as_u64).unwrap_or(0)
    ));
    out.push_str(&format!("socket: {}\n", text(status, "socket")));
    out.push_str(&format!(
        "port: {}\n",
        status.get("port").and_then(Value::as_u64).unwrap_or(0)
    ));
    let counts = status.get("counts").cloned().unwrap_or(Value::Null);
    for state in ITEM_STATES {
        let n = counts.get(*state).and_then(Value::as_i64).unwrap_or(0);
        out.push_str(&format!("{state}: {n}\n"));
    }
    if let Some(map) = counts.as_object() {
        let mut extra: Vec<&String> = map
            .keys()
            .filter(|key| !ITEM_STATES.contains(&key.as_str()))
            .collect();
        extra.sort();
        for key in extra {
            let n = map[key].as_i64().unwrap_or(0);
            out.push_str(&format!("{key}: {n}\n"));
        }
    }
    let leases = status.get("leases").and_then(Value::as_array);
    let n = leases.map(|rows| rows.len()).unwrap_or(0);
    let cap = status.get("lease_cap").and_then(Value::as_i64).unwrap_or(4);
    out.push_str(&format!("leases: {n}/{cap}\n"));
    if let Some(rows) = leases {
        for lease in rows {
            let unsupervised = lease
                .get("unsupervised")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            out.push_str(&format!(
                "lease {} {} pid={} starttime={} lease_until={} worker={} role={}{}\n",
                text(lease, "kind"),
                text(lease, "id"),
                dash(lease.get("pid")),
                dash(lease.get("starttime")),
                dash(lease.get("lease_until")),
                dash(lease.get("worker")),
                dash(lease.get("role")),
                if unsupervised { " unsupervised" } else { "" },
            ));
        }
    }
    out
}

fn push_line(out: &mut String, key: &str, value: &str) {
    out.push_str(key);
    out.push_str(": ");
    out.push_str(value);
    out.push('\n');
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn dash(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "-".into(),
        Some(Value::String(text)) if text.is_empty() => "-".into(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::{boot_at, scratch_dir, Rm};
    use crate::db::open;
    use rusqlite::params;

    #[test]
    fn commands_talk_to_the_socket() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(
            &cfg,
            &[
                "add".into(),
                "--repo".into(),
                "/tmp/repo".into(),
                "--branch".into(),
                "main".into(),
                "--body".into(),
                "Say what happens".into(),
                "Pin the walk".into(),
            ],
        )
        .unwrap();
        let id: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        let repo: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT repo FROM items WHERE id = ?1", params![id], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(repo, "/tmp/repo");
        let canonical = _rm.0.join("data/docs").join(&id).join("card.html");
        let published = _rm.0.join("artifacts/keel").join(&id).join("card.html");
        assert!(canonical.is_file());
        assert!(published.is_file());
        dispatch(&cfg, &["inbox".into()]).unwrap();
        dispatch(&cfg, &["list".into(), "--state".into(), "inbox".into()]).unwrap();
        dispatch(&cfg, &["show".into(), id.clone()]).unwrap();
        dispatch(&cfg, &["url".into(), id.clone()]).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let missing = dispatch(&cfg, &["show".into(), "nope".into()]).unwrap_err();
        assert!(
            missing.to_string().contains("not found") || missing.to_string().contains("404"),
            "{missing}"
        );
        assert!(dispatch(&cfg, &["add".into()]).is_err());
        drop(running);
    }

    #[test]
    fn show_text_is_newest_ten_events() {
        let item = json!({
            "id": "k_abc",
            "state": "inbox",
            "source": "cli",
            "title": "T",
            "repo": null,
            "target_branch": "main",
            "created_at": "t0",
            "updated_at": "t1",
            "body": "hello",
            "events": [
                {"id": 12, "at": "t", "actor": "human", "kind": "note", "from_state": null, "to_state": null, "detail": "n"},
                {"id": 3, "at": "t", "actor": "human", "kind": "create", "from_state": null, "to_state": "inbox", "detail": null}
            ]
        });
        let text = render_show(&item);
        let events = text.split("events:\n").nth(1).unwrap();
        let lines: Vec<&str> = events.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("12 "));
        assert!(lines[1].starts_with("3 "));
        assert!(lines[1].contains("create"));
        assert!(lines[1].contains(" inbox "));
        assert!(text.contains("repo: -"));
        assert!(text.contains("branch: main"));
        let status = render_status(&json!({
            "profile": "default",
            "pid": 9,
            "socket": "/tmp/keel.sock",
            "port": 7431,
            "lease_cap": 4,
            "counts": {"inbox": 2, "planning": 0},
            "leases": []
        }));
        assert!(status.contains("inbox: 2\n"));
        assert!(status.contains("planning: 0\n"));
        assert!(status.contains("leases: 0/4\n"));
        assert!(status.contains("pid: 9\n"));
        let unsupervised = render_status(&json!({
            "profile": "default",
            "pid": 9,
            "socket": "/tmp/keel.sock",
            "port": 7431,
            "lease_cap": 4,
            "counts": {"inbox": 0},
            "leases": [{
                "kind": "item",
                "id": "k",
                "pid": null,
                "unsupervised": true,
                "worker": "fake:1",
                "role": "plan",
                "starttime": null,
                "lease_until": "t"
            }]
        }));
        assert!(unsupervised.contains("pid=-"));
        assert!(unsupervised.contains(" unsupervised\n"));
    }

    #[test]
    fn claim_heartbeat_and_human_release_round_trip() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["add".into(), "Hold a lease".into()]).unwrap();
        let id: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        let prefix = id.chars().take(6).collect::<String>();
        dispatch(
            &cfg,
            &["claim".into(), prefix, "--role".into(), "plan".into()],
        )
        .unwrap();
        let (state, pid, token): (String, Option<i64>, String) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state, claim_pid, claim_token FROM items WHERE id = ?1",
                rusqlite::params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "planning");
        assert!(pid.is_none());
        assert!(!token.is_empty());
        dispatch(
            &cfg,
            &["heartbeat".into(), id.clone(), "--token".into(), token],
        )
        .unwrap();
        dispatch(&cfg, &["release".into(), id.clone()]).unwrap();
        let state: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                rusqlite::params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "inbox");
        let token: Option<String> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token FROM items WHERE id = ?1",
                rusqlite::params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(token.is_none());
        assert!(dispatch(&cfg, &["heartbeat".into(), id.clone()]).is_err());
        assert!(dispatch(
            &cfg,
            &["claim".into(), id, "--role".into(), "implement".into()]
        )
        .is_err());
        drop(running);
    }

    #[test]
    fn note_submit_approve_and_drop_round_trip() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let repo = _rm.0.join("repo");
        let head = crate::git::init_repo(&repo);
        dispatch(
            &cfg,
            &[
                "add".into(),
                "--repo".into(),
                repo.to_str().unwrap().into(),
                "--branch".into(),
                "main".into(),
                "Pin the walk".into(),
            ],
        )
        .unwrap();
        let id: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        dispatch(
            &cfg,
            &["claim".into(), id.clone(), "--role".into(), "plan".into()],
        )
        .unwrap();
        assert_eq!(crate::git::rev_parse(&repo, "HEAD").unwrap(), head);
        let token: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        let doc_path = _rm.0.join("plan.json");
        fs::write(
            &doc_path,
            r#"{"summary":"from cli","jobs":[{"title":"Walk","acceptance":"yes"}]}"#,
        )
        .unwrap();
        dispatch(
            &cfg,
            &[
                "submit".into(),
                id.clone(),
                "--token".into(),
                token,
                "--file".into(),
                doc_path.to_str().unwrap().to_string(),
            ],
        )
        .unwrap();
        assert_eq!(state_of(&cfg, &id), "awaiting_plan_review");
        assert!(_rm
            .0
            .join("data/docs")
            .join(&id)
            .join("plan")
            .join("r1.html")
            .is_file());
        assert!(_rm
            .0
            .join("artifacts/keel")
            .join(&id)
            .join("plan.html")
            .is_file());
        dispatch(&cfg, &["note".into(), id.clone(), "block it".into()]).unwrap();
        assert_eq!(state_of(&cfg, &id), "revision_requested");
        let err = dispatch(
            &cfg,
            &[
                "approve".into(),
                id.clone(),
                "--base".into(),
                "fixture-sha".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("409"), "{err}");
        let point: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT id FROM open_points WHERE item_id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        dispatch(&cfg, &["waive".into(), id.clone(), point]).unwrap();
        assert_eq!(state_of(&cfg, &id), "awaiting_plan_review");
        dispatch(
            &cfg,
            &[
                "approve".into(),
                id.clone(),
                "--base".into(),
                "fixture-sha".into(),
            ],
        )
        .unwrap();
        let (state, base, jobs): (String, String, i64) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT i.state, i.base_sha, (SELECT COUNT(*) FROM jobs j WHERE j.item_id = i.id)
                 FROM items i WHERE i.id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state, "awaiting_implementation");
        assert_eq!(base, "fixture-sha");
        assert_eq!(jobs, 1);

        dispatch(&cfg, &["add".into(), "Loose".into()]).unwrap();
        let loose: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items WHERE title = 'Loose'", [], |row| {
                row.get(0)
            })
            .unwrap();
        dispatch(&cfg, &["drop".into(), loose.clone()]).unwrap();
        assert_eq!(state_of(&cfg, &loose), "dropped");

        dispatch(&cfg, &["add".into(), "Held".into()]).unwrap();
        let held: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items WHERE title = 'Held'", [], |row| {
                row.get(0)
            })
            .unwrap();
        dispatch(
            &cfg,
            &["claim".into(), held.clone(), "--role".into(), "plan".into()],
        )
        .unwrap();
        let err = dispatch(&cfg, &["drop".into(), held.clone()]).unwrap_err();
        assert!(err.to_string().contains("409"), "{err}");
        assert_eq!(state_of(&cfg, &held), "planning");
        dispatch(&cfg, &["drop".into(), "--force".into(), held.clone()]).unwrap();
        assert_eq!(state_of(&cfg, &held), "dropped");
        let token: Option<String> = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_token FROM items WHERE id = ?1",
                params![held],
                |row| row.get(0),
            )
            .unwrap();
        assert!(token.is_none());
        drop(running);
    }

    #[test]
    fn run_uses_the_fake_adapter_and_leaves_primary_head() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let repo = _rm.0.join("repo");
        let head = crate::git::init_repo(&repo);
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
                "Run it".into(),
            ],
        )
        .unwrap();
        let id: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT id FROM items", [], |row| row.get(0))
            .unwrap();
        let refused = dispatch(
            &cfg,
            &[
                "run".into(),
                id.clone(),
                "--role".into(),
                "plan".into(),
                "--adapter".into(),
                "grok".into(),
            ],
        )
        .unwrap_err();
        assert!(refused.to_string().contains("fake"), "{refused}");
        assert_eq!(state_of(&cfg, &id), "inbox");
        dispatch(
            &cfg,
            &["run".into(), id.clone(), "--role".into(), "plan".into()],
        )
        .unwrap();
        let (pid, pgid, start): (i64, i64, String) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_pid, claim_pgid, claim_starttime FROM items WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let _guard = crate::proc::GroupGuard {
            pgid: Some(pgid),
            pid: Some(pid),
        };
        assert_eq!(pid, pgid);
        assert_eq!(
            crate::daemon::read_proc_starttime(pid as u32).as_deref(),
            Some(start.as_str())
        );
        assert_eq!(crate::git::rev_parse(&repo, "HEAD").unwrap(), head);
        assert_eq!(crate::git::snapshot(&repo).unwrap().branch, "main");
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        let worktree = cfg.data_dir.join("worktrees").join(&id).join("plan");
        assert!(worktree.is_dir());
        let tree = crate::git::snapshot(&worktree).unwrap();
        assert_eq!(tree.head, head);
        assert_eq!(tree.branch, "HEAD");
        let prompt =
            fs::read_to_string(cfg.data_dir.join("prompts").join(format!("{id}.txt"))).unwrap();
        assert!(prompt.contains("untrusted body"));
        assert!(prompt.contains("Do not edit the repo. Do not git checkout."));
        let until: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT lease_until FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!until.is_empty());
        let marker = worktree.join(crate::proc::GRANDCHILD_FILE);
        let mut grandchild = String::new();
        for _ in 0..100 {
            if let Ok(text) = fs::read_to_string(&marker) {
                if !text.trim().is_empty() {
                    grandchild = text;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let gpid: i64 = grandchild
            .trim()
            .parse()
            .expect("grandchild did not write the worktree");
        assert_ne!(gpid, pid);
        assert!(std::path::Path::new(&format!("/proc/{gpid}")).exists());
        dispatch(&cfg, &["release".into(), id.clone()]).unwrap();
        assert_eq!(state_of(&cfg, &id), "inbox");
        assert!(crate::proc::group_empty(pgid).unwrap());
        assert!(!std::path::Path::new(&format!("/proc/{gpid}")).exists());
        assert_eq!(crate::git::rev_parse(&repo, "HEAD").unwrap(), head);
        drop(running);
    }

    fn state_of(cfg: &crate::config::Config, id: &str) -> String {
        open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap()
    }
}
