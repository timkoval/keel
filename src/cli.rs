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
        Some("doctor") => {
            no_args("doctor", args)?;
            // Status uses 5s. Doctor also walks a bounded first-parent history
            // and the artifact root, so this request gets a budget that covers
            // that walk instead of sharing the short one.
            let body =
                daemon_json_timeout(config, "GET", "/v1/doctor", None, Duration::from_secs(30))?;
            if let Some(lines) = body.get("lines").and_then(Value::as_array) {
                for line in lines {
                    println!("{}", line.as_str().unwrap_or(""));
                }
            }
            Ok(())
        }
        Some("claim") => claim_cmd(config, &args[1..]),
        Some("run") => run_cmd(config, &args[1..]),
        Some("permit") => permit_cmd(config, &args[1..]),
        Some("heartbeat") => heartbeat_cmd(config, &args[1..]),
        Some("release") => release_cmd(config, &args[1..]),
        Some("note") => note_cmd(config, &args[1..]),
        Some("waive") => waive_cmd(config, &args[1..]),
        Some("submit") => submit_cmd(config, &args[1..]),
        Some("approve") => approve_cmd(config, &args[1..]),
        Some("drop") => drop_cmd(config, &args[1..]),
        Some("accept") => accept_cmd(config, &args[1..]),
        Some("reject") => reject_cmd(config, &args[1..]),
        Some("merge") => merge_cmd(config, &args[1..]),
        Some("mcp") => {
            if args.len() != 1 {
                return Err(Error::Invalid("mcp takes no arguments".into()));
            }
            crate::mcp::serve(config)
        }
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
  keel doctor
  keel add [--repo PATH] [--branch NAME] [--body TEXT] TITLE
  keel inbox
  keel list [--state STATE]
  keel show ID
  keel url ID
  keel open ID
  keel claim ID --role plan|revise|implement|review
  keel run ID --role plan|revise|implement|review [--adapter fake|grok|claude]
  keel permit ID [--once|--deny|--always]
  keel heartbeat ID --token TOKEN
  keel release ID [--force]
  keel note ID TEXT
  keel waive ID POINT
  keel submit ID --token TOKEN [--file PATH]
  keel approve ID --base SHA
  keel drop ID [--force]
  keel accept ID
  keel reject ID TEXT
  keel merge ID [--continue|--abort]
  keel mcp"
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
    if adapter == "grok" {
        return run_grok(config, id, role, worker);
    }
    if adapter == "claude" {
        return run_claude(config, id, role, worker);
    }
    // The fake adapter asks the daemon to spawn a test child in its own
    // process group, then heartbeats once. It is not a model.
    if adapter != "fake" {
        return Err(Error::Invalid(
            "adapter must be fake, grok, or claude".into(),
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

fn run_grok(config: &Config, id: String, role: String, worker: Option<String>) -> Result<()> {
    // Probe before the claim so a missing binary does not take a lease.
    crate::adapters::grok::ensure_grok_on_path()?;
    let role = crate::claim::parse_role(&role)?;
    let resolved = resolve(config, &id)?;
    let kind = resolved["kind"].as_str().unwrap_or("");
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = claim_path(role, kind, full_id)?;
    let mut payload = json!({
        "role": role.as_str(),
        "adapter": "grok",
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
    let claimed_id = claimed["id"].as_str().unwrap_or(full_id).to_string();
    let until = crate::claim::fake_heartbeat(&config.socket_path, role, &claimed_id, token)?;
    println!("{claimed_id}");
    println!("{token}");
    println!("{}", claimed["worktree"].as_str().unwrap_or("-"));
    println!("{}", claimed["prompt_path"].as_str().unwrap_or(""));
    println!("{}", claimed["snapshot"]["head"].as_str().unwrap_or("-"));
    println!("{until}");
    if io::stdin().is_terminal() {
        // This process is the permit prompt. It does not return while the
        // claim is held, so a permission cannot sit unanswered on a tty.
        permit_until_idle(config, &claimed_id)?;
    } else {
        eprintln!("no tty; if a tool asks, run `keel permit {claimed_id}`");
    }
    Ok(())
}

fn run_claude(config: &Config, id: String, role: String, worker: Option<String>) -> Result<()> {
    // Probe before the claim so a missing binary does not take a lease.
    crate::adapters::claude::ensure_claude_on_path()?;
    let role = crate::claim::parse_role(&role)?;
    let resolved = resolve(config, &id)?;
    let kind = resolved["kind"].as_str().unwrap_or("");
    let full_id = resolved["id"].as_str().unwrap_or("");
    let path = claim_path(role, kind, full_id)?;
    let mut payload = json!({
        "role": role.as_str(),
        "adapter": "claude",
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
    // The adapter heartbeats while the child is alive. A CLI heartbeat here
    // races a short claude that has already submitted and cleared the token.
    println!("{claimed_id}");
    println!("{token}");
    println!("{}", claimed["worktree"].as_str().unwrap_or("-"));
    println!("{}", claimed["prompt_path"].as_str().unwrap_or(""));
    println!("{}", claimed["snapshot"]["head"].as_str().unwrap_or("-"));
    println!("{}", claimed["lease_until"].as_str().unwrap_or(""));
    Ok(())
}

fn permit_cmd(config: &Config, args: &[String]) -> Result<()> {
    let (id, choice) = parse_permit(args)?;
    let resolved = resolve(config, &id)?;
    let full_id = resolved["id"].as_str().unwrap_or("");
    let kind = resolved["kind"].as_str().unwrap_or("");
    let lease = daemon_json(
        config,
        "GET",
        &format!("/v1/lease/{}", url_encode(full_id)),
        None,
    )?;
    if let Some(pending) = lease.get("pending_permit") {
        if !pending.is_null() {
            eprint_permit(pending);
        }
    }
    let choice = match choice {
        Some(choice) => choice,
        None if io::stdin().is_terminal() => read_choice()?,
        None => {
            return Err(Error::Invalid(
                "no tty; pass --once, --deny, or --always".into(),
            ));
        }
    };
    let path = match kind {
        "item" => format!("/v1/items/{full_id}/permit"),
        "job" => format!("/v1/jobs/{full_id}/permit"),
        other => return Err(Error::Invalid(format!("unknown kind {}", other))),
    };
    let answered = daemon_json(
        config,
        "POST",
        &path,
        Some(json!({ "choice": choice }).to_string().into_bytes()),
    )?;
    println!("{}", answered["result"]);
    Ok(())
}

fn permit_until_idle(config: &Config, id: &str) -> Result<()> {
    let mut answered = String::new();
    loop {
        let lease = match daemon_json(
            config,
            "GET",
            &format!("/v1/lease/{}", url_encode(id)),
            None,
        ) {
            Ok(lease) => lease,
            Err(err) => {
                // A blip must not drop the prompt while the ACP task is blocked.
                eprintln!("permit: {err}");
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        if !lease
            .get("claimed")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let waiting = lease
            .get("pending_permit")
            .filter(|pending| pending.get("phase").and_then(Value::as_str) == Some("waiting"));
        let Some(pending) = waiting else {
            answered.clear();
            thread::sleep(Duration::from_millis(200));
            continue;
        };
        let key = pending
            .get("request_id")
            .map(|value| value.to_string())
            .unwrap_or_default();
        if answered == key {
            thread::sleep(Duration::from_millis(200));
            continue;
        }
        eprint_permit(pending);
        let choice = match read_choice() {
            Ok(choice) => choice,
            Err(err) => {
                eprintln!("permit: {err}");
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        let kind = lease.get("kind").and_then(Value::as_str).unwrap_or("job");
        let path = match kind {
            "item" => format!("/v1/items/{id}/permit"),
            _ => format!("/v1/jobs/{id}/permit"),
        };
        match daemon_json(
            config,
            "POST",
            &path,
            Some(json!({ "choice": choice }).to_string().into_bytes()),
        ) {
            Ok(_) => answered = key,
            Err(err) => {
                // Still waiting. Print the error and ask again on the next pass.
                eprintln!("permit: {err}");
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn parse_permit(args: &[String]) -> Result<(String, Option<String>)> {
    let mut id = None;
    let mut choice = None;
    for arg in args {
        match arg.as_str() {
            "--once" | "--deny" | "--always" => {
                if choice.is_some() {
                    return Err(Error::Invalid("one permit choice".into()));
                }
                choice = Some(arg.clone());
            }
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
    Ok((id, choice))
}

fn read_choice() -> Result<String> {
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    let choice = line.trim().to_string();
    if choice.is_empty() {
        return Err(Error::Invalid("missing permit choice".into()));
    }
    Ok(choice)
}

fn eprint_permit(pending: &Value) {
    let kind = pending
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("permission");
    let tool = pending
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or("tool");
    eprintln!("permit {kind}: {tool}");
    if let Some(question) = pending.get("question").and_then(Value::as_str) {
        if !question.is_empty() {
            eprintln!("{question}");
        }
    }
    if let Some(options) = pending.get("options").and_then(Value::as_array) {
        for (index, opt) in options.iter().enumerate() {
            let name = opt
                .get("name")
                .or_else(|| opt.get("label"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if kind == "question" {
                eprintln!("  {}. {name}", index + 1);
            } else {
                let id = opt.get("optionId").and_then(Value::as_str).unwrap_or("");
                let opt_kind = opt.get("kind").and_then(Value::as_str).unwrap_or("");
                eprintln!("  {id}  {name}  [{opt_kind}]");
            }
        }
    }
    if kind == "question" {
        eprintln!("digit or n:");
    } else {
        eprintln!("y / n / A:");
    }
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

fn accept_cmd(config: &Config, args: &[String]) -> Result<()> {
    if args.len() != 1 {
        return Err(Error::Invalid("usage: keel accept ID".into()));
    }
    let path = job_action_path(config, &args[0], "accept")?;
    let accepted = daemon_json(config, "POST", &path, Some(b"{}".to_vec()))?;
    println!(
        "{} {} {}",
        accepted["id"].as_str().unwrap_or(""),
        accepted["state"].as_str().unwrap_or(""),
        accepted["item_state"].as_str().unwrap_or("")
    );
    Ok(())
}

fn reject_cmd(config: &Config, args: &[String]) -> Result<()> {
    if args.len() != 2 {
        return Err(Error::Invalid("usage: keel reject ID TEXT".into()));
    }
    let path = job_action_path(config, &args[0], "reject")?;
    let body = json!({ "text": args[1] });
    let rejected = daemon_json(config, "POST", &path, Some(body.to_string().into_bytes()))?;
    println!(
        "{} {} {}",
        rejected["id"].as_str().unwrap_or(""),
        rejected["state"].as_str().unwrap_or(""),
        rejected["item_state"].as_str().unwrap_or("")
    );
    Ok(())
}

fn merge_cmd(config: &Config, args: &[String]) -> Result<()> {
    let mut id = None;
    let mut cont = false;
    let mut abort = false;
    for arg in args {
        match arg.as_str() {
            "--continue" => cont = true,
            "--abort" => abort = true,
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
    if cont && abort {
        return Err(Error::Invalid("merge cannot continue and abort".into()));
    }
    let id =
        id.ok_or_else(|| Error::Invalid("usage: keel merge ID [--continue|--abort]".into()))?;
    let path = item_action_path(config, &id, "merge")?;
    let body = json!({ "continue": cont, "abort": abort });
    // Several git merges run inside one request. The usual status timeout is too short.
    let merged = daemon_json_timeout(
        config,
        "POST",
        &path,
        Some(body.to_string().into_bytes()),
        Duration::from_secs(30),
    )?;
    println!(
        "{} {}",
        merged["id"].as_str().unwrap_or(""),
        merged["state"].as_str().unwrap_or("")
    );
    if let Some(sha) = merged["merged_sha"].as_str() {
        if !sha.is_empty() {
            println!("{sha}");
        }
    }
    if merged["conflict_kind"].as_str().is_some() {
        if let Some(worktree) = merged["worktree"].as_str() {
            println!("{worktree}");
        }
    }
    Ok(())
}

fn job_action_path(config: &Config, id: &str, action: &str) -> Result<String> {
    let resolved = resolve(config, id)?;
    if resolved["kind"].as_str() != Some("job") {
        return Err(Error::Invalid(format!("{action} takes a job id")));
    }
    let full_id = resolved["id"].as_str().unwrap_or("");
    if full_id.is_empty() {
        return Err(Error::NotFound(id.to_string()));
    }
    Ok(format!("/v1/jobs/{full_id}/{action}"))
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
    daemon_json_timeout(config, method, path, body, Duration::from_secs(5))
}

fn daemon_json_timeout(
    config: &Config,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    timeout: Duration,
) -> Result<Value> {
    let response = exchange_timeout(
        config,
        method,
        path,
        body,
        timeout,
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

/// Socket call with no CLI surface header. MCP uses this so a tool cannot
/// reach approve, merge, or the other routes that check `x-keel-surface`.
pub(crate) fn daemon_exchange(
    config: &Config,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
) -> Result<http::Response> {
    exchange_timeout(config, method, path, body, Duration::from_secs(5), &[])
}

fn exchange_timeout(
    config: &Config,
    method: &str,
    path: &str,
    body: Option<Vec<u8>>,
    timeout: Duration,
    headers: &[(&str, &str)],
) -> Result<http::Response> {
    let mut stream = connect(config)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    http::exchange_with(&mut stream, method, path, body.as_deref(), headers)
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
        let missing = crate::adapters::grok::ensure_grok("/no/such").unwrap_err();
        assert!(
            missing.to_string().contains("grok is not on PATH"),
            "{missing}"
        );
        if crate::adapters::grok::ensure_grok(&std::env::var("PATH").unwrap_or_default()).is_err() {
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
            assert!(
                refused.to_string().contains("grok is not on PATH"),
                "{refused}"
            );
            assert_eq!(state_of(&cfg, &id), "inbox");
        }
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

    fn insert_item(
        cfg: &crate::config::Config,
        id: &str,
        repo: Option<&str>,
        base: Option<&str>,
        state: &str,
    ) {
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO items (
                    id, title, body, source, repo, target_branch, base_sha, state,
                    created_at, updated_at
                 ) VALUES (
                    ?1, 'Merge', '', 'cli', ?2, 'main', ?3, ?4,
                    '2026-11-25T00:00:00Z', '2026-11-25T00:00:00Z'
                 )",
                params![id, repo, base, state],
            )
            .unwrap();
    }

    fn insert_job(
        cfg: &crate::config::Config,
        id: &str,
        item: &str,
        ord: i64,
        state: &str,
        branch: Option<&str>,
        worktree: Option<&str>,
        base: &str,
    ) {
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO jobs (
                    id, item_id, title, acceptance, state, depth, ord, branch, worktree,
                    base_sha, updated_at
                 ) VALUES (
                    ?1, ?2, 'leaf', '', ?3, 1, ?4, ?5, ?6, ?7, '2026-11-25T00:00:00Z'
                 )",
                params![id, item, state, ord, branch, worktree, base],
            )
            .unwrap();
    }

    fn merge_head(repo: &std::path::Path) -> bool {
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["rev-parse", "--verify", "--quiet", "MERGE_HEAD"])
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .status()
            .unwrap()
            .success()
    }

    #[test]
    fn accept_and_reject_do_not_run_git() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        // The socket is bound before the schema commit. Wait until a request is served.
        dispatch(&cfg, &["status".into()]).unwrap();
        insert_item(&cfg, "k_accept", None, None, "implementing");
        insert_job(
            &cfg,
            "j_keep",
            "k_accept",
            0,
            "feedback_pending",
            Some("keel/k_accept/j_keep"),
            None,
            "",
        );
        insert_job(
            &cfg,
            "j_note",
            "k_accept",
            1,
            "feedback_pending",
            Some("keel/k_accept/j_note"),
            None,
            "",
        );
        let bare = {
            let mut stream = UnixStream::connect(&cfg.socket_path).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            crate::http::exchange(&mut stream, "POST", "/v1/jobs/j_keep/accept", Some(b"{}"))
                .unwrap()
        };
        assert_eq!(bare.status, 400, "{}", String::from_utf8_lossy(&bare.body));
        assert!(String::from_utf8_lossy(&bare.body).contains("cli only"));
        let tcp = {
            let mut stream = std::net::TcpStream::connect(("127.0.0.1", cfg.port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            crate::http::exchange(&mut stream, "POST", "/v1/jobs/j_keep/accept", Some(b"{}"))
                .unwrap()
        };
        assert_eq!(tcp.status, 405);

        dispatch(
            &cfg,
            &["reject".into(), "j_note".into(), "needs a test".into()],
        )
        .unwrap();
        let (job_state, item_state, branch): (String, String, String) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT j.state, i.state, j.branch
                 FROM jobs j JOIN items i ON i.id = j.item_id
                 WHERE j.id = 'j_note'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(job_state, "queued");
        assert_eq!(item_state, "implementing");
        assert_eq!(branch, "keel/k_accept/j_note");
        let note: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT text FROM job_notes WHERE job_id = 'j_note'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(note, "needs a test");
        let rejected = dispatch(&cfg, &["accept".into(), "j_note".into()]).unwrap_err();
        assert!(rejected.to_string().contains("409"), "{rejected}");

        dispatch(&cfg, &["accept".into(), "j_keep".into()]).unwrap();
        assert_eq!(state_of(&cfg, "k_accept"), "implementing");
        let kept: String = open(&cfg.db_path)
            .unwrap()
            .query_row("SELECT state FROM jobs WHERE id = 'j_keep'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(kept, "accepted");

        insert_item(&cfg, "k_last", None, None, "implementing");
        insert_job(
            &cfg,
            "j_last",
            "k_last",
            0,
            "feedback_pending",
            Some("keel/k_last/j_last"),
            None,
            "",
        );
        dispatch(&cfg, &["accept".into(), "j_last".into()]).unwrap();
        assert_eq!(state_of(&cfg, "k_last"), "awaiting_merge");
        assert!(dispatch(&cfg, &["merge".into(), "j_last".into()]).is_err());
        assert!(dispatch(
            &cfg,
            &[
                "merge".into(),
                "k_last".into(),
                "--continue".into(),
                "--abort".into()
            ]
        )
        .is_err());
        drop(running);
    }

    #[test]
    fn primary_conflict_stays_off_the_primary() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let branch = "keel/k_conflict/j_leaf";
        let leaf = _rm.0.join("leaf");
        crate::git::add_branch(&repo, &leaf, branch, &base).unwrap();
        crate::git::commit_file(&leaf, "README", b"leaf\n");
        let moved = crate::git::commit_file(&repo, "README", b"primary\n");
        assert_ne!(moved, base);
        insert_item(
            &cfg,
            "k_conflict",
            Some(repo.to_str().unwrap()),
            Some(&base),
            "awaiting_merge",
        );
        insert_job(
            &cfg,
            "j_leaf",
            "k_conflict",
            0,
            "accepted",
            Some(branch),
            Some(leaf.to_str().unwrap()),
            &base,
        );

        dispatch(&cfg, &["merge".into(), "k_conflict".into()]).unwrap();
        let primary = crate::git::snapshot(&repo).unwrap();
        assert_eq!(primary.head, moved);
        assert_eq!(primary.branch, "main");
        assert!(primary.porcelain.is_empty(), "{}", primary.porcelain);
        assert!(!merge_head(&repo));
        assert_eq!(
            fs::read_to_string(repo.join("README")).unwrap(),
            "primary\n"
        );

        let integ = cfg
            .data_dir
            .join("worktrees")
            .join("k_conflict")
            .join("integrate");
        assert!(merge_head(&integ));
        assert!(!crate::git::snapshot(&integ).unwrap().porcelain.is_empty());
        let (state, kind, integrate_sha, ord): (String, String, String, i64) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state, conflict_kind, integrate_sha, integrate_ord FROM items WHERE id = 'k_conflict'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state, "merge_conflict");
        assert_eq!(kind, "primary");
        assert_eq!(ord, 0);
        assert_eq!(
            integrate_sha,
            crate::git::rev_parse(&integ, "HEAD").unwrap()
        );
        assert_ne!(integrate_sha, moved);
        assert!(leaf.is_dir());
        assert!(crate::git::branch_exists(&repo, branch).unwrap());

        let early = dispatch(
            &cfg,
            &["merge".into(), "k_conflict".into(), "--continue".into()],
        )
        .unwrap_err();
        assert!(early.to_string().contains("unmerged"), "{early}");
        assert!(
            early.to_string().contains(integ.to_str().unwrap()),
            "{early}"
        );
        assert_eq!(state_of(&cfg, "k_conflict"), "merge_conflict");
        assert_eq!(crate::git::snapshot(&repo).unwrap().head, moved);

        // Staged but not committed: `--continue` must finish the merge without MERGE_HEAD gone yet.
        fs::write(integ.join("README"), "resolved\n").unwrap();
        let added = Command::new("git")
            .arg("-C")
            .arg(&integ)
            .args(["add", "--", "README"])
            .status()
            .unwrap();
        assert!(added.success());
        assert!(merge_head(&integ));
        dispatch(
            &cfg,
            &["merge".into(), "k_conflict".into(), "--continue".into()],
        )
        .unwrap();
        let primary = crate::git::snapshot(&repo).unwrap();
        assert!(primary.porcelain.is_empty(), "{}", primary.porcelain);
        assert_ne!(primary.head, moved);
        assert!(!merge_head(&repo));
        assert_eq!(
            fs::read_to_string(repo.join("README")).unwrap(),
            "resolved\n"
        );
        assert_eq!(state_of(&cfg, "k_conflict"), "merged");
        let merged: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT merged_sha FROM items WHERE id = 'k_conflict'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(merged, primary.head);
        assert!(Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["merge-base", "--is-ancestor", &moved, "HEAD"])
            .status()
            .unwrap()
            .success());
        assert!(!leaf.exists());
        assert!(!crate::git::branch_exists(&repo, branch).unwrap());
        assert!(crate::git::branch_exists(&repo, "keel/k_conflict/integrate").unwrap());
        let again = dispatch(&cfg, &["merge".into(), "k_conflict".into()]).unwrap_err();
        assert!(again.to_string().contains("409"), "{again}");
        drop(running);
    }

    #[test]
    fn dirty_primary_is_refused_and_ords_are_not_remerged() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let branch = "keel/k_dirty/j_leaf";
        let leaf = _rm.0.join("leaf");
        crate::git::add_branch(&repo, &leaf, branch, &base).unwrap();
        crate::git::commit_file(&leaf, "extra.txt", b"from-leaf\n");
        fs::write(repo.join("README"), "dirty\n").unwrap();
        insert_item(
            &cfg,
            "k_dirty",
            Some(repo.to_str().unwrap()),
            Some(&base),
            "awaiting_merge",
        );
        insert_job(
            &cfg,
            "j_dirty",
            "k_dirty",
            0,
            "accepted",
            Some(branch),
            Some(leaf.to_str().unwrap()),
            &base,
        );
        let refused = dispatch(&cfg, &["merge".into(), "k_dirty".into()]).unwrap_err();
        assert!(refused.to_string().contains("dirty"), "{refused}");
        assert_eq!(crate::git::snapshot(&repo).unwrap().head, base);
        assert!(!crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        assert_eq!(state_of(&cfg, "k_dirty"), "awaiting_merge");
        let integrate_sha: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT integrate_sha FROM items WHERE id = 'k_dirty'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            integrate_sha,
            crate::git::rev_parse(&repo, "keel/k_dirty/integrate").unwrap()
        );
        fs::write(repo.join("README"), "one\n").unwrap();
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        dispatch(&cfg, &["merge".into(), "k_dirty".into()]).unwrap();
        assert_eq!(
            crate::git::rev_parse(&repo, "keel/k_dirty/integrate").unwrap(),
            integrate_sha
        );
        let primary = crate::git::snapshot(&repo).unwrap();
        assert!(primary.porcelain.is_empty(), "{}", primary.porcelain);
        assert_ne!(primary.head, base);
        assert_eq!(state_of(&cfg, "k_dirty"), "merged");
        assert!(!crate::git::branch_exists(&repo, branch).unwrap());
        let again = dispatch(&cfg, &["merge".into(), "k_dirty".into()]).unwrap_err();
        assert!(again.to_string().contains("409"), "{again}");
        drop(running);
    }

    #[test]
    fn abort_throws_integrate_away_and_keeps_the_primary() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let first = "keel/k_abort/j_one";
        let second = "keel/k_abort/j_two";
        let one = _rm.0.join("one");
        let two = _rm.0.join("two");
        crate::git::add_branch(&repo, &one, first, &base).unwrap();
        crate::git::commit_file(&one, "only.txt", b"one\n");
        crate::git::add_branch(&repo, &two, second, &base).unwrap();
        crate::git::commit_file(&two, "only.txt", b"two\n");
        insert_item(
            &cfg,
            "k_abort",
            Some(repo.to_str().unwrap()),
            Some(&base),
            "awaiting_merge",
        );
        insert_job(
            &cfg,
            "j_one",
            "k_abort",
            0,
            "accepted",
            Some(first),
            Some(one.to_str().unwrap()),
            &base,
        );
        insert_job(
            &cfg,
            "j_two",
            "k_abort",
            1,
            "accepted",
            Some(second),
            Some(two.to_str().unwrap()),
            &base,
        );
        dispatch(&cfg, &["merge".into(), "k_abort".into()]).unwrap();
        assert_eq!(crate::git::snapshot(&repo).unwrap().head, base);
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        assert!(!merge_head(&repo));
        let integ = cfg
            .data_dir
            .join("worktrees")
            .join("k_abort")
            .join("integrate");
        assert!(merge_head(&integ));
        let kind: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT conflict_kind FROM items WHERE id = 'k_abort'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(kind, "integrate");
        assert_eq!(state_of(&cfg, "k_abort"), "merge_conflict");
        let recorded: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT integrate_sha FROM items WHERE id = 'k_abort'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let status = Command::new("git")
            .arg("-C")
            .arg(&integ)
            .args(["merge", "--abort"])
            .status()
            .unwrap();
        assert!(status.success());
        let refused = dispatch(
            &cfg,
            &["merge".into(), "k_abort".into(), "--continue".into()],
        )
        .unwrap_err();
        assert!(refused.to_string().contains("--abort"), "{refused}");
        assert_eq!(state_of(&cfg, "k_abort"), "merge_conflict");
        let still: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT integrate_sha FROM items WHERE id = 'k_abort'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(still, recorded);

        dispatch(&cfg, &["merge".into(), "k_abort".into(), "--abort".into()]).unwrap();
        assert_eq!(state_of(&cfg, "k_abort"), "awaiting_merge");
        assert_eq!(crate::git::rev_parse(&integ, "HEAD").unwrap(), base);
        assert_eq!(crate::git::snapshot(&repo).unwrap().head, base);
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        let (sha, ord, conflict): (Option<String>, Option<i64>, Option<String>) = open(
            &cfg.db_path,
        )
        .unwrap()
        .query_row(
            "SELECT integrate_sha, integrate_ord, conflict_kind FROM items WHERE id = 'k_abort'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
        assert!(sha.is_none());
        assert!(ord.is_none());
        assert!(conflict.is_none());
        assert!(crate::git::branch_exists(&repo, first).unwrap());
        assert!(crate::git::branch_exists(&repo, second).unwrap());
        assert!(one.is_dir());
        assert!(two.is_dir());
        drop(running);
    }

    #[test]
    fn continue_merges_leaves_after_the_conflict() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let branches = ["keel/k_three/j0", "keel/k_three/j1", "keel/k_three/j2"];
        let wts = [_rm.0.join("w0"), _rm.0.join("w1"), _rm.0.join("w2")];
        crate::git::add_branch(&repo, &wts[0], branches[0], &base).unwrap();
        crate::git::commit_file(&wts[0], "shared.txt", b"zero\n");
        crate::git::add_branch(&repo, &wts[1], branches[1], &base).unwrap();
        crate::git::commit_file(&wts[1], "shared.txt", b"one\n");
        crate::git::add_branch(&repo, &wts[2], branches[2], &base).unwrap();
        let third = crate::git::commit_file(&wts[2], "third.txt", b"three\n");
        insert_item(
            &cfg,
            "k_three",
            Some(repo.to_str().unwrap()),
            Some(&base),
            "awaiting_merge",
        );
        for (ord, (branch, wt)) in branches.iter().zip(wts.iter()).enumerate() {
            insert_job(
                &cfg,
                &format!("j{ord}"),
                "k_three",
                ord as i64,
                "accepted",
                Some(branch),
                Some(wt.to_str().unwrap()),
                &base,
            );
        }
        dispatch(&cfg, &["merge".into(), "k_three".into()]).unwrap();
        let integ = cfg
            .data_dir
            .join("worktrees")
            .join("k_three")
            .join("integrate");
        assert!(merge_head(&integ));
        assert!(!integ.join("third.txt").exists());
        assert!(crate::git::branch_exists(&repo, branches[2]).unwrap());
        assert_eq!(state_of(&cfg, "k_three"), "merge_conflict");
        fs::write(integ.join("shared.txt"), "resolved\n").unwrap();
        assert!(Command::new("git")
            .arg("-C")
            .arg(&integ)
            .args(["add", "--", "shared.txt"])
            .status()
            .unwrap()
            .success());
        dispatch(
            &cfg,
            &["merge".into(), "k_three".into(), "--continue".into()],
        )
        .unwrap();
        assert_eq!(state_of(&cfg, "k_three"), "merged");
        assert_eq!(
            fs::read_to_string(repo.join("third.txt")).unwrap(),
            "three\n"
        );
        assert!(Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["merge-base", "--is-ancestor", &third, "HEAD"])
            .status()
            .unwrap()
            .success());
        assert!(!crate::git::branch_exists(&repo, branches[2]).unwrap());
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        drop(running);
    }

    #[test]
    fn continue_after_a_dirty_primary_does_not_demand_abort() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        dispatch(&cfg, &["status".into()]).unwrap();
        let repo = _rm.0.join("repo");
        let base = crate::git::init_repo(&repo);
        let first = "keel/k_retry/j0";
        let second = "keel/k_retry/j1";
        let one = _rm.0.join("one");
        let two = _rm.0.join("two");
        crate::git::add_branch(&repo, &one, first, &base).unwrap();
        crate::git::commit_file(&one, "shared.txt", b"zero\n");
        crate::git::add_branch(&repo, &two, second, &base).unwrap();
        crate::git::commit_file(&two, "shared.txt", b"one\n");
        insert_item(
            &cfg,
            "k_retry",
            Some(repo.to_str().unwrap()),
            Some(&base),
            "awaiting_merge",
        );
        insert_job(
            &cfg,
            "j0",
            "k_retry",
            0,
            "accepted",
            Some(first),
            Some(one.to_str().unwrap()),
            &base,
        );
        insert_job(
            &cfg,
            "j1",
            "k_retry",
            1,
            "accepted",
            Some(second),
            Some(two.to_str().unwrap()),
            &base,
        );
        dispatch(&cfg, &["merge".into(), "k_retry".into()]).unwrap();
        let integ = cfg
            .data_dir
            .join("worktrees")
            .join("k_retry")
            .join("integrate");
        assert!(merge_head(&integ));
        fs::write(repo.join("README"), "dirty\n").unwrap();
        fs::write(integ.join("shared.txt"), "resolved\n").unwrap();
        assert!(Command::new("git")
            .arg("-C")
            .arg(&integ)
            .args(["add", "--", "shared.txt"])
            .status()
            .unwrap()
            .success());
        let refused = dispatch(
            &cfg,
            &["merge".into(), "k_retry".into(), "--continue".into()],
        )
        .unwrap_err();
        assert!(refused.to_string().contains("dirty"), "{refused}");
        assert!(!refused.to_string().contains("--abort"), "{refused}");
        assert_eq!(state_of(&cfg, "k_retry"), "merge_conflict");
        let (kind, sha): (Option<String>, String) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT conflict_kind, integrate_sha FROM items WHERE id = 'k_retry'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(kind.is_none());
        assert_eq!(sha, crate::git::rev_parse(&integ, "HEAD").unwrap());
        assert!(crate::git::branch_exists(&repo, second).unwrap());
        let again = dispatch(
            &cfg,
            &["merge".into(), "k_retry".into(), "--continue".into()],
        )
        .unwrap_err();
        assert!(again.to_string().contains("dirty"), "{again}");
        assert!(!again.to_string().contains("--abort"), "{again}");
        let sha_again: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT integrate_sha FROM items WHERE id = 'k_retry'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sha_again, sha);
        fs::write(repo.join("README"), "one\n").unwrap();
        dispatch(
            &cfg,
            &["merge".into(), "k_retry".into(), "--continue".into()],
        )
        .unwrap();
        assert_eq!(state_of(&cfg, "k_retry"), "merged");
        assert!(crate::git::snapshot(&repo).unwrap().porcelain.is_empty());
        assert_eq!(
            fs::read_to_string(repo.join("shared.txt")).unwrap(),
            "resolved\n"
        );
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
