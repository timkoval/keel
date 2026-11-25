//! `keel mcp` speaks newline-delimited JSON-RPC on stdin/stdout.
//! Each tool is one Unix-socket call. This process does not open SQLite and
//! does not send `x-keel-surface: cli`, so the tool list cannot merge.

use std::io::{self, BufRead, Write};

use serde_json::{json, Map, Value};

use crate::cli;
use crate::config::Config;
use crate::db::{Error, Result};
use crate::render;

const PROTOCOL: &str = "2024-11-05";

const TOOLS: &[&str] = &[
    "keel_add",
    "keel_list",
    "keel_show",
    "keel_queue",
    "keel_claim",
    "keel_heartbeat",
    "keel_submit",
    "keel_release",
];

pub fn serve(config: &Config) -> Result<()> {
    let stdin = io::stdin();
    let mut reader = io::BufReader::new(stdin.lock());
    let mut stdout = io::stdout().lock();
    loop {
        let Some(bytes) = read_frame(&mut reader)? else {
            return Ok(());
        };
        if bytes.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let response = match handle_bytes(config, &bytes) {
            Ok(Some(response)) => response,
            Ok(None) => continue,
            Err(err) => rpc_error(Value::Null, -32603, &err.to_string()),
        };
        write_message(&mut stdout, &response)?;
    }
}

fn read_frame(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    let mut first = String::new();
    loop {
        first.clear();
        if reader.read_line(&mut first)? == 0 {
            return Ok(None);
        }
        if !first.trim().is_empty() {
            break;
        }
    }
    if first.trim_start().starts_with('{') {
        return Ok(Some(first.into_bytes()));
    }
    // LSP-style framing. A client that still sends Content-Length is the same protocol.
    let mut headers = first;
    if !headers.to_ascii_lowercase().starts_with("content-length:") {
        return Err(Error::Invalid("mcp message is not json".into()));
    }
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Err(Error::Invalid("eof".into()));
        }
        if line == "\n" || line == "\r\n" {
            break;
        }
        headers.push_str(&line);
    }
    let length = content_length(&headers)?;
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    Ok(Some(body))
}

fn content_length(headers: &str) -> Result<usize> {
    for line in headers.split(['\n', '\r']) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse()
                .map_err(|_| Error::Invalid("bad content-length".into()));
        }
    }
    Err(Error::Invalid("missing content-length".into()))
}

fn write_message(out: &mut impl Write, value: &Value) -> Result<()> {
    let mut line =
        serde_json::to_vec(value).map_err(|err| Error::Invalid(format!("json: {err}")))?;
    line.push(b'\n');
    out.write_all(&line)?;
    out.flush()?;
    Ok(())
}

pub(crate) fn handle_bytes(config: &Config, bytes: &[u8]) -> Result<Option<Value>> {
    let value: Value = match serde_json::from_slice(bytes) {
        Ok(value) => value,
        Err(_) => return Ok(Some(rpc_error(Value::Null, -32700, "parse error"))),
    };
    handle_value(config, &value)
}

pub(crate) fn handle_value(config: &Config, message: &Value) -> Result<Option<Value>> {
    let Some(object) = message.as_object() else {
        return Ok(Some(rpc_error(Value::Null, -32600, "invalid request")));
    };
    let id = object.get("id").cloned().unwrap_or(Value::Null);
    let method = match object.get("method").and_then(Value::as_str) {
        Some(method) => method,
        None => return Ok(Some(rpc_error(id, -32600, "invalid request"))),
    };
    // Notifications have no id. A response would be another request to the client.
    let notification = !object.contains_key("id") || method.starts_with("notifications/");
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    let result = match method {
        "initialize" => Ok(initialize(&params)),
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": tool_defs() })),
        "tools/call" => tools_call(config, &params),
        "notifications/initialized" | "notifications/cancelled" => Ok(Value::Null),
        _ => Err(rpc_failure(-32601, "method not found")),
    };
    if notification {
        return Ok(None);
    }
    match result {
        Ok(result) => Ok(Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }))),
        Err(failure) => Ok(Some(rpc_error(id, failure.code, &failure.message))),
    }
}

struct RpcFailure {
    code: i64,
    message: String,
}

fn rpc_failure(code: i64, message: impl Into<String>) -> RpcFailure {
    RpcFailure {
        code,
        message: message.into(),
    }
}

fn initialize(params: &Value) -> Value {
    let requested = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL);
    let version = if matches!(requested, "2024-11-05" | "2025-03-26" | "2025-06-18") {
        requested
    } else {
        PROTOCOL
    };
    json!({
        "protocolVersion": version,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": "keel", "version": env!("CARGO_PKG_VERSION") },
    })
}

fn tool_defs() -> Vec<Value> {
    vec![
        tool(
            "keel_add",
            "Capture an inbox item. Returns the id and card URL.",
            json!({
                "type": "object",
                "properties": {
                    "title": { "type": "string" },
                    "body": { "type": "string" },
                    "repo": { "type": "string" },
                    "target_branch": { "type": "string" },
                    "external_id": { "type": "string" }
                },
                "required": ["title"]
            }),
        ),
        tool(
            "keel_list",
            "List items. Pass state to filter.",
            json!({
                "type": "object",
                "properties": { "state": { "type": "string" } }
            }),
        ),
        tool(
            "keel_show",
            "Show one item, including its last ten events.",
            json!({
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"]
            }),
        ),
        tool(
            "keel_queue",
            "List the plan-review or feedback queue.",
            json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string", "enum": ["plan-review", "feedback"] }
                },
                "required": ["name"]
            }),
        ),
        tool(
            "keel_claim",
            "Claim an item or job. pid is the client process, stored as an external lease. A null pid is refused. The daemon does not signal that pid.",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string", "enum": ["item", "job"] },
                    "role": { "type": "string", "enum": ["plan", "revise", "implement", "review"] },
                    "pid": { "type": "integer" },
                    "worker_id": { "type": "string" }
                },
                "required": ["id", "kind", "role", "pid"]
            }),
        ),
        tool(
            "keel_heartbeat",
            "Slide the lease for the holding token.",
            id_kind_token_schema(),
        ),
        tool(
            "keel_submit",
            "Submit the held plan, revision, implementation, or review.",
            json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "kind": { "type": "string", "enum": ["item", "job"] },
                    "claim_token": { "type": "string" },
                    "document": {}
                },
                "required": ["id", "kind", "claim_token", "document"]
            }),
        ),
        tool(
            "keel_release",
            "Release the held claim. A live external pid is refused and is not signaled.",
            id_kind_token_schema(),
        ),
    ]
}

fn id_kind_token_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "kind": { "type": "string", "enum": ["item", "job"] },
            "claim_token": { "type": "string" }
        },
        "required": ["id", "kind", "claim_token"]
    })
}

fn tool(name: &str, description: &str, schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": schema,
    })
}

fn tools_call(config: &Config, params: &Value) -> std::result::Result<Value, RpcFailure> {
    let object = params
        .as_object()
        .ok_or_else(|| rpc_failure(-32602, "tools/call params must be an object"))?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| rpc_failure(-32602, "tool name is required"))?;
    if !TOOLS.contains(&name) {
        // Not a JSON-RPC failure: the call happened, and it cannot change a row.
        let message = format!("unknown tool {name}");
        return Ok(tool_result(&message, true));
    }
    let args = match object.get("arguments") {
        None | Some(Value::Null) => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => return Err(rpc_failure(-32602, "arguments must be an object")),
    };
    match dispatch_tool(config, name, &args) {
        Ok(value) => {
            let text = serde_json::to_string(&value)
                .map_err(|err| rpc_failure(-32603, format!("json: {err}")))?;
            Ok(tool_result(&text, false))
        }
        Err(err) => Ok(tool_result(&err.to_string(), true)),
    }
}

fn tool_result(text: &str, is_error: bool) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": is_error,
    })
}

fn dispatch_tool(config: &Config, name: &str, args: &Map<String, Value>) -> Result<Value> {
    match name {
        "keel_add" => tool_add(config, args),
        "keel_list" => tool_list(config, args),
        "keel_show" => tool_show(config, args),
        "keel_queue" => tool_queue(config, args),
        "keel_claim" => tool_claim(config, args),
        "keel_heartbeat" => tool_heartbeat(config, args),
        "keel_submit" => tool_submit(config, args),
        "keel_release" => tool_release(config, args),
        _ => Err(Error::Invalid(format!("unknown tool {name}"))),
    }
}

fn tool_add(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let title = required_str(args, "title")?;
    let mut body = json!({
        "source": "mcp",
        "title": title,
        "body": optional_str(args, "body")?.unwrap_or_default(),
    });
    if let Some(repo) = optional_str(args, "repo")? {
        body["repo"] = json!(repo);
    }
    if let Some(branch) = optional_str(args, "target_branch")? {
        body["target_branch"] = json!(branch);
    }
    if let Some(external_id) = optional_str(args, "external_id")? {
        body["external_id"] = json!(external_id);
    }
    socket(config, "POST", "/v1/items", Some(body))
}

fn tool_list(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let path = match optional_str(args, "state")? {
        Some(state) => {
            if state.is_empty() || !state.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(Error::Invalid("state is not a token".into()));
            }
            format!("/v1/items?state={state}")
        }
        None => "/v1/items".into(),
    };
    socket(config, "GET", &path, None)
}

fn tool_show(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let id = require_id(args)?;
    socket(config, "GET", &format!("/v1/items/{id}"), None)
}

fn tool_queue(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let name = required_str(args, "name")?;
    if name != "plan-review" && name != "feedback" {
        return Err(Error::Invalid(
            "queue name must be plan-review or feedback".into(),
        ));
    }
    socket(config, "GET", &format!("/v1/queues/{name}"), None)
}

fn tool_claim(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let id = require_id(args)?;
    let kind = require_kind(args)?;
    let role = required_str(args, "role")?;
    let matches_kind = matches!(
        (kind.as_str(), role.as_str()),
        ("item", "plan" | "revise") | ("job", "implement" | "review")
    );
    if !matches_kind {
        return Err(Error::Invalid(format!("role {role} does not match {kind}")));
    }
    // A missing pid must not become a daemon-spawned claim. Those rows are signaled.
    let pid = require_pid(args)?;
    let mut body = json!({
        "role": role,
        "adapter": "mcp",
        "pid": pid,
    });
    if let Some(worker) = optional_str(args, "worker_id")? {
        body["worker_id"] = json!(worker);
    }
    let path = action_path(&kind, &id, "claim")?;
    socket(config, "POST", &path, Some(body))
}

fn tool_heartbeat(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let (path, token) = token_path(args, "heartbeat")?;
    socket(config, "POST", &path, Some(json!({ "claim_token": token })))
}

fn tool_submit(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let (path, token) = token_path(args, "submit")?;
    let Some(document) = args.get("document") else {
        return Err(Error::Invalid("document is required".into()));
    };
    if !matches!(document, Value::Object(_) | Value::String(_)) {
        return Err(Error::Invalid(
            "document must be a JSON object or a JSON string".into(),
        ));
    }
    socket(
        config,
        "POST",
        &path,
        Some(json!({ "claim_token": token, "document": document })),
    )
}

fn tool_release(config: &Config, args: &Map<String, Value>) -> Result<Value> {
    let (path, token) = token_path(args, "release")?;
    // Do not forward force, cause, or pid_gone. The daemon sets pid_gone only
    // after it has read /proc, and it does not signal an external pid.
    socket(config, "POST", &path, Some(json!({ "claim_token": token })))
}

fn token_path(args: &Map<String, Value>, action: &str) -> Result<(String, String)> {
    let id = require_id(args)?;
    let kind = require_kind(args)?;
    let token = required_str(args, "claim_token")?;
    if token.is_empty() {
        return Err(Error::Invalid("claim_token is empty".into()));
    }
    Ok((action_path(&kind, &id, action)?, token))
}

fn action_path(kind: &str, id: &str, action: &str) -> Result<String> {
    match kind {
        "item" => Ok(format!("/v1/items/{id}/{action}")),
        "job" => Ok(format!("/v1/jobs/{id}/{action}")),
        _ => Err(Error::Invalid("kind must be item or job".into())),
    }
}

fn require_id(args: &Map<String, Value>) -> Result<String> {
    let id = required_str(args, "id")?;
    if !render::safe_id(&id) {
        return Err(Error::NotFound(id));
    }
    Ok(id)
}

fn require_kind(args: &Map<String, Value>) -> Result<String> {
    let kind = required_str(args, "kind")?;
    if kind != "item" && kind != "job" {
        return Err(Error::Invalid("kind must be item or job".into()));
    }
    Ok(kind)
}

fn require_pid(args: &Map<String, Value>) -> Result<i64> {
    match args.get("pid") {
        None => Err(Error::Invalid("pid is required".into())),
        Some(Value::Null) => Err(Error::Invalid("null pid is refused".into())),
        Some(Value::Number(number)) => {
            let pid = number
                .as_i64()
                .ok_or_else(|| Error::Invalid("pid must be an integer".into()))?;
            if pid <= 0 {
                return Err(Error::Invalid("pid is not alive".into()));
            }
            Ok(pid)
        }
        Some(_) => Err(Error::Invalid("pid must be an integer".into())),
    }
}

fn required_str(args: &Map<String, Value>, key: &str) -> Result<String> {
    match args.get(key) {
        Some(Value::String(value)) => Ok(value.clone()),
        Some(Value::Null) => Err(Error::Invalid(format!("{key} is required"))),
        None => Err(Error::Invalid(format!("{key} is required"))),
        Some(_) => Err(Error::Invalid(format!("{key} must be a string"))),
    }
}

fn optional_str(args: &Map<String, Value>, key: &str) -> Result<Option<String>> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.is_empty() => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::Invalid(format!("{key} must be a string"))),
    }
}

fn socket(config: &Config, method: &str, path: &str, body: Option<Value>) -> Result<Value> {
    let bytes = match body {
        Some(body) => {
            Some(serde_json::to_vec(&body).map_err(|err| Error::Invalid(format!("json: {err}")))?)
        }
        None => None,
    };
    let response = cli::daemon_exchange(config, method, path, bytes)?;
    let text = String::from_utf8_lossy(&response.body).into_owned();
    if !(200..300).contains(&response.status) {
        return Err(Error::Invalid(format!("daemon {} {text}", response.status)));
    }
    serde_json::from_str(&text).map_err(|err| Error::Invalid(format!("json: {err}")))
}

fn rpc_error(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::{boot_at, scratch_dir, Rm};
    use crate::db::open;
    use rusqlite::params;
    use std::path::Path;
    use std::process::Command;

    fn call(cfg: &Config, method: &str, params: Value) -> Value {
        handle_value(
            cfg,
            &json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        )
        .unwrap()
        .expect("response")
    }

    fn tool(cfg: &Config, name: &str, arguments: Value) -> Value {
        let response = call(
            cfg,
            "tools/call",
            json!({"name": name, "arguments": arguments}),
        );
        assert!(response.get("error").is_none(), "{response}");
        response["result"].clone()
    }

    fn tool_text(result: &Value) -> String {
        result["content"][0]["text"]
            .as_str()
            .unwrap_or("")
            .to_string()
    }

    fn item_state(cfg: &Config, id: &str) -> String {
        open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT state FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn proxy_forwards_tools_and_cannot_merge() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let init = call(&cfg, "initialize", json!({"protocolVersion": "2024-11-05"}));
        assert_eq!(init["result"]["serverInfo"]["name"], "keel");
        assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
        assert!(handle_value(
            &cfg,
            &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
        .unwrap()
        .is_none());

        let listed = call(&cfg, "tools/list", json!({}));
        let names: Vec<&str> = listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, TOOLS);
        for name in [
            "keel_approve",
            "keel_accept",
            "keel_reject",
            "keel_merge",
            "keel_drop",
            "keel_waive",
            "keel_permit",
            "keel_retry",
        ] {
            assert!(!names.contains(&name), "{name}");
        }
        let claim_schema = &listed["result"]["tools"][4]["inputSchema"]["required"];
        assert!(claim_schema
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "pid"));
        let release_schema = &listed["result"]["tools"][7]["inputSchema"]["required"];
        for key in ["id", "kind", "claim_token"] {
            assert!(release_schema
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == key));
        }

        let added = tool(
            &cfg,
            "keel_add",
            json!({"title": "Pin the walk", "body": "notes"}),
        );
        assert_eq!(added["isError"], false);
        let created: Value = serde_json::from_str(&tool_text(&added)).unwrap();
        let id = created["id"].as_str().unwrap().to_string();
        let source: String = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT source FROM items WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source, "mcp");

        let rows = tool(&cfg, "keel_list", json!({"state": "inbox"}));
        assert!(tool_text(&rows).contains(&id));
        let shown = tool(&cfg, "keel_show", json!({"id": id}));
        assert!(tool_text(&shown).contains("Pin the walk"));

        let null_pid = tool(
            &cfg,
            "keel_claim",
            json!({"id": id, "kind": "item", "role": "plan", "pid": null}),
        );
        assert_eq!(null_pid["isError"], true);
        assert!(tool_text(&null_pid).contains("null pid"));
        assert_eq!(item_state(&cfg, &id), "inbox");
        let leases: i64 = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM items WHERE claim_token IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(leases, 0);

        let dead = tool(
            &cfg,
            "keel_claim",
            json!({"id": id, "kind": "item", "role": "plan", "pid": 2147483646}),
        );
        assert_eq!(dead["isError"], true);
        assert!(tool_text(&dead).contains("pid"), "{}", tool_text(&dead));
        assert_eq!(item_state(&cfg, &id), "inbox");

        let mut child = Command::new("sleep").arg("60").spawn().unwrap();
        let pid = i64::from(child.id());
        let claimed = tool(
            &cfg,
            "keel_claim",
            json!({"id": id, "kind": "item", "role": "plan", "pid": pid}),
        );
        assert_eq!(claimed["isError"], false, "{}", tool_text(&claimed));
        let claim: Value = serde_json::from_str(&tool_text(&claimed)).unwrap();
        let token = claim["claim_token"].as_str().unwrap().to_string();
        let (external, claim_pid, pgid): (i64, i64, Option<i64>) = open(&cfg.db_path)
            .unwrap()
            .query_row(
                "SELECT claim_external, claim_pid, claim_pgid FROM items WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(external, 1);
        assert_eq!(claim_pid, pid);
        assert!(pgid.is_none());
        assert!(Path::new(&format!("/proc/{pid}")).exists());

        let beat = tool(
            &cfg,
            "keel_heartbeat",
            json!({"id": id, "kind": "item", "claim_token": token}),
        );
        assert_eq!(beat["isError"], false, "{}", tool_text(&beat));
        assert!(tool_text(&beat).contains("lease_until"));

        let refused = tool(
            &cfg,
            "keel_release",
            json!({
                "id": id,
                "kind": "item",
                "claim_token": token,
                "force": true,
                "pid_gone": true,
                "cause": "expiry",
            }),
        );
        assert_eq!(refused["isError"], true, "{}", tool_text(&refused));
        assert!(
            tool_text(&refused).contains("409"),
            "{}",
            tool_text(&refused)
        );
        assert_eq!(item_state(&cfg, &id), "planning");
        // Live external pid: release did not signal it.
        assert!(Path::new(&format!("/proc/{pid}")).exists());

        let _ = child.kill();
        let _ = child.wait();
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        let released = tool(
            &cfg,
            "keel_release",
            json!({"id": id, "kind": "item", "claim_token": token}),
        );
        assert_eq!(released["isError"], false, "{}", tool_text(&released));
        assert_eq!(item_state(&cfg, &id), "inbox");

        let again = tool(
            &cfg,
            "keel_claim",
            json!({"id": id, "kind": "item", "role": "plan", "pid": std::process::id()}),
        );
        assert_eq!(again["isError"], false, "{}", tool_text(&again));
        let again: Value = serde_json::from_str(&tool_text(&again)).unwrap();
        let token = again["claim_token"].as_str().unwrap();
        let submitted = tool(
            &cfg,
            "keel_submit",
            json!({
                "id": id,
                "kind": "item",
                "claim_token": token,
                "document": {"summary": "s", "state": "merged", "jobs": [{"title": "Do"}]}
            }),
        );
        assert_eq!(submitted["isError"], false, "{}", tool_text(&submitted));
        assert_eq!(item_state(&cfg, &id), "awaiting_plan_review");
        assert_ne!(item_state(&cfg, &id), "merged");

        let queue = tool(&cfg, "keel_queue", json!({"name": "plan-review"}));
        assert_eq!(queue["isError"], false, "{}", tool_text(&queue));
        assert!(tool_text(&queue).contains(&id));

        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET state = 'awaiting_merge' WHERE id = ?1",
                params![id],
            )
            .unwrap();
        for name in [
            "keel_merge",
            "keel_approve",
            "keel_accept",
            "keel_reject",
            "keel_drop",
            "keel_waive",
            "keel_permit",
            "keel_retry",
        ] {
            let result = tool(&cfg, name, json!({"id": id, "state": "merged"}));
            assert_eq!(result["isError"], true, "{name} {}", tool_text(&result));
            assert_eq!(item_state(&cfg, &id), "awaiting_merge");
        }
        let sneak = tool(
            &cfg,
            "keel_submit",
            json!({
                "id": id,
                "kind": "item",
                "claim_token": "nope",
                "document": {"state": "merged"}
            }),
        );
        assert_eq!(sneak["isError"], true, "{}", tool_text(&sneak));
        assert_eq!(item_state(&cfg, &id), "awaiting_merge");

        let mut stream = std::net::TcpStream::connect(("127.0.0.1", cfg.port)).unwrap();
        let posted = crate::http::exchange(&mut stream, "POST", "/v1/items", Some(b"{}")).unwrap();
        assert_eq!(posted.status, 405);
        drop(running);
    }

    #[test]
    fn job_submit_uses_the_worktree_and_does_not_merge() {
        let _rm = Rm(scratch_dir());
        let (cfg, running) = boot_at(&_rm.0).unwrap();
        let added = tool(&cfg, "keel_add", json!({"title": "Implement"}));
        let created: Value = serde_json::from_str(&tool_text(&added)).unwrap();
        let item_id = created["id"].as_str().unwrap().to_string();
        let repo = _rm.0.join("repo");
        let head = crate::git::init_repo(&repo);
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "UPDATE items SET state = 'awaiting_implementation', repo = ?1, target_branch = 'main', base_sha = ?2
                 WHERE id = ?3",
                params![repo.to_str().unwrap(), head, item_id],
            )
            .unwrap();
        open(&cfg.db_path)
            .unwrap()
            .execute(
                "INSERT INTO jobs (id, item_id, title, acceptance, state, depth, ord, base_sha, updated_at)
                 VALUES ('j_mcp', ?1, 'Leaf', '', 'queued', 1, 0, ?2, '2026-11-25T00:00:00Z')",
                params![item_id, head],
            )
            .unwrap();
        let claimed = tool(
            &cfg,
            "keel_claim",
            json!({
                "id": "j_mcp",
                "kind": "job",
                "role": "implement",
                "pid": std::process::id(),
                "worker_id": "mcp:impl"
            }),
        );
        assert_eq!(claimed["isError"], false, "{}", tool_text(&claimed));
        let claim: Value = serde_json::from_str(&tool_text(&claimed)).unwrap();
        let token = claim["claim_token"].as_str().unwrap();
        let worktree = claim["worktree"].as_str().unwrap();
        crate::git::commit_file(Path::new(worktree), "leaf.txt", b"leaf\n");
        let submitted = tool(
            &cfg,
            "keel_submit",
            json!({
                "id": "j_mcp",
                "kind": "job",
                "claim_token": token,
                "document": {"state": "merged"}
            }),
        );
        assert_eq!(submitted["isError"], false, "{}", tool_text(&submitted));
        let body: Value = serde_json::from_str(&tool_text(&submitted)).unwrap();
        assert_eq!(body["state"], "ready_for_review");
        assert_eq!(body["item_state"], "implementing");
        assert_ne!(item_state(&cfg, &item_id), "merged");
        drop(running);
    }
}
