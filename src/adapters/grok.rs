//! Private `grok agent --no-leader serve` and the one ACP client for that job.
//! A second socket would replace Grok's relay and steal permission requests.
//! The per-job bearer secret stays in memory and a 0600 file under logs/.
//! It is not a column, not an HTML field, and not a query parameter.

use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tungstenite::client::IntoClientRequest;
use tungstenite::{Message, WebSocket};

use crate::claim::{self, Target};
use crate::daemon::read_proc_starttime;
use crate::db::{Error, Result};
use crate::proc;
use crate::state::{self, Role};

const FORBIDDEN_PORT: u16 = 2419;
const HEARTBEAT_EVERY: Duration = Duration::from_secs(60);

type Socket = WebSocket<TcpStream>;

/// One live client. A failed dial does not take the relay, so the bind retry
/// may connect. After a socket is up, another dial is refused before it opens.
#[derive(Default)]
struct Slot {
    live: bool,
}

impl Slot {
    fn connect_once<T>(&mut self, dial: impl FnOnce() -> Result<T>) -> Result<T> {
        if self.live {
            return Err(Error::Invalid(
                "second websocket refused; the daemon is the only ACP client".into(),
            ));
        }
        let socket = dial()?;
        self.live = true;
        Ok(socket)
    }
}

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

struct Session<'a> {
    db: &'a Mutex<rusqlite::Connection>,
    target: Target,
    id: String,
    token: String,
    log_path: PathBuf,
    secret: String,
}

pub(crate) fn ensure_grok(path: &str) -> Result<PathBuf> {
    for dir in path.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = Path::new(dir).join("grok");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(Error::Invalid("grok is not on PATH".into()))
}

pub(crate) fn ensure_grok_on_path() -> Result<PathBuf> {
    ensure_grok(&std::env::var("PATH").unwrap_or_default())
}

pub(crate) fn serve_args(port: u16, secret: &str) -> Result<Vec<String>> {
    if port == 0 || port == FORBIDDEN_PORT {
        return Err(Error::Invalid(
            "refusing to bind grok to port 0 or 2419".into(),
        ));
    }
    if secret.is_empty() || secret.contains([' ', '\n', '\r']) {
        return Err(Error::Invalid("grok secret is empty".into()));
    }
    // Clap has no `ask` value. `default` is the accepted spelling that turns
    // config `always-approve` and `auto` off for this launch. It has to sit
    // before `agent`; `serve` itself rejects `--permission-mode`.
    Ok(vec![
        "--permission-mode".into(),
        "default".into(),
        "agent".into(),
        "--no-leader".into(),
        "serve".into(),
        "--bind".into(),
        format!("127.0.0.1:{port}"),
        "--secret".into(),
        secret.into(),
    ])
}

/// `empty_group` uses this message when SIGKILL leaves the group occupied.
pub(crate) fn group_still_live(err: &Error) -> bool {
    matches!(
        err,
        Error::Conflict { message, .. } if message == "process group is not empty"
    )
}

pub(crate) fn ws_url(port: u16) -> String {
    format!("ws://127.0.0.1:{port}/ws")
}

pub(crate) fn execute(
    db: &Mutex<rusqlite::Connection>,
    data_dir: &Path,
    launch: &Launch,
) -> Result<RunDone> {
    if !crate::render::safe_id(&launch.id) {
        return Err(Error::Invalid("id is not a path segment".into()));
    }
    let bin = ensure_grok_on_path()?;
    let secret = new_secret()?;
    let log_dir = data_dir.join("logs");
    ensure_private_dir(&log_dir)?;
    let log_path = log_dir.join(format!("{}.log", launch.id));
    write_private(
        &log_dir.join(format!("{}.secret", launch.id)),
        secret.as_bytes(),
    )?;
    if launch.target == Target::Job {
        let mut conn = lock_db(db)?;
        state::set_job_permission_ask(&mut conn, &launch.id, &launch.token)?;
        drop(conn);
    }
    let _ = fs::create_dir_all(data_dir.join("results"));
    let cwd = launch
        .worktree
        .clone()
        .unwrap_or_else(|| data_dir.to_path_buf());
    let output_path = data_dir.join("results").join(format!("{}.json", launch.id));
    let prompt = prompt_text(&launch.prompt_path, &output_path);

    let mut slot = Slot::default();
    let mut last_err = Error::Invalid("grok serve did not start".into());
    // The pid already stored on the row. A bind retry replaces it only after
    // that group is empty, so a release during dial still goes through the
    // supervisor instead of the null-pid path.
    let mut previous_pid: Option<i64> = None;
    for attempt in 0..2 {
        let port = pick_port()?;
        let args = serve_args(port, &secret)?;
        let spawned = spawn_grouped(&bin, &args, &log_path)?;
        let starttime = spawned.starttime.clone();
        let pid = spawned.pid;
        let pgid = spawned.pgid;
        let mut held = ChildGuard {
            child: Some(spawned.child),
            pid,
            pgid,
            settled: false,
        };
        let recorded = {
            let mut conn = lock_db(db)?;
            match previous_pid {
                None => state::attach_process(
                    &mut conn,
                    &launch.id,
                    &launch.token,
                    pid,
                    pgid,
                    &starttime,
                ),
                Some(old) => state::replace_attached_process(
                    &mut conn,
                    &launch.id,
                    &launch.token,
                    old,
                    pid,
                    pgid,
                    &starttime,
                ),
            }
        };
        if let Err(err) = recorded {
            return stop_held(&mut held, err);
        }
        // A dead leader is a failed bind. Kill it, and pick another port only
        // when the group is actually empty.
        if leader_dead(pid) {
            last_err = Error::Invalid(format!(
                "grok serve exited {}",
                log_tail(&log_path, &secret)
            ));
            match held.reap() {
                Err(live) => return Err(live),
                Ok(()) if attempt == 0 => {
                    previous_pid = Some(pid);
                    continue;
                }
                Ok(()) => return Err(last_err),
            }
        }
        match slot.connect_once(|| dial(port, &secret, pid)) {
            Ok(socket) => {
                let stop = Arc::new(AtomicBool::new(false));
                let spoken = thread::scope(|scope| {
                    let flag = Arc::clone(&stop);
                    let id = launch.id.clone();
                    let token = launch.token.clone();
                    scope.spawn(move || {
                        heartbeat_loop(db, launch.target, id, token, flag);
                    });
                    let session = Session {
                        db,
                        target: launch.target,
                        id: launch.id.clone(),
                        token: launch.token.clone(),
                        log_path: log_path.clone(),
                        secret: secret.clone(),
                    };
                    let result = speak(&cwd, &prompt, socket, &session);
                    stop.store(true, Ordering::SeqCst);
                    result
                });
                held.reap()?;
                spoken?;
                return Ok(RunDone {
                    output: read_output(&output_path)?,
                });
            }
            Err(err) => {
                let dead = leader_dead(pid);
                last_err = Error::Invalid(format!("{err} {}", log_tail(&log_path, &secret)));
                match held.reap() {
                    Err(live) => return Err(live),
                    Ok(()) if attempt == 0 && dead => {
                        previous_pid = Some(pid);
                        continue;
                    }
                    Ok(()) => return Err(last_err),
                }
            }
        }
    }
    Err(last_err)
}

/// The child is already in its own group. A live group must stay leased.
fn stop_held<T>(held: &mut ChildGuard, err: Error) -> Result<T> {
    match held.reap() {
        Err(live) => Err(live),
        Ok(()) => Err(err),
    }
}

fn speak(cwd: &Path, prompt: &str, mut socket: Socket, session: &Session) -> Result<()> {
    let cwd = cwd
        .to_str()
        .ok_or_else(|| Error::Invalid("worktree path is not utf-8".into()))?;
    rpc(
        &mut socket,
        session,
        "initialize",
        json!({
            "protocolVersion": 1,
            "clientCapabilities": {
                "fs": { "readTextFile": false, "writeTextFile": false },
                "terminal": false
            },
            "clientInfo": { "name": "keel", "version": "0.1.0" }
        }),
    )?;
    rpc(
        &mut socket,
        session,
        "session/new",
        json!({ "cwd": cwd, "mcpServers": [] }),
    )?;
    rpc(
        &mut socket,
        session,
        "session/prompt",
        json!({
            "sessionId": session_id_placeholder(),
            "prompt": [{ "type": "text", "text": prompt }]
        }),
    )?;
    Ok(())
}

/// `session/new` returns the id the prompt must echo. The placeholder is replaced
/// once that response arrives. Tests and the live loop share `rpc`.
fn session_id_placeholder() -> String {
    SESSION_ID.with(|cell| cell.borrow().clone())
}

thread_local! {
    static SESSION_ID: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
    static NEXT_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
}

fn prompt_text(prompt_path: &Path, output_path: &Path) -> String {
    format!(
        "Read the prompt file at {} and do that work. Write the Keel result JSON to {}. Do not git push.",
        prompt_path.display(),
        output_path.display()
    )
}

fn rpc(socket: &mut Socket, session: &Session, method: &str, mut params: Value) -> Result<Value> {
    if method == "session/prompt" {
        let sid = SESSION_ID.with(|cell| cell.borrow().clone());
        if sid.is_empty() {
            return Err(Error::Invalid(
                "session/new did not return sessionId".into(),
            ));
        }
        params["sessionId"] = json!(sid);
    }
    let id = NEXT_ID.with(|cell| {
        let id = cell.get();
        cell.set(id + 1);
        id
    });
    let body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    send_text(socket, &body.to_string())?;
    loop {
        let msg = read_json(socket)?;
        match classify(&msg) {
            Incoming::RpcOk { id: rid, result } if rid == json!(id) => {
                if method == "session/new" {
                    let sid = result
                        .get("sessionId")
                        .and_then(|value| value.as_str())
                        .filter(|sid| !sid.is_empty())
                        .ok_or_else(|| Error::Invalid("session/new missing sessionId".into()))?
                        .to_string();
                    SESSION_ID.with(|cell| *cell.borrow_mut() = sid);
                }
                return Ok(result);
            }
            Incoming::RpcErr { id: rid, message } if rid == json!(id) => {
                return Err(Error::Invalid(format!("acp {method}: {message}")));
            }
            Incoming::Server(req) => handle_server(socket, session, req)?,
            Incoming::RpcOk { .. } | Incoming::RpcErr { .. } | Incoming::Ignore => {}
        }
    }
}

fn handle_server(socket: &mut Socket, session: &Session, req: ServerReq) -> Result<()> {
    match req {
        ServerReq::ExitPlan { id } => {
            note(&session.log_path, "exit_plan_mode cancelled");
            send_text(socket, &plan_cancelled_reply(id).to_string())
        }
        ServerReq::Unknown {
            id: Some(id),
            method,
        } => send_text(socket, &unknown_method_reply(id, &method).to_string()),
        ServerReq::Unknown { id: None, .. } => Ok(()),
        ServerReq::Wait(pending) => {
            let view = pending.view(&session.secret);
            let result = wait_for_permit(session, &view)?;
            let id = pending.request_id;
            send_text(
                socket,
                &json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": result,
                })
                .to_string(),
            )
        }
    }
}

fn wait_for_permit(session: &Session, view: &Value) -> Result<Value> {
    let (tx, rx) = mpsc::channel();
    install_waiter(&session.id, tx);
    if session.target == Target::Job {
        let mut conn = lock_db(session.db)?;
        if let Err(err) =
            state::arm_job_permit(&mut conn, &session.id, &session.token, &view.to_string())
        {
            remove_waiter(&session.id);
            return Err(err);
        }
    }
    publish_public(&session.id, view.clone());
    let result = recv_permit(session, rx);
    clear_public(&session.id);
    remove_waiter(&session.id);
    if session.target == Target::Job {
        let mut conn = lock_db(session.db)?;
        let _ = state::clear_job_permit(&mut conn, &session.id, &session.token);
    }
    result
}

fn recv_permit(session: &Session, rx: Receiver<Value>) -> Result<Value> {
    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Ok(value) => return Ok(value),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if session.target == Target::Job {
                    if let Some(value) = sqlite_answer(session.db, &session.id)? {
                        return Ok(value);
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                if session.target == Target::Job {
                    if let Some(value) = sqlite_answer(session.db, &session.id)? {
                        return Ok(value);
                    }
                }
                return Err(Error::Conflict {
                    state: "permission".into(),
                    message: "permit waiter is gone".into(),
                });
            }
        }
    }
}

fn sqlite_answer(db: &Mutex<rusqlite::Connection>, id: &str) -> Result<Option<Value>> {
    let conn = lock_db(db)?;
    let Some(blob) = state::read_permit_blob(&conn, id)? else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(&blob).unwrap_or(Value::Null);
    if value.get("phase").and_then(|phase| phase.as_str()) == Some("answered") {
        return Ok(Some(value.get("result").cloned().unwrap_or(Value::Null)));
    }
    Ok(None)
}

struct Waiters {
    tx: Mutex<std::collections::HashMap<String, Sender<Value>>>,
    public: Mutex<std::collections::HashMap<String, Value>>,
}

fn waiters() -> &'static Waiters {
    static WAITERS: std::sync::OnceLock<Waiters> = std::sync::OnceLock::new();
    WAITERS.get_or_init(|| Waiters {
        tx: Mutex::new(std::collections::HashMap::new()),
        public: Mutex::new(std::collections::HashMap::new()),
    })
}

fn install_waiter(id: &str, tx: Sender<Value>) {
    if let Ok(mut map) = waiters().tx.lock() {
        map.insert(id.to_string(), tx);
    }
}

fn remove_waiter(id: &str) {
    if let Ok(mut map) = waiters().tx.lock() {
        map.remove(id);
    }
}

fn publish_public(id: &str, view: Value) {
    if let Ok(mut map) = waiters().public.lock() {
        map.insert(id.to_string(), view);
    }
}

fn clear_public(id: &str) {
    if let Ok(mut map) = waiters().public.lock() {
        map.remove(id);
    }
}

pub(crate) fn pending_public(id: &str) -> Option<Value> {
    waiters().public.lock().ok()?.get(id).cloned()
}

fn send_waiter(id: &str, result: Value) -> Result<()> {
    let tx = waiters()
        .tx
        .lock()
        .map_err(|_| Error::Invalid("permit map poisoned".into()))?
        .get(id)
        .cloned();
    match tx {
        Some(tx) => tx.send(result).map_err(|_| Error::Conflict {
            state: "permission".into(),
            message: "permit waiter is gone".into(),
        }),
        None => Err(Error::Conflict {
            state: "permission".into(),
            message: "no permission is waiting".into(),
        }),
    }
}

pub(crate) fn answer(
    db: &Mutex<rusqlite::Connection>,
    target: Target,
    id: &str,
    choice: &str,
) -> Result<Value> {
    let pending = pending_public(id).ok_or_else(|| Error::Conflict {
        state: "permission".into(),
        message: "no permission is waiting".into(),
    })?;
    let result = map_choice(&pending, choice)?;
    if target == Target::Job {
        let blob = json!({ "phase": "answered", "result": result }).to_string();
        let mut conn = lock_db(db)?;
        state::write_permit_answer(&mut conn, id, &blob)?;
    }
    send_waiter(id, result.clone())?;
    Ok(result)
}

pub(crate) fn map_choice(pending: &Value, choice: &str) -> Result<Value> {
    let choice = choice.trim();
    if choice.is_empty() {
        return Err(Error::Invalid("missing permit choice".into()));
    }
    match pending.get("kind").and_then(|value| value.as_str()) {
        Some("question") => map_question(pending, choice),
        _ => map_permission(pending, choice),
    }
}

fn map_permission(pending: &Value, choice: &str) -> Result<Value> {
    match choice {
        "y" | "--once" => {
            let option_id = find_option(pending, |opt| {
                kind_of(opt).contains("allow_once") || option_id_of(opt) == "allow-once"
            })
            .ok_or_else(|| Error::Invalid("allow_once is not one of the options".into()))?;
            Ok(selected(&option_id))
        }
        "n" | "--deny" => Ok(json!({ "outcome": { "outcome": "cancelled" } })),
        "A" | "--always" => {
            let option_id = find_option(pending, |opt| {
                kind_of(opt).contains("allow_always")
                    || option_id_of(opt) == "allow-always"
                    || option_id_of(opt).contains("allow_always")
            })
            .ok_or_else(|| {
                Error::Invalid("allow_always is not one of the options; refusing".into())
            })?;
            Ok(selected(&option_id))
        }
        _ => Err(Error::Invalid(
            "permit choice must be y, n, A, --once, --deny, or --always".into(),
        )),
    }
}

fn map_question(pending: &Value, choice: &str) -> Result<Value> {
    if choice == "n" || choice == "--deny" {
        return Ok(json!({ "outcome": "cancelled" }));
    }
    let Some(index) = choice
        .parse::<u32>()
        .ok()
        .filter(|_| choice.len() == 1 && choice != "0")
    else {
        return Err(Error::Invalid("a user question takes a digit or n".into()));
    };
    let options = pending
        .get("options")
        .and_then(|value| value.as_array())
        .ok_or_else(|| Error::Invalid("question has no options".into()))?;
    let opt = options
        .get((index as usize).saturating_sub(1))
        .ok_or_else(|| Error::Invalid(format!("option {choice} is not in the question")))?;
    let label = opt
        .get("label")
        .and_then(|value| value.as_str())
        .or_else(|| opt.get("name").and_then(|value| value.as_str()))
        .filter(|label| !label.is_empty())
        .ok_or_else(|| Error::Invalid("question option has no label".into()))?;
    let question = pending
        .get("question")
        .and_then(|value| value.as_str())
        .filter(|question| !question.is_empty())
        .ok_or_else(|| Error::Invalid("question text is missing".into()))?;
    Ok(json!({
        "outcome": "accepted",
        "answers": { question: [label] }
    }))
}

fn selected(option_id: &str) -> Value {
    json!({ "outcome": { "outcome": "selected", "optionId": option_id } })
}

fn find_option(pending: &Value, pred: impl Fn(&Value) -> bool) -> Option<String> {
    pending
        .get("options")
        .and_then(|value| value.as_array())
        .and_then(|options| {
            options.iter().find(|opt| pred(opt)).and_then(|opt| {
                opt.get("optionId")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
        })
}

fn kind_of(opt: &Value) -> String {
    opt.get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string()
}

fn option_id_of(opt: &Value) -> String {
    opt.get("optionId")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string()
}

#[derive(Debug)]
struct PendingReq {
    request_id: Value,
    kind: &'static str,
    tool: String,
    question: Option<String>,
    options: Vec<Value>,
}

impl PendingReq {
    fn view(&self, secret: &str) -> Value {
        json!({
            "phase": "waiting",
            "kind": self.kind,
            "request_id": self.request_id,
            "tool": redact(&self.tool, secret),
            "question": self.question.as_ref().map(|text| redact(text, secret)),
            "options": self.options.iter().map(|opt| redact_option(opt, secret)).collect::<Vec<_>>(),
        })
    }
}

fn redact_option(opt: &Value, secret: &str) -> Value {
    let mut out = opt.clone();
    if let Some(object) = out.as_object_mut() {
        for key in ["name", "label", "kind", "optionId"] {
            if let Some(Value::String(text)) = object.get_mut(key) {
                *text = redact(text, secret);
            }
        }
    }
    out
}

#[derive(Debug)]
enum ServerReq {
    Wait(PendingReq),
    ExitPlan { id: Value },
    Unknown { id: Option<Value>, method: String },
}

#[derive(Debug)]
enum Incoming {
    RpcOk { id: Value, result: Value },
    RpcErr { id: Value, message: String },
    Server(ServerReq),
    Ignore,
}

fn classify(msg: &Value) -> Incoming {
    if let Some(method) = msg.get("method").and_then(|value| value.as_str()) {
        let id = msg.get("id").cloned();
        let params = unwrap_params(msg.get("params").cloned().unwrap_or(json!({})));
        return Incoming::Server(server_req(method, id, &params));
    }
    if let Some(id) = msg.get("id").cloned() {
        if let Some(err) = msg.get("error") {
            let message = err
                .get("message")
                .and_then(|value| value.as_str())
                .unwrap_or("rpc error")
                .to_string();
            return Incoming::RpcErr { id, message };
        }
        return Incoming::RpcOk {
            id,
            result: msg.get("result").cloned().unwrap_or(Value::Null),
        };
    }
    Incoming::Ignore
}

fn server_req(method: &str, id: Option<Value>, params: &Value) -> ServerReq {
    let bare = method.strip_prefix('_').unwrap_or(method);
    match bare {
        "session/update" => ServerReq::Unknown {
            id: None,
            method: method.to_string(),
        },
        "session/request_permission" => match id {
            Some(id) => ServerReq::Wait(PendingReq {
                request_id: id,
                kind: "permission",
                tool: tool_label(params),
                question: None,
                options: params
                    .get("options")
                    .and_then(|value| value.as_array())
                    .cloned()
                    .unwrap_or_default(),
            }),
            None => ServerReq::Unknown {
                id: None,
                method: method.to_string(),
            },
        },
        "x.ai/exit_plan_mode" => match id {
            Some(id) => ServerReq::ExitPlan { id },
            None => ServerReq::Unknown {
                id: None,
                method: method.to_string(),
            },
        },
        "x.ai/ask_user_question" => match id {
            Some(id) => {
                let (question, options) = first_question(params);
                ServerReq::Wait(PendingReq {
                    request_id: id,
                    kind: "question",
                    tool: "ask_user_question".into(),
                    question,
                    options,
                })
            }
            None => ServerReq::Unknown {
                id: None,
                method: method.to_string(),
            },
        },
        _ => ServerReq::Unknown {
            id,
            method: method.to_string(),
        },
    }
}

fn tool_label(params: &Value) -> String {
    let tool = params.get("toolCall").cloned().unwrap_or(Value::Null);
    for key in ["title", "name", "kind"] {
        if let Some(text) = tool.get(key).and_then(|value| value.as_str()) {
            if !text.is_empty() {
                let raw = tool.get("rawInput").map(|value| value.to_string());
                return match raw {
                    Some(raw) if raw != "null" => format!("{text} {raw}"),
                    _ => text.to_string(),
                };
            }
        }
    }
    params
        .get("toolName")
        .and_then(|value| value.as_str())
        .unwrap_or("tool")
        .to_string()
}

fn first_question(params: &Value) -> (Option<String>, Vec<Value>) {
    let Some(question) = params
        .get("questions")
        .and_then(|value| value.as_array())
        .and_then(|list| list.first())
    else {
        return (None, Vec::new());
    };
    let text = question
        .get("question")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let options = question
        .get("options")
        .and_then(|value| value.as_array())
        .map(|list| {
            list.iter()
                .map(|opt| {
                    json!({
                        "optionId": opt.get("label").and_then(|v| v.as_str()).unwrap_or(""),
                        "name": opt.get("label").and_then(|v| v.as_str()).unwrap_or(""),
                        "label": opt.get("label").and_then(|v| v.as_str()).unwrap_or(""),
                        "kind": "question",
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    (text, options)
}

fn unwrap_params(params: Value) -> Value {
    if params.get("method").is_some() {
        if let Some(inner) = params.get("params") {
            return inner.clone();
        }
    }
    params
}

pub(crate) fn plan_cancelled_reply(id: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": { "outcome": "cancelled" }
    })
}

pub(crate) fn unknown_method_reply(id: Value, method: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32601, "message": format!("Method not found: {method}") }
    })
}

fn dial(port: u16, secret: &str, pid: i64) -> Result<Socket> {
    let url = ws_url(port);
    if url.contains('?') || url.contains(secret) {
        return Err(Error::Invalid(
            "websocket url must not carry the secret".into(),
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut last = Error::Invalid("websocket connect failed".into());
    while Instant::now() < deadline {
        if leader_dead(pid) {
            return Err(Error::Invalid("grok serve exited before /ws".into()));
        }
        match dial_once(port, secret) {
            Ok(socket) => return Ok(socket),
            Err(err) => {
                last = err;
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Err(last)
}

fn leader_dead(pid: i64) -> bool {
    let Ok(text) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    let Some(end) = text.rfind(')') else {
        return true;
    };
    // A zombie still occupies the process group until the parent waits.
    matches!(text[end + 1..].split_whitespace().next(), Some("Z") | None)
}

fn dial_once(port: u16, secret: &str) -> Result<Socket> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let stream = TcpStream::connect_timeout(&addr, Duration::from_millis(200))?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut request = ws_url(port)
        .into_client_request()
        .map_err(|err| Error::Invalid(format!("websocket url: {err}")))?;
    let header = format!("Bearer {secret}");
    let value = header
        .parse()
        .map_err(|_| Error::Invalid("bearer header is not valid".into()))?;
    request.headers_mut().insert("Authorization", value);
    let (socket, _) = tungstenite::client::client(request, stream)
        .map_err(|err| Error::Invalid(format!("websocket handshake failed: {err}")))?;
    // The handshake used a short read timeout. A quiet model must not trip it.
    let tcp = socket.get_ref();
    let _ = tcp.set_read_timeout(None);
    let _ = tcp.set_write_timeout(Some(Duration::from_secs(30)));
    Ok(socket)
}

fn send_text(socket: &mut Socket, text: &str) -> Result<()> {
    socket
        .send(Message::Text(text.to_string()))
        .map_err(|err| Error::Invalid(format!("websocket send failed: {err}")))
}

fn read_json(socket: &mut Socket) -> Result<Value> {
    loop {
        let message = socket
            .read()
            .map_err(|err| Error::Invalid(format!("websocket read failed: {err}")))?;
        match message {
            Message::Text(text) => {
                return serde_json::from_str(&text)
                    .map_err(|err| Error::Invalid(format!("acp frame is not json: {err}")));
            }
            Message::Ping(payload) => {
                socket
                    .send(Message::Pong(payload))
                    .map_err(|err| Error::Invalid(format!("websocket pong failed: {err}")))?;
            }
            Message::Close(_) => {
                return Err(Error::Invalid("websocket closed".into()));
            }
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

/// Slide the lease every minute. The mutex is not held across the sleep, and
/// not across the ACP read, so a permit wait still heartbeats.
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
    /// `Err` means the group survived SIGKILL. Do not wait: the child is still
    /// running, and a blocked `wait` would hide that from the caller.
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

fn spawn_grouped(program: &Path, args: &[String], log_path: &Path) -> Result<SpawnedProc> {
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
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .process_group(0);
    let mut child = cmd.spawn()?;
    let pid = i64::from(child.id());
    let spawned = (|| -> Result<(String, i64)> {
        let starttime = read_proc_starttime(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned grok has no starttime".into()))?;
        let pgid = read_pgrp(pid as u32)
            .ok_or_else(|| Error::Invalid("spawned grok has no pgid".into()))?;
        if pgid != pid {
            return Err(Error::Invalid(
                "spawned grok pgid does not equal pid".into(),
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
                // Wait after the signal. A zombie still keeps the process group.
                let _ = child.wait();
                Err(err)
            }
            Err(live) => Err(live),
        },
    }
}

fn read_pgrp(pid: u32) -> Option<i64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let end = text.rfind(')')?;
    let fields: Vec<&str> = text[end + 1..].split_whitespace().collect();
    fields.get(2)?.parse().ok()
}

fn pick_port() -> Result<u16> {
    for _ in 0..8 {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let port = listener.local_addr()?.port();
        drop(listener);
        if port != 0 && port != FORBIDDEN_PORT {
            return Ok(port);
        }
    }
    Err(Error::Invalid("could not pick a loopback port".into()))
}

fn new_secret() -> Result<String> {
    let mut bytes = [0u8; 32];
    let mut file = fs::File::open("/dev/urandom")?;
    file.read_exact(&mut bytes)?;
    let mut out = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    Ok(out)
}

fn redact(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        return text.to_string();
    }
    text.replace(secret, "[redacted]")
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

fn note(path: &Path, line: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn log_tail(path: &Path, secret: &str) -> String {
    let text = fs::read_to_string(path).unwrap_or_default();
    let start = text.len().saturating_sub(400);
    let tail = text.get(start..).unwrap_or(&text);
    redact(tail, secret)
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

fn lock_db(
    db: &Mutex<rusqlite::Connection>,
) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>> {
    db.lock()
        .map_err(|_| Error::Invalid("database lock poisoned".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{self, CardView};

    #[test]
    fn serve_command_is_the_private_agent_serve() {
        let secret = "not-a-url-secret";
        let args = serve_args(4321, secret).unwrap();
        assert_eq!(
            args,
            vec![
                "--permission-mode",
                "default",
                "agent",
                "--no-leader",
                "serve",
                "--bind",
                "127.0.0.1:4321",
                "--secret",
                secret,
            ]
        );
        assert!(serve_args(2419, secret).is_err());
        assert!(serve_args(0, secret).is_err());
        assert!(!args.iter().any(|arg| arg.contains("always-approve")));
        let url = ws_url(4321);
        assert_eq!(url, "ws://127.0.0.1:4321/ws");
        assert!(!url.contains('?'));
        assert!(!url.contains(secret));
    }

    #[test]
    fn secret_is_redacted_out_of_the_permit_and_html() {
        let secret = "s3cret-value";
        let pending = PendingReq {
            request_id: json!(7),
            kind: "permission",
            tool: format!("bash {secret}"),
            question: None,
            options: vec![json!({
                "optionId": "allow-once",
                "name": format!("Allow {secret}"),
                "kind": "allow_once"
            })],
        };
        let view = pending.view(secret);
        let text = view.to_string();
        assert!(!text.contains(secret));
        assert!(text.contains("[redacted]"));
        assert!(!ws_url(9).contains(secret));
        let page = render::card_page(&CardView {
            id: "k_abc".into(),
            title: "Pin".into(),
            body: "body".into(),
            source: "cli".into(),
            repo: None,
            target_branch: None,
            state: "inbox".into(),
            summary: "".into(),
            plan_rev: 0,
            created_at: "2026-11-25T00:00:00Z".into(),
            updated_at: "2026-11-25T00:00:00Z".into(),
            open_points: vec![],
        });
        assert!(!page.contains(secret));
        assert!(!page.contains("pending_permit"));
    }

    #[test]
    fn permission_mapping_matches_y_n_a_and_a_question_digit() {
        let pending = json!({
            "phase": "waiting",
            "kind": "permission",
            "tool": "bash",
            "options": [
                {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                {"optionId": "allow-always", "name": "Always", "kind": "allow_always"}
            ]
        });
        let once = map_choice(&pending, "y").unwrap();
        assert_eq!(once["outcome"]["optionId"], "allow-once");
        let once = map_choice(&pending, "--once").unwrap();
        assert_eq!(once["outcome"]["outcome"], "selected");
        let deny = map_choice(&pending, "n").unwrap();
        assert_eq!(deny["outcome"]["outcome"], "cancelled");
        assert!(deny["outcome"].get("optionId").is_none());
        let always = map_choice(&pending, "A").unwrap();
        assert_eq!(always["outcome"]["optionId"], "allow-always");
        let always = map_choice(&pending, "--always").unwrap();
        assert_eq!(always["outcome"]["optionId"], "allow-always");

        let no_always = json!({
            "kind": "permission",
            "options": [
                {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"}
            ]
        });
        let err = map_choice(&no_always, "A").unwrap_err();
        assert!(err.to_string().contains("refusing"), "{err}");
        assert!(map_choice(&no_always, "y").is_ok());

        let question = json!({
            "kind": "question",
            "question": "Which file?",
            "options": [
                {"label": "keep", "name": "keep"},
                {"label": "drop", "name": "drop"}
            ]
        });
        let picked = map_choice(&question, "2").unwrap();
        assert_eq!(picked["outcome"], "accepted");
        assert_eq!(picked["answers"]["Which file?"][0], "drop");
        let cancelled = map_choice(&question, "n").unwrap();
        assert_eq!(cancelled["outcome"], "cancelled");
        assert!(map_choice(&question, "y").is_err());
        assert!(map_choice(&question, "9").is_err());
    }

    #[test]
    fn exit_plan_is_cancelled_and_unknown_methods_are_not_found() {
        let exit = plan_cancelled_reply(json!(4));
        assert_eq!(exit["result"]["outcome"], "cancelled");
        assert_eq!(exit["error"], Value::Null);
        let unknown = unknown_method_reply(json!("x"), "session/fork");
        assert_eq!(unknown["error"]["code"], -32601);
        assert!(unknown["error"]["message"]
            .as_str()
            .unwrap()
            .contains("session/fork"));
        let frame = json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "x.ai/exit_plan_mode",
            "params": {"sessionId": "s"}
        });
        match classify(&frame) {
            Incoming::Server(ServerReq::ExitPlan { id }) => assert_eq!(id, json!(4)),
            other => panic!("unexpected {other:?}"),
        }
        let ask = json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "_x.ai/ask_user_question",
            "params": {
                "questions": [{
                    "question": "Ship it?",
                    "options": [{"label": "yes"}, {"label": "no"}]
                }]
            }
        });
        match classify(&ask) {
            Incoming::Server(ServerReq::Wait(pending)) => {
                assert_eq!(pending.kind, "question");
                assert_eq!(pending.question.as_deref(), Some("Ship it?"));
                assert_eq!(pending.options.len(), 2);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_second_connection_is_refused_before_it_dials() {
        let mut slot = Slot::default();
        let dials = Mutex::new(0);
        let first = slot.connect_once(|| {
            *dials.lock().unwrap() += 1;
            Ok(())
        });
        assert!(first.is_ok());
        let err = slot
            .connect_once(|| {
                *dials.lock().unwrap() += 1;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(*dials.lock().unwrap(), 1, "second dial must not run");
        assert!(err.to_string().contains("second websocket"), "{err}");
        let mut failed = Slot::default();
        assert!(failed
            .connect_once(|| Err::<(), _>(Error::Invalid("bind".into())))
            .is_err());
        assert!(failed.connect_once(|| Ok(())).is_ok());
    }

    #[test]
    fn spawned_child_is_its_own_process_group() {
        let dir = std::env::temp_dir().join(format!("keel-grok-pg-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let log = dir.join("child.log");
        let sleep = ensure_tool("sleep");
        let spawned = spawn_grouped(&sleep, &["30".into()], &log).unwrap();
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
    fn missing_grok_is_a_clear_error() {
        let err = ensure_grok("/no/such/bin").unwrap_err();
        assert!(err.to_string().contains("grok is not on PATH"), "{err}");
    }

    #[test]
    fn one_fake_server_answers_permission_exit_plan_and_unknown() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let port = listener.local_addr().unwrap().port();
        let secret = "test-secret";
        let server = thread::spawn(move || fake_acp(listener, secret));
        let db = Mutex::new(crate::db::open_in_memory().unwrap());
        let log = std::env::temp_dir().join(format!("keel-acp-{}.log", std::process::id()));
        let id = format!("jobfake{}", std::process::id());
        let session = Session {
            db: &db,
            target: Target::Item,
            id: id.clone(),
            token: "token".into(),
            log_path: log.clone(),
            secret: secret.into(),
        };
        let result = thread::scope(|scope| {
            let handle = scope.spawn(|| drive_fake(port, secret, &session));
            let deadline = Instant::now() + Duration::from_secs(3);
            while pending_public(&id).is_none() {
                if Instant::now() > deadline {
                    panic!("permission was not published");
                }
                thread::sleep(Duration::from_millis(20));
            }
            let db = Mutex::new(crate::db::open_in_memory().unwrap());
            answer(&db, Target::Item, &id, "y").unwrap();
            handle.join().unwrap()
        });
        result.unwrap();
        server.join().unwrap().unwrap();
        clear_public(&id);
        remove_waiter(&id);
        let _ = fs::remove_file(&log);
    }

    fn drive_fake(port: u16, secret: &str, session: &Session) -> Result<Value> {
        let mut slot = Slot::default();
        let mut socket = slot.connect_once(|| dial_once(port, secret))?;
        NEXT_ID.with(|cell| cell.set(1));
        SESSION_ID.with(|cell| cell.borrow_mut().clear());
        rpc(
            &mut socket,
            session,
            "initialize",
            json!({
                "protocolVersion": 1,
                "clientInfo": { "name": "keel", "version": "0.1.0" }
            }),
        )?;
        rpc(
            &mut socket,
            session,
            "session/new",
            json!({ "cwd": "/tmp", "mcpServers": [] }),
        )?;
        rpc(
            &mut socket,
            session,
            "session/prompt",
            json!({
                "prompt": [{ "type": "text", "text": "Read the prompt file and write the output file." }]
            }),
        )
    }

    fn fake_acp(listener: TcpListener, secret: &str) -> Result<()> {
        let (stream, _) = listener.accept()?;
        let mut socket =
            tungstenite::accept(stream).map_err(|err| Error::Invalid(format!("accept: {err}")))?;
        expect_method(&mut socket, "initialize")?;
        reply_ok(&mut socket, json!(1), json!({"protocolVersion": 1}))?;
        expect_method(&mut socket, "session/new")?;
        reply_ok(&mut socket, json!(2), json!({"sessionId": "s1"}))?;
        let prompt = expect_method(&mut socket, "session/prompt")?;
        assert!(prompt.to_string().contains("prompt file"), "{prompt}");
        assert!(!prompt.to_string().contains(secret));
        send_text(
            &mut socket,
            &json!({
                "jsonrpc": "2.0",
                "id": 50,
                "method": "x.ai/exit_plan_mode",
                "params": {"sessionId": "s1"}
            })
            .to_string(),
        )?;
        let cancelled = read_json(&mut socket)?;
        assert_eq!(cancelled["result"]["outcome"], "cancelled");
        send_text(
            &mut socket,
            &json!({
                "jsonrpc": "2.0",
                "id": 51,
                "method": "x.ai/nope",
                "params": {}
            })
            .to_string(),
        )?;
        let missing = read_json(&mut socket)?;
        assert_eq!(missing["error"]["code"], -32601);
        send_text(
            &mut socket,
            &json!({
                "jsonrpc": "2.0",
                "id": 99,
                "method": "session/request_permission",
                "params": {
                    "sessionId": "s1",
                    "toolCall": {"title": "bash", "rawInput": {"command": "git status"}},
                    "options": [
                        {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                        {"optionId": "allow-always", "name": "Always", "kind": "allow_always"}
                    ]
                }
            })
            .to_string(),
        )?;
        let permit = read_json(&mut socket)?;
        assert_eq!(permit["id"], 99);
        assert_eq!(permit["result"]["outcome"]["optionId"], "allow-once");
        reply_ok(&mut socket, json!(3), json!({"stopReason": "end_turn"}))?;
        Ok(())
    }

    fn expect_method(socket: &mut Socket, method: &str) -> Result<Value> {
        let msg = read_json(socket)?;
        assert_eq!(msg["method"], method, "{msg}");
        Ok(msg)
    }

    fn reply_ok(socket: &mut Socket, id: Value, result: Value) -> Result<()> {
        send_text(
            socket,
            &json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string(),
        )
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
}
