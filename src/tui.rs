//! Ratatui list over the Unix socket.
//! The key handler does not touch a terminal, so tests can drive it.
//! It does not open SQLite and it does not speak ACP. Permit posts the same
//! `y` / `n` / `A` choice the CLI sends; the daemon maps it. Merge posts only
//! after `m` then `M`, and that body never asks for a push.

use std::io::{self, IsTerminal};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Frame;
use serde_json::{json, Value};

use crate::cli::{daemon_json, daemon_json_timeout};
use crate::config::Config;
use crate::db::{Error, Result};
use crate::render;

const HELP: &str =
    "j/k move   o open   n note   a approve   m then M merge   y/n/A permit   r refresh   q quit";
const NOTE_CAP: usize = 2000;
const SHA_CAP: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Item,
    Job,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Row {
    pub kind: Kind,
    pub id: String,
    pub state: String,
    pub title: String,
    pub permit_kind: Option<String>,
    pub permit_text: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Normal,
    Note,
    Approve,
    MergeArmed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Key {
    Char(char),
    Enter,
    Backspace,
    Esc,
    Up,
    Down,
    Quit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Effect {
    Quit,
    Refresh,
    Open {
        id: String,
    },
    Note {
        id: String,
        text: String,
    },
    Approve {
        id: String,
        base_sha: String,
    },
    Merge {
        id: String,
    },
    Permit {
        kind: Kind,
        id: String,
        choice: String,
    },
}

pub(crate) struct Call {
    pub method: &'static str,
    pub path: String,
    pub body: Option<Value>,
    pub timeout_secs: u64,
}

struct App {
    rows: Vec<Row>,
    selected: usize,
    mode: Mode,
    buffer: String,
    message: String,
    /// Lease GETs only. A note, approve, merge, or permit line stays in `message`.
    lease_error: Option<String>,
    /// `xdg-open` children. Reaped with `try_wait` so the event loop does not block.
    openers: Vec<Child>,
}

pub fn run(config: &Config) -> Result<()> {
    // `cargo test` has no tty. Refuse before raw mode so the suite cannot hang.
    if !io::stdout().is_terminal() {
        return Err(Error::Invalid("keel tui needs a tty".into()));
    }
    let mut app = App::new();
    if let Err(err) = reload(config, &mut app) {
        app.message = one_line(&err.to_string());
    }
    let mut terminal = ratatui::try_init().map_err(Error::Io)?;
    let result = event_loop(config, &mut terminal, &mut app);
    ratatui::restore();
    result
}

fn event_loop(
    config: &Config,
    terminal: &mut ratatui::DefaultTerminal,
    app: &mut App,
) -> Result<()> {
    loop {
        // Non-blocking. A slow opener must not freeze keys or swallow Ctrl-C.
        reap(app);
        terminal.draw(|frame| draw(frame, app))?;
        if !event::poll(Duration::from_millis(250))? {
            if app.mode == Mode::Normal {
                if let Err(err) = reload(config, app) {
                    app.message = one_line(&err.to_string());
                }
            }
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }
        let Some(mapped) = map_key(key.code, key.modifiers) else {
            continue;
        };
        let effects = app.on_key(mapped);
        if apply(config, app, effects)? {
            return Ok(());
        }
    }
}

fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::vertical([
        Constraint::Min(3),
        Constraint::Length(8),
        Constraint::Length(1),
    ])
    .split(frame.area());
    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|row| ListItem::new(row_label(row)))
        .collect();
    let mut state = ListState::default();
    if !app.rows.is_empty() {
        state.select(Some(app.selected.min(app.rows.len() - 1)));
    }
    let list = List::new(items)
        .block(Block::default().title("keel").borders(Borders::ALL))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("> ");
    frame.render_stateful_widget(list, chunks[0], &mut state);
    let info = Paragraph::new(info_text(app)).wrap(Wrap { trim: false });
    frame.render_widget(info, chunks[1]);
    frame.render_widget(Paragraph::new(HELP), chunks[2]);
}

fn row_label(row: &Row) -> String {
    let kind = match row.kind {
        Kind::Item => "item",
        Kind::Job => "job",
    };
    let permit = if row.permit_kind.is_some() {
        " permit"
    } else {
        ""
    };
    format!("{kind}  {}  {}  {}{permit}", row.state, row.id, row.title)
}

fn info_text(app: &App) -> String {
    let mut lines = Vec::new();
    match app.mode {
        Mode::Note => lines.push(format!("note: {}", app.buffer)),
        Mode::Approve => lines.push(format!("base sha: {}", app.buffer)),
        Mode::MergeArmed => {
            lines.push("press M to merge locally. nothing is pushed. esc cancels".into())
        }
        Mode::Normal => {}
    }
    if let Some(text) = app
        .rows
        .get(app.selected)
        .and_then(|row| row.permit_text.as_deref())
    {
        lines.push(text.to_string());
    }
    if !app.message.is_empty() {
        lines.push(app.message.clone());
    }
    if let Some(err) = &app.lease_error {
        lines.push(err.clone());
    }
    lines.join("\n")
}

fn reload(config: &Config, app: &mut App) -> Result<()> {
    let listed = daemon_json(config, "GET", "/v1/items", None)?;
    let status = daemon_json(config, "GET", "/v1/status", None)?;
    let mut rows = item_rows(&listed);
    let mut lease_error = None;
    if let Some(leases) = status.get("leases").and_then(Value::as_array) {
        for lease in leases {
            let kind = lease.get("kind").and_then(Value::as_str);
            if kind != Some("item") && kind != Some("job") {
                continue;
            }
            let Some(id) = lease.get("id").and_then(Value::as_str) else {
                continue;
            };
            if !render::safe_id(id) {
                continue;
            }
            match daemon_json(config, "GET", &format!("/v1/lease/{id}"), None) {
                Ok(body) => apply_lease(&mut rows, &body),
                Err(err) => lease_error = Some(one_line(&err.to_string())),
            }
        }
    }
    // Every lease this pass was fetched. Drop a previous failure without
    // touching the action summary in `message`.
    finish_reload(app, rows, lease_error);
    Ok(())
}

fn finish_reload(app: &mut App, rows: Vec<Row>, lease_error: Option<String>) {
    let previous = app
        .rows
        .get(app.selected)
        .map(|row| (row.kind, row.id.clone()));
    app.rows = rows;
    app.selected = restored_index(&app.rows, previous);
    app.lease_error = lease_error;
}

/// Item leases update the list row. Job leases are their own rows.
fn apply_lease(rows: &mut Vec<Row>, body: &Value) {
    match body.get("kind").and_then(Value::as_str) {
        Some("job") => {
            if let Some(row) = job_row(body) {
                rows.push(row);
            }
        }
        Some("item") => {
            let Some((kind, text)) = waiting_permit(body) else {
                return;
            };
            let Some(id) = body.get("id").and_then(Value::as_str) else {
                return;
            };
            if let Some(row) = rows
                .iter_mut()
                .find(|row| row.kind == Kind::Item && row.id == id)
            {
                row.permit_kind = Some(kind);
                row.permit_text = Some(text);
            } else if render::safe_id(id) {
                rows.push(Row {
                    kind: Kind::Item,
                    id: id.to_string(),
                    state: text_field(body, "state"),
                    title: text_field(body, "worker_id"),
                    permit_kind: Some(kind),
                    permit_text: Some(text),
                });
            }
        }
        _ => {}
    }
}

fn item_rows(body: &Value) -> Vec<Row> {
    body.get("items")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(item_row).collect())
        .unwrap_or_default()
}

fn item_row(value: &Value) -> Option<Row> {
    let id = value.get("id").and_then(Value::as_str).unwrap_or("");
    if !render::safe_id(id) {
        return None;
    }
    Some(Row {
        kind: Kind::Item,
        id: id.to_string(),
        state: text_field(value, "state"),
        title: text_field(value, "title"),
        permit_kind: None,
        permit_text: None,
    })
}

fn job_row(value: &Value) -> Option<Row> {
    if value.get("kind").and_then(Value::as_str) != Some("job") {
        return None;
    }
    let id = value.get("id").and_then(Value::as_str).unwrap_or("");
    if !render::safe_id(id) {
        return None;
    }
    let (permit_kind, permit_text) = match waiting_permit(value) {
        Some((kind, text)) => (Some(kind), Some(text)),
        None => (None, None),
    };
    let worker = value.get("worker_id").and_then(Value::as_str).unwrap_or("");
    Some(Row {
        kind: Kind::Job,
        id: id.to_string(),
        state: text_field(value, "state"),
        title: if worker.is_empty() {
            "job".into()
        } else {
            worker.into()
        },
        permit_kind,
        permit_text,
    })
}

fn waiting_permit(value: &Value) -> Option<(String, String)> {
    let pending = value
        .get("pending_permit")
        .filter(|value| !value.is_null())?;
    if pending.get("phase").and_then(Value::as_str) != Some("waiting") {
        return None;
    }
    let kind = pending
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("permission")
        .to_string();
    Some((kind, format_permit(pending)))
}

/// Display only. The daemon still maps `y` / `n` / `A`.
fn format_permit(pending: &Value) -> String {
    let kind = pending
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("permission");
    let tool = pending
        .get("tool")
        .and_then(Value::as_str)
        .unwrap_or("tool");
    let mut out = format!("permit {kind}: {tool}");
    if let Some(question) = pending.get("question").and_then(Value::as_str) {
        if !question.is_empty() {
            out.push('\n');
            out.push_str(question);
        }
    }
    if let Some(options) = pending.get("options").and_then(Value::as_array) {
        for (index, opt) in options.iter().enumerate() {
            let name = opt
                .get("name")
                .or_else(|| opt.get("label"))
                .and_then(Value::as_str)
                .unwrap_or("");
            out.push('\n');
            if kind == "question" {
                out.push_str(&format!("  {}. {name}", index + 1));
            } else {
                let id = opt.get("optionId").and_then(Value::as_str).unwrap_or("");
                let opt_kind = opt.get("kind").and_then(Value::as_str).unwrap_or("");
                out.push_str(&format!("  {id}  {name}  [{opt_kind}]"));
            }
        }
    }
    out.push('\n');
    if kind == "question" {
        out.push_str("digit or n");
    } else {
        out.push_str("y / n / A");
    }
    out
}

fn text_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn restored_index(rows: &[Row], previous: Option<(Kind, String)>) -> usize {
    if rows.is_empty() {
        return 0;
    }
    previous
        .and_then(|(kind, id)| rows.iter().position(|row| row.kind == kind && row.id == id))
        .unwrap_or(0)
        .min(rows.len() - 1)
}

impl App {
    fn new() -> Self {
        Self {
            rows: Vec::new(),
            selected: 0,
            mode: Mode::Normal,
            buffer: String::new(),
            message: String::new(),
            lease_error: None,
            openers: Vec::new(),
        }
    }

    fn on_key(&mut self, key: Key) -> Vec<Effect> {
        if key == Key::Quit {
            return vec![Effect::Quit];
        }
        match self.mode {
            Mode::Normal => self.on_normal(key),
            Mode::MergeArmed => self.on_merge_armed(key),
            Mode::Note | Mode::Approve => self.on_text(key),
        }
    }

    fn on_normal(&mut self, key: Key) -> Vec<Effect> {
        match key {
            Key::Up | Key::Char('k') => {
                self.move_by(-1);
                vec![]
            }
            Key::Down | Key::Char('j') => {
                self.move_by(1);
                vec![]
            }
            Key::Char('q') => vec![Effect::Quit],
            Key::Char('r') => vec![Effect::Refresh],
            Key::Char('o') => self.open_effect(),
            Key::Char('a') => self.begin_approve(),
            Key::Char('m') => self.arm_merge(),
            // On a waiting permit, n is deny. Otherwise n starts a note.
            Key::Char(ch @ ('y' | 'n' | 'A')) => {
                let effects = self.permit(ch);
                if effects.is_empty() && ch == 'n' {
                    self.begin_note()
                } else {
                    effects
                }
            }
            Key::Char(ch) if ch.is_ascii_digit() && ch != '0' => self.permit_if_question(ch),
            _ => vec![],
        }
    }

    fn on_merge_armed(&mut self, key: Key) -> Vec<Effect> {
        // Anything but M drops the arm. One key must not merge.
        self.mode = Mode::Normal;
        if key == Key::Char('M') {
            if let Some(row) = self.rows.get(self.selected) {
                if row.kind == Kind::Item && row.state == "awaiting_merge" {
                    let id = row.id.clone();
                    self.message.clear();
                    return vec![Effect::Merge { id }];
                }
            }
            self.message = "merge confirm is only for awaiting_merge".into();
            return vec![];
        }
        self.message = "merge cancelled".into();
        vec![]
    }

    fn on_text(&mut self, key: Key) -> Vec<Effect> {
        match key {
            Key::Esc => {
                self.mode = Mode::Normal;
                self.buffer.clear();
                self.message = "cancelled".into();
                vec![]
            }
            Key::Enter => self.submit_text(),
            Key::Backspace => {
                self.buffer.pop();
                vec![]
            }
            Key::Char(ch) => {
                let cap = if self.mode == Mode::Approve {
                    SHA_CAP
                } else {
                    NOTE_CAP
                };
                if self.buffer.chars().count() < cap {
                    self.buffer.push(ch);
                }
                vec![]
            }
            _ => vec![],
        }
    }

    fn submit_text(&mut self) -> Vec<Effect> {
        let text = self.buffer.trim().to_string();
        let mode = self.mode;
        self.mode = Mode::Normal;
        self.buffer.clear();
        let Some(id) = self.selected_item_id() else {
            self.message = "that action takes an item".into();
            return vec![];
        };
        if text.is_empty() {
            self.message = match mode {
                Mode::Approve => "base sha is empty".into(),
                _ => "note text is empty".into(),
            };
            return vec![];
        }
        match mode {
            Mode::Note => vec![Effect::Note { id, text }],
            Mode::Approve => vec![Effect::Approve { id, base_sha: text }],
            _ => vec![],
        }
    }

    fn begin_note(&mut self) -> Vec<Effect> {
        if self.selected_kind() != Some(Kind::Item) {
            self.message = "note takes an item".into();
            return vec![];
        }
        self.mode = Mode::Note;
        self.buffer.clear();
        self.message.clear();
        vec![]
    }

    fn begin_approve(&mut self) -> Vec<Effect> {
        if self.selected_kind() != Some(Kind::Item) {
            self.message = "approve takes an item".into();
            return vec![];
        }
        self.mode = Mode::Approve;
        self.buffer.clear();
        self.message.clear();
        vec![]
    }

    fn arm_merge(&mut self) -> Vec<Effect> {
        match self.rows.get(self.selected) {
            Some(row) if row.kind == Kind::Item && row.state == "awaiting_merge" => {
                self.mode = Mode::MergeArmed;
                self.message = "press M to merge locally; nothing is pushed".into();
            }
            Some(_) => {
                self.message = "merge confirm is only for awaiting_merge".into();
            }
            None => self.message = "nothing selected".into(),
        }
        vec![]
    }

    fn open_effect(&mut self) -> Vec<Effect> {
        match self.rows.get(self.selected) {
            Some(row) if row.kind == Kind::Item => {
                let id = row.id.clone();
                vec![Effect::Open { id }]
            }
            Some(_) => {
                self.message = "open is the item card".into();
                vec![]
            }
            None => vec![],
        }
    }

    fn permit(&mut self, choice: char) -> Vec<Effect> {
        let Some(row) = self.rows.get(self.selected) else {
            return vec![];
        };
        if row.permit_kind.is_none() {
            return vec![];
        }
        let id = row.id.clone();
        let kind = row.kind;
        vec![Effect::Permit {
            kind,
            id,
            choice: choice.to_string(),
        }]
    }

    fn permit_if_question(&mut self, choice: char) -> Vec<Effect> {
        let is_question = self
            .rows
            .get(self.selected)
            .and_then(|row| row.permit_kind.as_deref())
            == Some("question");
        if !is_question {
            return vec![];
        }
        self.permit(choice)
    }

    fn move_by(&mut self, delta: isize) {
        if self.rows.is_empty() {
            self.selected = 0;
            return;
        }
        let next = self.selected as isize + delta;
        let last = self.rows.len() as isize - 1;
        self.selected = next.clamp(0, last) as usize;
    }

    fn selected_kind(&self) -> Option<Kind> {
        self.rows.get(self.selected).map(|row| row.kind)
    }

    fn selected_item_id(&self) -> Option<String> {
        self.rows.get(self.selected).and_then(|row| {
            if row.kind == Kind::Item {
                Some(row.id.clone())
            } else {
                None
            }
        })
    }
}

fn map_key(code: KeyCode, mods: KeyModifiers) -> Option<Key> {
    if mods.contains(KeyModifiers::CONTROL) && matches!(code, KeyCode::Char('c' | 'C')) {
        return Some(Key::Quit);
    }
    if mods.contains(KeyModifiers::CONTROL) || mods.contains(KeyModifiers::ALT) {
        return None;
    }
    match code {
        KeyCode::Char(ch) => Some(Key::Char(ch)),
        KeyCode::Enter => Some(Key::Enter),
        KeyCode::Backspace => Some(Key::Backspace),
        KeyCode::Esc => Some(Key::Esc),
        KeyCode::Up => Some(Key::Up),
        KeyCode::Down => Some(Key::Down),
        _ => None,
    }
}

/// Socket routes only. `None` means the id must not be interpolated into a path.
pub(crate) fn effect_call(effect: &Effect) -> Option<Call> {
    let (method, path, body, timeout_secs) = match effect {
        Effect::Note { id, text } if render::safe_id(id) => (
            "POST",
            format!("/v1/items/{id}/note"),
            Some(json!({ "text": text })),
            5,
        ),
        Effect::Approve { id, base_sha } if render::safe_id(id) => (
            "POST",
            format!("/v1/items/{id}/approve"),
            Some(json!({ "base_sha": base_sha })),
            5,
        ),
        Effect::Merge { id } if render::safe_id(id) => (
            "POST",
            format!("/v1/items/{id}/merge"),
            // Local merge only. There is no push field for the daemon to honor.
            Some(json!({ "continue": false, "abort": false })),
            30,
        ),
        Effect::Permit { kind, id, choice } if render::safe_id(id) => {
            let root = match kind {
                Kind::Item => "items",
                Kind::Job => "jobs",
            };
            (
                "POST",
                format!("/v1/{root}/{id}/permit"),
                Some(json!({ "choice": choice })),
                5,
            )
        }
        Effect::Open { id } if render::safe_id(id) => ("GET", format!("/v1/items/{id}"), None, 5),
        _ => return None,
    };
    Some(Call {
        method,
        path,
        body,
        timeout_secs,
    })
}

/// Same field `keel open` prints. The TUI does not invent a URL.
pub(crate) fn open_target(item: &Value) -> Result<String> {
    match item.get("card_url").and_then(Value::as_str) {
        Some(url) if !url.is_empty() => Ok(url.to_string()),
        _ => Err(Error::Invalid("card url is missing".into())),
    }
}

fn open_item(config: &Config, app: &mut App, id: &str) -> Result<String> {
    let call = effect_call(&Effect::Open { id: id.to_string() })
        .ok_or_else(|| Error::NotFound(id.to_string()))?;
    let item = daemon_json_timeout(
        config,
        call.method,
        &call.path,
        None,
        Duration::from_secs(call.timeout_secs),
    )?;
    let url = open_target(&item)?;
    // Raw mode has cleared ISIG. Do not inherit the tty or wait on xdg-open.
    app.openers.push(spawn_detached("xdg-open", &[&url])?);
    Ok(url)
}

fn spawn_detached(program: &str, args: &[&str]) -> Result<Child> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        cmd.pre_exec(|| {
            if setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    cmd.spawn().map_err(Error::Io)
}

fn reap(app: &mut App) {
    let mut still = Vec::new();
    for mut child in app.openers.drain(..) {
        match child.try_wait() {
            Ok(None) => still.push(child),
            _ => {}
        }
    }
    app.openers = still;
}

extern "C" {
    fn setsid() -> i32;
}

fn apply(config: &Config, app: &mut App, effects: Vec<Effect>) -> Result<bool> {
    for effect in effects {
        match effect {
            Effect::Quit => return Ok(true),
            Effect::Refresh => {
                if let Err(err) = reload(config, app) {
                    app.message = one_line(&err.to_string());
                }
            }
            Effect::Open { ref id } => match open_item(config, app, id) {
                Ok(url) => app.message = format!("opened {url}"),
                Err(err) => app.message = one_line(&err.to_string()),
            },
            other => match effect_call(&other) {
                None => app.message = "refusing that id".into(),
                Some(call) => {
                    let body = call
                        .body
                        .as_ref()
                        .map(|value| value.to_string().into_bytes());
                    match daemon_json_timeout(
                        config,
                        call.method,
                        &call.path,
                        body,
                        Duration::from_secs(call.timeout_secs),
                    ) {
                        Ok(value) => {
                            app.message = summarize(&other, &value);
                            if let Err(err) = reload(config, app) {
                                app.message = one_line(&format!("{}; {err}", app.message));
                            }
                        }
                        Err(err) => app.message = one_line(&err.to_string()),
                    }
                }
            },
        }
    }
    Ok(false)
}

fn summarize(effect: &Effect, value: &Value) -> String {
    let state = value.get("state").and_then(Value::as_str).unwrap_or("");
    match effect {
        Effect::Note { .. } => format!("noted {state}"),
        Effect::Approve { .. } => format!("approved {state}"),
        Effect::Merge { .. } => {
            let mut line = format!("merge {state}");
            let sha = value
                .get("merged_sha")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !sha.is_empty() {
                line.push(' ');
                line.push_str(sha);
            }
            // Same facts `keel merge` prints once the integrate worktree is in conflict.
            if let Some(kind) = value.get("conflict_kind").and_then(Value::as_str) {
                if !kind.is_empty() {
                    line.push(' ');
                    line.push_str(kind);
                    if let Some(worktree) = value.get("worktree").and_then(Value::as_str) {
                        if !worktree.is_empty() {
                            line.push(' ');
                            line.push_str(worktree);
                        }
                    }
                }
            }
            line
        }
        Effect::Permit { choice, .. } => format!("permit {choice}"),
        _ => state.to_string(),
    }
}

fn one_line(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(200)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, state: &str) -> Row {
        Row {
            kind: Kind::Item,
            id: id.into(),
            state: state.into(),
            title: "title".into(),
            permit_kind: None,
            permit_text: None,
        }
    }

    fn job(id: &str, permit_kind: Option<&str>) -> Row {
        Row {
            kind: Kind::Job,
            id: id.into(),
            state: "implementing".into(),
            title: "grok:w".into(),
            permit_kind: permit_kind.map(str::to_string),
            permit_text: permit_kind.map(|_| "y / n / A".into()),
        }
    }

    fn app(rows: Vec<Row>) -> App {
        App {
            rows,
            selected: 0,
            mode: Mode::Normal,
            buffer: String::new(),
            message: String::new(),
            lease_error: None,
            openers: Vec::new(),
        }
    }

    #[test]
    fn run_does_not_require_a_tty() {
        if io::stdout().is_terminal() {
            return;
        }
        let config = Config {
            profile: "default".into(),
            port: 7431,
            data_dir: std::path::PathBuf::from("/tmp/keel-tui-unused"),
            runtime_dir: std::path::PathBuf::from("/tmp/keel-tui-unused"),
            socket_path: std::path::PathBuf::from("/tmp/keel-tui-unused/keel.sock"),
            lock_path: std::path::PathBuf::from("/tmp/keel-tui-unused/keel.lock"),
            db_path: std::path::PathBuf::from("/tmp/keel-tui-unused/keel.db"),
            docs_dir: std::path::PathBuf::from("/tmp/keel-tui-unused/docs"),
            publish_root: std::path::PathBuf::from("/tmp/keel-tui-unused/artifacts"),
        };
        let err = run(&config).unwrap_err();
        assert!(err.to_string().contains("tty"), "{err}");
    }

    #[test]
    fn key_handler_note_approve_merge_open_and_permit_without_a_terminal() {
        let mut app = app(vec![
            item("k_plan", "awaiting_plan_review"),
            item("k_merge", "awaiting_merge"),
            job("j_perm", Some("permission")),
            job("j_ask", Some("question")),
            job("j_idle", None),
        ]);

        assert!(app.on_key(Key::Char('a')).is_empty());
        assert_eq!(app.mode, Mode::Approve);
        assert!(app.on_key(Key::Enter).is_empty());
        assert_eq!(app.message, "base sha is empty");
        app.on_key(Key::Char('a'));
        app.on_key(Key::Char('a'));
        app.on_key(Key::Char('b'));
        app.on_key(Key::Char('c'));
        assert_eq!(
            app.on_key(Key::Enter),
            vec![Effect::Approve {
                id: "k_plan".into(),
                base_sha: "abc".into(),
            }]
        );

        app.on_key(Key::Char('n'));
        app.on_key(Key::Char('h'));
        app.on_key(Key::Char('i'));
        assert_eq!(
            app.on_key(Key::Enter),
            vec![Effect::Note {
                id: "k_plan".into(),
                text: "hi".into(),
            }]
        );
        app.on_key(Key::Char('n'));
        app.on_key(Key::Char('x'));
        assert!(app.on_key(Key::Esc).is_empty());
        assert_eq!(app.mode, Mode::Normal);

        assert_eq!(
            app.on_key(Key::Char('o')),
            vec![Effect::Open {
                id: "k_plan".into()
            }]
        );

        assert!(app.on_key(Key::Char('m')).is_empty());
        assert_eq!(app.message, "merge confirm is only for awaiting_merge");
        assert_eq!(app.mode, Mode::Normal);
        assert!(app.on_key(Key::Char('M')).is_empty());

        app.on_key(Key::Down);
        assert!(app.on_key(Key::Char('m')).is_empty());
        assert_eq!(app.mode, Mode::MergeArmed);
        assert!(app.on_key(Key::Char('m')).is_empty());
        assert_eq!(app.message, "merge cancelled");
        app.on_key(Key::Char('m'));
        assert_eq!(
            app.on_key(Key::Char('M')),
            vec![Effect::Merge {
                id: "k_merge".into()
            }]
        );

        app.on_key(Key::Down);
        assert_eq!(
            app.on_key(Key::Char('y')),
            vec![Effect::Permit {
                kind: Kind::Job,
                id: "j_perm".into(),
                choice: "y".into(),
            }]
        );
        assert_eq!(
            app.on_key(Key::Char('n')),
            vec![Effect::Permit {
                kind: Kind::Job,
                id: "j_perm".into(),
                choice: "n".into(),
            }]
        );
        assert_eq!(
            app.on_key(Key::Char('A')),
            vec![Effect::Permit {
                kind: Kind::Job,
                id: "j_perm".into(),
                choice: "A".into(),
            }]
        );
        assert!(app.on_key(Key::Char('1')).is_empty());

        app.on_key(Key::Down);
        assert_eq!(
            app.on_key(Key::Char('2')),
            vec![Effect::Permit {
                kind: Kind::Job,
                id: "j_ask".into(),
                choice: "2".into(),
            }]
        );
        app.on_key(Key::Down);
        assert!(app.on_key(Key::Char('y')).is_empty());
        assert!(app.on_key(Key::Char('o')).is_empty());
        assert_eq!(app.message, "open is the item card");

        app.selected = 0;
        assert!(app.on_key(Key::Char('y')).is_empty());
        assert_eq!(app.on_key(Key::Char('q')), vec![Effect::Quit]);
        assert_eq!(app.on_key(Key::Quit), vec![Effect::Quit]);
    }

    #[test]
    fn actions_hit_socket_routes_and_merge_does_not_push() {
        let merge = effect_call(&Effect::Merge { id: "k_1".into() }).unwrap();
        assert_eq!(merge.method, "POST");
        assert_eq!(merge.path, "/v1/items/k_1/merge");
        assert_eq!(merge.timeout_secs, 30);
        let body = merge.body.unwrap();
        assert_eq!(body["continue"], false);
        assert_eq!(body["abort"], false);
        assert!(body.get("push").is_none());
        let rendered = body.to_string();
        assert!(!rendered.contains("2419"));
        assert!(!rendered.contains("grokback"));

        let permit = effect_call(&Effect::Permit {
            kind: Kind::Job,
            id: "j_1".into(),
            choice: "A".into(),
        })
        .unwrap();
        assert_eq!(permit.path, "/v1/jobs/j_1/permit");
        assert_eq!(permit.body.unwrap()["choice"], "A");
        let item_permit = effect_call(&Effect::Permit {
            kind: Kind::Item,
            id: "k_1".into(),
            choice: "y".into(),
        })
        .unwrap();
        assert_eq!(item_permit.path, "/v1/items/k_1/permit");

        let note = effect_call(&Effect::Note {
            id: "k_1".into(),
            text: "cap".into(),
        })
        .unwrap();
        assert_eq!(note.path, "/v1/items/k_1/note");
        let approve = effect_call(&Effect::Approve {
            id: "k_1".into(),
            base_sha: "abc".into(),
        })
        .unwrap();
        assert_eq!(approve.path, "/v1/items/k_1/approve");
        assert_eq!(approve.body.unwrap()["base_sha"], "abc");
        let open = effect_call(&Effect::Open { id: "k_1".into() }).unwrap();
        assert_eq!(open.method, "GET");
        assert_eq!(open.path, "/v1/items/k_1");

        assert!(effect_call(&Effect::Note {
            id: "a/b".into(),
            text: "x".into(),
        })
        .is_none());
        assert!(effect_call(&Effect::Permit {
            kind: Kind::Job,
            id: "../x".into(),
            choice: "y".into(),
        })
        .is_none());
    }

    #[test]
    fn open_uses_the_daemon_card_url() {
        let item = json!({ "card_url": "http://127.0.0.1:7431/i/k_1/card" });
        assert_eq!(
            open_target(&item).unwrap(),
            "http://127.0.0.1:7431/i/k_1/card"
        );
        assert!(open_target(&json!({})).is_err());
        assert!(open_target(&json!({ "card_url": "" })).is_err());
    }

    #[test]
    fn job_row_shows_a_waiting_permit_and_drops_the_claim_token() {
        let row = job_row(&json!({
            "id": "j_1",
            "kind": "job",
            "state": "implementing",
            "worker_id": "grok:018f",
            "claim_token": "secret-token",
            "pending_permit": {
                "phase": "waiting",
                "kind": "permission",
                "tool": "bash",
                "options": [
                    {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"}
                ]
            }
        }))
        .unwrap();
        assert_eq!(row.permit_kind.as_deref(), Some("permission"));
        let text = row.permit_text.unwrap();
        assert!(text.contains("bash"));
        assert!(text.contains("allow-once"));
        assert!(text.contains("y / n / A"));
        assert!(!text.contains("secret-token"));
        assert!(!text.contains("2419"));
        assert!(!text.contains("grokback"));

        let answered = job_row(&json!({
            "id": "j_1",
            "kind": "job",
            "state": "implementing",
            "pending_permit": {"phase": "answered"}
        }))
        .unwrap();
        assert!(answered.permit_kind.is_none());
    }

    #[test]
    fn list_rows_keep_socket_order_and_selection() {
        let rows = item_rows(&json!({
            "items": [
                {"id": "k_b", "state": "inbox", "title": "b"},
                {"id": "nope/id", "state": "inbox", "title": "bad"},
                {"id": "k_a", "state": "awaiting_merge", "title": "a"}
            ]
        }));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "k_b");
        assert_eq!(rows[1].state, "awaiting_merge");
        let kept = restored_index(&rows, Some((Kind::Item, "k_a".into())));
        assert_eq!(kept, 1);
        assert_eq!(restored_index(&[], None), 0);
    }

    #[test]
    fn map_key_does_not_open_a_terminal() {
        assert_eq!(
            map_key(KeyCode::Char('c'), KeyModifiers::CONTROL),
            Some(Key::Quit)
        );
        assert_eq!(
            map_key(KeyCode::Char('M'), KeyModifiers::SHIFT),
            Some(Key::Char('M'))
        );
        assert_eq!(
            map_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(Key::Enter)
        );
        assert_eq!(map_key(KeyCode::Esc, KeyModifiers::NONE), Some(Key::Esc));
    }

    #[test]
    fn lease_error_does_not_replace_an_action_line_and_clears_when_every_lease_is_fetched() {
        let mut view = app(vec![item("k_1", "inbox")]);
        view.message = "noted revision_requested".into();
        view.lease_error = Some("stale lease".into());
        let rows = view.rows.clone();
        finish_reload(&mut view, rows, Some("daemon 500".into()));
        assert_eq!(view.message, "noted revision_requested");
        assert_eq!(view.lease_error.as_deref(), Some("daemon 500"));
        let text = info_text(&view);
        assert!(text.contains("noted revision_requested"));
        assert!(text.contains("daemon 500"));
        let rows = view.rows.clone();
        finish_reload(&mut view, rows, None);
        assert!(view.lease_error.is_none());
        assert_eq!(view.message, "noted revision_requested");
        assert!(!info_text(&view).contains("daemon 500"));
    }

    #[test]
    fn merge_status_includes_conflict_kind_and_worktree() {
        let line = summarize(
            &Effect::Merge { id: "k".into() },
            &json!({
                "state": "merge_conflict",
                "merged_sha": "",
                "conflict_kind": "integrate",
                "worktree": "/data/worktrees/k/integrate",
            }),
        );
        assert_eq!(
            line,
            "merge merge_conflict integrate /data/worktrees/k/integrate"
        );
        let clean = summarize(
            &Effect::Merge { id: "k".into() },
            &json!({
                "state": "merged",
                "merged_sha": "deadbeef",
                "conflict_kind": null,
                "worktree": "/data/worktrees/k/integrate",
            }),
        );
        assert_eq!(clean, "merge merged deadbeef");
    }

    #[test]
    fn item_lease_permit_posts_the_item_route() {
        let mut rows = item_rows(&json!({
            "items": [{"id": "k_1", "state": "planning", "title": "plan"}]
        }));
        apply_lease(
            &mut rows,
            &json!({
                "id": "k_1",
                "kind": "item",
                "state": "planning",
                "worker_id": "grok:018f",
                "claim_token": "secret-token",
                "pending_permit": {
                    "phase": "waiting",
                    "kind": "permission",
                    "tool": "bash",
                    "options": []
                }
            }),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].permit_kind.as_deref(), Some("permission"));
        assert!(!rows[0]
            .permit_text
            .as_deref()
            .unwrap_or("")
            .contains("secret-token"));
        let mut view = app(rows);
        let effects = view.on_key(Key::Char('n'));
        assert_eq!(
            effects,
            vec![Effect::Permit {
                kind: Kind::Item,
                id: "k_1".into(),
                choice: "n".into(),
            }]
        );
        assert_eq!(
            effect_call(&effects[0]).unwrap().path,
            "/v1/items/k_1/permit"
        );
        apply_lease(
            &mut view.rows,
            &json!({
                "id": "j_1",
                "kind": "job",
                "state": "implementing",
                "pending_permit": {"phase": "waiting", "kind": "permission", "tool": "bash"}
            }),
        );
        assert_eq!(view.rows.len(), 2);
        assert_eq!(view.rows[1].kind, Kind::Job);
    }

    #[test]
    fn opener_spawn_returns_before_the_child_exits() {
        let started = std::time::Instant::now();
        let child = spawn_detached("sleep", &["30"]).expect("sleep");
        assert!(started.elapsed() < Duration::from_secs(2));
        let mut view = app(vec![]);
        view.message = "opened http://127.0.0.1:7431/i/k_1/card".into();
        view.openers.push(child);
        let reaped = std::time::Instant::now();
        reap(&mut view);
        assert!(reaped.elapsed() < Duration::from_secs(2));
        assert_eq!(view.openers.len(), 1);
        assert_eq!(view.message, "opened http://127.0.0.1:7431/i/k_1/card");
        view.openers[0].kill().unwrap();
        view.openers[0].wait().unwrap();
    }
}
