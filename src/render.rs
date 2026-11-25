//! Card, plan, queue, and board shells. The daemon is the only writer. Text is escaped.
//! There is no markdown twin and no author JavaScript.

use serde_json::{json, Value};

const TOKENS: &str = include_str!("../design/tokens.css");
const CARD: &str = include_str!("../design/card.html");
const PLAN: &str = include_str!("../design/plan.html");
const QUEUE: &str = include_str!("../design/queue.html");
const BOARD: &str = include_str!("../design/board.html");

#[derive(Debug, Clone)]
pub struct PointView {
    pub id: String,
    pub text: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct CardView {
    pub id: String,
    pub title: String,
    pub body: String,
    pub source: String,
    pub repo: Option<String>,
    pub target_branch: Option<String>,
    pub state: String,
    pub summary: String,
    pub plan_rev: i64,
    pub created_at: String,
    pub updated_at: String,
    pub open_points: Vec<PointView>,
}

#[derive(Debug, Clone)]
pub struct PlanSection {
    pub heading: String,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct PlanJobView {
    pub id: Option<String>,
    pub parent_id: Option<String>,
    pub title: String,
    pub acceptance: String,
    /// True when this row has leaves. The plan page chips it.
    pub container: bool,
}

#[derive(Debug, Clone)]
pub struct PlanView {
    pub id: String,
    pub title: String,
    pub state: String,
    pub summary: String,
    pub plan_rev: i64,
    pub updated_at: String,
    pub previous_url: Option<String>,
    pub open_points: Vec<PointView>,
    pub sections: Vec<PlanSection>,
    pub jobs: Vec<PlanJobView>,
}

#[derive(Debug, Clone)]
pub struct QueueLink {
    pub id: String,
    pub title: String,
    pub summary: String,
    pub url: String,
}

#[derive(Debug, Clone)]
pub struct QueueView {
    pub heading: String,
    /// `plan-review` or `feedback`. Not stored.
    pub name: String,
    pub rows: Vec<QueueLink>,
}

#[derive(Debug, Clone)]
pub struct BoardJobView {
    pub id: String,
    pub title: String,
    pub state: String,
    pub parent_id: Option<String>,
    /// True when another job on this item lists this id as its parent.
    pub container: bool,
}

#[derive(Debug, Clone)]
pub struct BoardItemView {
    pub id: String,
    pub title: String,
    pub state: String,
    pub card_url: String,
    pub plan_url: Option<String>,
    pub jobs: Vec<BoardJobView>,
    /// Set only for `merge_conflict`. The integrate checkout is a directory.
    pub integrate_worktree: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BoardView {
    pub columns: Vec<String>,
    pub items: Vec<BoardItemView>,
}

pub fn card_page(card: &CardView) -> String {
    let doc = card_doc(card);
    let repo = card.repo.as_deref().filter(|value| !value.is_empty());
    let branch = card
        .target_branch
        .as_deref()
        .filter(|value| !value.is_empty());
    let repo_branch = escape(&match (repo, branch) {
        (None, None) => "unset".to_string(),
        (repo, branch) => format!(
            "{} / {}",
            repo.unwrap_or("unset"),
            branch.unwrap_or("unset")
        ),
    });
    let rev = card.plan_rev.to_string();
    fill(
        CARD,
        &[
            ("tokens", TOKENS),
            ("title", &escape(&card.title)),
            ("item_id", &escape(&card.id)),
            ("state", &escape(&card.state)),
            ("rev", &rev),
            ("source", &escape(&card.source)),
            ("created_at", &escape(&card.created_at)),
            ("repo_branch", &repo_branch),
            ("body", &escape(&card.body)),
            ("doc", &json_for_html(&doc)),
        ],
    )
}

pub fn plan_page(plan: &PlanView) -> String {
    let doc = plan_doc(plan);
    let rev = plan.plan_rev.to_string();
    let previous = match &plan.previous_url {
        Some(url) => format!("<a href=\"{}\">r{}</a>", escape(url), plan.plan_rev - 1),
        None => "none".to_string(),
    };
    fill(
        PLAN,
        &[
            ("tokens", TOKENS),
            ("title", &escape(&plan.title)),
            ("item_id", &escape(&plan.id)),
            ("state", &escape(&plan.state)),
            ("rev", &rev),
            ("summary", &escape(&plan.summary)),
            ("sections", &sections_html(&plan.sections)),
            ("points", &points_html(&plan.open_points)),
            ("jobs", &jobs_html(&plan.jobs)),
            ("previous", &previous),
            ("doc", &json_for_html(&doc)),
        ],
    )
}

pub fn queue_page(queue: &QueueView) -> String {
    let mut rows = String::new();
    for row in &queue.rows {
        rows.push_str("    <li><a href=\"");
        rows.push_str(&escape(&row.url));
        rows.push_str("\"><span class=\"grow\">");
        rows.push_str(&escape(&row.title));
        rows.push_str("</span><span class=\"dim\">");
        rows.push_str(&escape(&row.summary));
        rows.push_str("</span></a></li>\n");
    }
    let items: Vec<Value> = queue
        .rows
        .iter()
        .map(|row| {
            json!({
                "id": row.id,
                "title": row.title,
                "summary": row.summary,
                "url": row.url,
            })
        })
        .collect();
    let doc = json!({
        "keel": 1,
        "kind": "queue",
        "queue": queue.name,
        "items": items,
    });
    fill(
        QUEUE,
        &[
            ("tokens", TOKENS),
            ("heading", &escape(&queue.heading)),
            ("rows", &rows),
            ("doc", &json_for_html(&doc)),
        ],
    )
}

pub fn board_page(board: &BoardView) -> String {
    let mut columns = String::new();
    for column in &board.columns {
        // `dropped` stays on the board but is not an active flow step.
        if column == "dropped" {
            columns.push_str("<section class=\"col dropped\">\n");
        } else {
            columns.push_str("<section class=\"col\">\n");
        }
        columns.push_str("<h2>");
        columns.push_str(&escape(column));
        columns.push_str("</h2>\n");
        for item in board.items.iter().filter(|item| item.state == *column) {
            columns.push_str(&board_item_html(item));
        }
        columns.push_str("</section>\n");
    }
    let items: Vec<Value> = board
        .items
        .iter()
        .map(|item| {
            let jobs: Vec<Value> = item
                .jobs
                .iter()
                .map(|job| {
                    json!({
                        "id": job.id,
                        "title": job.title,
                        "state": job.state,
                        "parent_id": job.parent_id,
                        "container": job.container,
                    })
                })
                .collect();
            json!({
                "id": item.id,
                "title": item.title,
                "state": item.state,
                "card_url": item.card_url,
                "plan_url": item.plan_url,
                "jobs": jobs,
            })
        })
        .collect();
    let doc = json!({
        "keel": 1,
        "kind": "board",
        "columns": board.columns,
        "items": items,
    });
    fill(
        BOARD,
        &[
            ("tokens", TOKENS),
            ("columns", &columns),
            ("doc", &json_for_html(&doc)),
        ],
    )
}

fn board_item_html(item: &BoardItemView) -> String {
    let mut out = String::new();
    out.push_str("<article class=\"card\">\n<h3><a href=\"");
    out.push_str(&escape(&item.card_url));
    out.push_str("\">");
    out.push_str(&escape(&item.title));
    out.push_str("</a></h3>\n");
    if let Some(plan) = &item.plan_url {
        out.push_str("<a class=\"plan\" href=\"");
        out.push_str(&escape(plan));
        out.push_str("\">plan</a>\n");
    }
    if !item.jobs.is_empty() {
        out.push_str("<ul class=\"jobs\">\n");
        for job in &item.jobs {
            out.push_str(&board_job_html(job));
        }
        out.push_str("</ul>\n");
    }
    if let Some(path) = &item.integrate_worktree {
        out.push_str("<p class=\"mono dim worktree\">");
        out.push_str(&escape(path));
        out.push_str("</p>\n");
    }
    out.push_str("</article>\n");
    out
}

fn board_job_html(job: &BoardJobView) -> String {
    let mut class = String::new();
    if job.container {
        class.push_str("label");
    }
    if job.parent_id.is_some() {
        if !class.is_empty() {
            class.push(' ');
        }
        class.push_str("sub");
    }
    if job.state == "cancelled" {
        if !class.is_empty() {
            class.push(' ');
        }
        class.push_str("cancelled");
    }
    let mut out = String::new();
    if class.is_empty() {
        out.push_str("    <li>");
    } else {
        out.push_str("    <li class=\"");
        out.push_str(&class);
        out.push_str("\">");
    }
    // A container is a grouping label. Its own state is not an implementing chip.
    if !job.container {
        out.push_str("<span class=\"chip\">");
        out.push_str(&escape(&job.state));
        out.push_str("</span>");
    }
    out.push_str("<span class=\"grow\">");
    out.push_str(&escape(&job.title));
    out.push_str("</span></li>\n");
    out
}

fn plan_doc(plan: &PlanView) -> Value {
    let points: Vec<Value> = plan
        .open_points
        .iter()
        .map(|point| {
            json!({
                "id": point.id,
                "text": point.text,
                "status": point.status,
            })
        })
        .collect();
    let jobs: Vec<Value> = plan
        .jobs
        .iter()
        .map(|job| {
            json!({
                "id": job.id,
                "parent_id": job.parent_id,
                "title": job.title,
                "acceptance": job.acceptance,
            })
        })
        .collect();
    json!({
        "keel": 1,
        "kind": "plan",
        "item_id": plan.id,
        "job_id": Value::Null,
        "rev": plan.plan_rev,
        "state": plan.state,
        "title": plan.title,
        "updated_at": plan.updated_at,
        "summary": plan.summary,
        "open_points": points,
        "jobs": jobs,
        "diff": Value::Null,
        "verdict": Value::Null,
    })
}

fn sections_html(sections: &[PlanSection]) -> String {
    let mut out = String::new();
    for section in sections {
        out.push_str("  <section><h2>");
        out.push_str(&escape(&section.heading));
        out.push_str("</h2><pre class=\"body\">");
        out.push_str(&escape(&section.body));
        out.push_str("</pre></section>\n");
    }
    out
}

fn points_html(points: &[PointView]) -> String {
    let mut out = String::new();
    for point in points {
        out.push_str("    <li><span class=\"mono dim\">");
        out.push_str(&escape(&point.id));
        out.push_str("</span><span class=\"chip\">");
        out.push_str(&escape(&point.status));
        out.push_str("</span><span class=\"grow\">");
        out.push_str(&escape(&point.text));
        out.push_str("</span></li>\n");
    }
    out
}

fn jobs_html(jobs: &[PlanJobView]) -> String {
    let mut out = String::new();
    for job in jobs {
        // A set parent is a leaf under a container, before or after rows exist.
        if job.parent_id.is_some() {
            out.push_str("    <li class=\"sub\">");
        } else {
            out.push_str("    <li>");
        }
        if let Some(id) = &job.id {
            out.push_str("<span class=\"mono dim\">");
            out.push_str(&escape(id));
            out.push_str("</span>");
        }
        if job.container {
            out.push_str("<span class=\"chip\">container</span>");
        }
        out.push_str("<span class=\"grow\">");
        out.push_str(&escape(&job.title));
        out.push_str("</span><span class=\"dim\">");
        out.push_str(&escape(&job.acceptance));
        out.push_str("</span></li>\n");
    }
    out
}

fn card_doc(card: &CardView) -> Value {
    let points: Vec<Value> = card
        .open_points
        .iter()
        .map(|point| {
            json!({
                "id": point.id,
                "text": point.text,
                "status": point.status,
            })
        })
        .collect();
    json!({
        "keel": 1,
        "kind": "card",
        "item_id": card.id,
        "job_id": Value::Null,
        "rev": card.plan_rev,
        "state": card.state,
        "title": card.title,
        "updated_at": card.updated_at,
        "summary": card.summary,
        "open_points": points,
        "jobs": [],
        "diff": Value::Null,
        "verdict": Value::Null,
    })
}

/// One pass. A value that itself contains `{{` is not expanded again.
fn fill(template: &str, pairs: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(template.len() + 1024);
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let key = &after[..end];
        if let Some((_, value)) = pairs.iter().find(|(name, _)| *name == key) {
            out.push_str(value);
        }
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

/// `<` inside the JSON block would close the script element. The escape stays valid JSON.
fn json_for_html(value: &Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "{}".into())
        .replace('<', "\\u003c")
}

pub fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 80
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_card() -> CardView {
        CardView {
            id: "k_abc".into(),
            title: "Pin <walk>".into(),
            body: "</script><script>alert(1)</script> & \"x\"".into(),
            source: "cli".into(),
            repo: None,
            target_branch: None,
            state: "inbox".into(),
            summary: "".into(),
            plan_rev: 0,
            created_at: "2026-11-25T00:00:00Z".into(),
            updated_at: "2026-11-25T00:00:00Z".into(),
            open_points: vec![],
        }
    }

    #[test]
    fn card_escapes_text_and_embeds_keel_doc() {
        let html = card_page(&sample_card());
        assert!(html.contains("--bg: #0B0D10"));
        assert!(html.contains("color-scheme: dark"));
        assert!(!html.contains("fonts.googleapis.com"));
        assert!(!html.to_ascii_lowercase().contains("<button"));
        assert!(!html.to_ascii_lowercase().contains("<form"));
        assert!(html.contains("Pin &lt;walk&gt;"));
        assert!(html.contains("&lt;/script&gt;"));
        assert!(html.contains("unset"));
        assert!(!html.contains("</script><script>"));
        let doc = doc_json(&html);
        assert_eq!(doc["keel"], 1);
        assert_eq!(doc["kind"], "card");
        assert_eq!(doc["item_id"], "k_abc");
        assert!(doc["job_id"].is_null());
        assert_eq!(doc["jobs"], json!([]));
        assert!(doc["diff"].is_null());
        assert!(doc["verdict"].is_null());
        assert_eq!(doc["title"], "Pin <walk>");
        assert!(doc["body"].is_null());
        assert!(html.contains("script-src 'none'"));
    }

    #[test]
    fn queue_rows_are_links_without_controls() {
        let html = queue_page(&QueueView {
            heading: "awaiting plan review".into(),
            name: "plan-review".into(),
            rows: vec![QueueLink {
                id: "k_abc".into(),
                title: "A <b>".into(),
                summary: "sum".into(),
                url: "http://127.0.0.1:7431/i/k_abc/plan".into(),
            }],
        });
        assert!(!html.to_ascii_lowercase().contains("<button"));
        assert!(!html.to_ascii_lowercase().contains("<form"));
        assert!(html.contains("awaiting plan review"));
        assert!(html.contains("A &lt;b&gt;"));
        assert!(html.contains("http://127.0.0.1:7431/i/k_abc/plan"));
        let doc = doc_json(&html);
        assert_eq!(doc["kind"], "queue");
        assert_eq!(doc["items"][0]["title"], "A <b>");
        assert_eq!(doc["items"][0]["summary"], "sum");
    }

    #[test]
    fn plan_jobs_chip_a_container_and_indent_its_leaves() {
        let html = plan_page(&PlanView {
            id: "k_abc".into(),
            title: "Nested".into(),
            state: "awaiting_plan_review".into(),
            summary: "s".into(),
            plan_rev: 1,
            updated_at: "2026-11-25T00:00:00Z".into(),
            previous_url: None,
            open_points: vec![],
            sections: vec![],
            jobs: vec![
                PlanJobView {
                    id: None,
                    parent_id: None,
                    title: "box".into(),
                    acceptance: "hold".into(),
                    container: true,
                },
                PlanJobView {
                    id: None,
                    parent_id: Some("box".into()),
                    title: "leaf".into(),
                    acceptance: "done".into(),
                    container: false,
                },
            ],
        });
        assert!(html.contains("<span class=\"chip\">container</span>"));
        assert!(html.contains("<li class=\"sub\">"));
        assert!(html.contains("padding-left: 1.25rem"));
        let doc = doc_json(&html);
        assert_eq!(doc["jobs"][1]["parent_id"], "box");
        assert!(doc["jobs"][0]["parent_id"].is_null());
    }

    #[test]
    fn board_indents_a_leaf_and_does_not_chip_its_container() {
        let columns = [
            "inbox",
            "planning",
            "awaiting_plan_review",
            "revision_requested",
            "revising",
            "awaiting_implementation",
            "implementing",
            "awaiting_merge",
            "merge_conflict",
            "merged",
            "dropped",
        ];
        let html = board_page(&BoardView {
            columns: columns.iter().map(|name| (*name).to_string()).collect(),
            items: vec![
                BoardItemView {
                    id: "k_box".into(),
                    title: "Pin <walk>".into(),
                    state: "inbox".into(),
                    card_url: "http://127.0.0.1:7431/i/k_box/card".into(),
                    plan_url: Some("http://127.0.0.1:7431/i/k_box/plan".into()),
                    integrate_worktree: None,
                    jobs: vec![
                        BoardJobView {
                            id: "j_box".into(),
                            title: "Hold".into(),
                            state: "implementing".into(),
                            parent_id: None,
                            container: true,
                        },
                        BoardJobView {
                            id: "j_leaf".into(),
                            title: "Leaf <x>".into(),
                            state: "queued".into(),
                            parent_id: Some("j_box".into()),
                            container: false,
                        },
                        BoardJobView {
                            id: "j_stop".into(),
                            title: "Stop".into(),
                            state: "cancelled".into(),
                            parent_id: Some("j_box".into()),
                            container: false,
                        },
                    ],
                },
                BoardItemView {
                    id: "k_merge".into(),
                    title: "Stuck".into(),
                    state: "merge_conflict".into(),
                    card_url: "http://127.0.0.1:7431/i/k_merge/card".into(),
                    plan_url: None,
                    integrate_worktree: Some("/tmp/keel/worktrees/k_merge/integrate".into()),
                    jobs: vec![],
                },
            ],
        });
        assert!(html.contains("script-src 'none'"));
        assert!(html.contains("http-equiv=\"refresh\" content=\"30\""));
        assert!(html.contains("<section class=\"col dropped\">"));
        assert!(!html.to_ascii_lowercase().contains("<form"));
        assert!(!html.to_ascii_lowercase().contains("<button"));
        assert_eq!(script_count(&html), 1);
        let mut last = 0;
        for name in columns {
            let at = html.find(&format!("<h2>{name}</h2>")).unwrap();
            assert!(at >= last, "{name}");
            last = at;
        }
        assert!(html.contains("Pin &lt;walk&gt;"));
        assert!(html.contains("http://127.0.0.1:7431/i/k_box/card"));
        assert!(html.contains("http://127.0.0.1:7431/i/k_box/plan"));
        assert!(html.contains("Leaf &lt;x&gt;"));
        assert!(html.contains("<li class=\"sub\">"));
        assert!(html.contains("<span class=\"chip\">queued</span>"));
        assert!(!html.contains("<span class=\"chip\">implementing</span>"));
        assert!(html.contains("class=\"sub cancelled\""));
        assert!(html.contains("padding-left: 1.25rem"));
        assert!(html.contains("/tmp/keel/worktrees/k_merge/integrate"));
        let doc = doc_json(&html);
        assert_eq!(doc["keel"], 1);
        assert_eq!(doc["kind"], "board");
        assert_eq!(doc["columns"].as_array().unwrap().len(), columns.len());
        assert_eq!(doc["columns"][0], "inbox");
        assert_eq!(doc["columns"][10], "dropped");
        assert_eq!(doc["items"][0]["title"], "Pin <walk>");
        assert_eq!(doc["items"][0]["jobs"][0]["container"], true);
        assert_eq!(doc["items"][0]["jobs"][0]["state"], "implementing");
        assert!(doc["items"][0]["jobs"][0]["parent_id"].is_null());
        assert_eq!(doc["items"][0]["jobs"][1]["parent_id"], "j_box");
        assert_eq!(doc["items"][0]["jobs"][1]["container"], false);
        assert_eq!(
            doc["items"][0]["plan_url"],
            "http://127.0.0.1:7431/i/k_box/plan"
        );
        assert!(doc["items"][1]["plan_url"].is_null());
    }

    fn script_count(html: &str) -> usize {
        let mut rest = html;
        let mut count = 0;
        while let Some(idx) = rest.find("<script") {
            count += 1;
            assert!(
                rest[idx..].starts_with("<script type=\"application/json\" id=\"keel-doc\">"),
                "executable script"
            );
            rest = &rest[idx + 7..];
        }
        count
    }

    fn doc_json(html: &str) -> Value {
        let start = html
            .find(r#"<script type="application/json" id="keel-doc">"#)
            .unwrap();
        let rest = &html[start..];
        let json_start = rest.find('>').unwrap() + 1;
        let json_end = rest.find("</script>").unwrap();
        let raw = &rest[json_start..json_end];
        serde_json::from_str(raw).unwrap()
    }
}
