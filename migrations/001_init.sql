CREATE TABLE items (
  id              TEXT PRIMARY KEY,
  title           TEXT NOT NULL,
  body            TEXT NOT NULL,
  source          TEXT NOT NULL,
  external_id     TEXT,
  repo            TEXT,
  target_branch   TEXT,
  base_sha        TEXT,
  state           TEXT NOT NULL,
  plan_rev        INTEGER NOT NULL DEFAULT 0,
  summary         TEXT NOT NULL DEFAULT '',
  plan_json       TEXT,                       -- last submitted document; re-render source
  card_relpath    TEXT,
  plan_relpath    TEXT,
  integrate_sha   TEXT,
  integrate_ord   INTEGER,                    -- last leaf ord merged into integrate
  conflict_kind   TEXT,                       -- null | integrate | primary
  merged_sha      TEXT,
  validation_json TEXT,                       -- optional commands for the Claude bundle
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  claim_worker    TEXT,
  claim_token     TEXT,
  claim_role      TEXT,
  claim_external  INTEGER NOT NULL DEFAULT 0, -- 1 = MCP client pid, do not signal
  claim_pid       INTEGER,
  claim_pgid      INTEGER,
  claim_starttime TEXT,
  lease_until     TEXT,
  UNIQUE (source, external_id)
);

CREATE TABLE open_points (
  id          TEXT PRIMARY KEY,
  item_id     TEXT NOT NULL REFERENCES items(id),
  text        TEXT NOT NULL,
  status      TEXT NOT NULL,                  -- open | resolved | waived
  created_at  TEXT NOT NULL,
  resolved_at TEXT
);

CREATE TABLE jobs (
  id                  TEXT PRIMARY KEY,
  item_id             TEXT NOT NULL REFERENCES items(id),
  parent_job_id       TEXT REFERENCES jobs(id),
  title               TEXT NOT NULL,
  acceptance          TEXT NOT NULL DEFAULT '',
  state               TEXT NOT NULL,
  depth               INTEGER NOT NULL,       -- 1 in slice-1; 2 after nesting
  ord                 INTEGER NOT NULL,
  branch              TEXT,
  worktree            TEXT,
  base_sha            TEXT,
  diff_ref            TEXT,
  review_rev          INTEGER NOT NULL DEFAULT 0,
  review_relpath      TEXT,
  review_json         TEXT,
  implementer_worker  TEXT,
  reviewer_worker     TEXT,
  permission_mode     TEXT,                   -- ask | plan | acceptEdits | auto
  blocked_reason      TEXT,                   -- permission | dirty | null
  claim_worker        TEXT,
  claim_token         TEXT,
  claim_role          TEXT,
  claim_external      INTEGER NOT NULL DEFAULT 0,
  claim_pid           INTEGER,
  claim_pgid          INTEGER,
  claim_starttime     TEXT,
  lease_until         TEXT,
  pending_permit      TEXT,                   -- JSON of the ACP request waiting on keel permit
  updated_at          TEXT NOT NULL
);

CREATE TABLE job_notes (
  id         TEXT PRIMARY KEY,
  job_id     TEXT NOT NULL REFERENCES jobs(id),
  text       TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE events (
  id         INTEGER PRIMARY KEY,
  at         TEXT NOT NULL,
  actor      TEXT NOT NULL,
  item_id    TEXT,
  job_id     TEXT,
  kind       TEXT NOT NULL,
  from_state TEXT,
  to_state   TEXT,
  detail     TEXT
);

CREATE INDEX idx_items_state ON items(state, updated_at);
CREATE INDEX idx_jobs_item ON jobs(item_id, ord);
CREATE INDEX idx_jobs_state ON jobs(state);
CREATE INDEX idx_points_open ON open_points(item_id, status);
CREATE INDEX idx_events_item ON events(item_id, id);
