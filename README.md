# Keel

Keel is the local pipeline from an inbox item to a local merge. This crate is the SQLite schema (`user_version` 1), the item and job transitions, and the local daemon.

`keel serve` listens on a Unix socket and on `127.0.0.1` for GET document routes. `keel add` is the intake path. `keel inbox`, `keel list`, `keel show`, `keel url`, `keel open`, `keel status`, and `keel tui` talk to the socket. They do not open SQLite.

Design: `~/agent-artifacts/shared/plans/keel.md`

That file is the specification. This repository does not carry a second copy.
