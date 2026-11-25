# Keel

Keel is the local pipeline from an inbox item to a local merge. This crate is the SQLite schema (`user_version` 1), the item and job transitions, and the local daemon.

`keel serve` listens on a Unix socket and on `127.0.0.1` for GET document routes. `keel add` is the intake path. `keel inbox`, `keel list`, `keel show`, `keel url`, `keel open`, and `keel status` talk to the socket. They do not open SQLite.

Design: `~/agent-artifacts/shared/plans/keel.md`

That file is the specification. This repository does not carry a second copy.

## Ports

TCP `127.0.0.1:7431` is GET-only (health and documents). Counts and every mutation stay on the Unix socket. Keel does not own `grokback-bridge` on 7429 or `grok agent serve` on 2419.

A future Tailscale Serve of 7431 must stay GET-only and must not be pointed at the Unix socket. This repository does not install that rule.

## User unit

`systemd/keel.service` runs `keel serve` with no arguments, which is the default profile (`default`, unless `~/.config/keel/config.toml` sets `profile`). It is a user unit. It is not part of `grokback.target`. Checking out this repository does not install or enable it.

`keel` must be on the `systemd --user` PATH (the unit calls `/usr/bin/env keel serve`; it does not hard-code a binary path). To install it yourself:

```
mkdir -p ~/.config/systemd/user
ln -s /path/to/keel/systemd/keel.service ~/.config/systemd/user/keel.service
systemctl --user daemon-reload
systemctl --user enable --now keel.service
```

Stop it with `systemctl --user stop keel.service`.
