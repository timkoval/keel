# Using Keel

Keel is the local queue from an inbox item to a merge on this machine. You add an item, an agent writes a plan, you review it, agents implement and review the jobs, and you merge. The merge stays local. Keel does not push.

The daemon starts with `keel serve`, and a command such as `keel status` starts it when the socket is missing. On this machine it is already running as the user service `keel.service`.

Commands talk to the Unix socket. They do not open the database. The pages are the board, the queues, and each item's card and plan. Those pages are read-only.

## See the queue

```
keel status
keel inbox
keel list
keel list --state awaiting_plan_review
keel show ID
keel url --board
keel open --board
```

`keel url --board` prints the board on this host, `http://127.0.0.1:7431/board`. `keel url ID` and `keel open ID` are the item card. A tailnet device cannot open `127.0.0.1`. Publishing the document port is a Tailscale Serve of that port, and it must stay GET-only.

`keel tui` is the action list over the socket. It is not a second queue.

| Key | Action |
|---|---|
| `j` / `k` or arrows | Move |
| `r` | Refresh |
| `o` | Open the card in a browser |
| `n` | Note on the selected item, unless a permit is waiting |
| `a`, then the base SHA, then Enter | Approve the plan |
| `y` / `n` / `A` | Answer a waiting permit: once, deny, or always for this claim |
| `1`–`9` | Pick a numbered question |
| `m`, then `M` | Merge the selected item locally. Nothing is pushed |
| `q` | Quit |

Esc cancels a note or an approval. Any key other than `M` cancels an armed merge.

## Add an item

```
keel add --repo ~/git-local/some-repo --branch main "Short title"
keel add --repo ~/git-local/some-repo --body "What to change" "Short title"
```

The title is one argument. `--repo` is the checkout the agents will use. `--branch` is the branch they start from and the branch `keel merge` updates. Set both. Keel does not substitute the repo's current branch, and a plan against a repo with no branch stops with `target_branch is unset`. Without `--body`, a terminal records an empty body. A pipe becomes the body, up to 32 KiB.

The command prints the item id and the card URL. `keel add` does not start an agent.

## One item, end to end

Use the id from `keel add`, or a unique prefix of it. If a prefix matches both an item and a job, pass the full id.

1. Plan. This takes the item id.

   ```
   keel run ID --role plan --adapter grok
   ```

   `--adapter` is required for a real agent. The default is `fake`, which is a test child, not a model. `claude` is the other adapter. Claude refuses a plan or revise when the item has no worktree.

   `grok` keeps this terminal and asks here when it wants permission. `claude` returns after spawn. If a tool asks and this terminal is not a tty, run `keel permit ID` in another terminal. `y` allows once, `n` denies, `A` allows the rest of this claim. `--once`, `--deny`, and `--always` are the same answers without a prompt.

2. Read the plan. `keel open ID` opens the card. The plan link is on that page, and `keel url --board` opens the board.

3. Send it back, or accept it.

   ```
   keel note ID "The second job is still too big"
   keel waive ID POINT
   keel approve ID --base $(git -C ~/git-local/some-repo rev-parse HEAD)
   ```

   A note is an open point. The item goes to `revision_requested`. `keel run ID --role revise --adapter grok` addresses the open points. Waiving the last open point returns the item to plan review without another revise.

   Approve needs the base SHA, the commit the jobs must start from. It is refused while any point is still open. Approve prints one job id per leaf. Jobs are only two levels deep: a container is a label, and the leaves are the claimable jobs.

4. Implement each leaf. This takes a job id, not the item id.

   ```
   keel run JOB --role implement --adapter grok
   ```

   The branch is `keel/<item>/<job>`. The same checkout is reused if you release and run it again.

5. Review with a different run. The implementer cannot review its own job.

   ```
   keel run JOB --role review --adapter claude
   ```

6. Accept or send the job back.

   ```
   keel accept JOB
   keel reject JOB "The migration is missing"
   ```

   Accept records your decision. It does not touch git. Reject keeps the branch and queues the job again.

7. Merge when every job is accepted and the item is `awaiting_merge`.

   ```
   keel merge ID
   ```

   This merges the accepted leaves into the target branch on this machine. It does not push. In the TUI the same action is `m`, then `M`.

   If git stops, the item is `merge_conflict` and the command prints the integrate worktree. Fix that worktree, then `keel merge ID --continue`. `keel merge ID --abort` returns the item to `awaiting_merge` and drops the integrate worktree. It does not change the target branch or the job branches.

`keel drop ID` leaves the queue. `--force` is required while an agent is holding the item, and it stops a Keel-spawned process group. It does not signal an external claim.

`keel release ID` drops a claim you no longer want. A plan or revise worktree is removed. An implement worktree stays, so the next implement run reuses it.

## What the states mean

Item states, in the order the board shows them:

| State | What you do |
|---|---|
| `inbox` | Run `plan` |
| `planning` | Wait, or answer `keel permit` |
| `awaiting_plan_review` | Note, waive, or approve |
| `revision_requested` | Run `revise`, or waive the last point |
| `revising` | Wait, or answer `keel permit` |
| `awaiting_implementation` | Run `implement` on each queued leaf |
| `implementing` | Wait for the leaves to be accepted |
| `awaiting_merge` | `keel merge` |
| `merge_conflict` | Fix the printed worktree, then `--continue` or `--abort` |
| `merged` | Done |
| `dropped` | Left the queue |

A job moves `queued` → `implementing` → `ready_for_review` → `in_review` → `feedback_pending`. From `feedback_pending`, accept finishes it and reject returns it to `queued`.

A claim lasts fifteen minutes and is renewed while the agent runs. If it expires, Keel stops the process group it spawned and returns the row to the queue. A claim Keel did not spawn is not signalled. Release that one yourself.

## Pages

On the host:

| Page | URL |
|---|---|
| Board | `http://127.0.0.1:7431/board` |
| Plan queue | `http://127.0.0.1:7431/queue/plan-review` |
| Review queue | `http://127.0.0.1:7431/queue/feedback` |
| Card | printed by `keel add` and `keel url ID` |
| Health | `http://127.0.0.1:7431/health` |

The board refreshes every 30 seconds. It has no forms. `POST` to that port is rejected. Counts, notes, approval, and merge stay on the Unix socket, which is what the CLI and the TUI use.

## Check the daemon

```
keel doctor
```

Doctor reports the schema version, whether the HTML matches its JSON, backup state, leftover worktrees, lease mismatches, and whether a recorded merge still matches the target branch. It does not move git. The only rewrite it does is to rebuild a missing current plan or review page from the JSON already stored.

The database for the default profile is `$XDG_DATA_HOME/keel/default/keel.db` (`~/.local/share/keel/default/keel.db` when `XDG_DATA_HOME` is unset). The socket is `$XDG_RUNTIME_DIR/keel/default/keel.sock`.

A second profile is a second daemon and a second database, not a filter on this one. Set `profile` and `port` in `~/.config/keel/config.toml`, or pass `KEEL_PROFILE` and `KEEL_PORT`. The profile name is ASCII letters, digits, `_`, or `-`.

## Commands you rarely type

`keel claim`, `keel heartbeat`, `keel submit`, and `keel mcp` are the agent path. `keel mcp` is a stdio proxy to the same socket. It can add, list, claim, and submit. It cannot approve, accept, merge, drop, or permit.

`keel run` without `--adapter grok` or `--adapter claude` does not call a model.
