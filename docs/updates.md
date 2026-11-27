# Updating components with warren-update

`warren-update` installs signed releases of the programs Warren runs as
services, restarts them, and puts the previous version back when the new one
crashes or does not become healthy. It is an ordinary Warren service plus a
command line; Warren itself knows nothing about releases, keys or manifests. It
uses only the [control API](control-api.md) and the public `client` and
`confload` packages.

This release covers **service components installed from a directory** (`--from
file://DIR`). Updating `warren` and `warren-update` themselves, HTTP and mesh
sources, automatic installs, and the launcher that guards Warren's own update
come later; the config refuses `warren` and `warren-update` components and
`auto` for now.

## What you trust

The updater needs the control socket and write access to the binaries it
replaces, so it is **as trusted as Warren**: it runs as Warren's user, as a
child of Warren. Its config file holds the public keys, so whoever can write
that file can add a key and so install code; make it owned by root and
read-only to Warren's user. The keys are per component: a signature made with
the key of `myapp` cannot install anything else, and the component name is
inside the signed bytes. The key is a boundary on the update path, not a
sandbox: the component then runs as the same user as Warren.

Nothing from a release is ever run by the updater. Manifests name logical
targets, never paths; the updater's config maps targets to absolute paths. Only
bytes whose size and SHA-256 equal the signed values are kept.

## Running it

Run it as a service of Warren, with its own config file:

```yaml
# Warren's config
services:
  dir: /run/warren/services
  list:
    - name: warren-update
      allow: ["*"]
      run: [/var/lib/warren/bin/warren-update, run, --config, /etc/warren/update.yaml]
```

`warren-update run` waits for Warren's control socket, undoes anything an earlier
run left half done (see Recovery), takes the lease of its service name, opens
its own socket, `<state_dir>/updater.sock` (mode 0600), and then watches the
installs it makes. `warren-update reload` or SIGHUP reads its config again, all or nothing: the new config
is checked and its records prepared before it replaces the old one. A component removed
from it is refused while its record is an install in flight or an unfinished revert (settle
first, or revert it); a probation is closed
as committed, unwatched, and a restored version still being watched is closed as
settled. A change of `service` or `targets` is refused while the component is being
installed, watched or reverted, with one exception: explicit `targets` may be added to a
component with default ones while its revert is unfinished (its service is gone, so the
default target cannot be found), which lets the watcher finish the revert. Every target the
record needs must be there: it must have the name the record uses (normally the component
name) and point at the file the service ran; otherwise the reload is refused, naming the
missing targets. A reload waits
for an install in progress, since it needs the components' locks.
[examples/update.yaml](../examples/update.yaml) is a complete config:

- `state_dir`: one `<component>/state.json` per component, the staging area and the
  socket. Mode 0700, owned by the user it runs as.
- `warren.control`: Warren's control socket.
- per component: `keys` (minisign public keys by channel, at most 4 each; `key` is
  shorthand for one stable key), `channel` (`stable` by default), `service` (the
  Warren service it restarts and watches, required), `targets` (logical target to
  absolute path; by default the one target named like the component, at the `exe`
  Warren reports), `settle` (30 s, 5 s to 5 m), `max_size` (256 MiB per file) and
  `pinned`.
- a component with `pinned` may omit `keys`; it installs nothing.
- global: `canary` (how long a healthy install is watched before it is committed:
  15 minutes, 2 on the dev channel; 1 minute to 24 hours) and `health_timeout` (how long
  a new version has to become healthy: 5 minutes; 1 minute to 1 hour).

A stable component needs a stable key and may not list a dev key. A dev component
needs a dev key and may omit the stable one. The same key may not be listed for
both channels. A dev manifest must carry a pre-release version, a stable one must
not. Two components may not name the same service. A target must be an absolute, clean path whose directory exists, is owned by
the updater's user and is not writable by group or others; its last element must
not be a symlink. No target may be under `state_dir` (checked with symlinks resolved), and no two
components may share a file; the same rules apply to a default target, the service's
exe.

Keep the binaries in a directory the updater's user owns, for example
`~/.local/lib/warren/` or `/var/lib/warren/bin`. Next to each binary the updater
makes `<name>.new`, `<name>.prev.new` and `<name>.prev`; do not put them in a
directory on `PATH`.

## Making a release

```
warren-release keygen --component myapp --channel dev
    writes myapp-dev.key (encrypted, 0600) and myapp-dev.pub, and prints the config line

go build -o /tmp/myapp_linux_amd64 ./cmd/myapp
warren-release publish --component myapp --channel dev --version 1.4.0-dev.7 \
    --file myapp=linux/amd64=/tmp/myapp_linux_amd64 \
    --key ~/.config/warren/keys/myapp-dev.key --out dist

warren-release verify --key ~/.config/warren/keys/myapp-dev.pub dist
```

`publish` and `verify` take `--max-size` (the most any one file may be; default 256 MiB, as
in the updater's config).

The key password comes from `--password-file`, else `WARREN_RELEASE_PASSWORD`,
else a prompt. `publish` computes sizes and digests, checks the manifest (a dev
version is a pre-release, every platform lists the same targets, sizes), signs it,
verifies what it signed as an updater would, hashes each file again while copying it
(a file that changed since it was signed is refused and nothing is published), and writes
the source layout:

```
myapp.dev.manifest.json            the latest of the channel (a mutable pointer)
myapp.dev.manifest.json.minisig
myapp-1.4.0-dev.7.manifest.json    this exact release
myapp-1.4.0-dev.7.manifest.json.minisig
myapp_1.4.0-dev.7_linux_amd64      the files, named by the manifest
```

The exact release is written before the pointer, and each pair signature first, manifest
last, each by rename. A manifest and its signature cannot be replaced together, so a reader
that catches a pointer half way sees a pair that does not verify; `warren-update` looks
again once after 200 ms.

For `warren` and `warren-update` components `publish` also runs the host-platform
binary with `version` and refuses when the binary's `version` output differs from
`--version`.

The manifest is JSON (at most 16 KiB, format `warren-manifest/1`) and is signed with
minisign, prehashed (`ED`) only; the signature also carries the trusted comment
`warren-manifest/1 <component> <version> <channel>`, so the stock `minisign -V`
shows what was signed. `manual: true` (`--manual`) is signed and parsed already;
it will mean "never install automatically".

`publish` refuses to move a channel pointer to a lower version than the one `--out`
already holds, unless `--allow-downgrade` is given. Manifests are parsed strictly: a
repeated key, or a key spelled in another case than the format's, is refused.

Keep stable keys offline. Dev keys may live on a developer's machine or in CI for
the component that CI builds.

## Installing

```
warren-update install myapp --from file://./dist [--version 1.4.0-dev.7]
                          [--allow-downgrade] [--force] [--dry-run] [--wait]
```

The command reads the source with its own rights and uploads the manifest, its
signature and the files of this platform to the running updater, which verifies
everything again and is the authority. Without `--version` it reads the pointer
of the component's channel (and, for a dev component that holds a stable key, the
stable pointer too; the higher verified version wins). `--wait` blocks until the
component is healthy or committed (exit 0), or reverted or failed (exit 1).
`--dry-run` plans and reports. `--force` skips prudence only: the bad list and
"the service held a lease before", and a failed record. It never skips the
install mutex, the version floor (that is `--allow-downgrade`), nor any check of
signature, key, path or digest.

One install runs at a time per node. A component on probation (an install not yet
healthy) keeps every other component from installing, because a failure has to be
attributable. A restored version that is still waiting for its restart keeps only its own
component from installing, and `--force` goes past it. A revert that is not finished is
completed by the watcher first: `--force` does not skip it, and an install of that
component is refused until it is done. `--force` never bypasses an install that is running,
nor a probation.

An install is refused when: the version is not newer than the installed one; it
is on the bad list; the manifest has no build for this platform or its targets
are not the configured targets; the service is not managed now, does not hold a
lease, or runs another file than the configured target (a reload may have changed
its `run:`); another install is in progress; the component is failed; the node or
the component is **pinned**; or the binary is **not updatable**.

### The install, step by step

Everything except the last step happens with the service still running.

1. The record is written first: phase `installing`, the new and previous digests, the
   deadline.
2. Each new file is written next to its target as `<target>.new` while hashed, and
   the old file is hard-linked as `<target>.prev.new`.
3. The signature is verified again with the keys in force now, and the targets and
   the service's `exe` are checked again.
4. For each target `.prev.new` becomes `.prev` and `.new` becomes the target: both
   renames are atomic, the target is never missing, and the running process keeps
   its old file.
   (A failure from here until the restart request undoes the swap by the hash rule;
   the service is restarted only if the swap had begun, since a process started from
   then on may run the new file.)
5. Phase `probation`, then Warren is asked to restart the service. The answer
   carries `min_start_seq`: every start at or after that number began after the
   swap and runs the new file.

### Healthy, crash loop, timeout

The updater polls the service's status once a second. The service is **healthy**
when it is running with `start_seq` at or above `min_start_seq` and has held the
same lease (the same `start_seq` and `lease.epoch`, `lease.held` true) for the
settle time. At first healthy the install mutex is released and the canary runs;
at the end of the canary the version is `committed`.

- **Crash loop**: 3 unrequested failures (exits that were not asked for, and starts that
  failed) within 5 minutes, during probation or the canary, revert the install and mark
  the version bad.
- **Not healthy in time**: not healthy by the deadline reverts the install and does not
  mark it bad.
- A service that is briefly absent from Warren (a reload removed it and has not added
  it back) or whose `exe` no longer equals a target counts as nothing: the poll is
  retried, and the deadline still runs.
- If Warren restarts during probation (a new `boot`) the counters start over, and the
  deadline is set again to one settle window plus the health timeout from then, as long as
  the install (for a restored version, the revert request) is less than three health
  timeouts old; after that it is left alone, so a unit that keeps restarting cannot keep a
  version on probation for ever (the worst case is the anchor plus four health timeouts plus
  a settle window).
- Counters are cumulative per service name for a boot and survive a reload, so a
  missed poll loses no failure.

### Revert

A revert writes its verdict first (`reverted`, with the version marked bad when it
crash-looped), then puts each file back by its hash (a target that already equals the
previous version is left alone, one that equals the new version gets `.prev`
renamed over it, anything else makes the component `failed`), then records the
previous version as the installed one, and asks Warren to restart the service. The
restored version is watched like a new one, but never reverted again: if it does not
become healthy the component is `failed`, and keeps restarting under Warren's own
backoff. The way out of `failed` is `install --force`, after a hand fix or with a
release.

A restart request and the boot it was given in are recorded together: when Warren
restarts (a new `boot`), a request accepted in the old boot is satisfied by any start of
the new one, which began from the files already on disk, and a request not accepted yet
is simply sent again. After a new boot the deadline is set to one settle window plus the health timeout
from then, but only while the anchor is less than three health timeouts old (the install
time of a probation; for a restored version the time its process first proved itself, which
is also when its deadline is armed). After that the deadline is left alone, so a unit that
keeps restarting cannot keep a version on probation for ever. A service that has settled is
judged healthy before the deadline is looked at, in the same poll.

A revert to a version the updater never recorded (the file was there before it kept
state) keeps the version that was reverted away as a floor in `installed.floor`, so the
no-downgrade rule still holds; `--allow-downgrade` goes below it.

`warren-update revert <component>` does the same by hand (and marks a committed
version bad with the reason `manual`). It is refused for a failed component and when
there is nothing to go back to. `revert --offline` takes `update.lock` (so it cannot
run beside a daemon), swaps the files back and touches no process; restart the
service yourself; a record in `installing` is an orphan then (the lock is held) and is reverted too. It needs explicit `targets` in the config: the default target is the
service's exe, which only a running Warren reports, and a revert that cannot find its
files is refused before anything is written.

### Recovery

When `warren-update run` starts it removes leftover `.new` files, turns any record in
`installing` into an unmarked `reverted`, finishes every unfinished revert (a
`reverted` record that still has a previous version) by the hash rule above, and
asks Warren to restart the service; when Warren does not accept (the service is
absent, or Warren is not reachable), the health watcher sends the request again until
it does. It decides only by hashing, so it is safe to run at any time and any number
of times. The watcher also settles an `installing` record that no live install owns (an
install that could not even record its own failure) the same way. A revert whose
targets cannot be resolved (the config has none and Warren does not report the
service's exe) writes nothing and is tried again.
A record in `probation` simply carries on: deadlines and failure stamps are
wall-clock values in `state.json`.

A component with no record gets `needs_manual: state-lost`. A binary replaced by hand
(its hash no longer equals the installed one) sets `needs_manual: drift`. One
successful manual install clears either.

## state.json

`<state_dir>/<component>/state.json` is written by temp file, fsync, rename and a
directory fsync, only by the updater under `update.lock`. Its format is frozen:
`schema`, `component`, `phase` (`installing`, `probation`, `committed`, `reverted`,
`failed`; anything else means hands off), `installed`, `prev`, `request`, `deadline`,
`healthy_since`, `canary_ends`, `failures`, `watch`, `bad`, `needs_manual`,
`last_error`. A `reverted` record with a non-empty `prev` is an unfinished revert.
New fields are additive; unknown fields are kept when the file is rewritten.

## Pinned and not updatable

`pinned: <reason>` on a component, or on the node (Warren's `pinned:`), makes
`install` and `revert` refuse with that reason. Warren also reports, per service,
`updatable` and `not_updatable` (`read-only`; `container` for a binary in a
container's own layer, which would be lost when the container is recreated; or
`mount-point` for a binary that is a mount point of its own, such as a bind-mounted file); an
install or revert is refused for a target that is not updatable, quoting the
reason. `warren-update status` shows the pin, whether the binary is updatable and
why not, and, for a pinned component, the installed version when it is known
(`unknown` otherwise, since a service does not report its version).

## Commands

```
warren-update run --config path
warren-update install <component> --from file://DIR [--version v] [--allow-downgrade] [--force] [--dry-run] [--wait]
warren-update revert [--offline] <component>
warren-update status [--json]
warren-update reload
warren-update version
```

All but `run` and `version` take `--config path` (default `/etc/warren/update.yaml`)
to find the updater's socket, and talk to the running updater. They may run as another
user, root included: only the daemon enforces the owner and mode of target directories.
The exception is `revert --offline`, which writes the updater's state: it loads the config
with the daemon's rules, must run as the user that owns `state_dir`, and refuses a
`state_dir` that does not exist instead of creating it.
`install` of a pinned component is refused with its pin before the source is read.
