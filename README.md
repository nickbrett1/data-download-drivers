# data-download-drivers

A data-download-drivers project generated with genproj

## Capabilities

This project includes the following capabilities:

- **Doppler Secrets Management**: Integrates Doppler for secure secrets management. Enables the various MCP servers that rely on privileged tokens to access their services (e.g. Buildkite, GitHub, SonarQube).
- **AI Coding Agents**: Sets up the AI coding agents in the devcontainer: goose (config, MCP servers and spec-first recipes) plus the Antigravity CLI.
- **Container Agent**: Every generated devcontainer brings up and registers its own a2a-goose agent (`<repo>-dev`), reached over the tailnet by the LiteLLM proxy; reuses the a2a-goose GitHub release channel, so the container has the same self-update path as a host.
- **Secret Scanning (gitleaks)**: Required on every project: gitleaks scans staged changes in a pre-commit hook and the full history in CI. Free, offline, no account and no quota.
- **Docker**: Adds Docker support for containerised builds and tooling.
- **Python DevContainer**: Sets up a VS Code DevContainer with Python environment.
- **Ruff (Python code quality)**: Adds fast, zero-configuration Python linting with Ruff (rules live in pyproject.toml [tool.ruff]). Lint locally with `ruff check`. Requires a Python devcontainer. The generated CI pipeline also runs `ruff check`.
- **Docker Container**: Containerize the project and publish to the GitHub Container Registry (GHCR) for deployment to a NAS or self-hosted host via Docker Compose. Mutually exclusive with other deployment systems. Set `workload: batch` for a scheduled job image (runs and exits; no port, no healthcheck, no Watchtower) instead of a long-running service.
- **Buildkite Integration**: Runs CI on a self-hosted Buildkite agent (Apple silicon) instead of a metered cloud fleet. The pipeline and its GitHub webhook are created during generation, so there is no manual "set up project" step. It can coexist with an existing CI provider, so a repository can migrate without a flag day.
- **Editor Configuration**: Shared VS Code extensions and workspace settings for consistent tooling across the team.
- **Shell & Terminal**: Zsh shell with the Powerlevel10k prompt and productivity plugins.

## Setup

1. Clone the repository
2. Create a virtualenv and install the package with dev extras:

   ```bash
   python3 -m venv .venv
   . .venv/bin/activate
   pip install -e ".[dev]"
   ```

3. Run the checks:

   ```bash
   ruff check src tests
   pytest -v
   ```

## Secret scanning

Every commit is scanned for credentials by `gitleaks`, and CI scans the
**full history**. The local hook is convenience; CI is the gate you cannot skip
with `git commit --no-verify`.

```bash
scripts/setup-git-hooks.sh   # installs gitleaks and sets core.hooksPath
```

If a commit is blocked, **rotate the credential first** — a finding means it may
already be exposed. Then remove it from the staged changes and commit again.

Only for a verified false positive, add its fingerprint to `.gitleaksignore`,
which is a committed and reviewable diff. Never bypass the hook with
`--no-verify`: it leaves no record and the next commit is unscanned too.

This is detection, not storage (Doppler holds secrets) and not rotation (that is
a human step). Allowlist edits in `.gitleaks.toml` are reviewable changes on
purpose.

## Doppler

This project uses Doppler for secrets from the shared `common` project
(config `dev`) — no per-repo Doppler project is created. First use (links
the shared project and `dev` config):

```bash
doppler setup --project common --config dev
```

If your repo needs app-specific secrets that shouldn't live in the shared
`common` project, regenerate it with the doppler capability set to
`projectStrategy: "new"` to get a dedicated project.

The Doppler CLI is installed in the devcontainer — it must be on PATH for the
VS Code extension and `doppler run` to work. Auth is persisted via the host
`~/.doppler` bind-mount.

### Env-var precedence (read this if `doppler run` hits the wrong project)

Doppler resolves its target as **environment variables > `doppler.yaml` >
`~/.doppler` scoped config**. If your shell — or the session that launched
the devcontainer (e.g. an agent runtime) — exports `DOPPLER_PROJECT` /
`DOPPLER_CONFIG` / `DOPPLER_ENVIRONMENT`, those silently override this
repo's `doppler.yaml` and every `doppler` command targets the wrong
project. The devcontainer's post-create setup pins this repo's context
(`common`/`dev`) in `~/.bashrc` and `~/.zshrc` and warns at
setup if resolution still mismatches. To force the correct context manually:

```bash
unset DOPPLER_PROJECT DOPPLER_CONFIG DOPPLER_ENVIRONMENT
doppler setup --no-interactive --project common --config dev
```

## Deployment

See `deploy/README.md` for the deployment runbook (Buildkite -> GHCR ->
Watchtower -> Docker host). Deploy with:

```bash
docker compose up -d
```

## The container's agent

This devcontainer brings up its own `a2a-goose` agent, registered in the hub as
`data-download-drivers-dev` - one agent per repo, so a restart reclaims the same entry
instead of adding a second one. Turns are billed through the LiteLLM proxy
configured in Doppler (`LITELLM_BASE_URL`).

```bash
scripts/agent-dev.sh start    # write secrets + config, fetch the launcher, run it
scripts/agent-dev.sh status   # running or not, the card URL, the log tail
scripts/agent-dev.sh stop     # SIGTERM, wait for a clean deregister, confirm gone
```

`start` runs from the devcontainer's post-start hook, so the agent is normally
already up when you arrive. It fails open: with no network on a first start it
prints why it did not start and leaves the project usable. Secrets come from
Doppler into `~/.config/a2a-goose/env` (mode 0600) and never into the image or
`containerEnv`.

## The driver runtime

This container is the **untrusted half** of `data-download`: it holds the vendor
credential and the network egress, and it holds no request store. The app reaches
it over the `data-download-internal` compose network and resolves it by service
name. Spec: `data-download-driver-protocol` (vault).

### What it serves

| route | what it is |
|---|---|
| `GET /healthz` | liveness, and the protocol version |
| `GET /v1/drivers` | what is installed: `name`, `version`, `vendor`, `protocol` |
| `POST /v1/drivers/install` | write an artefact into the drivers volume (see below) |
| `POST /v1/sessions` | spawn a driver artefact for one Job; returns a `session_id` |
| `DELETE /v1/sessions/{id}` | kill it |
| `POST /v1/<verb>` | `resolve` `cost` `submit` `poll` `list_files` `fetch` `convert` `verify` |

A **session is a process**. One session = one interpreted Job = one child
process, so a hang is a `kill` and no state leaks between jobs. `submit` is
idempotent within a session: a repeat replays the first answer (with
`replayed: true` and the ceilings it was authorised under) and never buys twice.

### Artefacts

A driver is a **directory**, not an image:

```
/drivers/<vendor>/<version>/manifest.json    name, version, vendor, protocol, entrypoint
/drivers/<vendor>/<version>/driver.py        calls data_download_drivers.child.serve(build)
```

`drivers/stub/1/` in this repo is the reference artefact: placeholder bytes that
begin `STUB:`, so no byte it produces can be mistaken for market data. Installing
a driver is an atomic directory rename into the mounted volume — no image, no
restart. Discovery runs per request, so a new directory shows up in
`GET /v1/drivers` on the next call.

### Installing one

The runtime owns the volume, so nothing else writes to it. The control plane
fetches the merged tree from the forge, then pushes it here as data:

```
POST /v1/drivers/install            Authorization: Bearer $DRIVER_RUNTIME_TOKEN
{
  "name": "zzzdryrun", "version": "1", "vendor": "Zzz",
  "files": {"manifest.json": "<base64>", "driver.py": "<base64>"},
  "replace_sha256": "<optional: digest of the install this replaces>"
}
```

It answers `{"name", "version", "vendor", "path", "sha256", "replaced"}`. A tree
without a `manifest.json`, a manifest that names a different driver or version
than it is installed under, a path that climbs out of the artefact, a protocol
this host does not speak, or an entrypoint that is not in the tree are each
refused with `install_refused` (409); a body that cannot be read is
`bad_request` (400).

A version that already exists is refused rather than silently overwritten,
unless `replace_sha256` names the digest of the install already there — which is
how a caller proves the bytes it is replacing are the bytes it installed. The
`sha256` in the answer is over names and bytes in sorted order, identical to
`data_download.driver_build.artifact_digest` on the app side.

The tree is written into a temporary directory *inside* the volume and renamed
into `<name>/<version>/`, so a half-written artefact is never visible to
discovery, and a refused install leaves nothing behind.

### `drivers/databento/1/` — the first real vendor

Batch jobs against Databento: submit, poll, download, convert to parquet. The
chain of trust is the reason for the shape:

1. **`list_files` is the vendor's own manifest** — a name, a size and a sha256
   per file. It is the only statement about the bytes that comes from Databento.
2. **`fetch` downloads one named file at a time**, so what lands is the file that
   manifest hashes. The whole-job path returns a single `.zip` whose hash is the
   archive's, and the SDK refuses `keep_zip=True` once a filename is given.
   Each file is hashed as it lands and compared to the manifest entry: the SDK
   itself only *warns* on a checksum mismatch, having already written the file.
3. **`convert`** turns each `.dbn.zst` into `.parquet`, counting the records it
   read **before** converting, so a lossy conversion is a row-count
   contradiction rather than a quietly smaller file. The vendor's bytes are
   removed afterwards, so no file exists that the manifest does not list.
4. **`verify`** reports in landed (parquet) vocabulary on success, and in the
   vendor's vocabulary when an integrity check failed — so the contradiction
   names the file that disagreed rather than a parquet derived from it.

Two limits, written down rather than papered over: `resolve` has no window in
the Job, so it resolves a recent 30 days and says so in its answer (it exists to
catch an unresolvable symbol before anything is spent); and conversion costs two
passes because the record count comes from the vendor's bytes.

The vendor key is `DATABENTO_API_KEY` in this container's environment. It is
checked where it is used, not at session creation, so a missing key reaches the
app as `bad_request: DATABENTO_API_KEY is unset…` on a request rather than as
"the driver process exited".

**The image's own `drivers/` is not what the host reads.** `COPY . .` puts the
repo's artefacts at `/app/drivers`, but discovery reads `DATA_DOWNLOAD_DRIVERS_DIR`
(default `/drivers`), which compose mounts from the host. A bare `docker run` with
no mount therefore starts healthy and lists **no drivers** — by design, because a
second copy inside the image would be a second answer to "what is installed".
To run the stub:

```bash
docker run --rm -p 127.0.0.1:8831:8831 \
  -e DRIVER_RUNTIME_TOKEN=dev \
  -v "$PWD/drivers:/drivers" \
  -v /tmp/datasets:/datasets \
  ghcr.io/nickbrett1/data-download-drivers:latest
```

### Environment

| variable | default | meaning |
|---|---|---|
| `DATA_DOWNLOAD_DRIVERS_DIR` | `/drivers` | artefacts, mounted **read-write**: this container is the only writer |
| `DATA_DOWNLOAD_DATASETS_DIR` | `/datasets` | where bytes land; must be the same path the app uses |
| `DRIVER_RUNTIME_TOKEN` | unset | bearer token. Unset means **anyone on the network may drive a vendor** — dev only |
| `DRIVER_VERB_DEADLINE_S` | per-verb table | caps every verb deadline (tests) |
| `DRIVER_SESSION_DEADLINE_S` | `3600` | a session older than this is killed |
| `DATABENTO_API_KEY` | unset | the vendor credential, and the only secret here |

### Tests

```bash
pip install -e ".[dev]" && pytest        # wire shapes, host rules, the stub, databento
ruff check src tests drivers
```

`tests/test_databento_driver.py` swaps the SDK for a fake module, so it needs no
network, no key and no spend: it asserts the two SDK behaviours the artefact has
to work around (the `keep_zip` refusal, the job subdirectory) and that a corrupt
download is never converted.

## Generated by genproj

This project was generated using the genproj tool.
