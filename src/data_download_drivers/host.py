"""The protocol host: sessions, artefacts, and the one place policy lives.

`GET  /healthz`              liveness (the Dockerfile's healthcheck)
`GET  /v1/drivers`           what is installed: name, version, vendor, protocol
`POST /v1/drivers/install`   write an artefact into the drivers volume
`POST /v1/sessions`          spawn a driver artefact for one Job
`DELETE /v1/sessions/{id}`   kill it and report the exit status
`POST /v1/<verb>`            the eight verbs, forwarded to the session's process

This is the trusted half of the runtime. It owns everything the driver must not
be trusted with: which directory bytes may land in, how long a verb may take,
whether a session is still alive, whether a `submit` may happen given the
ceiling it was handed, and whether a repeated `submit` is a repeat or a second
purchase. The driver owns the vendor; the host owns the boundary.

Design notes worth keeping:

* **Sessions are processes.** One session = one interpreted Job = one child, so
  there is no session map to grow stale and no cross-job state to leak. The
  map that does exist is keyed by a random id and mirrored by a live process.
* **`submit` is idempotent within a session.** A retried HTTP request that
  reached the child twice would buy twice; the host caches the first answer and
  replays it with `"replayed": true`. Nothing in this file retries anything.
* **Deadlines are enforced here, not asked for.** `wire.DEADLINES` is the
  budget; a child that exceeds it is killed and the call answers 504. The app's
  `MAX_POLLS` bounds the number of asks; this bounds the time.
"""

from __future__ import annotations

import json
import os
import secrets
import select
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from . import install as installs
from . import wire

MAX_BODY = 32 * 1024 * 1024
DEFAULT_PORT = 8831


@dataclass(frozen=True)
class Artifact:
    """One installed driver: a directory and the manifest that describes it."""

    name: str
    version: str
    vendor: str
    protocol: int
    path: Path
    entrypoint: str
    installed_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "vendor": self.vendor,
            "protocol": self.protocol,
            "installed_at": self.installed_at,
        }


def discover(drivers_dir: Path) -> list[Artifact]:
    """`<drivers>/<vendor>/<version>/manifest.json`, per the phase order §3."""
    found: list[Artifact] = []
    if not drivers_dir.is_dir():
        return found
    for manifest_path in sorted(drivers_dir.glob("*/*/manifest.json")):
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        name = data.get("name")
        version = data.get("version")
        if not isinstance(name, str) or not isinstance(version, str):
            continue
        entrypoint = data.get("entrypoint") or "driver.py"
        if not isinstance(entrypoint, str):
            continue
        found.append(
            Artifact(
                name=name,
                version=version,
                vendor=str(data.get("vendor") or ""),
                protocol=int(data.get("protocol") or 0),
                path=manifest_path.parent,
                entrypoint=entrypoint,
                installed_at=str(data.get("installed_at") or ""),
            )
        )
    return found


class ChildChannel:
    """JSON lines to a child process, with a deadline on every read.

    `select` + `os.read` rather than a thread per call: a timed-out read must
    leave the process killable, and a blocking `readline` cannot be interrupted
    without one. Leftover bytes are buffered, so a child that answers faster
    than it is asked cannot desynchronise the conversation.
    """

    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        assert proc.stdin is not None and proc.stdout is not None
        self.proc = proc
        self._fd = proc.stdout.fileno()
        self._buf = b""

    def send(self, payload: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(
            (json.dumps(payload, default=str) + "\n").encode("utf-8")
        )
        self.proc.stdin.flush()

    def recv(self, timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while b"\n" not in self._buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if not ready:
                raise TimeoutError
            chunk = os.read(self._fd, 65536)
            if not chunk:
                raise EOFError("the driver process exited")
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\n")
        return json.loads(line.decode("utf-8"))

    def kill(self) -> None:
        if self.proc.poll() is not None:
            return
        try:
            self.proc.kill()
        except OSError:
            pass


@dataclass
class Session:
    id: str
    artifact: Artifact
    root: Path
    channel: ChildChannel
    expires_at: float
    lock: threading.Lock = field(default_factory=threading.Lock)
    submitted: dict[str, Any] | None = None
    closed: bool = False

    def kill(self) -> None:
        self.closed = True
        self.channel.kill()


class SessionManager:
    def __init__(
        self,
        drivers_dir: Path,
        datasets_dir: Path,
        session_deadline_s: int = 3600,
        verb_deadline_cap_s: int | None = None,
    ) -> None:
        self.drivers_dir = drivers_dir
        self.datasets_dir = datasets_dir
        self.session_deadline_s = session_deadline_s
        self.verb_deadline_cap_s = verb_deadline_cap_s
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()

    # -- discovery ----------------------------------------------------------

    def artifacts(self) -> list[Artifact]:
        return discover(self.drivers_dir)

    def find(self, name: str, version: str) -> Artifact | None:
        for artifact in self.artifacts():
            if artifact.name == name and artifact.version == version:
                return artifact
        return None

    def install(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Write an artefact into the drivers volume (see `install.py`).

        `drivers_dir` lives here, so installation lives here too: one object
        knows where artefacts go, and no caller can be handed a path to write.
        """
        return installs.install(payload, self.drivers_dir)

    # -- lifecycle ----------------------------------------------------------

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        protocol = payload.get("protocol")
        if protocol != wire.PROTOCOL_VERSION:
            raise wire.WireError(
                wire.BAD_REQUEST,
                f"protocol {protocol!r} unsupported; this host speaks "
                f"{wire.PROTOCOL_VERSION}",
            )
        name = payload.get("driver")
        version = payload.get("driver_version")
        if not isinstance(name, str) or not isinstance(version, str):
            raise wire.WireError(
                wire.BAD_REQUEST, "driver and driver_version are required"
            )

        artifact = self.find(name, version)
        if artifact is None:
            raise wire.WireError(
                wire.DRIVER_MISSING, f"no installed driver {name!r} version {version!r}"
            )
        if artifact.protocol != wire.PROTOCOL_VERSION:
            raise wire.WireError(
                wire.DRIVER_MISSING,
                f"driver {name!r} speaks protocol {artifact.protocol}, "
                f"host speaks {wire.PROTOCOL_VERSION}",
            )

        root = self._root(payload.get("root"))
        root.mkdir(parents=True, exist_ok=True)

        session_id = secrets.token_urlsafe(18)
        proc = self._spawn(artifact, root, payload.get("driver_options"))
        session = Session(
            id=session_id,
            artifact=artifact,
            root=root,
            channel=ChildChannel(proc),
            expires_at=time.monotonic() + self.session_deadline_s,
        )
        with self._lock:
            self._sessions[session_id] = session
        return {
            "session_id": session_id,
            "driver": artifact.name,
            "driver_version": artifact.version,
            "protocol": wire.PROTOCOL_VERSION,
            "root": str(root),
        }

    def call(self, verb: str, payload: dict[str, Any]) -> dict[str, Any]:
        args = wire.validate(verb, payload)
        session = self.get(str(args.get("session_id")))

        timeout = wire.DEADLINES[verb]
        if self.verb_deadline_cap_s is not None:
            timeout = min(timeout, self.verb_deadline_cap_s)

        with session.lock:
            self._check_alive(session)
            if verb == "submit":
                return self._submit(session, args, timeout)
            return self._forward(session, verb, args, timeout)

    def close(self, session_id: str) -> dict[str, Any]:
        session = self.get(session_id)
        with session.lock:
            if not session.closed:
                session.channel.send({"verb": "__close__", "args": {}})
                session.kill()
        return {
            "session_id": session_id,
            "driver": session.artifact.name,
            "driver_version": session.artifact.version,
        }

    def get(self, session_id: str | None) -> Session:
        with self._lock:
            session = self._sessions.get(session_id or "")
        if session is None:
            raise wire.WireError(wire.UNKNOWN_SESSION, "no such session")
        return session

    # -- the rules ----------------------------------------------------------

    def _submit(
        self, session: Session, args: dict[str, Any], timeout: int
    ) -> dict[str, Any]:
        if session.submitted is not None:
            # A repeat is a replay, never a second purchase. The app does not
            # retry `submit`; the network does not consult the app's intentions.
            #
            # The recorded ceilings travel with the replay on purpose: a client
            # that repeats a submit carrying a *lower* authorised ceiling must
            # not read a 200 as "your new limit was accepted". The money was
            # already committed under the first ceiling, and refusing here would
            # claim nothing was bought -- which would be false.
            return {**session.submitted, "replayed": True}

        authorised = args.get("authorised_usd")
        declared = args.get("declared_usd")
        if isinstance(authorised, (int, float)) and isinstance(declared, (int, float)):
            if declared > authorised:
                raise wire.WireError(
                    wire.BUDGET_REFUSED,
                    f"declared ${declared:.2f} exceeds the authorised "
                    f"${authorised:.2f}; nothing was bought",
                )

        result = self._forward(session, "submit", args, timeout)
        if not isinstance(result.get("vendor_job_id"), str):
            raise wire.WireError(
                wire.DRIVER_ERROR, "the driver returned no vendor_job_id"
            )
        session.submitted = {
            **result,
            "authorised_usd": args.get("authorised_usd"),
            "declared_usd": args.get("declared_usd"),
        }
        return session.submitted

    def _forward(
        self, session: Session, verb: str, args: dict[str, Any], timeout: int
    ) -> dict[str, Any]:
        payload = {k: v for k, v in args.items() if k != "session_id"}
        try:
            session.channel.send({"verb": verb, "args": payload})
            answer = session.channel.recv(timeout)
        except TimeoutError:
            # A hang is a kill, not a wedge: the session dies with the call.
            session.kill()
            raise wire.WireError(
                wire.TIMEOUT, f"{verb} exceeded {timeout}s; the driver was killed"
            ) from None
        except (EOFError, ValueError) as exc:
            session.kill()
            raise wire.WireError(
                wire.DRIVER_ERROR, f"{verb}: {exc}"
            ) from None

        if not isinstance(answer, dict):
            raise wire.WireError(wire.DRIVER_ERROR, f"{verb}: malformed reply")
        if not answer.get("ok"):
            error = answer.get("error") or {}
            code = str(error.get("code") or wire.DRIVER_ERROR)
            detail = str(error.get("detail") or "")
            if code not in wire.STATUS:
                code = wire.DRIVER_ERROR
            raise wire.WireError(code, detail or f"{verb} failed")
        result = answer.get("result")
        return result if isinstance(result, dict) else {"result": result}

    def _check_alive(self, session: Session) -> None:
        if session.closed:
            raise wire.WireError(wire.SESSION_EXPIRED, "the session was closed")
        if time.monotonic() > session.expires_at:
            session.kill()
            raise wire.WireError(
                wire.SESSION_EXPIRED, "the session outlived its deadline"
            )
        if session.channel.proc.poll() is not None:
            session.closed = True
            raise wire.WireError(
                wire.DRIVER_ERROR,
                f"the driver process exited with {session.channel.proc.returncode}",
            )

    # -- processes and paths ------------------------------------------------

    def _root(self, value: Any) -> Path:
        """The request root, resolved and checked against the dataset store.

        Resolve-then-check, so a symlink is judged by where it points rather
        than by how it is spelled -- the same rule `datasets.resolve_under`
        applies on the app side.
        """
        if not isinstance(value, str) or not value.strip():
            raise wire.WireError(wire.BAD_REQUEST, "root is required")
        root = Path(value)
        if not root.is_absolute():
            raise wire.WireError(wire.BAD_REQUEST, "root must be absolute")
        resolved_root = root.resolve()
        resolved_base = self.datasets_dir.resolve()
        if resolved_base != resolved_root and resolved_base not in resolved_root.parents:
            raise wire.WireError(
                wire.BAD_REQUEST,
                f"root {value!r} is outside {resolved_base}",
            )
        return resolved_root

    def _spawn(self, artifact: Artifact, root: Path, options: Any) -> subprocess.Popen[bytes]:
        env = dict(os.environ)
        # The child must not inherit the runtime's own credential. It is
        # agent-written code, and holding the token would let it call back into
        # this host and open a session for any *other* installed vendor, driving
        # that vendor's key. One vendor today makes that a no-op; two vendors
        # make it a privilege escalation across the boundary this container
        # exists to draw.
        #
        # The vendor key is deliberately NOT scrubbed: `DATABENTO_API_KEY` is the
        # credential the driver is here to use, and hiding it would break the
        # only thing that needs it.
        env.pop("DRIVER_RUNTIME_TOKEN", None)
        env["DRIVER_SESSION_ROOT"] = str(root)
        env["DATA_DOWNLOAD_DATASETS_DIR"] = str(self.datasets_dir)
        env["DATA_DOWNLOAD_DRIVERS_DIR"] = str(self.drivers_dir)
        if isinstance(options, dict):
            env["DRIVER_SESSION_OPTIONS"] = json.dumps(options)
        else:
            env.pop("DRIVER_SESSION_OPTIONS", None)

        entrypoint = artifact.path / artifact.entrypoint
        if not entrypoint.is_file():
            raise wire.WireError(
                wire.DRIVER_MISSING, f"{artifact.name}: {entrypoint} is missing"
            )
        # The interpreter that can import this package, not whatever `python3`
        # happens to be first on PATH: a data-download-drivers installed in a venv
        # whose bin/ is not on PATH would otherwise spawn children that cannot
        # import `data_download_drivers.child` and every verb would answer
        # "the driver process exited". The channel is JSON over stdio, so the
        # child has no reason to want a different interpreter from this one.
        python = sys.executable
        try:
            return subprocess.Popen(  # noqa: S603 - the artefact is the payload
                [python, str(entrypoint)],
                cwd=str(artifact.path),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=None,
                env=env,
                bufsize=0,
                start_new_session=True,
            )
        except OSError as exc:
            raise wire.WireError(
                wire.DRIVER_ERROR, f"{artifact.name} would not start: {exc}"
            ) from None

    def shutdown(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            try:
                session.channel.proc.send_signal(signal.SIGTERM)
            except OSError:
                pass


def make_handler(manager: SessionManager, token: str | None) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "driver-runtime"

        def log_message(self, fmt: str, *args: Any) -> None:
            message = fmt % args
            sys.stderr.write(f"{self.address_string()} - {message}\n")

        # -- routing --------------------------------------------------------

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's API
            path = self.path.split("?", 1)[0]
            if path == "/healthz":
                self._json(200, {"status": "ok", "protocol": wire.PROTOCOL_VERSION})
                return
            if path == "/v1/drivers":
                if not self._authorised():
                    return
                self._json(
                    200,
                    {
                        "protocol": wire.PROTOCOL_VERSION,
                        "drivers": [a.as_dict() for a in manager.artifacts()],
                    },
                )
                return
            self._json(404, {"error": {"code": "not_found", "detail": path}})

        def do_POST(self) -> None:  # noqa: N802
            if not self._authorised():
                return
            path = self.path.split("?", 1)[0]
            try:
                body = self._body()
            except wire.WireError as exc:
                self._json(exc.status, exc.as_body())
                return

            try:
                if path == "/v1/sessions":
                    self._json(201, manager.create(body))
                    return
                if path == "/v1/drivers/install":
                    self._json(200, manager.install(body))
                    return
                if not path.startswith("/v1/"):
                    self._json(404, {"error": {"code": "not_found", "detail": path}})
                    return
                self._json(200, manager.call(path[len("/v1/"):], body))
            except wire.WireError as exc:
                self._json(exc.status, exc.as_body())
            except Exception as exc:  # noqa: BLE001 - last resort, never a traceback
                self._json(
                    500,
                    {"error": {"code": wire.DRIVER_ERROR, "detail": str(exc)}},
                )

        def do_DELETE(self) -> None:  # noqa: N802
            if not self._authorised():
                return
            path = self.path.split("?", 1)[0]
            prefix = "/v1/sessions/"
            if not path.startswith(prefix):
                self._json(404, {"error": {"code": "not_found", "detail": path}})
                return
            try:
                self._json(200, manager.close(path[len(prefix):]))
            except wire.WireError as exc:
                self._json(exc.status, exc.as_body())

        # -- plumbing -------------------------------------------------------

        def _authorised(self) -> bool:
            if not token:
                return True
            header = self.headers.get("Authorization") or ""
            expected = f"Bearer {token}"
            if secrets.compare_digest(header, expected):
                return True
            self._json(401, {"error": {"code": wire.UNAUTHORISED, "detail": "bearer token"}})
            return False

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                raise wire.WireError(wire.BAD_REQUEST, "body too large")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError) as exc:
                raise wire.WireError(
                    wire.BAD_REQUEST, f"body is not JSON: {exc}"
                ) from None
            if not isinstance(parsed, dict):
                raise wire.WireError(wire.BAD_REQUEST, "body must be an object")
            return parsed

        def _json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def serve(host: str, port: int) -> int:
    drivers_dir = Path(os.environ.get("DATA_DOWNLOAD_DRIVERS_DIR") or "/drivers")
    datasets_dir = Path(os.environ.get("DATA_DOWNLOAD_DATASETS_DIR") or "/datasets")
    token = (os.environ.get("DRIVER_RUNTIME_TOKEN") or "").strip() or None
    cap_raw = os.environ.get("DRIVER_VERB_DEADLINE_S")
    deadline_raw = os.environ.get("DRIVER_SESSION_DEADLINE_S")

    manager = SessionManager(
        drivers_dir=drivers_dir,
        datasets_dir=datasets_dir,
        session_deadline_s=int(deadline_raw) if deadline_raw else 3600,
        verb_deadline_cap_s=int(cap_raw) if cap_raw else None,
    )

    if token is None:
        sys.stderr.write(
            "warning: DRIVER_RUNTIME_TOKEN is unset - every caller on this "
            "network may drive a vendor. Dev only.\n"
        )

    server = ThreadingHTTPServer((host, port), make_handler(manager, token))
    server.daemon_threads = True
    print(
        f"driver-runtime on {host}:{port} - drivers from {drivers_dir}, "
        f"bytes under {datasets_dir}, protocol {wire.PROTOCOL_VERSION}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()
        server.server_close()
    return 0
