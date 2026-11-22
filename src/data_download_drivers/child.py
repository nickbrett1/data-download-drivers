"""The other end of the wire: a driver artefact as a child process.

The host spawns one process per session; this module is what that process runs.
It is **internal to the runtime** and not part of the wire -- an artefact's
`driver.py` calls `serve(build)` and the host speaks JSON lines to it. When the
child contract changes, the host translates, and the app never notices.

Protocol, one request per line, one response per line:

    -> {"verb": "submit", "args": {...}}
    <- {"ok": true, "result": {...}}
    <- {"ok": false, "error": {"code": "vendor_error", "detail": "..."}}

Why a process at all: the artefact is code an agent wrote. A process gives the
host a `kill` for a hang (`rlimit`s and a deadline live in `host.py`), and it
means a driver that segfaults takes down one job rather than the service.

The child is deliberately dumb about policy: it does not check the budget, does
not decide whether a session is alive, and does not classify a verdict. All
three belong to the trusted side.
"""

from __future__ import annotations

import json
import os
import sys
import traceback
from collections.abc import Callable
from typing import Any, Protocol

from .wire import BAD_REQUEST, DRIVER_ERROR, VENDOR_ERROR, WireError


class Handler(Protocol):
    """What an artefact provides: one callable per wire verb."""

    def resolve(self, **kwargs: Any) -> dict[str, Any]: ...
    def cost(self, **kwargs: Any) -> dict[str, Any]: ...
    def submit(self, **kwargs: Any) -> dict[str, Any]: ...
    def poll(self, **kwargs: Any) -> dict[str, Any]: ...
    def list_files(self, **kwargs: Any) -> dict[str, Any]: ...
    def fetch(self, **kwargs: Any) -> dict[str, Any]: ...
    def convert(self, **kwargs: Any) -> dict[str, Any]: ...
    def verify(self, **kwargs: Any) -> dict[str, Any]: ...


#: Build one handler for the session's root. `options` is the session's optional
#: `{"driver_options": {...}}`, which is how the stub selects its mode.
Builder = Callable[[str, dict[str, Any]], Handler]


def _error(code: str, detail: str) -> dict[str, Any]:
    return {"ok": False, "error": {"code": code, "detail": detail}}


def serve(build: Builder) -> int:
    """Read requests from stdin, answer on stdout, until stdin closes."""
    root = os.environ.get("DRIVER_SESSION_ROOT", "")
    options: dict[str, Any] = {}
    raw_options = os.environ.get("DRIVER_SESSION_OPTIONS")
    if raw_options:
        try:
            parsed = json.loads(raw_options)
            if isinstance(parsed, dict):
                options = parsed
        except ValueError:
            options = {}

    handler = build(root, options)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except ValueError as exc:
            _write(_error(BAD_REQUEST, f"not JSON: {exc}"))
            continue
        if not isinstance(request, dict):
            _write(_error(BAD_REQUEST, "request must be an object"))
            continue

        verb = request.get("verb")
        args = request.get("args") or {}
        method = getattr(handler, str(verb), None)
        if not callable(method):
            _write(_error(BAD_REQUEST, f"unknown verb {verb!r}"))
            continue
        if not isinstance(args, dict):
            _write(_error(BAD_REQUEST, "args must be an object"))
            continue

        try:
            _write({"ok": True, "result": method(**args)})
        except WireError as exc:
            _write(_error(exc.code, exc.detail))
        except Exception as exc:  # noqa: BLE001 - the boundary: a driver must not kill the loop
            # The traceback goes to stderr (container logs); the app gets a code.
            # A vendor SDK almost always raises its own types, and the host is
            # the only place that can decide what a vendor failure means.
            traceback.print_exc(file=sys.stderr)
            code = VENDOR_ERROR if _looks_like_vendor(exc) else DRIVER_ERROR
            _write(_error(code, f"{type(exc).__name__}: {exc}"))
    return 0


def _looks_like_vendor(exc: BaseException) -> bool:
    """A vendor failure is a dependency's exception, not the artefact's own.

    Checked by module of origin rather than by name: a driver that defines its
    own `VendorError` has authored a claim about someone else's system.
    """
    module = type(exc).__module__ or ""
    return not module.startswith(("builtins", "data_download_drivers"))


def _write(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, default=str) + "\n")
    sys.stdout.flush()
