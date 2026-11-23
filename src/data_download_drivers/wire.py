"""The driver wire: eight verbs, JSON in, JSON out (spec `driver-protocol` §4).

This module is the contract *as data*: the verb list, the fields each verb
requires, the deadline each verb gets, and the error codes. Nothing here talks
to a socket -- `host.py` serves it and `child.py` is the other end -- so the
same file is the single place a field is added or renamed.

Two rules it enforces rather than documents, because both are about where bytes
may land and neither is safe to leave to a driver's good behaviour:

* **`dest` is relative and contains no `..`.** The untrusted side does not get
  to decide where the filesystem boundary is.
* **A file name is a bare name.** No separator, no `..`, no absolute path -- a
  manifest entry is a name inside `dest`, never a path that could escape it.

The error codes are the design's own vocabulary (`fail, record, stop`), not
HTTP's: a code says *which* thing went wrong, and `STATUS` maps it to a status
so a client can be dumb.
"""

from __future__ import annotations

import re
from typing import Any

#: Bumped when a verb's shape changes meaning. Checked at session creation on
#: both sides; never inferred. Distinct from `interpreter.INTERPRETER_VERSION`
#: (which flow produced a verdict) and from a driver's own `version`.
PROTOCOL_VERSION = 1

VERBS = (
    "resolve",
    "cost",
    "submit",
    "poll",
    "list_files",
    "fetch",
    "convert",
    "verify",
)

#: Per-verb deadline, seconds, enforced by the host *and* by the client. A hung
#: `fetch` is otherwise an attempt that never settles and a lease that never
#: releases.
#: How long a verb may take before the host kills the session.
#:
#: These bound a *hang*, not the vendor's best case. `cost` is 120 s because
#: Databento prices a window server-side and the query scans it: one day of ES
#: trades measured 0.1 s to 28 s across repeated calls (2026-11-23), so 30 s
#: turned a slow-but-working vendor into "cost exceeded 30s". A price is free
#: and idempotent, which is why patience here costs nothing.
DEADLINES = {
    "resolve": 30,
    "cost": 120,
    "submit": 60,
    # `poll` is 60 for the same reason as `cost`: the vendor's own
    # `get_job_details` measured up to 28 s, and the driver's pause inside the
    # verb is a *budget* it shares with that call. 30 left no room for a slow
    # answer plus any pause at all.
    "poll": 60,
    "list_files": 120,
    "fetch": 300,
    "convert": 300,
    "verify": 120,
}

BAD_REQUEST = "bad_request"
UNAUTHORISED = "unauthorised"
UNKNOWN_SESSION = "unknown_session"
DRIVER_MISSING = "driver_missing"
SESSION_EXPIRED = "session_expired"
BUDGET_REFUSED = "budget_refused"
DRIVER_ERROR = "driver_error"
VENDOR_ERROR = "vendor_error"
TIMEOUT = "timeout"

STATUS = {
    BAD_REQUEST: 400,
    UNAUTHORISED: 401,
    UNKNOWN_SESSION: 404,
    DRIVER_MISSING: 409,
    SESSION_EXPIRED: 410,
    BUDGET_REFUSED: 422,
    DRIVER_ERROR: 500,
    VENDOR_ERROR: 502,
    TIMEOUT: 504,
}

#: Fields every verb needs, beyond `session_id`.
REQUIRED: dict[str, tuple[str, ...]] = {
    "resolve": ("dataset", "symbols", "stype_in"),
    "cost": ("dataset", "schema", "symbols", "stype_in", "start", "end"),
    "submit": (
        "dataset",
        "schema",
        "symbols",
        "stype_in",
        "start",
        "end",
        "encoding",
        "compression",
        "split_duration",
    ),
    "poll": ("vendor_job_id",),
    "list_files": ("vendor_job_id",),
    "fetch": ("vendor_job_id", "dest", "files"),
    "convert": ("dest", "from", "to"),
    "verify": ("dest", "claimed", "staged"),
}

#: A name inside `dest`, not a path.
BARE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class WireError(Exception):
    """A refusal with a code and an HTTP status. Never a traceback."""

    def __init__(self, code: str, detail: str, status: int | None = None) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.status = STATUS.get(code, 400) if status is None else status

    def as_body(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "detail": self.detail}}


def _need_str(args: dict[str, Any], name: str) -> str:
    value = args.get(name)
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, f"{name} must be a non-empty string")
    return value


def _need_strs(args: dict[str, Any], name: str) -> list[str]:
    value = args.get(name)
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise WireError(BAD_REQUEST, f"{name} must be a list of strings")
    return list(value)


def check_dest(value: Any) -> str:
    """A `dest` resolves under the session root, and may not climb out of it."""
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, "dest must be a non-empty string")
    path = value.strip()
    if path.startswith("/") or path.startswith("\\"):
        raise WireError(BAD_REQUEST, f"dest {value!r} is absolute")
    if ".." in path.replace("\\", "/").split("/"):
        raise WireError(BAD_REQUEST, f"dest {value!r} climbs out of the request root")
    return path


def _check_files(args: dict[str, Any]) -> list[dict[str, Any]]:
    value = args.get("files")
    if not isinstance(value, list):
        raise WireError(BAD_REQUEST, "files must be a list")
    entries: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise WireError(BAD_REQUEST, "each file must be an object")
        name = entry.get("name")
        if not isinstance(name, str) or not BARE_NAME.match(name):
            raise WireError(BAD_REQUEST, f"file name {name!r} is not a bare name")
        entries.append(dict(entry))
    return entries


def validate(verb: str, payload: Any) -> dict[str, Any]:
    """Return the verb's args, or raise `WireError(bad_request)`.

    Deliberately shallow: it checks presence and the two containment rules, not
    every vendor field. A driver is the authority on its own kwargs; the host is
    the authority on where bytes may land.
    """
    if verb not in VERBS:
        raise WireError(BAD_REQUEST, f"unknown verb {verb!r}")
    if not isinstance(payload, dict):
        raise WireError(BAD_REQUEST, "the request body must be a JSON object")

    args = dict(payload)
    _need_str(args, "session_id")
    for name in REQUIRED[verb]:
        if name not in args:
            raise WireError(BAD_REQUEST, f"{verb}: {name} is required")

    if "dataset" in REQUIRED[verb]:
        _need_str(args, "dataset")
    if "symbols" in REQUIRED[verb]:
        _need_strs(args, "symbols")
    for name in ("stype_in", "schema", "start", "end", "encoding", "compression", "split_duration"):
        if name in REQUIRED[verb]:
            _need_str(args, name)
    for name in ("vendor_job_id", "from", "to"):
        if name in REQUIRED[verb]:
            _need_str(args, name)
    if "dest" in REQUIRED[verb]:
        args["dest"] = check_dest(args.get("dest"))
    if "files" in REQUIRED[verb]:
        args["files"] = _check_files(args)
    for name in ("claimed", "staged"):
        if name in REQUIRED[verb]:
            _need_strs(args, name)
    return args
