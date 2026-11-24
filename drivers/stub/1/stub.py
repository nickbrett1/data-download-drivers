"""The stub vendor, as a driver artefact: files that are not data.

This is the app's phase-5 `stub.py`, moved verbatim in behaviour and retargeted
at the wire. It exists for one reason, unchanged: the runner's correctness --
the claim, the lease, the landing path, the digest, the settle CAS -- can be
built and tested without a credential, a vendor job, or a cent of spend. What it
cannot prove is anything about Databento, and it does not pretend to:

* every file it writes begins with `STUB:` and the vendor job id, so no byte it
  produces can be mistaken for market data;
* `cost` returns a fixed number, and nothing compares it to `estimate_usd`
  (§5.2: the one comparison happened at submit);
* `convert` does not convert. A real parquet conversion writes files the
  vendor's manifest does not list, and the shared verifier would correctly call
  that a contradiction -- so the stub leaves the source files alone rather than
  teaching `classify` a lie.

Three modes, so the failure paths are reachable without a network: `clean`,
`short` (the vendor published less than its job claimed) and `nothing`.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

MODES = ("clean", "short", "nothing")

#: A fixed number so `quote` has something to return. Never compared to
#: `estimate_usd`; that comparison is the app's, at submit.
FAKE_QUOTE_USD = 12.34

#: Placeholder rows per file: big enough that a row count is a real comparison,
#: small enough that a day is a few kilobytes.
ROWS_PER_FILE = 1000

NAME = "stub"
VERSION = "1"


class StubDriver:
    """Answers like Databento, touches nothing outside the session root."""

    def __init__(self, root: Path | str, mode: str = "clean") -> None:
        if mode not in MODES:
            raise ValueError(f"unknown mode {mode!r}; expected one of {', '.join(MODES)}")
        self.root = Path(root)
        self.mode = mode
        self._submitted: dict[str, Any] = {}
        self._vendor_job_id = ""

    # -- the nine verbs -----------------------------------------------------

    def describe(self, **kwargs: Any) -> dict[str, Any]:
        """A plausible catalogue answer, so the stub exercises the verb.

        Named as a stub, not as the code the caller asked about: a fake that
        echoed a real description would be indistinguishable from a vendor's
        answer, which is the same reason its landings say `StubDatabentoDriver`.
        """
        dataset = str(kwargs.get("dataset") or "")
        return {
            "dataset": dataset,
            "described": True,
            "description": f"Stub dataset {dataset}",
            "venue": dataset.split(".", 1)[0],
            "venues": [dataset.split(".", 1)[0]],
            "publishers": [],
            "range": {"start": "2026-01-01T00:00:00.000000000Z",
                      "end": "2026-12-31T00:00:00.000000000Z"},
            "schemas": ["ohlcv-1d"],
            "fetched_at": "2026-01-01T00:00:00Z",
            "stub": True,
        }

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        symbols = kwargs.get("symbols") or []
        return {
            "symbols": list(symbols),
            "resolved": len(symbols),
            "stype_in": kwargs.get("stype_in"),
            "stub": True,
        }

    def cost(self, **kwargs: Any) -> dict[str, Any]:
        return {"usd": FAKE_QUOTE_USD, "detail": {"stub": True}}

    def submit(self, **kwargs: Any) -> dict[str, Any]:
        self._submitted = {k: v for k, v in kwargs.items() if k != "session_id"}
        seed = json.dumps(self._submitted, sort_keys=True, default=str).encode("utf-8")
        # Remembered, not returned-and-forgotten: the day files embed the job id,
        # so `_claimed()` and `fetch()` must hash the same text.
        self._vendor_job_id = "stub_" + hashlib.sha256(seed).hexdigest()[:12]
        return {"vendor_job_id": self._vendor_job_id, "state": "submitted"}

    def poll(self, **kwargs: Any) -> dict[str, Any]:
        # Instant and always done: the stub has no clock, so its result must not
        # depend on one.
        return {"state": "done"}

    def list_files(self, **kwargs: Any) -> dict[str, Any]:
        return {"files": [dict(entry) for entry in self._claimed()]}

    def fetch(self, **kwargs: Any) -> dict[str, Any]:
        dest = self._dest(kwargs["dest"])
        claimed = self._claimed()
        if self.mode == "nothing":
            published: list[dict[str, Any]] = []
        elif self.mode == "short":
            published = claimed[:1]
        else:
            published = claimed

        dest.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        for entry in published:
            path = dest / entry["name"]
            path.write_bytes(_content(entry["name"], self._vendor_job_id))
            written.append(entry["name"])
        return {"written": written}

    def convert(self, **kwargs: Any) -> dict[str, Any]:
        # Deliberately a no-op: see the module docstring. The real driver
        # replaces the `.dbn.zst` with a `.parquet`; the stub leaves the source
        # in place so `staged` still matches the vendor manifest exactly.
        dest = self._dest(kwargs["dest"])
        return {"staged": sorted(p.name for p in dest.iterdir() if p.is_file())}

    def verify(self, **kwargs: Any) -> dict[str, Any]:
        """Evidence, not a verdict. The app's `classify()` decides (§3.2)."""
        claimed = [dict(entry) for entry in self._claimed()]
        shipped = claimed[:1] if self.mode == "short" else claimed
        return {
            "claimed": [entry["name"] for entry in claimed],
            "manifest": shipped,
            "staged": staged_files(self._dest(kwargs["dest"])),
        }

    # -- where the bytes go -------------------------------------------------

    def _dest(self, dest: Any) -> Path:
        """A Job's `dest` is relative on purpose: it resolves under the request
        root the host opened, never against the process's working directory."""
        path = Path(str(dest))
        resolved = path if path.is_absolute() else self.root / path
        resolved = resolved.resolve()
        if self.root.resolve() != resolved and self.root.resolve() not in resolved.parents:
            raise ValueError(f"dest {dest!r} escapes the request root")
        return resolved

    # -- the shape this stub believes ---------------------------------------

    def _claimed(self) -> list[dict[str, Any]]:
        start = _as_date(self._submitted.get("start"))
        end = _as_date(self._submitted.get("end"))
        dataset = self._submitted.get("dataset") or "DATASET"
        encoding = self._submitted.get("encoding") or "dbn"
        compression = self._submitted.get("compression") or "zstd"
        suffix = f".{encoding}" + (".zst" if compression != "none" else "")

        days: list[date] = []
        if start and end and end > start:
            cursor = start
            while cursor < end:
                days.append(cursor)
                cursor += timedelta(days=1)
                if len(days) > 31:  # a bounded stub, not a bounded vendor
                    break
        if not days:
            days = [start or date(2026, 1, 1)]

        job_id = self._vendor_job_id
        return [
            {
                "name": f"{dataset}-{day.isoformat()}{suffix}",
                "sha256": hashlib.sha256(
                    _content(f"{dataset}-{day.isoformat()}{suffix}", job_id)
                ).hexdigest(),
                "rows": ROWS_PER_FILE,
            }
            for day in days
        ]


def _content(name: str, vendor_job_id: str) -> bytes:
    """Deterministic bytes that announce themselves as a stub."""
    header = f"STUB: not market data. file={name} job={vendor_job_id}\n".encode()
    return header + b"row\n" * ROWS_PER_FILE


def staged_files(dest: Path) -> dict[str, dict[str, Any]]:
    """What is actually on disk, hashed -- the same reading the verifier does."""
    staged: dict[str, dict[str, Any]] = {}
    if not dest.is_dir():
        return staged
    for path in sorted(dest.iterdir()):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        staged[path.name] = {
            "sha256": hashlib.sha256(payload).hexdigest(),
            "rows": payload.count(b"\n") - 1,  # the header line is not a row
        }
    return staged


def _as_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None
