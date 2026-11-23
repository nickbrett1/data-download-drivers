"""Databento, as a driver artefact: the first real vendor behind the wire.

Everything here is the vendor's business, and nothing here decides policy. The
eight verbs answer what Databento says; whether the answer is good enough to
call a landing is `interpreter.classify`, in the trusted process.

The chain of trust, which is the whole reason this file is careful:

1. `list_files` is the **vendor's own manifest**: a name, a byte count, and a
   sha256 per file. It is the only statement about the bytes that comes from
   Databento rather than from us.
2. `fetch` downloads **one named file at a time**, so what lands on disk is the
   file the manifest hashes. (The whole-job path returns a single `.zip`, whose
   hash is the zip's, not the data's; `keep_zip=True` is refused by the SDK when
   a filename is given, which is why the choice is per-file rather than
   uncompressed.) Each download is hashed and compared to the manifest entry as
   it lands, because that is the last moment the vendor's claim can be checked
   against the vendor's bytes -- and because the SDK itself only *warns* on a
   checksum mismatch, having already written the file (see
   `_download_batch_file`: `warnings.warn(..., BentoWarning)`).
3. `convert` turns each `.dbn.zst` into `.parquet` and **counts the records it
   read before it converted**. That count is the expected row count in the
   evidence, so a conversion that silently drops records is a row-count
   contradiction rather than a quietly smaller file.
4. `verify` reports the landing in **landed** vocabulary -- parquet names, the
   parquet's own sha256, and the record count from before the conversion. The
   shared verifier compares `manifest` to what is on disk, so the manifest has
   to describe the files that are there. When a download failed its integrity
   check nothing is converted and the evidence stays in the *vendor's*
   vocabulary, so the contradiction names the vendor's file and the vendor's
   hash.

Two honest limits, written down rather than papered over:

* **`resolve` has no window.** The Job's `resolve` step carries a dataset, a
  symbol list and a `stype_in` -- no dates -- while Databento resolves symbology
  over a window. This calls it for a recent window, which catches an
  unresolvable symbol cheaply (the point of the step); the authoritative
  mapping for the requested window is what `submit_job` does anyway, because it
  is given `stype_in`.
* **The conversion is not free.** `to_parquet` decodes the file, and counting
  the records decodes it again, so a large day costs two passes. That is the
  price of a row count that comes from the vendor's bytes rather than from the
  file we just wrote, and the verb's 300 s deadline is the real bound.
"""

from __future__ import annotations

import hashlib
import os
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from data_download_drivers.wire import BAD_REQUEST, WireError

NAME = "databento"
VERSION = "1"

#: The vendor's credential. Read from the environment the container was given,
#: never from the request: a driver is code an agent wrote, and a secret passed
#: through the wire would be a secret in a log.
API_KEY_ENV = "DATABENTO_API_KEY"

#: How long a `poll` may sleep while the job is still running. The verb's
#: deadline is 30 s (`wire.DEADLINES`), so the pause stays well under it: a
#: sleep that races the deadline turns "still queued" into a `timeout` error.
POLL_SLEEP_CAP_S = 20

#: Databento's job states, mapped onto the four words the interpreter knows
#: (`queued` and `pending` mean ask again, `done` means stop, anything else is
#: terminal and therefore a failure worth naming).
PENDING_STATES = {"received": "queued", "queued": "queued", "processing": "running"}
DONE_STATE = "done"

#: A day is the split the store already holds, and the only one this driver
#: claims to verify: `split_duration` reaches the vendor as given, and the files
#: that come back are one per day per schema.
CONVERTIBLE = {".dbn", ".dbn.zst"}


class DatabentoDriver:
    """One session: one interpreted Job, one vendor conversation, one directory."""

    def __init__(self, root: Path | str, options: dict[str, Any] | None = None) -> None:
        self.root = Path(root)
        # `options` carries the app's `--mode` (clean/short/nothing), which is a
        # stub's vocabulary. A real vendor publishes what it publishes, so the
        # option is accepted and deliberately ignored rather than honoured with
        # a lie.
        self.options = dict(options or {})

        # The credential is checked where it is *used*, not here. The child
        # builds the driver once, before it reads a verb, and an exception in
        # the constructor kills that process: the app would be told "the driver
        # process exited" and the sentence naming the missing setting would be
        # only in the container log. A verb that fails can say what is wrong.
        self._key = (os.environ.get(API_KEY_ENV) or "").strip()
        self._db: Any = None
        self._client: Any = None

        self._job_id: str | None = None
        self._submitted: dict[str, Any] = {}
        #: name -> {"sha256": ..., "bytes": ...} straight from `list_files`.
        self._vendor_files: dict[str, dict[str, Any]] = {}
        #: name -> {"sha256": ..., "vendor_sha256": ..., "verified": bool}.
        self._downloaded: dict[str, dict[str, Any]] = {}
        #: landed name -> {"sha256": ..., "rows": ...} for the converted files.
        self._converted: dict[str, dict[str, Any]] = {}
        #: Where the landing is: "downloaded" (vendor bytes) or "converted".
        self._stage = "downloaded"

    # -- the vendor client ---------------------------------------------------

    def _connect(self) -> None:
        """Import the SDK and open the client, once, on first use."""
        if self._db is not None:
            return
        if not self._key:
            raise WireError(
                BAD_REQUEST,
                f"{API_KEY_ENV} is unset in the driver runtime; no Databento verb "
                "can be answered without it",
            )
        import databento as db  # noqa: PLC0415 - a missing SDK is a different error from a missing key

        self._db = db
        self._client = db.Historical(self._key)

    @property
    def client(self) -> Any:
        self._connect()
        return self._client

    @property
    def db(self) -> Any:
        self._connect()
        return self._db

    # -- the eight verbs -----------------------------------------------------

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        """Is the symbology resolvable at all? The cheapest failure there is.

        No window is available on this step (see the module docstring), so this
        asks for a recent one: a symbol that resolves nowhere in the last month
        will not resolve in the requested window either, and a symbol that does
        resolve is only *probably* right for the window -- which is why the
        answer says so instead of implying a guarantee.
        """
        dataset = _need(kwargs, "dataset")
        symbols = _symbols(kwargs)
        stype_in = _need(kwargs, "stype_in")
        end = date.today()
        start = end - timedelta(days=30)
        answer = self.client.symbology.resolve(
            dataset=dataset,
            symbols=symbols,
            stype_in=stype_in,
            stype_out="instrument_id",
            start_date=start,
            end_date=end,
        )
        resolved = answer.get("result") if isinstance(answer, dict) else None
        not_found = []
        mapped = 0
        for mapping in resolved or []:
            for symbol, intervals in (mapping.get("s") or {}).items():
                if any(_is_not_found(entry) for entry in intervals):
                    not_found.append(symbol)
                else:
                    mapped += 1
        if not_found:
            # A symbol that maps to nothing is not a vendor outage: it is a
            # request that cannot be answered, and submitting it would spend
            # money to learn that. Refused as a bad request, in the vendor's
            # own words.
            raise WireError(
                BAD_REQUEST,
                f"{dataset}: no instrument for {', '.join(sorted(not_found))} "
                f"as {stype_in}",
            )
        return {
            "symbols": symbols,
            "resolved": mapped,
            "stype_in": stype_in,
            "stype_out": "instrument_id",
            "window": {"start": start.isoformat(), "end": end.isoformat(), "note": "recent, not the request's"},
        }

    def cost(self, **kwargs: Any) -> dict[str, Any]:
        """The vendor's price for this exact window, in dollars.

        `metadata.get_cost` is free and authoritative; it is the number the app
        compares against the ceiling. Nothing here compares it to
        `estimate_usd` -- that comparison happened at submit, and repeating it
        would be a second opinion about one decision.
        """
        usd = self.client.metadata.get_cost(
            dataset=_need(kwargs, "dataset"),
            schema=_need(kwargs, "schema"),
            symbols=_symbols(kwargs),
            stype_in=_need(kwargs, "stype_in"),
            start=_need(kwargs, "start"),
            end=_need(kwargs, "end"),
        )
        return {
            "usd": float(usd),
            "detail": {
                "dataset": kwargs.get("dataset"),
                "schema": kwargs.get("schema"),
                "start": kwargs.get("start"),
                "end": kwargs.get("end"),
            },
        }

    def submit(self, **kwargs: Any) -> dict[str, Any]:
        """Start a batch job. The job id is the anchor for everything after it."""
        self._submitted = {k: v for k, v in kwargs.items() if k != "session_id"}
        job = self.client.batch.submit_job(
            dataset=_need(kwargs, "dataset"),
            schema=_need(kwargs, "schema"),
            symbols=_symbols(kwargs),
            stype_in=_need(kwargs, "stype_in"),
            # The mapping the vendor stores is what the files' symology is
            # written against, and `instrument_id` is the stable one: a raw
            # symbol can be reused by a different instrument later.
            stype_out="instrument_id",
            start=_need(kwargs, "start"),
            end=_need(kwargs, "end"),
            encoding=str(kwargs.get("encoding") or "dbn"),
            compression=str(kwargs.get("compression") or "zstd"),
            split_duration=str(kwargs.get("split_duration") or "day"),
            # One file per symbol would multiply the manifest by the symbol
            # list; the store wants a day, not a day per symbol.
            split_symbols=False,
        )
        job_id = str((job or {}).get("id") or "")
        if not job_id:
            raise WireError("vendor_error", "Databento accepted the job but returned no id")
        self._job_id = job_id
        return {"vendor_job_id": job_id, "state": str((job or {}).get("state") or "queued")}

    def poll(self, **kwargs: Any) -> dict[str, Any]:
        """Ask once, and wait a little if the answer is "not yet".

        The interpreter calls this in a loop with no sleep of its own, so the
        waiting belongs here -- and it is capped below the verb's deadline
        rather than trusting the Job's `interval_s` to be sane.
        """
        details = self.client.batch.get_job_details(job_id=_need(kwargs, "vendor_job_id"))
        raw = str((details or {}).get("state") or "").lower()
        state = PENDING_STATES.get(raw, raw)
        if state != DONE_STATE and state in {"queued", "running"}:
            pause = min(float(kwargs.get("interval_s") or 30), POLL_SLEEP_CAP_S)
            if pause > 0:
                time.sleep(pause)
        return {
            "state": state,
            "raw_state": raw,
            # The vendor's own count of what the job produced. Not used to judge
            # the landing (a per-file split is not available), but recorded so an
            # operator can compare it with the files.
            "record_count": (details or {}).get("record_count"),
            "billable_size": (details or {}).get("billable_size"),
        }

    def list_files(self, **kwargs: Any) -> dict[str, Any]:
        """The vendor's manifest: what it says it published, byte for byte."""
        files = self.client.batch.list_files(job_id=_need(kwargs, "vendor_job_id"))
        entries: list[dict[str, Any]] = []
        for raw in files or []:
            name = str((raw or {}).get("filename") or "")
            if not name:
                raise WireError("vendor_error", "Databento listed a file with no filename")
            entry = {
                "name": name,
                # The API's `hash` is `<algo>:<hex>` today and a bare digest in
                # older responses; only sha256 is usable, and a hash we do not
                # understand must read as "no hash" rather than as a mismatch we
                # cannot explain.
                "sha256": _digest((raw or {}).get("hash")),
                "bytes": int((raw or {}).get("size") or 0),
            }
            self._vendor_files[name] = entry
            entries.append(entry)
        return {"files": entries}

    def fetch(self, **kwargs: Any) -> dict[str, Any]:
        """Download exactly the files the manifest named, and check each one.

        Two SDK behaviours are handled rather than trusted:

        * a per-file download lands in `output_dir/<job_id>/<name>` (the client
          appends the job id), so each file is moved up into `dest` -- the
          directory every later verb and the app's own landing reader look in.
          A rename inside one filesystem, so nothing is copied;
        * `keep_zip` is refused when a filename is given, and is unnecessary
          anyway: the file is streamed to its own name, `.dbn.zst` and all, with
          no unzipping step in the way.
        """
        job_id = _need(kwargs, "vendor_job_id")
        dest = self._dest(_literal(kwargs.get("dest"), "dest"))
        dest.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        for entry in kwargs.get("files") or []:
            name = str((entry or {}).get("name") or "")
            if not name:
                continue
            self.client.batch.download(
                job_id=job_id,
                output_dir=str(dest),
                filename_to_download=name,
            )
            path = _hoist(dest, job_id, name)
            if not path.is_file():
                # Downloaded under a different name: the vendor's vocabulary and
                # ours have diverged, and guessing which file is which is how a
                # wrong file gets verified against a right hash.
                raise WireError(
                    "vendor_error",
                    f"Databento published {name!r} but the download did not create it",
                )
            payload = path.read_bytes()
            actual = hashlib.sha256(payload).hexdigest()
            expected = str(
                (entry or {}).get("sha256")
                or (self._vendor_files.get(name) or {}).get("sha256")
                or ""
            )
            self._downloaded[name] = {
                "sha256": actual,
                "vendor_sha256": expected,
                "bytes": len(payload),
                "verified": bool(expected) and actual == expected,
            }
            written.append(name)
        return {"written": written}

    def convert(self, **kwargs: Any) -> dict[str, Any]:
        """`.dbn.zst` -> `.parquet`, keeping the record count from the source.

        Nothing is converted when a download failed its integrity check: bytes
        that are not the vendor's are not worth converting, and keeping them in
        the vendor's vocabulary is what lets `verify` name the file that failed.
        """
        dest = self._dest(_literal(kwargs.get("dest"), "dest"))
        target = str(kwargs.get("to") or "")
        if target != "parquet":
            raise WireError(BAD_REQUEST, f"this driver converts to parquet, not {target!r}")

        if self._mismatches():
            self._stage = "downloaded"
            return {
                "staged": sorted(p.name for p in dest.iterdir() if p.is_file()),
                "skipped": "an integrity check failed; the vendor's bytes are left as they landed",
            }

        for source in sorted(p for p in dest.iterdir() if p.is_file()):
            if not _is_convertible(source.name):
                continue
            landed = source.with_name(landed_name(source.name))
            rows = self._count_records(source)
            self.db.DBNStore.from_file(source).to_parquet(landed)
            payload = landed.read_bytes()
            self._converted[landed.name] = {
                "sha256": hashlib.sha256(payload).hexdigest(),
                "rows": rows,
                "bytes": len(payload),
                "source": source.name,
                # Provenance: this parquet describes these vendor bytes, and the
                # count next to it is the count read from them.
                "source_sha256": (self._downloaded.get(source.name) or {}).get("sha256"),
            }
            # The landing is the parquet. Leaving the source behind would be a
            # file the manifest does not list, which the shared verifier would
            # rightly call a contradiction.
            source.unlink()
        self._stage = "converted"
        return {"staged": sorted(p.name for p in dest.iterdir() if p.is_file())}

    def verify(self, **kwargs: Any) -> dict[str, Any]:
        """Evidence, in whichever vocabulary describes what is on disk now."""
        dest = self._dest(_literal(kwargs.get("dest"), "dest"))
        staged = staged_files(dest)

        if self._mismatches():
            # The vendor's bytes are not what the vendor said. Report in the
            # vendor's names, with the vendor's hashes, so the contradiction
            # names the file that disagreed rather than a parquet derived from it.
            manifest = [
                {
                    "name": name,
                    "sha256": (self._vendor_files.get(name) or {}).get("sha256"),
                    "rows": None,
                }
                for name in sorted(self._downloaded)
            ]
            return {
                "claimed": sorted(self._downloaded),
                "manifest": manifest,
                "staged": staged,
                "integrity": self._mismatches(),
            }

        manifest = [
            {"name": name, "sha256": entry["sha256"], "rows": entry["rows"]}
            for name, entry in sorted(self._converted.items())
        ]
        # Scope, in landed names: a conversion that dropped a file leaves its
        # parquet absent from `manifest` while still being claimed, which is a
        # shortfall -- the vendor did publish it, we just failed to land it.
        claimed = sorted(entry["name"] for entry in self._claimed_landed())
        return {"claimed": claimed, "manifest": manifest, "staged": staged}

    # -- internals -----------------------------------------------------------

    def _claimed_landed(self) -> list[dict[str, Any]]:
        """One landed name per vendor file the job listed, in landed vocabulary."""
        return [
            {"name": landed_name(name), "source": name}
            for name in sorted(self._vendor_files)
        ]

    def _mismatches(self) -> list[dict[str, Any]]:
        return [
            {
                "file": name,
                "vendor_sha256": entry["vendor_sha256"],
                "file_sha256": entry["sha256"],
                "bytes": entry["bytes"],
                **({} if entry["vendor_sha256"] else {"problem": "the vendor published no hash"}),
            }
            for name, entry in sorted(self._downloaded.items())
            if not entry["verified"]
        ]

    def _count_records(self, source: Path) -> int:
        """How many records the vendor's file holds, decoded once.

        Iterated rather than read from a header: a DBN header carries a window
        and a limit, not a record count, and a count we did not read from the
        bytes would be a guess dressed as a check.
        """
        store = self.db.DBNStore.from_file(source)
        return sum(1 for _ in store)

    def _dest(self, dest: Any) -> Path:
        """`dest` is relative to the session root; the host has already refused
        an absolute one or one that climbs out, and this refuses again from the
        inside because a boundary checked by one side only is a suggestion."""
        path = Path(str(dest))
        resolved = (path if path.is_absolute() else self.root / path).resolve()
        root = self.root.resolve()
        if resolved != root and root not in resolved.parents:
            raise WireError(BAD_REQUEST, f"dest {dest!r} escapes the session root")
        return resolved


def staged_files(dest: Path) -> dict[str, dict[str, Any]]:
    """What is on disk, hashed, with the row count each file's format allows.

    The same reading the shared verifier does, with one difference that a real
    landing forces: a parquet file's rows come from its footer, not from
    counting newlines. A binary file that is not parquet reports `rows: None`
    rather than a number derived from byte counts.
    """
    staged: dict[str, dict[str, Any]] = {}
    if not dest.is_dir():
        return staged
    for path in sorted(dest.iterdir()):
        if not path.is_file():
            continue
        payload = path.read_bytes()
        staged[path.name] = {
            "sha256": hashlib.sha256(payload).hexdigest(),
            "rows": _rows(path, payload),
            "bytes": len(payload),
        }
    return staged


def _rows(path: Path, payload: bytes) -> int | None:
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq  # noqa: PLC0415 - only a parquet landing needs it

        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    if _is_convertible(path.name):
        # A DBN file is binary; the footer is the only cheap count and the
        # driver reads it when it converts. Counting newlines here would be a
        # number that means nothing.
        return None
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return payload.count(b"\n") - 1  # the header line is not a row


def landed_name(vendor_name: str) -> str:
    """`X.dbn.zst` -> `X.parquet`: the name the landing has after conversion.

    One function because two places derive it -- the conversion that writes the
    file and the evidence that claims it -- and a landing whose claimed name is
    not its written name is a contradiction with no file to blame.
    """
    stem = vendor_name
    for suffix in (".zst", ".dbn"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return f"{stem}.parquet"


def _is_convertible(name: str) -> bool:
    return any(name.endswith(suffix) for suffix in CONVERTIBLE)


def _is_not_found(entry: Any) -> bool:
    """Does this symbology interval say "no instrument"?"""
    if isinstance(entry, dict):
        return bool(entry.get("not_found")) or entry.get("s") in (None, "", 0)
    return entry in (None, "", 0)


def _digest(raw: Any) -> str:
    """`sha256:<hex>` or `<hex>` -> `<hex>`; anything else -> "" (unknown)."""
    algo, _, hexdigest = str(raw or "").partition(":")
    if not hexdigest:  # a bare digest: assume the only algorithm Databento uses
        algo, hexdigest = "sha256", algo
    return hexdigest.strip().lower() if algo.strip().lower() == "sha256" else ""


def _hoist(dest: Path, job_id: str, name: str) -> Path:
    """Move a downloaded file out of the SDK's `<dest>/<job_id>/` into `dest`.

    The client appends the job id to `output_dir`; the rest of this driver (and
    the app, which reads the landing directory and not its children) expects the
    files directly in `dest`.
    """
    landed = dest / name
    staged = dest / job_id / name
    if staged.is_file():
        landed.unlink(missing_ok=True)
        staged.replace(landed)
        try:
            (dest / job_id).rmdir()
        except OSError:
            pass  # another file for this job is still there
    return landed


def _need(kwargs: dict[str, Any], name: str) -> str:
    """A required non-empty string. The host checked presence; this checks shape.

    Both, because the host's check is shallow on purpose ("a driver is the
    authority on its own kwargs") and a value that reached a vendor call as
    `None` would be spent before anyone noticed.
    """
    value = kwargs.get(name)
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, f"{name} must be a non-empty string")
    return value


def _symbols(kwargs: dict[str, Any]) -> list[str]:
    value = kwargs.get("symbols")
    if not isinstance(value, list) or not value:
        raise WireError(BAD_REQUEST, "symbols must be a non-empty list")
    return [str(symbol) for symbol in value]


def _literal(value: Any, name: str) -> str:
    """`dest` and `to`, which are strings that arrive through kwargs.get()."""
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, f"{name} must be a non-empty string")
    return value
