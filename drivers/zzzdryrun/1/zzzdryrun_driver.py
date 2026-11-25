"""Zzzdryrun, as a driver artefact: a dry-run NBBO vendor behind the wire.

The vendor is a *dry run*: it has no upstream feed, no credential and no spend.
It fabricates a deterministic, plausible NBBO quote series locally, so the whole
chain -- submit, poll, manifest, download, convert, verify -- can be exercised
without a network. Every byte is derived from the job's symbol and window, so
two sessions over the same window land identical bytes and a digest comparison
means something.

The offering is deliberately narrow, and the two constraints are enforced
rather than documented:

* **one symbol per job** -- a daily NBBO file is keyed by the instrument, and a
  request naming two symbols cannot be answered as one series. Refused at
  `resolve`/`cost`/`submit`, before a job exists.
* **daily splits** -- the vendor publishes one file per calendar day in
  ``[start, end)``. Any other `split_duration` is refused at `submit`.

The chain of trust mirrors the real vendors':

1. `list_files` is the vendor's manifest: a bare name, a byte count, a sha256
   and the row count for every daily file.
2. `fetch` writes exactly those bytes, hashing each file as it lands and
   comparing the digest to the manifest entry. A mismatch is recorded, not
   silently accepted.
3. `convert` turns the vendor's CSV/NDJSON into parquet and counts the source
   rows *before* converting, so a lossy conversion is a row-count contradiction
   rather than a quietly smaller file. The source is removed afterwards, so no
   file exists that the manifest does not list.
4. `verify` reports in whichever vocabulary describes what is on disk: landed
   names when the landing is healthy, the vendor's names when an integrity
   check failed.

The wire has no "schema" for a dry-run catalogue, so `describe` answers for any
dataset code rather than pretending only some codes exist. Unknown codes are a
normal answer, not a broken driver.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_download_drivers.wire import BAD_REQUEST, WireError

NAME = "zzzdryrun"
VERSION = "1"

#: The session's local market clock. NBBO quotes are stamped in the venue's
#: timezone, so a reader can subtract them from a session open without guessing.
MARKET_TZ = ZoneInfo("America/New_York")

#: One quote per minute of the regular session, 09:30 through 15:59. Daily
#: files are the split the store holds; the row count is a real comparison.
ROWS_PER_DAY = 390
SESSION_OPEN_MINUTE = 9 * 60 + 30

#: A window is a human-scale request. Bounded so a typo cannot ask a dry run to
#: fabricate decades of files -- and so the *whole* job stays in memory.
MAX_DAYS = 3660

#: What this vendor can write. `parquet` is passed through; the text encodings
#: are converted by `convert`. Anything else is coerced to CSV rather than
#: refused: the dry-run vendor's format is its own business, and the result
#: records what was actually delivered.
TEXT_ENCODINGS = {"csv": "csv", "json": "ndjson", "jsonl": "ndjson", "ndjson": "ndjson"}
PARQUET = "parquet"

#: Compression is applied to the vendor's text files; parquet carries its own.
COMPRESSIONS = {"none": "none", "": "none", "uncompressed": "none",
                "zstd": "zstd", "zst": "zstd", "gzip": "gzip", "gz": "gzip"}

#: The parquet schema, in the order the CSV header carries it. The price and
#: size columns appear under both the short (`bid`) and long (`bid_price`)
#: names a reader may expect; they are the same values, and a superset costs a
#: dry run nothing while sparing every consumer a rename.
COLUMNS = (
    "date",
    "timestamp",
    "symbol",
    "bid",
    "ask",
    "bid_price",
    "ask_price",
    "bid_size",
    "ask_size",
    "bid_exchange",
    "ask_exchange",
)

PRICE_COLUMNS = ("bid", "ask", "bid_price", "ask_price")
SIZE_COLUMNS = ("bid_size", "ask_size")
EXCHANGES = ("Q", "N", "P", "K")

#: A bare instrument name, as the wire's manifest rule already requires for file
#: names. A symbol with a separator could not be a bare file name either.
SAFE_SYMBOL = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")


class ZzzdryrunDriver:
    """One session: one interpreted Job, one fabricated series, one directory."""

    def __init__(self, root: Path | str, options: dict[str, Any] | None = None) -> None:
        self.root = Path(root)
        self.options = dict(options or {})
        #: The submitted kwargs, remembered so `list_files` and `fetch` agree on
        #: the same bytes even if they are asked separately.
        self._submitted: dict[str, Any] = {}
        self._job_id: str | None = None
        #: The plan built at submit: one entry per published daily file.
        self._plan: list[dict[str, Any]] = []
        #: name -> manifest entry, straight from `list_files`.
        self._vendor_files: dict[str, dict[str, Any]] = {}
        #: name -> {"sha256", "vendor_sha256", "bytes", "verified"}.
        self._downloaded: dict[str, dict[str, Any]] = {}
        #: landed name -> {"sha256", "rows", "bytes", "source"}.
        self._converted: dict[str, dict[str, Any]] = {}
        self._stage = "downloaded"

    # -- the nine verbs -----------------------------------------------------

    def describe(self, **kwargs: Any) -> dict[str, Any]:
        """What the vendor says this dataset code is. Free, and not a Job.

        A dry-run catalogue has no real publishers, so the answer is the shape
        of one rather than a claim about a venue. `described` is always True:
        the vendor *can* answer for any code, and pretending otherwise would
        make a valid request look like a broken driver.
        """
        dataset = _need(kwargs, "dataset")
        prefix = dataset.split(".", 1)[0]
        return {
            "dataset": dataset,
            "described": True,
            "description": f"Zzzdryrun fabricated NBBO quotes ({dataset})",
            "venue": prefix,
            "venues": [prefix] if prefix else [],
            "publishers": [{"venue": prefix, "description": "deterministic dry-run feed"}],
            "range": {
                "start": "2000-01-01T00:00:00.000000000Z",
                "end": _now_date(),
            },
            "schemas": ["nbbo", "quotes"],
            "fetched_at": _now(),
            "dry_run": True,
        }

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        """The symbol is always resolvable; the constraint is that there is one.

        No window is carried on the resolve step (`wire.REQUIRED`), which is
        fine here: the dry-run vendor maps a bare symbol directly, so the check
        that matters is the one-symbol rule, made before anything is spent.
        """
        symbols = _one_symbol(kwargs)
        stype_in = _need(kwargs, "stype_in")
        return {
            "symbols": symbols,
            "resolved": 1,
            "stype_in": stype_in,
            "stype_out": "symbol",
            "dry_run": True,
            "note": "the dry-run vendor resolves every bare symbol",
        }

    def cost(self, **kwargs: Any) -> dict[str, Any]:
        """The vendor's price: zero. A dry run spends nothing by construction.

        The window is still validated, because a request that could never be
        submitted should fail at the free step rather than after a purchase.
        """
        _one_symbol(kwargs)
        days = _window(kwargs)
        return {
            "usd": 0.0,
            "detail": {
                "dry_run": True,
                "files": len(days),
                "rows": len(days) * ROWS_PER_DAY,
            },
        }

    def submit(self, **kwargs: Any) -> dict[str, Any]:
        """Fix the series: one symbol, daily files over ``[start, end)``."""
        symbol = _one_symbol(kwargs)[0]
        days = _window(kwargs)
        _need(kwargs, "dataset")
        _need(kwargs, "schema")
        _need(kwargs, "stype_in")
        split = _need(kwargs, "split_duration")
        if split.lower() not in {"day", "daily", "1d"}:
            raise WireError(
                BAD_REQUEST,
                f"this vendor publishes daily NBBO files, not split_duration={split!r}",
            )

        encoding = _encoding(kwargs.get("encoding"))
        compression = _compression(kwargs.get("compression"))

        self._submitted = {k: v for k, v in kwargs.items() if k != "session_id"}
        seed = json.dumps(
            {**self._submitted, "encoding": encoding, "compression": compression},
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        self._job_id = "zzz_" + hashlib.sha256(seed).hexdigest()[:12]

        self._plan = [
            self._plan_entry(symbol, day, encoding, compression) for day in days
        ]
        self._vendor_files = {
            entry["name"]: dict(entry) for entry in self._manifest_entries()
        }
        self._downloaded.clear()
        self._converted.clear()
        self._stage = "downloaded"
        return {
            "vendor_job_id": self._job_id,
            "state": "submitted",
            "symbol": symbol,
            "files": len(self._plan),
            "rows_per_file": ROWS_PER_DAY,
            "encoding": encoding,
            "compression": compression,
            "dry_run": True,
        }

    def poll(self, **kwargs: Any) -> dict[str, Any]:
        """Instant and always done: a dry run has no vendor queue or clock."""
        self._require_job(kwargs)
        return {
            "state": "done",
            "record_count": len(self._plan) * ROWS_PER_DAY,
        }

    def list_files(self, **kwargs: Any) -> dict[str, Any]:
        """The vendor's own manifest: name, bytes, sha256 and rows per file."""
        self._require_job(kwargs)
        return {"files": self._manifest_entries()}

    def fetch(self, **kwargs: Any) -> dict[str, Any]:
        """Write exactly the files the manifest hashes, one bare name at a time."""
        self._require_job(kwargs)
        dest = self._dest(kwargs.get("dest"))
        requested = kwargs.get("files")
        if not isinstance(requested, list):
            raise WireError(BAD_REQUEST, "files must be a list")

        dest.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        for entry in requested:
            name = str((entry or {}).get("name") or "")
            planned = self._planned(name)
            if planned is None:
                raise WireError(
                    BAD_REQUEST,
                    f"{name!r} is not a file this job's manifest lists",
                )
            payload = self._render(planned)
            path = dest / name
            path.write_bytes(payload)
            actual, size = _file_digest(path)
            expected = str((entry or {}).get("sha256") or planned["sha256"])
            self._downloaded[name] = {
                "sha256": actual,
                "vendor_sha256": expected,
                "bytes": size,
                "verified": bool(expected) and actual == expected,
            }
            written.append(name)
        return {"written": written}

    def convert(self, **kwargs: Any) -> dict[str, Any]:
        """Vendor text -> parquet, keeping the source's row count.

        Nothing is converted when a download failed its integrity check: bytes
        that are not the vendor's are not worth promoting, and leaving them in
        the vendor's vocabulary is what lets `verify` name the file that
        disagreed.
        """
        dest = self._dest(kwargs.get("dest"))
        target = _need(kwargs, "to")
        if target != PARQUET:
            raise WireError(BAD_REQUEST, f"this driver converts to parquet, not {target!r}")
        _need(kwargs, "from")

        if self._mismatches():
            self._stage = "downloaded"
            return {
                "staged": sorted(p.name for p in dest.iterdir() if p.is_file()),
                "skipped": "an integrity check failed; the vendor's bytes are left as they landed",
            }
        if not dest.is_dir():
            return {"staged": []}

        for source in sorted(p for p in dest.iterdir() if p.is_file()):
            if source.suffix == ".parquet":
                # The dry-run vendor can deliver parquet directly; there is
                # nothing to convert, but it still earns a digest and a count.
                digest, size = _file_digest(source)
                self._converted[source.name] = {
                    "sha256": digest,
                    "rows": _rows(source),
                    "bytes": size,
                    "source": source.name,
                }
                continue
            if not _is_text_vendor_file(source.name):
                continue
            table, rows = _read_vendor_text(source)
            landed = source.with_name(landed_name(source.name))
            _write_parquet(table, landed)
            digest, size = _file_digest(landed)
            self._converted[landed.name] = {
                "sha256": digest,
                "rows": rows,
                "bytes": size,
                "source": source.name,
                "source_sha256": (self._downloaded.get(source.name) or {}).get("sha256"),
            }
            # The landing is the parquet; leaving the source would be a file the
            # manifest does not list, which the shared verifier would rightly
            # call a contradiction.
            source.unlink()
        self._stage = "converted"
        return {"staged": sorted(p.name for p in dest.iterdir() if p.is_file())}

    def verify(self, **kwargs: Any) -> dict[str, Any]:
        """Evidence, in whichever vocabulary describes what is on disk now."""
        dest = self._dest(kwargs.get("dest"))
        staged = staged_files(dest)

        if self._mismatches():
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
                "dry_run": True,
            }

        manifest = []
        claimed: list[str] = []
        for source in sorted(self._vendor_files):
            landed = landed_name(source) if _is_text_vendor_file(source) else source
            claimed.append(landed)
            if landed not in staged:
                continue  # never landed: claimed and absent is the shortfall
            converted = self._converted.get(landed) or {}
            downloaded = self._downloaded.get(source) or {}
            manifest.append(
                {
                    "name": landed,
                    "sha256": converted.get("sha256") or downloaded.get("sha256"),
                    "rows": converted.get("rows"),
                }
            )
        return {
            "claimed": claimed,
            "manifest": manifest,
            "staged": staged,
            "symbol": self._submitted.get("symbols") or [],
            "dry_run": True,
        }

    # -- planning and fabricating -------------------------------------------

    def _plan_entry(
        self, symbol: str, day: date, encoding: str, compression: str
    ) -> dict[str, Any]:
        suffix = "" if encoding == PARQUET else _compression_suffix(compression)
        extension = "parquet" if encoding == PARQUET else "csv" if encoding == "csv" else "ndjson"
        name = f"{symbol}-{day.isoformat()}-nbbo.{extension}{suffix}"
        payload = self._render_for(symbol, day, encoding, compression)
        return {
            "name": name,
            "day": day,
            "symbol": symbol,
            "encoding": encoding,
            "compression": compression,
            "rows": ROWS_PER_DAY,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }

    def _manifest_entries(self) -> list[dict[str, Any]]:
        return [
            {
                "name": entry["name"],
                "sha256": entry["sha256"],
                "bytes": entry["bytes"],
                "size": entry["bytes"],
                "rows": entry["rows"],
            }
            for entry in self._plan
        ]

    def _planned(self, name: str) -> dict[str, Any] | None:
        for entry in self._plan:
            if entry["name"] == name:
                return entry
        return None

    def _render(self, planned: dict[str, Any]) -> bytes:
        return self._render_for(
            planned["symbol"],
            planned["day"],
            planned["encoding"],
            planned["compression"],
        )

    def _render_for(
        self, symbol: str, day: date, encoding: str, compression: str
    ) -> bytes:
        rows = _quotes(symbol, day)
        if encoding == PARQUET:
            return _parquet_bytes(rows, compression)

        if encoding == "ndjson":
            text = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
        else:
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=COLUMNS, lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            text = buffer.getvalue()

        payload = text.encode("utf-8")
        if compression == "zstd":
            import zstandard  # noqa: PLC0415 - only a compressed landing needs it

            payload = zstandard.ZstdCompressor().compress(payload)
        elif compression == "gzip":
            payload = gzip.compress(payload)
        return payload

    def _require_job(self, kwargs: dict[str, Any]) -> str:
        job_id = _need(kwargs, "vendor_job_id")
        if self._job_id is None:
            raise WireError(BAD_REQUEST, "no job has been submitted in this session")
        if job_id != self._job_id:
            raise WireError(
                BAD_REQUEST, f"unknown vendor_job_id {job_id!r} for this session"
            )
        return job_id

    def _dest(self, dest: Any) -> Path:
        """`dest` resolves under the session root; the host checked it first and
        this checks again from the inside, because a boundary one side enforces
        alone is a suggestion."""
        if not isinstance(dest, str) or not dest.strip():
            raise WireError(BAD_REQUEST, "dest must be a non-empty string")
        path = Path(dest)
        resolved = (path if path.is_absolute() else self.root / path).resolve()
        root = self.root.resolve()
        if resolved != root and root not in resolved.parents:
            raise WireError(BAD_REQUEST, f"dest {dest!r} escapes the session root")
        return resolved

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


# -- the fabricated series ---------------------------------------------------


def _quotes(symbol: str, day: date) -> list[dict[str, Any]]:
    """One deterministic minute-by-minute NBBO series for `symbol` on `day`.

    Seeded by symbol and date, so the same request two sessions apart lands
    identical bytes -- a digest comparison that means something. Bid and ask are
    kept on either side of a random walk, with a minimum spread, which is the
    shape a quote file has rather than a trade tape.
    """
    seed = int.from_bytes(
        hashlib.sha256(f"{symbol}|{day.isoformat()}".encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    mid = 20.0 + (seed % 48_000) / 100.0
    opened = datetime(day.year, day.month, day.day, tzinfo=MARKET_TZ) + timedelta(
        minutes=SESSION_OPEN_MINUTE
    )
    rows: list[dict[str, Any]] = []
    for minute in range(ROWS_PER_DAY):
        mid = max(1.0, mid + rng.gauss(0.0, mid * 0.0004))
        half_spread = round(0.005 + rng.random() * 0.02, 4)
        bid = round(mid - half_spread, 4)
        ask = round(mid + half_spread, 4)
        bid_size = rng.randint(1, 50) * 100
        ask_size = rng.randint(1, 50) * 100
        rows.append(
            {
                "date": day.isoformat(),
                "timestamp": (opened + timedelta(minutes=minute)).isoformat(),
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "bid_price": bid,
                "ask_price": ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "bid_exchange": rng.choice(EXCHANGES),
                "ask_exchange": rng.choice(EXCHANGES),
            }
        )
    return rows


# -- vendor text, parquet, and the evidence reader ---------------------------


def _read_vendor_text(path: Path) -> tuple[Any, int]:
    """Read a vendor CSV/NDJSON file into a parquet-ready table and its row count.

    The count is taken from the rows *read*, before they are written, so a
    conversion that drops records is a row-count contradiction. A header is a
    header, not a row.
    """
    import pyarrow as pa  # noqa: PLC0415 - only a conversion needs it

    payload = _decompress(path.name, path.read_bytes())
    text = payload.decode("utf-8")
    if path.name.endswith((".ndjson", ".jsonl", ".json")):
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
    else:
        reader = csv.DictReader(io.StringIO(text))
        records = list(reader)

    columns: dict[str, list[Any]] = {column: [] for column in COLUMNS}
    for record in records:
        for column in COLUMNS:
            value = record.get(column)
            if column in PRICE_COLUMNS:
                columns[column].append(float(value))
            elif column in SIZE_COLUMNS:
                columns[column].append(int(value))
            else:
                columns[column].append(None if value is None else str(value))
    table = pa.table(_columns_to_arrays(columns))
    return table, len(records)


def _parquet_bytes(rows: list[dict[str, Any]], compression: str) -> bytes:
    import pyarrow as pa  # noqa: PLC0415 - only a parquet landing needs it
    import pyarrow.parquet as pq  # noqa: PLC0415

    columns = {column: [row[column] for row in rows] for column in COLUMNS}
    table = pa.table(_columns_to_arrays(columns))
    sink = io.BytesIO()
    pq.write_table(table, sink, compression=_parquet_compression(compression))
    return sink.getvalue()


def _columns_to_arrays(columns: dict[str, list[Any]]) -> dict[str, Any]:
    """One parquet-ready arrow array per column, with an explicit type.

    Explicit types, not inference: an empty daily file must still describe a
    price column as a float rather than as null.
    """
    import pyarrow as pa  # noqa: PLC0415 - only a landing needs arrow

    types: dict[str, Any] = {}
    for column in COLUMNS:
        if column in PRICE_COLUMNS:
            types[column] = pa.float64()
        elif column in SIZE_COLUMNS:
            types[column] = pa.int64()
        else:
            types[column] = pa.string()
    return {
        column: pa.array(columns[column], type=types[column]) for column in COLUMNS
    }


def _write_parquet(table: Any, path: Path) -> None:
    import pyarrow.parquet as pq  # noqa: PLC0415 - only a conversion needs it

    pq.write_table(table, str(path))


def staged_files(dest: Path) -> dict[str, dict[str, Any]]:
    """What is on disk, hashed and counted, without slurping a parquet file."""
    staged: dict[str, dict[str, Any]] = {}
    if not dest.is_dir():
        return staged
    for path in sorted(dest.iterdir()):
        if not path.is_file():
            continue
        digest, size = _file_digest(path)
        staged[path.name] = {
            "sha256": digest,
            "rows": _rows(path),
            "bytes": size,
        }
    return staged


def _rows(path: Path) -> int | None:
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq  # noqa: PLC0415 - only a parquet landing needs it

        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    if _is_text_vendor_file(path.name):
        return None
    payload = path.read_bytes()
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return payload.count(b"\n") - 1  # the header line is not a row


def _file_digest(path: Path, chunk_size: int = 1 << 20) -> tuple[str, int]:
    """`(sha256, bytes)` streamed, so a large landing does not have to fit."""
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def landed_name(vendor_name: str) -> str:
    """`X.csv.zst` -> `X.parquet`: the name a converted file lands under.

    One function because the conversion that writes the file and the evidence
    that claims it must agree; a landed name that disagrees is a contradiction
    with no file to blame.
    """
    stem = vendor_name
    for suffix in (".zst", ".gz", ".ndjson", ".jsonl", ".json", ".csv"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return f"{stem}.parquet"


def _is_text_vendor_file(name: str) -> bool:
    return name.endswith((".csv", ".csv.zst", ".csv.gz", ".ndjson", ".ndjson.zst",
                          ".ndjson.gz", ".json", ".json.zst", ".json.gz",
                          ".jsonl", ".jsonl.zst", ".jsonl.gz"))


def _decompress(name: str, payload: bytes) -> bytes:
    if name.endswith(".zst"):
        import zstandard  # noqa: PLC0415 - only a compressed landing needs it

        return zstandard.ZstdDecompressor().decompress(payload)
    if name.endswith(".gz"):
        return gzip.decompress(payload)
    return payload


def _parquet_compression(compression: str) -> str:
    return {"zstd": "zstd", "gzip": "gzip"}.get(compression, "none")


# -- the request's shape -----------------------------------------------------


def _window(kwargs: dict[str, Any]) -> list[date]:
    start = _as_date(kwargs.get("start"), "start")
    end = _as_date(kwargs.get("end"), "end")
    if end <= start:
        raise WireError(
            BAD_REQUEST,
            f"end ({end.isoformat()}) must be after start ({start.isoformat()}); "
            "the window is [start, end)",
        )
    days = (end - start).days
    if days > MAX_DAYS:
        raise WireError(
            BAD_REQUEST, f"a window of {days} days exceeds the vendor's {MAX_DAYS}-day limit"
        )
    return [start + timedelta(days=offset) for offset in range(days)]


def _as_date(value: Any, name: str) -> date:
    if not isinstance(value, str) or len(value) < 10:
        raise WireError(BAD_REQUEST, f"{name} must be an ISO date")
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        raise WireError(BAD_REQUEST, f"{name} {value!r} is not an ISO date") from None


def _one_symbol(kwargs: dict[str, Any]) -> list[str]:
    symbols = kwargs.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise WireError(BAD_REQUEST, "symbols must be a non-empty list")
    if len(symbols) != 1:
        raise WireError(
            BAD_REQUEST,
            "this vendor publishes daily NBBO quotes for exactly one symbol; "
            f"got {len(symbols)}",
        )
    symbol = str(symbols[0])
    if not symbol or any(ch not in SAFE_SYMBOL for ch in symbol):
        raise WireError(
            BAD_REQUEST,
            f"symbol {symbol!r} is not a bare instrument name "
            "(letters, digits, '.', '_' or '-')",
        )
    return [symbol]


def _encoding(value: Any) -> str:
    """The encoding actually delivered, which is the vendor's own business.

    `parquet` is delivered as parquet; the text encodings are delivered as
    themselves; anything else is coerced to CSV, because a dry run has no
    upstream format to honour. The answer records what was written so the
    caller never has to infer it from the request.
    """
    key = str(value or "").strip().lower()
    if key == PARQUET:
        return PARQUET
    return TEXT_ENCODINGS.get(key, "csv")


def _compression(value: Any) -> str:
    key = str(value or "").strip().lower()
    if key in COMPRESSIONS:
        return COMPRESSIONS[key]
    raise WireError(
        BAD_REQUEST,
        f"unsupported compression {value!r}; this vendor writes "
        f"{', '.join(sorted({'none', 'zstd', 'gzip'}))}",
    )


def _compression_suffix(compression: str) -> str:
    return {"zstd": ".zst", "gzip": ".gz"}.get(compression, "")


def _need(kwargs: dict[str, Any], name: str) -> str:
    value = kwargs.get(name)
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, f"{name} must be a non-empty string")
    return value


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_date() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
