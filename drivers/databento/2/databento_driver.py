"""Databento, as a driver artefact: the market_data_window vendor.

Everything here is the vendor's business, and nothing here decides policy. The
nine verbs answer what Databento says; whether the answer is good enough to
call a landing is `interpreter.classify`, in the trusted process.

`market_data_window` asks for a symbol-scoped window of records, split per day,
delivered in one of three encodings (dbn, csv, json) and always converted to
parquet. Databento answers exactly that, so this driver's job is to honour the
whole of the plan type without inventing a second vocabulary for it:

* **Every encoding the plan type offers is carried across.** DBN is decoded by
  the vendor SDK; CSV and JSON are parsed by pyarrow. In each case the record
  count is read from the vendor's own bytes *before* the parquet is written, so
  a lossy conversion is a row-count contradiction rather than a quietly smaller
  file. An encoding outside that set is refused at `submit`, before any spend.
* **A request that can never be answered is refused for free.** A schema the
  dataset does not carry, a window that starts before the dataset's own first
  day, and an `end` that is not after its `start` are all refused at `submit`
  from the vendor's catalogue, not discovered after a purchase. A catalogue that
  cannot answer is not a refusal: an outage must not look like a bad request.
* **Only the data files are converted.** The batch publishes `manifest.json`,
  `condition.json` and `metadata.json` beside the data; those land as
  themselves at every encoding, including `json`, so the evidence never claims
  a `manifest.parquet` that cannot exist.

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
   checksum mismatch, having already written the file.
3. `convert` turns each data file into `.parquet` and **counts the records it
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

The honest limits, written down rather than papered over:

* **`resolve` has no window from the binder.** The Job's `resolve` step carries
  a dataset, a symbol list and a `stype_in` -- no dates -- while Databento
  resolves symbology over a window. This uses the request's window when one is
  passed and otherwise a recent one, which catches an unresolvable symbol
  cheaply (the point of the step); the authoritative mapping for the requested
  window is what `submit_job` does anyway, because it is given `stype_in`.
* **The conversion is not free.** DBN is decoded twice (`to_parquet` and the
  record count), so a large day costs two passes. That is the price of a row
  count that comes from the vendor's bytes rather than from the file we just
  wrote, and the verb's 300 s deadline is the real bound.
"""

from __future__ import annotations

import hashlib
import os
import re
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from data_download_drivers.wire import BAD_REQUEST, WireError

NAME = "databento"
VERSION = "2"

#: The vendor's credential. Read from the environment the container was given,
#: never from the request: a driver is code an agent wrote, and a secret passed
#: through the wire would be a secret in a log.
API_KEY_ENV = "DATABENTO_API_KEY"

#: How long a `poll` should spend, in total, before it answers "not yet". The
#: verb's deadline is 30 s (`wire.DEADLINES`), so this is the budget for the
#: *whole* call -- the vendor's own `get_job_details` can take seconds, and a
#: fixed sleep on top of a slow call is how "still queued" became
#: "poll exceeded 30s; the driver was killed" (seen live, 2026-11-23).
POLL_BUDGET_S = 20

#: Databento's job states, mapped onto the four words the interpreter knows
#: (`queued` and `pending` mean ask again, `done` means stop, anything else is
#: terminal and therefore a failure worth naming).
PENDING_STATES = {"received": "queued", "queued": "queued", "processing": "running"}
DONE_STATE = "done"

#: The one split the plan type offers and the split the store already holds:
#: one data file per day per schema. Anything else is refused for free at
#: `submit` rather than discovered after the vendor has been paid.
SUPPORTED_SPLITS = {"day", "daily", "1d"}

#: The plan type's encodings, all of which this driver carries to parquet. The
#: default is DBN because the vendor SDK decodes it natively and encodes every
#: record type exactly; CSV and JSON are parsed generically, which is what
#: carries their record count across without a second record definition here.
SUPPORTED_ENCODINGS = ("dbn", "csv", "json")

#: The plan type's compressions. Databento writes `.zst` for `zstd` and nothing
#: for `none`; anything else has no decoder here and is refused at `submit`.
SUPPORTED_COMPRESSIONS = {"zstd", "none"}

#: The batch's own sidecars, published beside every dataset at every encoding.
#: They are the vendor describing the job, not the data, so they are never
#: converted -- a `manifest.json` promoted to `manifest.parquet` would claim a
#: file that can never exist, and the shared verifier reads that as a shortfall.
SIDECARS = {"manifest.json", "condition.json", "metadata.json"}

#: The suffix each encoding's data files carry (before the compression suffix).
ENCODING_SUFFIXES = {"dbn": (".dbn",), "csv": (".csv",), "json": (".json",)}


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
        #: What the Job asked the vendor to write. Remembered because `convert`
        #: is told only the encoding, and `verify` has to name the landed files
        #: in the same vocabulary the conversion used.
        self._encoding = SUPPORTED_ENCODINGS[0]
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

    # -- the nine verbs -----------------------------------------------------

    def describe(self, **kwargs: Any) -> dict[str, Any]:
        """What the vendor says this dataset code *is*. Free, and not a Job.

        A dataset code is the vendor's vocabulary, and `GLBX.MDP3` is a name
        only a market-data reader can expand. Databento publishes the expansion
        itself, in three answers:

        * `metadata.list_publishers()` -- `{dataset, venue, description}`, one
          row per publisher;
        * `metadata.get_dataset_range(dataset)` -- when data starts, overall and
          per schema;
        * `metadata.list_schemas(dataset)` -- which record types exist at all.

        The trap is that **descriptions are per publisher, and a code can have
        many**: `DBEQ.BASIC` has six, one per venue ("DBEQ Basic - NYSE Texas",
        "… - IEX", and so on), and picking the first would label a consolidated
        dataset after one exchange. The dataset-level name is the row whose
        venue equals the code's own prefix (`GLBX.MDP3` -> GLBX,
        `DBEQ.BASIC` -> DBEQ, `EQUS.MINI` -> EQUS); the rest are the venues
        inside it, which are returned as `venues` rather than thrown away.

        Answering "I do not know this code" is a real answer, not a failure:
        `described` is False and the client shows the code with no claim
        attached. A `WireError` here would make an unknown code look like a
        broken driver.
        """
        dataset = _need(kwargs, "dataset")
        client = self.client

        publishers = self._publishers(dataset)
        range_: dict[str, Any] = {}
        unavailable = ""
        try:
            range_ = client.metadata.get_dataset_range(dataset) or {}
        except Exception as exc:  # noqa: BLE001 - the catalogue is a courtesy
            # An unknown dataset is an error here and a normal answer below;
            # anything else is the vendor being unavailable for a fact we can
            # live without, so it is reported rather than raised. The note
            # travels in the answer, because "no range" and "could not ask"
            # are different facts about a code.
            unavailable = str(exc)
        if not isinstance(range_, dict):
            # An SDK that answers a list (or anything else) is answering
            # "unknown", not a range; `.get` on it must not become the
            # AttributeError that turns a catalogue wobble into a driver_error.
            range_ = {}
            unavailable = unavailable or "the catalogue did not answer a range"
        try:
            schemas = list(client.metadata.list_schemas(dataset) or [])
        except Exception:  # noqa: BLE001 - see above
            schemas = []

        prefix = dataset.split(".", 1)[0]
        chosen = next(
            (row for row in publishers if str(row.get("venue")) == prefix), None
        ) or (publishers[0] if publishers else None)

        return {
            "dataset": dataset,
            "described": chosen is not None,
            "description": str((chosen or {}).get("description") or ""),
            "venue": str((chosen or {}).get("venue") or ""),
            "venues": sorted(
                {str(row.get("venue")) for row in publishers if row.get("venue")}
            ),
            "publishers": [
                {
                    "venue": str(row.get("venue") or ""),
                    "description": str(row.get("description") or ""),
                }
                for row in publishers
            ],
            "range": (
                {"start": range_.get("start"), "end": range_.get("end")}
                if not unavailable
                else {"start": None, "end": None, "unavailable": unavailable}
            ),
            "schemas": schemas,
            "fetched_at": _now(),
        }

    def _publishers(self, dataset: str) -> list[dict[str, Any]]:
        """The vendor's publisher rows for one code, or `[]`.

        `list_publishers` answers for *every* dataset (155 rows live), so the
        filter is ours. It is fetched per call rather than cached across
        sessions because a driver session is one Job and a licence to keep state
        is a licence to serve a stale catalogue.
        """
        try:
            rows = self.client.metadata.list_publishers() or []
        except Exception:  # noqa: BLE001 - an unknown code answers "undescribed"
            return []
        return [
            row
            for row in rows
            if isinstance(row, dict) and str(row.get("dataset") or "") == dataset
        ]

    def resolve(self, **kwargs: Any) -> dict[str, Any]:
        """Is the symbology resolvable at all? The cheapest failure there is.

        The binder's `resolve` step has no window (see the module docstring), so
        the fallback is a recent one: a symbol that resolves nowhere in the last
        month will not resolve in the requested window either. When the caller
        does pass a window -- `diff`ing a Job by hand, or a future binder that
        carries it -- the request's own dates are used, and the answer says
        which window it actually asked about rather than leaving that to be
        inferred.
        """
        dataset = _need(kwargs, "dataset")
        symbols = _symbols(kwargs)
        stype_in = _need(kwargs, "stype_in")
        requested_start = _as_date(kwargs.get("start"))
        requested_end = _as_date(kwargs.get("end"))
        if (
            requested_start is not None
            and requested_end is not None
            and requested_end > requested_start
        ):
            start, end = requested_start, requested_end
            note = "the request's"
        else:
            end = date.today()
            start = end - timedelta(days=30)
            note = "recent, not the request's"
        answer = self.client.symbology.resolve(
            dataset=dataset,
            symbols=symbols,
            stype_in=stype_in,
            stype_out="instrument_id",
            start_date=start,
            end_date=end,
        )
        # The shape, as the API answers it: `result` is {symbol: [{d0, d1, s}]},
        # and `not_found` is a top-level list. `s` is the instrument id; an
        # interval with an empty `s` is a gap in the mapping, not a symbol.
        answer = answer if isinstance(answer, dict) else {}
        resolved = answer.get("result")
        resolved = resolved if isinstance(resolved, dict) else {}
        not_found = [str(symbol) for symbol in (answer.get("not_found") or [])]
        mapped = 0
        for symbol, intervals in resolved.items():
            usable = [entry for entry in (intervals or []) if not _is_not_found(entry)]
            if usable:
                mapped += 1
            elif symbol not in not_found:
                not_found.append(str(symbol))
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
            "partial": list(answer.get("partial") or []),
            "window": {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "note": note,
            },
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
        """Start a batch job, or refuse for free what the vendor would refuse.

        The plan type's parameter space is narrow on purpose: three encodings,
        two compressions, one split. Every one of those is checked here, before
        `submit_job` spends anything, and so is a window that cannot exist. The
        two catalogue checks are best-effort: a catalogue that cannot answer is
        an outage, not a bad request, so it falls through to the vendor rather
        than turning "we could not ask" into "you asked for the impossible".
        """
        dataset = _need(kwargs, "dataset")
        schema = _need(kwargs, "schema")
        symbols = _symbols(kwargs)
        stype_in = _need(kwargs, "stype_in")
        start = _need(kwargs, "start")
        end = _need(kwargs, "end")
        encoding = _normalise_encoding(kwargs.get("encoding"))
        compression = _normalise_compression(kwargs.get("compression"))
        split = str(kwargs.get("split_duration") or "day").strip().lower()
        if split not in SUPPORTED_SPLITS:
            raise WireError(
                BAD_REQUEST,
                f"this plan writes one file per day; split_duration {split!r} is "
                f"not one of {', '.join(sorted(SUPPORTED_SPLITS))}",
            )
        _window(start, end)
        self._refuse_absent_schema(dataset, schema)
        self._refuse_before_range(dataset, start)

        self._submitted = {k: v for k, v in kwargs.items() if k != "session_id"}
        self._encoding = encoding
        job = self.client.batch.submit_job(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            stype_in=stype_in,
            # The mapping the vendor stores is what the files' symbology is
            # written against, and `instrument_id` is the stable one: a raw
            # symbol can be reused by a different instrument later.
            stype_out="instrument_id",
            start=start,
            end=end,
            encoding=encoding,
            compression=compression,
            # Normalised to the one word the vendor speaks, even though the plan
            # type accepts its synonyms (`daily`, `1d`) as the same request.
            split_duration="day",
            # One file per symbol would multiply the manifest by the symbol
            # list; the store wants a day, not a day per symbol.
            split_symbols=False,
        )
        job_id = str((job or {}).get("id") or "")
        if not job_id:
            raise WireError("vendor_error", "Databento accepted the job but returned no id")
        self._job_id = job_id
        return {
            "vendor_job_id": job_id,
            "state": str((job or {}).get("state") or "queued"),
            "encoding": encoding,
            "compression": compression,
        }

    def _refuse_absent_schema(self, dataset: str, schema: str) -> None:
        """Refuse a schema this dataset does not carry, from the catalogue.

        `list_schemas` is a free read. If it answers and the schema is absent,
        the submitted job could only ever produce an empty landing, so saying so
        now costs nothing; if it cannot answer, the fall-through is the vendor's
        own refusal at submit.
        """
        try:
            schemas = self.client.metadata.list_schemas(dataset)
        except Exception:  # noqa: BLE001 - a catalogue outage is not a refusal
            return
        known = {str(name).lower() for name in schemas or []}
        if known and schema.lower() not in known:
            raise WireError(
                BAD_REQUEST,
                f"{dataset} carries no schema {schema!r}; it publishes "
                + ", ".join(sorted(known)),
            )

    def _refuse_before_range(self, dataset: str, start: str) -> None:
        """Refuse a window that starts before the dataset exists.

        Data does not move backwards, and the vendor's own range is the only
        authority on when a dataset begins. Only the *start* is compared: asking
        for a future end is a legitimate request for data that will exist, which
        is what a scheduled download is.
        """
        try:
            answer = self.client.metadata.get_dataset_range(dataset) or {}
        except Exception:  # noqa: BLE001 - see `_refuse_absent_schema`
            return
        first = str((answer or {}).get("start") or "")[:10]
        if not first:
            return
        try:
            first_day = date.fromisoformat(first)
        except ValueError:
            return  # a range we cannot read is not evidence against the request
        if _instant(start).date() < first_day:
            raise WireError(
                BAD_REQUEST,
                f"{dataset} starts on {first_day.isoformat()}; the request's "
                f"start is {_instant(start).date().isoformat()}",
            )

    def poll(self, **kwargs: Any) -> dict[str, Any]:
        """Ask once, and wait a little if the answer is "not yet".

        The interpreter calls this in a loop with no sleep of its own, so the
        waiting belongs here -- and it is capped below the verb's deadline
        rather than trusting the Job's `interval_s` to be sane.
        """
        started = time.monotonic()
        details = self.client.batch.get_job_details(job_id=_need(kwargs, "vendor_job_id"))
        raw = str((details or {}).get("state") or "").lower()
        state = PENDING_STATES.get(raw, raw)
        if state != DONE_STATE and state in {"queued", "running"}:
            # What is left of the budget after asking, never the interval alone:
            # the pause is what makes the verb's own duration unpredictable.
            pause = min(_seconds(kwargs.get("interval_s"), 30.0),
                        max(0.0, POLL_BUDGET_S - (time.monotonic() - started)))
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
        # The job id becomes a path component inside `_hoist`; a vendor id with
        # a separator in it would write outside the directory the host opened.
        _bare_name(job_id, "vendor_job_id")
        dest = self._dest(_literal(kwargs.get("dest"), "dest"))
        dest.mkdir(parents=True, exist_ok=True)
        written: list[str] = []
        for entry in kwargs.get("files") or []:
            name = str((entry or {}).get("name") or "")
            if not name:
                continue
            # A manifest entry is a name inside `dest`, never a path: the host
            # enforces this on the wire, and the driver enforces it again here
            # because a boundary checked by one side only is a suggestion.
            _bare_name(name, "file name")
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
            # Hashed in chunks, not read whole: a day of `mbo` is gigabytes and
            # this process has rlimits. The digest is over the same bytes the
            # SDK wrote, so the manifest comparison is unchanged.
            actual, size = _file_digest(path)
            expected = str(
                (entry or {}).get("sha256")
                or (self._vendor_files.get(name) or {}).get("sha256")
                or ""
            )
            self._downloaded[name] = {
                "sha256": actual,
                "vendor_sha256": expected,
                "bytes": size,
                "verified": bool(expected) and actual == expected,
            }
            written.append(name)
        return {"written": written}

    def convert(self, **kwargs: Any) -> dict[str, Any]:
        """The vendor's data file -> `.parquet`, keeping the source's record count.

        DBN is decoded by the vendor SDK, CSV and JSON by pyarrow; either way the
        count is read from the vendor's bytes before the parquet is written, so a
        lossy conversion is a row-count contradiction rather than a quietly
        smaller file. The batch's own sidecars are left as themselves.

        Nothing is converted when a download failed its integrity check: bytes
        that are not the vendor's are not worth converting, and keeping them in
        the vendor's vocabulary is what lets `verify` name the file that failed.
        """
        dest = self._dest(_literal(kwargs.get("dest"), "dest"))
        source_format = str(kwargs.get("from") or self._encoding)
        target = str(kwargs.get("to") or "")
        if target != "parquet":
            raise WireError(BAD_REQUEST, f"this driver converts to parquet, not {target!r}")
        if source_format not in SUPPORTED_ENCODINGS:
            raise WireError(
                BAD_REQUEST,
                f"this driver converts {', '.join(SUPPORTED_ENCODINGS)} to parquet, "
                f"not {source_format!r}; nothing on disk was changed",
            )
        # The conversion is the authority on what the landing looks like, so the
        # evidence below names files in exactly the vocabulary this wrote.
        self._encoding = source_format

        if self._mismatches():
            self._stage = "downloaded"
            return {
                "staged": sorted(p.name for p in dest.iterdir() if p.is_file()),
                "skipped": "an integrity check failed; the vendor's bytes are left as they landed",
            }
        if not dest.is_dir():
            return {"staged": []}

        for source in sorted(p for p in dest.iterdir() if p.is_file()):
            if not _is_data_file(source.name, source_format):
                continue  # a sidecar, or a file for another encoding
            landed = source.with_name(landed_name(source.name))
            if source_format == "dbn":
                rows = self._count_records(source)
                self.db.DBNStore.from_file(source).to_parquet(landed)
            else:
                rows = _write_text_parquet(source, landed)
            digest, size = _file_digest(landed)
            self._converted[landed.name] = {
                "sha256": digest,
                "rows": rows,
                "bytes": size,
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

        # `manifest` is every landed file this driver claims to have written, in
        # landed vocabulary: the parquet for a converted file, the file's own
        # name for a sidecar. It has to be *all* of them -- a file on disk that
        # the manifest does not list is a contradiction to the shared verifier,
        # not a harmless extra, and Databento publishes three sidecars beside
        # every dataset.
        manifest = []
        for source in sorted(self._vendor_files):
            landed = (
                landed_name(source)
                if _is_data_file(source, self._encoding)
                else source
            )
            if landed not in staged:
                continue  # never landed: claimed and absent, which is the shortfall
            converted = self._converted.get(landed) or {}
            downloaded = self._downloaded.get(source) or {}
            manifest.append(
                {
                    "name": landed,
                    "sha256": converted.get("sha256") or downloaded.get("sha256"),
                    # Only a converted file has a row count read from the
                    # vendor's bytes; a sidecar's line count would be a number
                    # about JSON, dressed as a check on the data.
                    "rows": converted.get("rows"),
                }
            )
        # Scope, in landed names: a conversion that dropped a file leaves its
        # parquet absent from `manifest` while still being claimed, which is a
        # shortfall -- the vendor did publish it, we just failed to land it.
        claimed = sorted(entry["name"] for entry in self._claimed_landed())
        return {"claimed": claimed, "manifest": manifest, "staged": staged}

    # -- internals -----------------------------------------------------------

    def _claimed_landed(self) -> list[dict[str, Any]]:
        """One landed name per vendor file the job listed, in landed vocabulary.

        Only the data files change name. Databento also publishes
        `manifest.json`, `condition.json` and `metadata.json` beside the data --
        its own description of the batch -- and those land as themselves. Mapping
        every published file through `landed_name` claimed `manifest.json.parquet`
        and friends, files that can never exist, which the shared verifier would
        read as a shortfall on every single job. The encoding is part of the
        predicate, or a `json` job would convert its own sidecars.
        """
        return [
            {
                "name": (
                    landed_name(name)
                    if _is_data_file(name, self._encoding)
                    else name
                ),
                "source": name,
            }
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
        digest, size = _file_digest(path)
        staged[path.name] = {
            "sha256": digest,
            "rows": _rows(path),
            "bytes": size,
        }
    return staged


def _file_digest(path: Path, chunk_size: int = 1 << 20) -> tuple[str, int]:
    """`(sha256, bytes)` without holding the file in memory.

    A day of `mbo` is measured in gigabytes; `read_bytes()` on it is an OOM in a
    process the host runs under rlimits, and the digest only needs one chunk at
    a time. The size comes from the bytes actually read, so a short read is a
    size that disagrees with the manifest rather than one that hides it.
    """
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _rows(path: Path) -> int | None:
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq  # noqa: PLC0415 - only a parquet landing needs it

        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    if _looks_like_data(path.name):
        # A data file is binary or compressed; its count comes from the
        # conversion, which reads the vendor's own bytes. Counting newlines here
        # would be a number that means nothing.
        return None
    payload = path.read_bytes()
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    return payload.count(b"\n") - 1  # the header line is not a row


def landed_name(vendor_name: str) -> str:
    """`X.dbn.zst` -> `X.parquet`, and the same for `csv` and `json`.

    One function because two places derive it -- the conversion that writes the
    file and the evidence that claims it -- and a landing whose claimed name is
    not its written name is a contradiction with no file to blame.
    """
    stem = vendor_name
    for suffix in (".zst", ".dbn", ".csv", ".json"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return f"{stem}.parquet"


def _is_data_file(name: str, encoding: str) -> bool:
    """Is this entry the vendor's data (rather than one of its own sidecars)?

    The encoding matters: a `json` job's data files and its `manifest.json`
    share a suffix, so the sidecars are excluded by name first. The compression
    suffix is stripped before matching, because `csv` arrives as `.csv.zst` when
    the Job asked for zstd.
    """
    if name in SIDECARS:
        return False
    stem = name[: -len(".zst")] if name.endswith(".zst") else name
    return any(stem.endswith(suffix) for suffix in ENCODING_SUFFIXES[encoding])


def _looks_like_data(name: str) -> bool:
    """The encoding-free form of `_is_data_file`, for the reader that has no Job."""
    if name in SIDECARS:
        return False
    stem = name[: -len(".zst")] if name.endswith(".zst") else name
    return any(
        stem.endswith(suffix)
        for suffixes in ENCODING_SUFFIXES.values()
        for suffix in suffixes
    )


def _write_text_parquet(source: Path, landed: Path) -> int:
    """Parse a vendor CSV/JSON file, write parquet, and return the rows read.

    The count is `table.num_rows` from the parsed table, so it is a count of the
    vendor's records rather than of the bytes written. Compression is the file's
    own suffix, not a request parameter: what landed is what gets read.
    """
    import pyarrow as pa  # noqa: PLC0415 - only a text encoding needs it
    import pyarrow.csv as pacsv  # noqa: PLC0415
    import pyarrow.json as pajson  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    payload = source.read_bytes()
    if source.name.endswith(".zst"):
        import zstandard  # noqa: PLC0415 - only a compressed landing needs it

        payload = zstandard.ZstdDecompressor().decompress(payload)
    buffer = pa.py_buffer(payload)
    stem = source.name[: -len(".zst")] if source.name.endswith(".zst") else source.name
    if stem.endswith(".csv"):
        table = pacsv.read_csv(buffer)
    else:
        # Databento's JSON encoding is newline-delimited records, which is the
        # shape pyarrow's JSON reader expects.
        table = pajson.read_json(buffer)
    pq.write_table(table, str(landed))
    return int(table.num_rows)


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


def _now() -> str:
    """UTC, to the second. The catalogue's age is shown, so it has to be a
    timestamp the reader can subtract from -- not a monotonic counter."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def _bare_name(value: str, name: str) -> str:
    """A name inside a directory, not a path that could climb out of it.

    The host already applies this rule to the wire payload; repeating it here
    costs nothing when it holds and is the only thing that holds when the driver
    is called directly (as the conformance tests do).
    """
    if (
        not value
        or value in (".", "..")
        or "/" in value
        or "\\" in value
        or Path(value).is_absolute()
    ):
        raise WireError(BAD_REQUEST, f"{name} {value!r} is not a bare file name")
    return value


def _seconds(value: Any, default: float) -> float:
    """A non-negative duration, or the default. A hint must not crash a verb."""
    try:
        seconds = float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    if seconds != seconds or seconds < 0:  # NaN or negative
        return default
    return seconds


# -- the request's shape -----------------------------------------------------


def _normalise_encoding(value: Any) -> str:
    """The plan type's encoding, or a refusal before the vendor is paid."""
    key = str(value or SUPPORTED_ENCODINGS[0]).strip().lower()
    if key not in SUPPORTED_ENCODINGS:
        raise WireError(
            BAD_REQUEST,
            f"this driver writes {', '.join(SUPPORTED_ENCODINGS)}, not encoding "
            f"{value!r}",
        )
    return key


def _normalise_compression(value: Any) -> str:
    key = str(value or "zstd").strip().lower()
    if key not in SUPPORTED_COMPRESSIONS:
        raise WireError(
            BAD_REQUEST,
            f"this driver reads {', '.join(sorted(SUPPORTED_COMPRESSIONS))} "
            f"compression, not {value!r}",
        )
    return key


def _window(start: str, end: str) -> tuple[datetime, datetime]:
    """The window as instants, refusing one that could never contain a record.

    The plan's window is half-open, so `end` must be strictly after `start`:
    an equal pair is an empty request, and a reversed pair would ask the vendor
    to run backwards. Timestamps are accepted as well as dates, because an
    intraday window on one day is a legitimate `market_data_window`.
    """
    first = _instant(start)
    last = _instant(end)
    if last <= first:
        raise WireError(
            BAD_REQUEST,
            f"end ({end!r}) must be after start ({start!r}); the window is "
            "[start, end)",
        )
    return first, last


def _instant(value: Any) -> datetime:
    """An ISO-8601 date or timestamp as a UTC instant. Bad shape is refused."""
    if not isinstance(value, str) or not value.strip():
        raise WireError(BAD_REQUEST, "a window bound must be an ISO date or timestamp")
    text = value.strip()
    # `fromisoformat` accepts at most six fractional digits and no bare `Z`.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise WireError(
            BAD_REQUEST,
            f"{value!r} is not an ISO-8601 date or timestamp",
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _as_date(value: Any) -> date | None:
    """A lenient first-ten-characters date, for a hint rather than a decision."""
    if not isinstance(value, str) or len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None
