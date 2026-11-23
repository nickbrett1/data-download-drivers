"""The Databento artefact, against a fake vendor.

No network, no key, no spend: the SDK is replaced by a module that answers the
same shapes. What is asserted is the part that is ours and can therefore be
wrong:

* one download per file, landed under the vendor's own name -- not the
  whole-job `.zip`, whose hash is the archive's rather than the data's, and not
  a name the SDK chose;
* each download is hashed as it lands and compared to the vendor's own sha256,
  and a mismatch stops the conversion rather than converting bytes that are not
  the vendor's;
* the landing is the parquet and the source is gone, so the evidence set has no
  file the manifest does not list;
* the record count in the evidence is the count read from the *vendor's* file,
  so a lossy conversion is a row-count mismatch;
* `poll` never sleeps past the verb's deadline, and maps Databento's states onto
  the four words the interpreter knows.
"""

from __future__ import annotations

import hashlib
import json
import sys
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_download_drivers import host, wire
from test_host_session import Runtime

REPO = Path(__file__).resolve().parents[1]
ARTIFACT = REPO / "drivers" / "databento" / "1"

WINDOW = {
    "dataset": "GLBX.MDP3",
    "schema": "trades",
    "symbols": ["ES.c.0"],
    "stype_in": "continuous",
    "start": "2026-10-01",
    "end": "2026-10-03",
    "encoding": "dbn",
    "compression": "zstd",
    "split_duration": "day",
}


class FakeBatch:
    def __init__(self, vendor: FakeVendor) -> None:
        self.vendor = vendor

    def submit_job(self, **kwargs):
        self.vendor.calls.append(("submit_job", kwargs))
        return {"id": "job-1", "state": "queued"}

    def get_job_details(self, job_id):
        self.vendor.calls.append(("get_job_details", {"job_id": job_id}))
        return self.vendor.states.pop(0) if self.vendor.states else {
            "state": "done",
            "record_count": self.vendor.records,
        }

    def list_files(self, job_id):
        self.vendor.calls.append(("list_files", {"job_id": job_id}))
        return [
            {
                "filename": name,
                "size": len(payload),
                # The API's shape: `<algo>:<hex>`.
                "hash": f"sha256:{hashlib.sha256(payload).hexdigest()}",
            }
            for name, payload in self.vendor.published.items()
        ]

    def download(self, job_id, output_dir, filename_to_download, keep_zip=False):
        """The SDK's behaviour, including the two parts we have to work around."""
        self.vendor.calls.append(
            (
                "download",
                {
                    "job_id": job_id,
                    "output_dir": output_dir,
                    "filename_to_download": filename_to_download,
                    "keep_zip": keep_zip,
                },
            )
        )
        if keep_zip:
            raise ValueError(
                "Cannot specify an individual file to download when `keep_zip=True`",
            )
        payload = self.vendor.served.get(filename_to_download)
        if payload is None:
            raise RuntimeError(f"no such file {filename_to_download}")
        # batch.download(...) appends the job id to output_dir.
        landed = Path(output_dir, job_id, filename_to_download)
        landed.parent.mkdir(parents=True, exist_ok=True)
        landed.write_bytes(payload)
        return [landed]


class FakeMetadata:
    def __init__(self, vendor: FakeVendor) -> None:
        self.vendor = vendor

    def get_cost(self, **kwargs):
        self.vendor.calls.append(("get_cost", kwargs))
        return self.vendor.usd


class FakeSymbology:
    def __init__(self, vendor: FakeVendor) -> None:
        self.vendor = vendor

    def resolve(self, **kwargs):
        """The API's shape, checked against the live endpoint 2026-11-23."""
        self.vendor.calls.append(("resolve", kwargs))
        symbols = kwargs["symbols"]
        if self.vendor.unresolvable:
            return {
                "result": {},
                "symbols": symbols,
                "not_found": list(symbols),
                "partial": [],
                "message": "OK",
                "status": 0,
            }
        return {
            "result": {
                symbol: [{"d0": "2026-01-01", "d1": "2026-12-31", "s": f"inst-{index}"}]
                for index, symbol in enumerate(symbols)
            },
            "symbols": symbols,
            "not_found": [],
            "partial": [],
            "message": "OK",
            "status": 0,
        }


class FakeStore:
    """Stands in for `DBNStore`: iterable records, and a real parquet on write."""

    def __init__(self, records: int, parquet_rows: int) -> None:
        self.records = records
        self.parquet_rows = parquet_rows

    def __iter__(self):
        return iter(range(self.records))

    def to_parquet(self, path):
        table = pa.table({"price": list(range(self.parquet_rows))})
        pq.write_table(table, str(path))


class FakeVendor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.published: dict[str, bytes] = {}
        self.served: dict[str, bytes] = {}
        self.states: list[dict] = []
        self.usd = 1.2345
        self.unresolvable = False
        self.records = 7
        self.parquet_rows = 7

    def module(self):
        vendor = self

        class Historical:
            def __init__(self, key: str) -> None:
                self.key = key
                self.batch = FakeBatch(vendor)
                self.metadata = FakeMetadata(vendor)
                self.symbology = FakeSymbology(vendor)

        class DBNStore:
            @staticmethod
            def from_file(path):
                return FakeStore(records=vendor.records, parquet_rows=vendor.parquet_rows)

        return types.SimpleNamespace(Historical=Historical, DBNStore=DBNStore)


@pytest.fixture
def vendor(monkeypatch):
    fake = FakeVendor()
    day_one = b"PAR1-dbn-" + b"\x00" * 16
    day_two = b"PAR1-dbn-" + b"\x01" * 16
    fake.published = {
        "GLBX.MDP3-20261001-TRD.dbn.zst": day_one,
        "GLBX.MDP3-20261002-TRD.dbn.zst": day_two,
    }
    fake.served = dict(fake.published)
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test-key")
    monkeypatch.setitem(sys.modules, "databento", fake.module())
    monkeypatch.syspath_prepend(str(ARTIFACT))
    return fake


def _driver(root: Path):
    from databento_driver import DatabentoDriver

    return DatabentoDriver(root, {"mode": "clean"})


def _wire_args(driver, dest: str = "files/") -> dict:
    """Drive the same eight verbs, in the Job's order, as the interpreter does."""
    driver.resolve(dataset="GLBX.MDP3", symbols=["ES.c.0"], stype_in="continuous")
    driver.cost(**{k: WINDOW[k] for k in ("dataset", "schema", "symbols", "stype_in", "start", "end")})
    submitted = driver.submit(**WINDOW)
    job_id = submitted["vendor_job_id"]
    driver.poll(vendor_job_id=job_id, interval_s=0)
    files = driver.list_files(vendor_job_id=job_id)["files"]
    written = driver.fetch(vendor_job_id=job_id, dest=dest, files=files)["written"]
    converted = driver.convert(dest=dest, **{"from": "dbn", "to": "parquet"})
    evidence = driver.verify(dest=dest, claimed=written, staged=converted["staged"])
    return job_id, files, written, converted, evidence


# -- the artefact ---------------------------------------------------------


def test_the_artefact_declares_itself_a_databento_driver():
    manifest = json.loads((ARTIFACT / "manifest.json").read_text())
    assert manifest["name"] == "databento"
    assert manifest["vendor"] == "Databento"
    assert manifest["protocol"] == wire.PROTOCOL_VERSION
    assert manifest["entrypoint"] == "driver.py"
    assert (ARTIFACT / "driver.py").is_file()


def test_discovery_finds_it_next_to_the_stub():
    found = {artifact.name: artifact for artifact in host.discover(REPO / "drivers")}
    assert set(found) >= {"stub", "databento"}
    assert found["databento"].vendor == "Databento"


def test_the_host_reports_a_missing_key_as_an_error_not_an_exit(monkeypatch, tmp_path):
    """The whole reason the key is checked lazily: over the wire, the app must
    read a sentence it can put on a request, not "the driver process exited"."""
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    run = Runtime(REPO / "drivers", datasets)
    try:
        status, body = run.call(
            "POST",
            "/v1/sessions",
            {
                "protocol": wire.PROTOCOL_VERSION,
                "driver": "databento",
                "driver_version": "1",
                "root": str(datasets / "databento" / "GLBX.MDP3" / "req_no_key"),
            },
        )
        assert status == 201, body
        status, body = run.call(
            "POST",
            "/v1/cost",
            {"session_id": body["session_id"], **WINDOW},
        )
        assert status == 400, body
        assert body["error"]["code"] == wire.BAD_REQUEST
        assert "DATABENTO_API_KEY" in body["error"]["detail"]
    finally:
        run.server.shutdown()


def test_every_verb_the_wire_requires_is_implemented(vendor, tmp_path):
    driver = _driver(tmp_path)
    for verb in wire.VERBS:
        assert callable(getattr(driver, verb, None)), verb


def test_no_key_is_named_by_the_verb_that_needs_it(monkeypatch, tmp_path):
    """Not at construction: the child builds the driver before it reads a verb,
    so a constructor that raised would reach the app as "the driver process
    exited" -- a mystery with the useful sentence stuck in a log."""
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "databento", FakeVendor().module())
    monkeypatch.syspath_prepend(str(ARTIFACT))
    from databento_driver import DatabentoDriver

    driver = DatabentoDriver(tmp_path, {})
    for verb, kwargs in (
        ("resolve", {"dataset": "GLBX.MDP3", "symbols": ["ES.c.0"], "stype_in": "continuous"}),
        ("cost", dict(WINDOW)),
        ("list_files", {"vendor_job_id": "job-1"}),
    ):
        with pytest.raises(wire.WireError) as caught:
            getattr(driver, verb)(**kwargs)
        assert caught.value.code == wire.BAD_REQUEST, verb
        assert "DATABENTO_API_KEY" in caught.value.detail, verb


# -- the chain of trust ---------------------------------------------------


def test_the_download_keeps_the_vendors_bytes(vendor, tmp_path):
    """One file at a time, straight to its own name: the bytes the manifest hashes.

    `keep_zip=True` is what the docstrings used to say and the SDK refuses it the
    moment a filename is given, so this pins the shape that actually works: a
    per-file download, no zip, and the file hoisted out of the SDK's job
    subdirectory into `dest` under the vendor's own name.
    """
    driver = _driver(tmp_path)
    _, files, written, _, _ = _wire_args(driver)

    downloads = [args for verb, args in vendor.calls if verb == "download"]
    assert len(downloads) == 2
    assert not any(args["keep_zip"] for args in downloads)
    assert sorted(args["filename_to_download"] for args in downloads) == sorted(
        vendor.published
    )
    assert sorted(written) == sorted(vendor.published)
    # Nothing is left behind under the SDK's job subdirectory: the bytes were
    # hoisted into `dest` under the vendor's name, and the rest of the chain ran
    # on them from there.
    assert not (tmp_path / "files" / "job-1").exists()
    assert {entry["name"]: entry["sha256"] for entry in files} == {
        name: hashlib.sha256(payload).hexdigest()
        for name, payload in vendor.published.items()
    }


def test_a_bare_digest_and_a_prefixed_one_both_verify(vendor, tmp_path):
    """The API has published both shapes; an unusable hash is "no hash", not a lie."""
    from databento_driver import _digest

    assert _digest("sha256:" + "a" * 64) == "a" * 64
    assert _digest("A" * 64) == "a" * 64
    assert _digest("md5:" + "a" * 32) == ""
    assert _digest(None) == ""


def test_the_landing_is_the_parquet_and_the_source_is_gone(vendor, tmp_path):
    driver = _driver(tmp_path)
    _wire_args(driver)

    staged = sorted(p.name for p in (tmp_path / "files").iterdir())
    assert staged == [
        "GLBX.MDP3-20261001-TRD.parquet",
        "GLBX.MDP3-20261002-TRD.parquet",
    ]


def test_the_evidence_describes_what_is_on_disk(vendor, tmp_path):
    driver = _driver(tmp_path)
    _, _, _, _, evidence = _wire_args(driver)

    assert evidence["claimed"] == [
        "GLBX.MDP3-20261001-TRD.parquet",
        "GLBX.MDP3-20261002-TRD.parquet",
    ]
    assert [entry["name"] for entry in evidence["manifest"]] == evidence["claimed"]
    # Rows come from the vendor's file, not from the file we just wrote.
    assert [entry["rows"] for entry in evidence["manifest"]] == [7, 7]
    assert [entry["rows"] for entry in evidence["staged"].values()] == [7, 7]


def test_a_lossy_conversion_shows_up_as_a_row_count_mismatch(vendor, tmp_path):
    vendor.parquet_rows = 3  # the vendor's file had 7; the conversion kept 3
    driver = _driver(tmp_path)
    _, _, _, _, evidence = _wire_args(driver)

    manifest_rows = [entry["rows"] for entry in evidence["manifest"]]
    staged_rows = [entry["rows"] for entry in evidence["staged"].values()]
    assert manifest_rows == [7, 7]
    assert staged_rows == [3, 3]


def test_a_corrupt_download_is_not_converted_and_names_the_vendors_file(vendor, tmp_path):
    vendor.served = {
        name: payload + b"tampered" for name, payload in vendor.published.items()
    }
    driver = _driver(tmp_path)
    _, _, _, _, evidence = _wire_args(driver)

    # Still the vendor's vocabulary, so the contradiction names the file that
    # disagreed rather than a parquet derived from it.
    assert [entry["name"] for entry in evidence["manifest"]] == sorted(vendor.published)
    assert evidence["manifest"][0]["sha256"] == hashlib.sha256(
        vendor.published["GLBX.MDP3-20261001-TRD.dbn.zst"]
    ).hexdigest()
    assert sorted(evidence["staged"]) == sorted(vendor.published)
    assert len(evidence["integrity"]) == 2


def test_a_dropped_file_is_a_shortfall_not_a_silent_landing(vendor, tmp_path):
    """The vendor listed two files; only one survives conversion."""
    driver = _driver(tmp_path)
    driver.submit(**WINDOW)
    files = driver.list_files(vendor_job_id="job-1")["files"]
    written = driver.fetch(vendor_job_id="job-1", dest="files/", files=files)["written"]
    (tmp_path / "files" / written[1]).unlink()  # the second file never arrives
    converted = driver.convert(dest="files/", **{"from": "dbn", "to": "parquet"})
    evidence = driver.verify(dest="files/", claimed=written, staged=converted["staged"])

    missing = set(evidence["claimed"]) - {entry["name"] for entry in evidence["manifest"]}
    assert missing == {"GLBX.MDP3-20261002-TRD.parquet"}


# -- the vendor's answers -------------------------------------------------


def test_poll_maps_states_and_never_sleeps_past_the_deadline(vendor, tmp_path, monkeypatch):
    from databento_driver import POLL_SLEEP_CAP_S

    slept: list[float] = []
    monkeypatch.setattr("databento_driver.time.sleep", slept.append)
    vendor.states = [
        {"state": "received"},
        {"state": "processing"},
        {"state": "done", "record_count": 7},
    ]
    driver = _driver(tmp_path)
    first = driver.poll(vendor_job_id="job-1", interval_s=300)
    second = driver.poll(vendor_job_id="job-1", interval_s=300)
    third = driver.poll(vendor_job_id="job-1", interval_s=300)

    assert (first["state"], second["state"], third["state"]) == ("queued", "running", "done")
    assert slept == [POLL_SLEEP_CAP_S, POLL_SLEEP_CAP_S]  # not the Job's 300 s
    assert slept[0] < wire.DEADLINES["poll"]
    assert third["record_count"] == 7


def test_an_expired_job_reports_a_terminal_state(vendor, tmp_path):
    vendor.states = [{"state": "expired"}]
    driver = _driver(tmp_path)
    assert driver.poll(vendor_job_id="job-1", interval_s=0)["state"] == "expired"


def test_an_unresolvable_symbol_is_refused_before_anything_is_spent(vendor, tmp_path):
    vendor.unresolvable = True
    driver = _driver(tmp_path)
    with pytest.raises(wire.WireError) as caught:
        driver.resolve(dataset="GLBX.MDP3", symbols=["NOPE.c.0"], stype_in="continuous")
    assert "NOPE.c.0" in caught.value.detail
    assert [verb for verb, _ in vendor.calls] == ["resolve"]


def test_resolve_reports_the_api_shape_it_was_given(vendor, tmp_path):
    """`result` is a dict, `not_found` is a top-level list.

    Getting this wrong is not cosmetic: the live endpoint answered 200 and the
    driver read `result` as a list of `{"s": {...}}`, which is the shape of a
    different Databento call. The session then failed with an AttributeError
    that said nothing about symbology.
    """
    driver = _driver(tmp_path)
    answer = driver.resolve(
        dataset="GLBX.MDP3", symbols=["ES.c.0", "NQ.c.0"], stype_in="continuous"
    )
    assert answer["resolved"] == 2
    assert answer["symbols"] == ["ES.c.0", "NQ.c.0"]
    assert answer["stype_out"] == "instrument_id"
    assert answer["window"]["note"] == "recent, not the request's"


def test_cost_answers_the_amount_the_vendor_quoted(vendor, tmp_path):
    driver = _driver(tmp_path)
    quoted = driver.cost(
        dataset="GLBX.MDP3",
        schema="trades",
        symbols=["ES.c.0"],
        stype_in="continuous",
        start="2026-10-01",
        end="2026-10-03",
    )
    assert quoted["usd"] == pytest.approx(1.2345)


def test_a_dest_that_escapes_the_session_root_is_refused(vendor, tmp_path):
    driver = _driver(tmp_path / "root")
    (tmp_path / "root").mkdir(parents=True, exist_ok=True)
    with pytest.raises(wire.WireError):
        driver.fetch(vendor_job_id="job-1", dest="../../etc", files=[])
