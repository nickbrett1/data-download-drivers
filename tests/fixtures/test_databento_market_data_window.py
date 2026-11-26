"""The Databento artefact, as the `market_data_window` driver.

No network, no key, no spend: the vendor SDK is replaced by a module that
answers the shapes the live API answered. What is asserted is the part that is
ours and can therefore be wrong:

* the plan type's whole parameter space is honoured: three encodings and two
  compressions, each landing as parquet with the record count read from the
  vendor's bytes before the conversion;
* the batch's own sidecars are never converted, including when the data is JSON
  and therefore shares their suffix;
* a request that could never be answered -- an unknown schema, a start before
  the dataset exists, an empty window, an encoding or split outside the plan --
  is refused before anything is spent, while a catalogue outage is not a
  refusal;
* the vendor's manifest is still the evidence: each download is hashed as it
  lands, and a tampered file is reported in the vendor's names rather than
  quietly converted.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import types
from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from data_download_drivers import host, wire

REPO = Path(__file__).resolve().parents[2]
ARTIFACT = REPO / "drivers" / "databento" / "2"

#: Loaded under a name of its own. `tests/fixtures/test_databento_conformance.py`
#: imports `databento_driver` from version 1 through `sys.path`, and the whole
#: suite runs in one process: reusing that name would silently test whichever
#: artefact was imported first.
_spec = importlib.util.spec_from_file_location(
    "databento_driver_v2", ARTIFACT / "databento_driver.py"
)
_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _module
_spec.loader.exec_module(_module)

SUPPORTED_COMPRESSIONS = _module.SUPPORTED_COMPRESSIONS
SUPPORTED_ENCODINGS = _module.SUPPORTED_ENCODINGS
DatabentoDriver = _module.DatabentoDriver
landed_name = _module.landed_name

WINDOW = {
    "dataset": "GLBX.MDP3",
    "schema": "ohlcv-1m",
    "symbols": ["ES.c.0"],
    "stype_in": "continuous",
    "start": "2026-11-18",
    "end": "2026-11-19",
    "encoding": "dbn",
    "compression": "zstd",
    "split_duration": "day",
}

DATA_ROWS = 5


# -- a fake vendor -----------------------------------------------------------


def _csv(rows: int) -> bytes:
    lines = ["ts_event,price,size"]
    for index in range(rows):
        lines.append(f"2026-11-18T14:3{index % 10}:00Z,{100 + index}.5,{index + 1}")
    return ("\n".join(lines) + "\n").encode()


def _ndjson(rows: int) -> bytes:
    records = [
        {"ts_event": f"2026-11-18T14:3{index % 10}:00Z", "price": 100 + index, "size": index + 1}
        for index in range(rows)
    ]
    return "".join(json.dumps(row) + "\n" for row in records).encode()


class FakeStore:
    def __init__(self, records: int, parquet_rows: int) -> None:
        self.records = records
        self.parquet_rows = parquet_rows

    def __iter__(self):
        return iter(range(self.records))

    def to_parquet(self, path):
        import pyarrow as pa

        table = pa.table({"price": list(range(self.parquet_rows))})
        pq.write_table(table, str(path))


class FakeBatch:
    def __init__(self, vendor: FakeVendor) -> None:
        self.vendor = vendor

    def submit_job(self, **kwargs):
        self.vendor.calls.append(("submit_job", kwargs))
        self.vendor.submitted = dict(kwargs)
        self.vendor.publish()
        return {"id": "job-1", "state": "queued"}

    def get_job_details(self, job_id):
        self.vendor.calls.append(("get_job_details", {"job_id": job_id}))
        return {"state": "done", "record_count": self.vendor.records}

    def list_files(self, job_id):
        self.vendor.calls.append(("list_files", {"job_id": job_id}))
        return [
            {
                "filename": name,
                "size": len(payload),
                "hash": "sha256:" + hashlib.sha256(payload).hexdigest(),
            }
            for name, payload in self.vendor.served.items()
        ]

    def download(self, job_id, output_dir, filename_to_download, keep_zip=False):
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
            raise ValueError("keep_zip is refused with a filename")
        payload = self.vendor.served[filename_to_download]
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

    def list_publishers(self):
        self.vendor.calls.append(("list_publishers", {}))
        return [dict(row) for row in self.vendor.publishers]

    def get_dataset_range(self, dataset):
        self.vendor.calls.append(("get_dataset_range", {"dataset": dataset}))
        if self.vendor.catalog_unavailable:
            raise RuntimeError("catalogue unavailable")
        return dict(self.vendor.ranges[dataset])

    def list_schemas(self, dataset):
        self.vendor.calls.append(("list_schemas", {"dataset": dataset}))
        if self.vendor.catalog_unavailable:
            raise RuntimeError("catalogue unavailable")
        return list(self.vendor.schemas.get(dataset, []))


class FakeSymbology:
    def __init__(self, vendor: FakeVendor) -> None:
        self.vendor = vendor

    def resolve(self, **kwargs):
        self.vendor.calls.append(("resolve", kwargs))
        symbols = kwargs["symbols"]
        if self.vendor.unresolvable:
            return {"result": {}, "not_found": list(symbols), "partial": []}
        return {
            "result": {
                symbol: [{"d0": "2026-01-01", "d1": "2026-12-31", "s": f"inst-{i}"}]
                for i, symbol in enumerate(symbols)
            },
            "not_found": [],
            "partial": [],
        }


class FakeVendor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.served: dict[str, bytes] = {}
        self.submitted: dict = {}
        self.usd = 0.42
        self.records = DATA_ROWS
        self.parquet_rows = DATA_ROWS
        self.unresolvable = False
        self.catalog_unavailable = False
        self.publishers = [
            {
                "publisher_id": 1,
                "dataset": "GLBX.MDP3",
                "venue": "GLBX",
                "description": "CME Globex MDP 3.0",
            }
        ]
        self.ranges = {
            "GLBX.MDP3": {
                "start": "2010-06-06T00:00:00.000000000Z",
                "end": "2026-11-23T19:14:00.000000000Z",
            }
        }
        self.schemas = {"GLBX.MDP3": ["mbo", "trades", "ohlcv-1m"]}

    def publish(self) -> None:
        """The files this job would publish: one data file per day, plus sidecars."""
        dataset = str(self.submitted.get("dataset") or "GLBX.MDP3")
        schema = str(self.submitted.get("schema") or "ohlcv-1m")
        encoding = str(self.submitted.get("encoding") or "dbn")
        compression = str(self.submitted.get("compression") or "zstd")
        start = date.fromisoformat(str(self.submitted["start"])[:10])
        end = date.fromisoformat(str(self.submitted["end"])[:10])
        days = [start + timedelta(days=offset) for offset in range((end - start).days)]

        suffix = "" if compression == "none" else ".zst"
        served: dict[str, bytes] = {}
        for day in days:
            name = f"{dataset}-{day:%Y%m%d}-{schema}.{encoding}{suffix}"
            if encoding == "csv":
                payload = _csv(self.records)
            elif encoding == "json":
                payload = _ndjson(self.records)
            else:
                payload = f"PAR1-dbn-{day.isoformat()}".encode()
            if compression == "zstd":
                import zstandard

                payload = zstandard.ZstdCompressor().compress(payload)
            served[name] = payload
        served["manifest.json"] = b'{"dataset": "GLBX.MDP3"}\n'
        served["condition.json"] = b'{"condition": "ok"}\n'
        served["metadata.json"] = b'{"schema": "ohlcv-1m"}\n'
        self.served = served

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
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test-key")
    monkeypatch.setitem(sys.modules, "databento", fake.module())
    return fake


def _driver(root: Path) -> DatabentoDriver:
    return DatabentoDriver(root, {})


def drive(root: Path, **overrides):
    window = {**WINDOW, **overrides}
    driver = _driver(root)
    driver.resolve(
        dataset=window["dataset"],
        symbols=window["symbols"],
        stype_in=window["stype_in"],
    )
    driver.cost(**{k: window[k] for k in ("dataset", "schema", "symbols", "stype_in", "start", "end")})
    job = driver.submit(**window)
    driver.poll(vendor_job_id=job["vendor_job_id"], interval_s=0)
    files = driver.list_files(vendor_job_id=job["vendor_job_id"])["files"]
    written = driver.fetch(
        vendor_job_id=job["vendor_job_id"], dest="files/", files=files
    )["written"]
    converted = driver.convert(dest="files/", **{"from": window["encoding"], "to": "parquet"})
    evidence = driver.verify(dest="files/", claimed=written, staged=converted["staged"])
    return driver, job, files, written, converted, evidence


# -- the artefact -----------------------------------------------------------


def test_the_artefact_declares_itself_the_market_data_window_driver():
    manifest = json.loads((ARTIFACT / "manifest.json").read_text())
    assert manifest["name"] == "databento"
    assert manifest["version"] == "2"
    assert manifest["vendor"] == "Databento"
    assert manifest["protocol"] == wire.PROTOCOL_VERSION
    assert manifest["entrypoint"] == "driver.py"
    assert (ARTIFACT / "driver.py").is_file()


def test_discovery_finds_version_two_above_version_one():
    found = {(a.name, a.version): a for a in host.discover(REPO / "drivers")}
    assert ("databento", "1") in found
    assert ("databento", "2") in found
    assert found[("databento", "2")].vendor == "Databento"


def test_every_verb_the_wire_requires_is_implemented(vendor, tmp_path):
    driver = _driver(tmp_path)
    for verb in wire.VERBS:
        assert callable(getattr(driver, verb, None)), verb


def test_the_plan_types_whole_parameter_space_is_supported():
    assert SUPPORTED_ENCODINGS == ("dbn", "csv", "json")
    assert SUPPORTED_COMPRESSIONS == {"zstd", "none"}


# -- every encoding lands as parquet ----------------------------------------


@pytest.mark.parametrize(
    ("encoding", "compression"),
    [("dbn", "zstd"), ("csv", "zstd"), ("csv", "none"), ("json", "zstd"), ("json", "none")],
)
def test_each_encoding_lands_as_parquet_with_the_vendors_row_count(
    vendor, tmp_path, encoding, compression
):
    _, _, files, written, converted, evidence = drive(
        tmp_path, encoding=encoding, compression=compression
    )

    data_file = next(
        entry["name"]
        for entry in files
        if entry["name"] not in {"manifest.json", "condition.json", "metadata.json"}
    )
    expected_parquet = landed_name(data_file)
    assert expected_parquet in converted["staged"]
    assert not (tmp_path / "files" / data_file).exists()
    assert evidence["claimed"] == [
        expected_parquet,
        "condition.json",
        "manifest.json",
        "metadata.json",
    ]
    assert [entry["name"] for entry in evidence["manifest"]] == evidence["claimed"]
    rows = {entry["name"]: entry["rows"] for entry in evidence["manifest"]}
    assert rows[expected_parquet] == DATA_ROWS
    assert rows["manifest.json"] is None
    # The staged reading is the manifest's own reading, so the shared verifier
    # would pass this landing rather than contradict it.
    assert set(evidence["staged"]) == set(evidence["claimed"])
    assert {name: got["sha256"] for name, got in evidence["staged"].items()} == {
        entry["name"]: entry["sha256"] for entry in evidence["manifest"]
    }
    assert sorted(written) == sorted(entry["name"] for entry in files)


def test_json_sidecars_are_not_converted_into_a_file_that_cannot_exist(vendor, tmp_path):
    """`manifest.json` and the JSON data share a suffix; only the data converts."""
    _, _, _, _, converted, evidence = drive(
        tmp_path, encoding="json", compression="none"
    )
    assert "manifest.parquet" not in converted["staged"]
    assert "manifest.json" in converted["staged"]
    assert "GLBX.MDP3-20261118-ohlcv-1m.parquet" in converted["staged"]


def test_the_landing_parquet_really_holds_the_rows(vendor, tmp_path):
    drive(tmp_path, encoding="csv", compression="none")
    landed = tmp_path / "files" / "GLBX.MDP3-20261118-ohlcv-1m.parquet"
    assert pq.ParquetFile(str(landed)).metadata.num_rows == DATA_ROWS


def test_a_lossy_conversion_is_a_row_count_contradiction(vendor, tmp_path):
    """The manifest's rows come from the vendor's bytes, not from what we wrote."""
    vendor.parquet_rows = 2
    _, _, _, _, _, evidence = drive(tmp_path)
    manifest_rows = [
        entry["rows"] for entry in evidence["manifest"] if entry["name"].endswith(".parquet")
    ]
    staged_rows = [
        got["rows"]
        for name, got in evidence["staged"].items()
        if name.endswith(".parquet")
    ]
    assert manifest_rows == [DATA_ROWS]
    assert staged_rows == [2]


# -- refusals before anything is spent --------------------------------------


def test_an_encoding_outside_the_plan_is_refused_before_the_vendor(vendor, tmp_path):
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "encoding": "parquet"})
    assert caught.value.code == wire.BAD_REQUEST
    assert [verb for verb, _ in vendor.calls] == []


def test_a_compression_outside_the_plan_is_refused_before_the_vendor(vendor, tmp_path):
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "compression": "gzip"})
    assert caught.value.code == wire.BAD_REQUEST
    assert [verb for verb, _ in vendor.calls] == []


def test_a_split_outside_the_plan_is_refused_before_the_vendor(vendor, tmp_path):
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "split_duration": "hour"})
    assert caught.value.code == wire.BAD_REQUEST
    assert [verb for verb, _ in vendor.calls] == []


@pytest.mark.parametrize(
    ("start", "end"),
    [("2026-11-18", "2026-11-18"), ("2026-11-19", "2026-11-18")],
)
def test_an_empty_or_reversed_window_is_refused(vendor, tmp_path, start, end):
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "start": start, "end": end})
    assert caught.value.code == wire.BAD_REQUEST
    assert [verb for verb, _ in vendor.calls] == []


def test_an_intraday_window_on_one_day_is_accepted(vendor, tmp_path):
    job = _driver(tmp_path).submit(
        **{**WINDOW, "start": "2026-11-18T09:30:00Z", "end": "2026-11-18T16:00:00Z"}
    )
    assert job["vendor_job_id"] == "job-1"


def test_a_schema_the_dataset_does_not_carry_is_refused_for_free(vendor, tmp_path):
    vendor.schemas["GLBX.MDP3"] = ["mbo", "trades"]
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "schema": "ohlcv-1m"})
    assert caught.value.code == wire.BAD_REQUEST
    assert "ohlcv-1m" in caught.value.detail
    assert [verb for verb, _ in vendor.calls] == ["list_schemas"]


def test_a_start_before_the_dataset_exists_is_refused_for_free(vendor, tmp_path):
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).submit(**{**WINDOW, "start": "2009-01-01", "end": "2009-01-02"})
    assert caught.value.code == wire.BAD_REQUEST
    assert "2010-06-06" in caught.value.detail
    assert "submit_job" not in [verb for verb, _ in vendor.calls]


def test_a_catalogue_outage_is_not_a_refusal(vendor, tmp_path):
    """`could not ask` must not read as `you asked for the impossible`."""
    vendor.catalog_unavailable = True
    job = _driver(tmp_path).submit(**WINDOW)
    assert job["vendor_job_id"] == "job-1"
    assert "submit_job" in [verb for verb, _ in vendor.calls]


def test_submit_normalises_the_splits_synonyms(vendor, tmp_path):
    _driver(tmp_path).submit(**{**WINDOW, "split_duration": "daily"})
    submitted = next(args for verb, args in vendor.calls if verb == "submit_job")
    assert submitted["split_duration"] == "day"


# -- resolve and the catalogue ----------------------------------------------


def test_resolve_uses_the_requests_window_when_it_is_given(vendor, tmp_path):
    answer = _driver(tmp_path).resolve(
        dataset="GLBX.MDP3",
        symbols=["ES.c.0"],
        stype_in="continuous",
        start="2026-11-18",
        end="2026-11-19",
    )
    call = next(args for verb, args in vendor.calls if verb == "resolve")
    assert call["start_date"] == date(2026, 11, 18)
    assert answer["window"] == {
        "start": "2026-11-18",
        "end": "2026-11-19",
        "note": "the request's",
    }


def test_resolve_asks_about_a_same_day_window(vendor, tmp_path):
    """A one-day window is `start == end`, not a reversal to fall back from.

    Treating it as "no window" fell back to the last month, which for a
    subscription dataset is exactly a range the licence does not cover: the
    vendor answered 422 `dataset_unavailable_range` before quoting anything.
    A same-day request must reach the vendor with those dates.
    """
    answer = _driver(tmp_path).resolve(
        dataset="GLBX.MDP3",
        symbols=["ES.c.0"],
        stype_in="continuous",
        start="2026-11-25",
        end="2026-11-25",
    )
    call = next(args for verb, args in vendor.calls if verb == "resolve")
    assert call["start_date"] == date(2026, 11, 25)
    assert call["end_date"] == date(2026, 11, 25)
    assert answer["window"] == {
        "start": "2026-11-25",
        "end": "2026-11-25",
        "note": "the request's",
    }


def test_resolve_falls_back_only_for_a_missing_or_reversed_window(vendor, tmp_path):
    """The fallback is for unusable pairs, not for a legal one-day window."""
    answer = _driver(tmp_path).resolve(
        dataset="GLBX.MDP3",
        symbols=["ES.c.0"],
        stype_in="continuous",
        start="2026-11-25",
        end="2026-11-20",
    )
    assert answer["window"]["note"] == "recent, not the request's"
    call = next(args for verb, args in vendor.calls if verb == "resolve")
    assert call["start_date"] < call["end_date"]


def test_resolve_falls_back_to_a_recent_window_without_one(vendor, tmp_path):
    answer = _driver(tmp_path).resolve(
        dataset="GLBX.MDP3", symbols=["ES.c.0"], stype_in="continuous"
    )
    assert answer["window"]["note"] == "recent, not the request's"


def test_an_unresolvable_symbol_is_refused_before_anything_is_spent(vendor, tmp_path):
    vendor.unresolvable = True
    with pytest.raises(wire.WireError) as caught:
        _driver(tmp_path).resolve(
            dataset="GLBX.MDP3", symbols=["NOPE.c.0"], stype_in="continuous"
        )
    assert "NOPE.c.0" in caught.value.detail


def test_describe_names_the_dataset_level_publisher(vendor, tmp_path):
    answer = _driver(tmp_path).describe(dataset="GLBX.MDP3")
    assert answer["described"] is True
    assert answer["description"] == "CME Globex MDP 3.0"
    assert answer["venue"] == "GLBX"
    assert answer["schemas"] == ["mbo", "trades", "ohlcv-1m"]


# -- the vendor's manifest is the evidence ----------------------------------


def test_a_tampered_download_is_not_converted_and_names_the_vendors_file(vendor, tmp_path):
    driver = _driver(tmp_path)
    driver.submit(**WINDOW)
    original = vendor.served
    files = driver.list_files(vendor_job_id="job-1")["files"]
    # Tamper after the manifest is taken, so the hash disagrees.
    vendor.served = {name: payload + b"tampered" for name, payload in original.items()}
    written = driver.fetch(vendor_job_id="job-1", dest="files/", files=files)["written"]
    converted = driver.convert(dest="files/", **{"from": "dbn", "to": "parquet"})
    evidence = driver.verify(dest="files/", claimed=written, staged=converted["staged"])

    assert converted.get("skipped")
    assert sorted(evidence["manifest"][0]) == ["name", "rows", "sha256"]
    assert {entry["name"] for entry in evidence["manifest"]} == set(original)
    assert len(evidence["integrity"]) == len(original)


def test_a_dest_that_escapes_the_session_root_is_refused(vendor, tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with pytest.raises(wire.WireError):
        _driver(root).fetch(vendor_job_id="job-1", dest="../../etc", files=[])
