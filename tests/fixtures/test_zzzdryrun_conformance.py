"""The Zzzdryrun artefact: a dry-run NBBO vendor, end to end and by itself.

No network and no credential exist to fake: the vendor fabricates its series.
What is asserted here is the part that is genuinely a driver's:

* the vendor is discoverable, speaks the protocol, and implements every verb;
* it publishes **daily** files for **exactly one** symbol, and refuses a second
  symbol before a job exists rather than landing an ambiguous series;
* the bytes it lists are the bytes it serves, and a tampered download is
  recorded rather than converted;
* the landing is parquet with quote columns, and its row count is the count read
  from the vendor's source, not from the file we just wrote.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from data_download_drivers import host, wire

REPO = Path(__file__).resolve().parents[2]
ARTIFACT = REPO / "drivers" / "zzzdryrun" / "1"
sys.path.insert(0, str(ARTIFACT))

from zzzdryrun_driver import ROWS_PER_DAY, ZzzdryrunDriver  # noqa: E402

WINDOW = {
    "dataset": "NBBO",
    "schema": "quotes",
    "symbols": ["AAPL"],
    "stype_in": "raw_symbol",
    "start": "2026-10-01",
    "end": "2026-10-03",
    "encoding": "csv",
    "compression": "none",
    "split_duration": "day",
}


def drive(root: Path, window: dict | None = None):
    """The Job's order, as the interpreter drives it."""
    window = {**WINDOW, **(window or {})}
    driver = ZzzdryrunDriver(root, {})
    driver.resolve(
        dataset=window["dataset"], symbols=window["symbols"], stype_in=window["stype_in"]
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


def test_the_artefact_declares_itself_a_zzzdryrun_driver():
    manifest = json.loads((ARTIFACT / "manifest.json").read_text())
    assert manifest["name"] == "zzzdryrun"
    assert manifest["vendor"] == "Zzzdryrun"
    assert manifest["protocol"] == wire.PROTOCOL_VERSION
    assert manifest["entrypoint"] == "driver.py"
    assert (ARTIFACT / "driver.py").is_file()


def test_discovery_finds_it_next_to_the_other_vendors():
    found = {artifact.name: artifact for artifact in host.discover(REPO / "drivers")}
    assert found["zzzdryrun"].vendor == "Zzzdryrun"


def test_every_verb_the_wire_requires_is_implemented(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    for verb in wire.VERBS:
        assert callable(getattr(driver, verb, None)), verb


# -- one symbol, daily files ------------------------------------------------


def test_daily_files_carry_one_symbol(tmp_path):
    _, _, files, _, _, _ = drive(tmp_path)

    assert [entry["name"] for entry in files] == [
        "AAPL-2026-10-01-nbbo.csv",
        "AAPL-2026-10-02-nbbo.csv",
    ]
    assert all(entry["rows"] == ROWS_PER_DAY for entry in files)


def test_a_second_symbol_is_refused_before_a_job_exists(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    with pytest.raises(wire.WireError) as caught:
        driver.resolve(dataset="NBBO", symbols=["AAPL", "MSFT"], stype_in="raw_symbol")
    assert caught.value.code == wire.BAD_REQUEST
    assert "one symbol" in caught.value.detail


def test_cost_and_submit_refuse_a_second_symbol_too(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    window = {**WINDOW, "symbols": ["AAPL", "MSFT"]}
    cost_args = {k: window[k] for k in ("dataset", "schema", "symbols", "stype_in", "start", "end")}
    with pytest.raises(wire.WireError):
        driver.cost(**cost_args)
    with pytest.raises(wire.WireError):
        driver.submit(**window)


def test_a_window_that_ends_before_it_starts_is_refused(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    driver.resolve(dataset="NBBO", symbols=["AAPL"], stype_in="raw_symbol")
    with pytest.raises(wire.WireError) as caught:
        driver.submit(**{**WINDOW, "start": "2026-10-03", "end": "2026-10-01"})
    assert caught.value.code == wire.BAD_REQUEST


def test_a_split_other_than_daily_is_refused(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    with pytest.raises(wire.WireError) as caught:
        driver.submit(**{**WINDOW, "split_duration": "week"})
    assert caught.value.code == wire.BAD_REQUEST
    assert "daily" in caught.value.detail


# -- the chain of trust -----------------------------------------------------


def test_the_landing_is_parquet_with_quote_columns(tmp_path):
    drive(tmp_path)
    landed = sorted(p.name for p in (tmp_path / "files").iterdir())
    assert landed == [
        "AAPL-2026-10-01-nbbo.parquet",
        "AAPL-2026-10-02-nbbo.parquet",
    ]
    table = pq.read_table(tmp_path / "files" / landed[0])
    assert table.num_rows == ROWS_PER_DAY
    assert {
        "date",
        "timestamp",
        "symbol",
        "bid",
        "ask",
        "bid_price",
        "ask_price",
        "bid_size",
        "ask_size",
    } <= set(table.column_names)
    rows = table.to_pylist()
    assert {row["symbol"] for row in rows} == {"AAPL"}
    assert all(row["ask"] > row["bid"] for row in rows)
    assert all(row["ask_price"] == row["ask"] for row in rows)


def test_the_evidence_describes_what_is_on_disk(tmp_path):
    _, _, _, _, _, evidence = drive(tmp_path)

    assert [entry["name"] for entry in evidence["manifest"]] == evidence["claimed"]
    assert set(evidence["claimed"]) == set(evidence["staged"])
    assert [entry["rows"] for entry in evidence["manifest"]] == [ROWS_PER_DAY, ROWS_PER_DAY]
    assert {entry["name"]: entry["sha256"] for entry in evidence["manifest"]} == {
        name: got["sha256"] for name, got in evidence["staged"].items()
    }


def test_a_tampered_download_is_recorded_and_not_converted(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    job_id = driver.submit(**WINDOW)["vendor_job_id"]
    files = driver.list_files(vendor_job_id=job_id)["files"]
    # The manifest the job was handed says the vendor's bytes have a different
    # digest, so what lands cannot be the file that was hashed.
    tampered = [
        {**entry, "sha256": "0" * 64} for entry in files
    ]
    written = driver.fetch(
        vendor_job_id=job_id, dest="files/", files=tampered
    )["written"]
    converted = driver.convert(dest="files/", **{"from": "csv", "to": "parquet"})
    evidence = driver.verify(dest="files/", claimed=written, staged=converted["staged"])

    assert len(evidence["integrity"]) == 2
    assert sorted(evidence["staged"]) == sorted(written)
    assert all(not name.endswith(".parquet") for name in evidence["claimed"])


def test_a_dropped_file_is_a_shortfall_not_a_silent_landing(tmp_path):
    driver = ZzzdryrunDriver(tmp_path, {})
    job_id = driver.submit(**WINDOW)["vendor_job_id"]
    files = driver.list_files(vendor_job_id=job_id)["files"]
    written = driver.fetch(vendor_job_id=job_id, dest="files/", files=files)["written"]
    (tmp_path / "files" / written[1]).unlink()
    converted = driver.convert(dest="files/", **{"from": "csv", "to": "parquet"})
    evidence = driver.verify(dest="files/", claimed=written, staged=converted["staged"])

    missing = set(evidence["claimed"]) - {entry["name"] for entry in evidence["manifest"]}
    assert missing == {"AAPL-2026-10-02-nbbo.parquet"}


def test_the_same_request_fabricates_the_same_bytes(tmp_path):
    _, _, first, _, _, _ = drive(tmp_path / "one")
    _, _, second, _, _, _ = drive(tmp_path / "two")
    assert [entry["sha256"] for entry in first] == [entry["sha256"] for entry in second]


def test_a_dest_that_escapes_the_session_root_is_refused(tmp_path):
    driver = ZzzdryrunDriver(tmp_path / "root", {})
    (tmp_path / "root").mkdir()
    with pytest.raises(wire.WireError):
        driver.fetch(vendor_job_id="whatever", dest="../etc", files=[])


# -- the encodings the dry run can carry ------------------------------------


@pytest.mark.parametrize(
    ("encoding", "compression", "name"),
    [
        ("csv", "zstd", "AAPL-2026-10-01-nbbo.csv.zst"),
        ("csv", "gzip", "AAPL-2026-10-01-nbbo.csv.gz"),
        ("ndjson", "none", "AAPL-2026-10-01-nbbo.ndjson"),
        # A dry run has no upstream format to honour, so an encoding it cannot
        # write is delivered as CSV rather than failing after a purchase.
        ("dbn", "none", "AAPL-2026-10-01-nbbo.csv"),
    ],
)
def test_text_encodings_convert_to_a_counted_parquet(tmp_path, encoding, compression, name):
    _, _, files, _, _, evidence = drive(
        tmp_path, {"encoding": encoding, "compression": compression}
    )
    assert files[0]["name"] == name
    assert [entry["rows"] for entry in evidence["manifest"]] == [ROWS_PER_DAY, ROWS_PER_DAY]
    assert all(name.endswith(".parquet") for name in evidence["claimed"])


def test_parquet_is_delivered_and_counted_without_a_conversion(tmp_path):
    _, _, files, _, converted, evidence = drive(tmp_path, {"encoding": "parquet"})
    assert files[0]["name"] == "AAPL-2026-10-01-nbbo.parquet"
    assert converted["staged"] == [entry["name"] for entry in files]
    assert [entry["rows"] for entry in evidence["manifest"]] == [ROWS_PER_DAY, ROWS_PER_DAY]


# -- through the host -------------------------------------------------------


def test_the_host_can_drive_the_whole_job(tmp_path):
    sys.path.insert(0, str(REPO / "tests"))
    from test_host_session import Runtime

    datasets = tmp_path / "datasets"
    datasets.mkdir()
    run = Runtime(REPO / "drivers", datasets)
    try:
        root = datasets / "zzzdryrun" / "NBBO" / "req_test01"
        status, body = run.call(
            "POST",
            "/v1/sessions",
            {
                "protocol": wire.PROTOCOL_VERSION,
                "driver": "zzzdryrun",
                "driver_version": "1",
                "root": str(root),
            },
        )
        assert status == 201, body
        session = body["session_id"]
        status, answered = run.call(
            "POST", "/v1/resolve", {"session_id": session, **{k: WINDOW[k] for k in ("dataset", "symbols", "stype_in")}}
        )
        assert status == 200, answered
        status, job = run.call("POST", "/v1/submit", {"session_id": session, **WINDOW})
        assert status == 200, job
        job_id = job["vendor_job_id"]
        status, files = run.call(
            "POST", "/v1/list_files", {"session_id": session, "vendor_job_id": job_id}
        )
        assert status == 200, files
        status, written = run.call(
            "POST",
            "/v1/fetch",
            {"session_id": session, "vendor_job_id": job_id, "dest": "files/", "files": files["files"]},
        )
        assert status == 200, written
        status, converted = run.call(
            "POST",
            "/v1/convert",
            {"session_id": session, "dest": "files/", "from": "csv", "to": "parquet"},
        )
        assert status == 200, converted
        status, evidence = run.call(
            "POST",
            "/v1/verify",
            {
                "session_id": session,
                "dest": "files/",
                "claimed": written["written"],
                "staged": converted["staged"],
            },
        )
        assert status == 200, evidence
        assert sorted(p.name for p in (root / "files").iterdir()) == sorted(
            evidence["claimed"]
        )
        assert all(entry["rows"] == ROWS_PER_DAY for entry in evidence["manifest"])
    finally:
        run.close()
