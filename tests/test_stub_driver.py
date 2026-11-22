"""The stub artefact's behaviour, without the host in the way.

These are the app's old `stub.py` tests, retargeted at the wire's return shapes.
Kept separate from `test_host_session.py` on purpose: that file proves the
*host's* rules, this one proves the *stub's* beliefs -- and the two are allowed
to fail independently.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

STUB_DIR = Path(__file__).resolve().parents[1] / "drivers" / "stub" / "1"
sys.path.insert(0, str(STUB_DIR))

import stub  # noqa: E402

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


def drive(tmp_path, mode="clean"):
    driver = stub.StubDriver(tmp_path, mode)
    driver.resolve(dataset="GLBX.MDP3", symbols=["ES.c.0"], stype_in="continuous")
    driver.cost(**WINDOW)
    job = driver.submit(**WINDOW)
    driver.poll(vendor_job_id=job["vendor_job_id"], interval_s=1)
    files = driver.list_files(vendor_job_id=job["vendor_job_id"])["files"]
    written = driver.fetch(
        vendor_job_id=job["vendor_job_id"], dest="files/", files=files
    )["written"]
    driver.convert(dest="files/", **{"from": "dbn", "to": "parquet"})
    evidence = driver.verify(
        dest="files/",
        claimed=[f["name"] for f in files],
        staged=written,
    )
    return driver, files, written, evidence


def test_clean_publishes_every_day_it_claimed(tmp_path):
    _, files, written, evidence = drive(tmp_path)
    assert [f["name"] for f in files] == [
        "GLBX.MDP3-2026-10-01.dbn.zst",
        "GLBX.MDP3-2026-10-02.dbn.zst",
    ]
    assert written == [f["name"] for f in files]
    assert evidence["claimed"] == written
    assert [e["name"] for e in evidence["manifest"]] == written
    assert set(evidence["staged"]) == set(written)


def test_short_publishes_less_than_it_claimed(tmp_path):
    _, files, written, evidence = drive(tmp_path, mode="short")
    assert len(files) == 2
    assert len(written) == 1
    # claimed has two, manifest has one: a *scope* shortfall, not an integrity
    # failure. That distinction is classify()'s, exercised in the app.
    assert len(evidence["claimed"]) == 2
    assert len(evidence["manifest"]) == 1
    assert len(evidence["staged"]) == 1


def test_nothing_stages_nothing_and_claims_everything(tmp_path):
    _, files, written, evidence = drive(tmp_path, mode="nothing")
    assert written == []
    assert len(evidence["claimed"]) == len(files)
    # The manifest still says two files were published and none arrived: the
    # *integrity* failure the app reads as a contradiction, not a shortfall.
    assert [e["name"] for e in evidence["manifest"]] == [f["name"] for f in files]
    assert evidence["staged"] == {}


def test_bytes_announce_themselves(tmp_path):
    _, _, written, _ = drive(tmp_path)
    payload = (tmp_path / "files" / written[0]).read_bytes()
    assert payload.startswith(b"STUB: not market data.")
    assert len(payload.splitlines()) == stub.ROWS_PER_FILE + 1


def test_the_same_window_is_the_same_job(tmp_path):
    first = stub.StubDriver(tmp_path / "a")
    second = stub.StubDriver(tmp_path / "b")
    assert first.submit(**WINDOW)["vendor_job_id"] == second.submit(**WINDOW)["vendor_job_id"]


def test_convert_is_a_no_op_on_purpose(tmp_path):
    _, files, written, evidence = drive(tmp_path)
    # A real parquet conversion writes files the vendor manifest does not list;
    # the stub leaves the source alone so `staged` still matches exactly.
    assert written == [f["name"] for f in files]
    assert set(evidence["staged"]) == set(written)


def test_an_unknown_mode_is_refused(tmp_path):
    with pytest.raises(ValueError):
        stub.StubDriver(tmp_path, "sideways")


def test_dest_cannot_escape_the_root(tmp_path):
    driver = stub.StubDriver(tmp_path)
    with pytest.raises(ValueError):
        driver.convert(dest="../../etc", **{"from": "dbn", "to": "parquet"})
