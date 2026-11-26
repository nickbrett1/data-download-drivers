"""Boundaries the market_data_window driver must hold on its own.

The host enforces the wire's containment rules, but the driver is also called
directly (by these tests, and by anyone `diff`ing a Job by hand). A rule that
lives only on the trusted side is a suggestion when the driver is the only thing
running, so the same three boundaries are pinned here:

* a manifest entry or job id that is a path is refused *before* the vendor is
  asked to write anything;
* a catalogue that answers something other than a range is "unknown", not an
  `AttributeError` dressed up as a driver failure;
* a malformed `interval_s` is a hint, not a reason for `poll` to crash.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from data_download_drivers import wire

REPO = Path(__file__).resolve().parents[2]
ARTIFACT = REPO / "drivers" / "databento" / "2"

_spec = importlib.util.spec_from_file_location(
    "databento_driver_v2_hardening", ARTIFACT / "databento_driver.py"
)
_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _module
_spec.loader.exec_module(_module)

DatabentoDriver = _module.DatabentoDriver


class _Batch:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def download(self, **kwargs):
        self.calls.append(kwargs["filename_to_download"])
        raise AssertionError("the vendor must not be asked to write a path")


def _driver(root: Path, batch: _Batch) -> DatabentoDriver:
    driver = DatabentoDriver(root, {})
    # Bypass the credential/SDK import: these tests are about the boundary.
    driver._db = object()
    driver._client = type("Client", (), {"batch": batch})()
    return driver


@pytest.mark.parametrize(
    "name",
    ["../escape.dbn.zst", "sub/dir.dbn.zst", "..", "/etc/passwd", "a\\b.dbn.zst"],
)
def test_fetch_refuses_a_manifest_entry_that_is_a_path(tmp_path, name):
    batch = _Batch()
    driver = _driver(tmp_path, batch)

    with pytest.raises(wire.WireError) as caught:
        driver.fetch(vendor_job_id="job-1", dest="files", files=[{"name": name}])

    assert caught.value.code == wire.BAD_REQUEST
    assert batch.calls == []
    # Nothing was created outside the landing directory either.
    assert not (tmp_path / "escape.dbn.zst").exists()


def test_fetch_refuses_a_job_id_that_is_a_path(tmp_path):
    batch = _Batch()
    driver = _driver(tmp_path, batch)

    with pytest.raises(wire.WireError) as caught:
        driver.fetch(
            vendor_job_id="../job",
            dest="files",
            files=[{"name": "GLBX.MDP3-20261118-ohlcv-1m.dbn.zst"}],
        )

    assert caught.value.code == wire.BAD_REQUEST
    assert batch.calls == []


class _Metadata:
    def list_publishers(self):
        return [
            {"dataset": "GLBX.MDP3", "venue": "GLBX", "description": "CME Globex"}
        ]

    def get_dataset_range(self, dataset):
        # A shape the SDK does not promise: not a dict.
        return ["2010-06-06", "2026-11-23"]

    def list_schemas(self, dataset):
        return ["ohlcv-1m"]


def test_describe_treats_a_non_dict_range_as_unavailable(tmp_path):
    driver = DatabentoDriver(tmp_path, {})
    driver._db = object()
    driver._client = type("Client", (), {"metadata": _Metadata()})()

    answer = driver.describe(dataset="GLBX.MDP3")

    assert answer["described"] is True
    assert answer["range"]["start"] is None
    assert answer["range"]["end"] is None
    assert answer["range"]["unavailable"]
    assert answer["schemas"] == ["ohlcv-1m"]


class _PollingBatch:
    def __init__(self) -> None:
        self.details = 0

    def get_job_details(self, job_id):
        self.details += 1
        return {"state": "queued"}


def test_poll_survives_an_unparseable_interval(tmp_path, monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(_module.time, "sleep", lambda seconds: slept.append(seconds))
    batch = _PollingBatch()
    driver = DatabentoDriver(tmp_path, {})
    driver._db = object()
    driver._client = type("Client", (), {"batch": batch})()

    answer = driver.poll(vendor_job_id="job-1", interval_s="not-a-number")

    assert answer["state"] == "queued"
    assert batch.details == 1
    # The fallback is the driver's own default, clamped to what remains of the
    # poll budget rather than slept on top of the vendor call.
    assert len(slept) == 1
    assert 0 < slept[0] <= _module.POLL_BUDGET_S
