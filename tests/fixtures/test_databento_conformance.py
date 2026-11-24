"""Conformance gaps the vendored fake in `tests/test_databento_driver.py` leaves open.

That file drives the whole chain with a fake SDK. These tests pin three things
it cannot see, because they are about *when* a decision is made and how bytes
are read rather than what the fake answers:

* an encoding this driver cannot convert is refused at ``submit`` -- before the
  vendor client is even constructed -- not discovered at ``convert`` after the
  job was paid for;
* ``convert`` refuses a source format it cannot honour instead of returning the
  CSV files it was given as a successful "conversion" to parquet;
* digests are streamed in chunks, so a multi-gigabyte ``mbo`` day does not have
  to fit in the driver's memory. The digest is still over the vendor's bytes.

Kept beside ``tests/`` rather than inside the artefact so the suite that ships
with the repository runs them, not only whoever installs the driver.
"""

from __future__ import annotations

import hashlib
import sys
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_download_drivers import wire

REPO = Path(__file__).resolve().parents[2]
ARTIFACT = REPO / "drivers" / "databento" / "1"
sys.path.insert(0, str(ARTIFACT))

from databento_driver import (  # noqa: E402
    SUPPORTED_ENCODING,
    DatabentoDriver,
    _file_digest,
    staged_files,
)

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


def test_the_supported_encoding_is_the_one_the_binder_defaults_to():
    """The plan type's default must be the driver's supported encoding, or every
    request would be refused for a reason the binder could have caught."""
    assert SUPPORTED_ENCODING == "dbn"


def test_an_unconvertible_encoding_is_refused_before_the_vendor_is_called(
    monkeypatch, tmp_path
):
    """`csv` is a legal Databento encoding but has no record decoder here, so a
    Job asking for it could never honour its parquet convert step. Refusing at
    submit costs nothing; the client must not even be constructed."""

    class Boom:
        def __init__(self, key: str) -> None:
            raise AssertionError("the vendor was contacted before the encoding check")

    monkeypatch.setenv("DATABENTO_API_KEY", "db-test-key")
    monkeypatch.setitem(sys.modules, "databento", types.SimpleNamespace(Historical=Boom))

    driver = DatabentoDriver(tmp_path, {})
    with pytest.raises(wire.WireError) as caught:
        driver.submit(**{**WINDOW, "encoding": "csv"})

    assert caught.value.code == wire.BAD_REQUEST
    assert "csv" in caught.value.detail
    assert SUPPORTED_ENCODING in caught.value.detail


def test_convert_refuses_a_source_format_it_cannot_convert(tmp_path):
    """A destination of parquet is not enough: the source has to be decodable.
    Reporting the CSV files as the converted landing would make a step that did
    nothing look successful, and would pass the verifier because the manifest
    lists the same names."""
    dest = tmp_path / "files"
    dest.mkdir()
    (dest / "GLBX.MDP3-20261118-ohlcv-1m.csv.zst").write_bytes(b"ts_event,price\n")
    driver = DatabentoDriver(tmp_path, {})

    with pytest.raises(wire.WireError) as caught:
        driver.convert(dest="files", **{"from": "csv", "to": "parquet"})

    assert caught.value.code == wire.BAD_REQUEST
    assert "csv" in caught.value.detail
    # Nothing was mutated: the file is still a CSV, and no parquet appeared.
    assert sorted(p.name for p in dest.iterdir()) == [
        "GLBX.MDP3-20261118-ohlcv-1m.csv.zst"
    ]


def test_convert_still_refuses_a_target_it_cannot_write(tmp_path):
    dest = tmp_path / "files"
    dest.mkdir()
    driver = DatabentoDriver(tmp_path, {})
    with pytest.raises(wire.WireError) as caught:
        driver.convert(dest="files", **{"from": "dbn", "to": "csv"})
    assert caught.value.code == wire.BAD_REQUEST


def test_a_digest_is_streamed_across_chunk_boundaries(tmp_path):
    """The size and digest must be over the whole file, not one chunk; chunk
    size is an implementation detail, so a boundary-crossing file is the case
    that would silently truncate."""
    payload = bytes(range(256)) * 10_240  # 2.5 MiB, several chunks
    path = tmp_path / "big.bin"
    path.write_bytes(payload)

    digest, size = _file_digest(path)
    assert (digest, size) == (hashlib.sha256(payload).hexdigest(), len(payload))

    # Any chunk size gives the same answer, including one that does not divide
    # the file evenly.
    for chunk in (1, 7, 65_536):
        assert _file_digest(path, chunk) == (digest, size)


def test_staged_files_reads_a_parquet_without_slurping_it(tmp_path, monkeypatch):
    """The landing reader is the last place that could OOM the driver: a
    converted mbo day is huge, and its row count comes from the footer while its
    digest streams. `Path.read_bytes` must not be involved."""
    table = pa.table({"price": [1.0, 2.0, 3.0], "size": [10, 20, 30]})
    path = tmp_path / "GLBX.MDP3-20261118-ohlcv-1m.parquet"
    pq.write_table(table, str(path))
    expected = hashlib.sha256(path.read_bytes()).hexdigest()

    def refuse(self: Path) -> bytes:
        raise AssertionError(f"read_bytes slurped {self.name}")

    monkeypatch.setattr(Path, "read_bytes", refuse)
    staged = staged_files(tmp_path)

    assert staged[path.name]["rows"] == 3
    assert staged[path.name]["sha256"] == expected
    assert staged[path.name]["bytes"] == path.stat().st_size


def test_staged_files_still_counts_a_text_sidecars_rows(tmp_path):
    """The refactor that streams digests must not change what a small text file
    reports: its line count minus the header, which is how the sidecars are
    read."""
    (tmp_path / "condition.json").write_bytes(b'{"condition": "ok"}\n')
    staged = staged_files(tmp_path)
    assert staged["condition.json"]["rows"] == 0
    assert staged["condition.json"]["bytes"] == 20


class _Batch:
    """Only what `fetch` uses: the SDK writes into `<output_dir>/<job_id>/`."""

    def __init__(self, served: dict[str, bytes]) -> None:
        self.served = served

    def download(self, job_id, output_dir, filename_to_download, keep_zip=False):
        assert keep_zip is False
        landed = Path(output_dir, job_id, filename_to_download)
        landed.parent.mkdir(parents=True, exist_ok=True)
        landed.write_bytes(self.served[filename_to_download])
        return [landed]


def _driver_with(tmp_path: Path, served: dict[str, bytes]) -> DatabentoDriver:
    driver = DatabentoDriver(tmp_path, {})
    # Bypass the vendor client: these tests are about the bytes on disk.
    driver._db = types.SimpleNamespace()
    driver._client = types.SimpleNamespace(batch=_Batch(served))
    return driver


def test_fetch_hashes_a_file_larger_than_one_chunk(tmp_path):
    payload = bytes(range(256)) * 10_240
    name = "GLBX.MDP3-20261118-ohlcv-1m.dbn.zst"
    driver = _driver_with(tmp_path, {name: payload})

    written = driver.fetch(
        vendor_job_id="job-1",
        dest="files",
        files=[{"name": name, "sha256": hashlib.sha256(payload).hexdigest()}],
    )["written"]

    assert written == [name]
    landed = driver._downloaded[name]
    assert landed["verified"] is True
    assert landed["bytes"] == len(payload)
    assert landed["sha256"] == hashlib.sha256(payload).hexdigest()
    # Hoisted out of the SDK's job subdirectory, as the rest of the chain expects.
    assert (tmp_path / "files" / name).is_file()
    assert not (tmp_path / "files" / "job-1").exists()


def test_a_tampered_download_is_recorded_as_unverified(tmp_path):
    name = "GLBX.MDP3-20261118-ohlcv-1m.dbn.zst"
    driver = _driver_with(tmp_path, {name: b"not the vendor's bytes"})
    driver.fetch(
        vendor_job_id="job-1",
        dest="files",
        files=[{"name": name, "sha256": hashlib.sha256(b"the vendor's bytes").hexdigest()}],
    )
    assert driver._downloaded[name]["verified"] is False
