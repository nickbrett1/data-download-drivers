"""The wire's own tests: shapes, containment, and the codes.

Nothing here starts a process. These are the rules a badly-behaved driver (or a
badly-behaved client) cannot talk its way past, so they are asserted directly.
"""

from __future__ import annotations

import pytest

from data_download_drivers import wire


def args(**kwargs):
    base = {"session_id": "s1"}
    base.update(kwargs)
    return base


def test_a_verb_must_be_one_of_the_eight():
    with pytest.raises(wire.WireError) as caught:
        wire.validate("delete", args())
    assert caught.value.code == wire.BAD_REQUEST
    assert caught.value.status == 400


def test_session_id_is_required():
    with pytest.raises(wire.WireError) as caught:
        wire.validate("poll", {"vendor_job_id": "v1"})
    assert "session_id" in caught.value.detail


def test_symbols_must_be_a_list_of_strings():
    with pytest.raises(wire.WireError):
        wire.validate(
            "resolve",
            args(dataset="GLBX.MDP3", symbols="ES.c.0", stype_in="continuous"),
        )


def test_a_required_field_absent_is_named():
    with pytest.raises(wire.WireError) as caught:
        wire.validate("submit", args(dataset="GLBX.MDP3"))
    assert "schema" in caught.value.detail or "symbols" in caught.value.detail


def test_dest_may_not_be_absolute():
    with pytest.raises(wire.WireError) as caught:
        wire.validate("convert", args(dest="/etc", **{"from": "dbn", "to": "parquet"}))
    assert "absolute" in caught.value.detail


def test_dest_may_not_climb_out():
    with pytest.raises(wire.WireError) as caught:
        wire.validate("verify", args(dest="../x", claimed=[], staged=[]))
    assert "climbs out" in caught.value.detail


def test_a_file_name_may_not_be_a_path():
    with pytest.raises(wire.WireError) as caught:
        wire.validate(
            "fetch",
            args(
                vendor_job_id="v1",
                dest="files/",
                files=[{"name": "../../etc/passwd"}],
            ),
        )
    assert "bare name" in caught.value.detail


def test_a_good_payload_passes_and_is_normalised():
    out = wire.validate(
        "fetch",
        args(vendor_job_id="v1", dest="files/", files=[{"name": "a.dbn.zst", "sha256": "x"}]),
    )
    assert out["dest"] == "files/"
    assert out["files"][0]["name"] == "a.dbn.zst"


def test_codes_map_to_statuses_and_none_are_invented():
    for status in wire.STATUS.values():
        assert 400 <= status <= 599
    assert wire.STATUS[wire.BUDGET_REFUSED] == 422
    assert wire.STATUS[wire.DRIVER_MISSING] == 409
    assert wire.STATUS[wire.TIMEOUT] == 504


def test_costing_is_allowed_to_be_slow():
    """A price is free and idempotent, so the deadline bounds a hang, not a scan.

    Databento prices a window server-side: one day of ES trades measured 0.1 s to
    28 s across repeated calls, and 30 s turned a working vendor into
    "cost exceeded 30s; the driver was killed".
    """
    assert wire.DEADLINES["cost"] >= 60
