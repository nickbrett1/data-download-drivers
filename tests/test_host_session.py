"""The host, end to end, over real HTTP against real child processes.

This is phase A's exit test in unit form: create a session, drive all eight
verbs, and land a file that says it is not data. Plus the four rules the host
owns and the driver must not be trusted with -- the budget, `dest` containment,
`submit` idempotency, and a deadline that kills rather than waits.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from data_download_drivers import host, wire

REPO_DRIVERS = Path(__file__).resolve().parents[1] / "drivers"
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


class Runtime:
    def __init__(self, drivers_dir: Path, datasets_dir: Path, cap: int | None = None):
        self.manager = host.SessionManager(
            drivers_dir=drivers_dir,
            datasets_dir=datasets_dir,
            verb_deadline_cap_s=cap,
        )
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), host.make_handler(self.manager, None)
        )
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def call(self, method: str, path: str, payload: dict | None = None):
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read() or b"{}")

    def close(self):
        self.manager.shutdown()
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def runtime(tmp_path):
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    run = Runtime(REPO_DRIVERS, datasets)
    yield run, datasets
    run.close()


def open_session(run, datasets, root_name="req_test01", options=None):
    root = datasets / "databento" / "GLBX.MDP3" / root_name
    payload = {
        "protocol": wire.PROTOCOL_VERSION,
        "driver": "stub",
        "driver_version": "1",
        "root": str(root),
    }
    if options:
        payload["driver_options"] = options
    status, body = run.call("POST", "/v1/sessions", payload)
    assert status == 201, body
    return body["session_id"], root


def test_healthz_and_discovery(runtime):
    run, _ = runtime
    status, body = run.call("GET", "/healthz")
    assert status == 200 and body["protocol"] == wire.PROTOCOL_VERSION

    status, body = run.call("GET", "/v1/drivers")
    assert status == 200
    assert body["drivers"] == [
        {
            "name": "stub",
            "version": "1",
            "vendor": "Stub",
            "protocol": 1,
            "installed_at": "2026-11-22T00:00:00Z",
        }
    ]


def test_the_eight_verbs_land_a_file_that_says_it_is_not_data(runtime):
    run, datasets = runtime
    sid, root = open_session(run, datasets)

    status, body = run.call(
        "POST", "/v1/resolve",
        {"session_id": sid, "dataset": "GLBX.MDP3", "symbols": ["ES.c.0"], "stype_in": "continuous"},
    )
    assert status == 200 and body["resolved"] == 1

    status, body = run.call("POST", "/v1/cost", {"session_id": sid, **WINDOW})
    assert status == 200 and body["usd"] == 12.34

    status, body = run.call(
        "POST", "/v1/submit",
        {"session_id": sid, **WINDOW, "authorised_usd": 20.0, "declared_usd": 12.34},
    )
    assert status == 200, body
    vendor_job_id = body["vendor_job_id"]
    assert vendor_job_id.startswith("stub_")

    status, body = run.call(
        "POST", "/v1/poll", {"session_id": sid, "vendor_job_id": vendor_job_id, "interval_s": 1}
    )
    assert status == 200 and body["state"] == "done"

    status, body = run.call(
        "POST", "/v1/list_files", {"session_id": sid, "vendor_job_id": vendor_job_id}
    )
    assert status == 200
    names = [f["name"] for f in body["files"]]
    assert names == ["GLBX.MDP3-2026-10-01.dbn.zst", "GLBX.MDP3-2026-10-02.dbn.zst"]

    status, body = run.call(
        "POST", "/v1/fetch",
        {"session_id": sid, "vendor_job_id": vendor_job_id, "dest": "files/", "files": body["files"]},
    )
    assert status == 200 and len(body["written"]) == 2

    landed = root / "files" / names[0]
    assert landed.is_file()
    assert landed.read_bytes().startswith(b"STUB: not market data.")

    status, body = run.call(
        "POST", "/v1/convert", {"session_id": sid, "dest": "files/", "from": "dbn", "to": "parquet"}
    )
    assert status == 200 and body["staged"] == names

    status, evidence = run.call(
        "POST", "/v1/verify",
        {"session_id": sid, "dest": "files/", "claimed": names, "staged": names},
    )
    assert status == 200
    # Evidence, not a verdict: three quantities, and they agree here.
    assert [e["name"] for e in evidence["manifest"]] == names
    assert evidence["claimed"] == names
    assert set(evidence["staged"]) == set(names)
    assert all(evidence["staged"][n]["rows"] == 1000 for n in names)

    status, body = run.call("DELETE", f"/v1/sessions/{sid}")
    assert status == 200 and body["driver"] == "stub"


def test_a_second_submit_replays_and_does_not_buy_twice(runtime):
    run, datasets = runtime
    sid, _ = open_session(run, datasets)
    first = run.call("POST", "/v1/submit", {"session_id": sid, **WINDOW})
    second = run.call("POST", "/v1/submit", {"session_id": sid, **WINDOW})
    assert first[1]["vendor_job_id"] == second[1]["vendor_job_id"]
    assert second[1].get("replayed") is True
    assert "replayed" not in first[1]


def test_a_replay_reports_the_ceiling_it_was_authorised_under(runtime):
    """A repeat with a *lower* ceiling must not read as "your limit was accepted".

    The purchase already happened under the first ceiling; refusing here would
    claim nothing was bought, which would be false. So the replay carries the
    numbers instead.
    """
    run, datasets = runtime
    sid, _ = open_session(run, datasets)
    run.call(
        "POST", "/v1/submit",
        {"session_id": sid, **WINDOW, "authorised_usd": 20.0, "declared_usd": 12.34},
    )
    status, body = run.call(
        "POST", "/v1/submit",
        {"session_id": sid, **WINDOW, "authorised_usd": 0.10, "declared_usd": 12.34},
    )
    assert status == 200
    assert body["replayed"] is True
    assert body["authorised_usd"] == 20.0
    assert body["declared_usd"] == 12.34


def test_a_submit_over_the_ceiling_buys_nothing(runtime):
    run, datasets = runtime
    sid, root = open_session(run, datasets)
    status, body = run.call(
        "POST", "/v1/submit",
        {"session_id": sid, **WINDOW, "authorised_usd": 1.00, "declared_usd": 12.34},
    )
    assert status == 422
    assert body["error"]["code"] == wire.BUDGET_REFUSED
    # Nothing was bought and nothing was staged.
    assert run.manager.get(sid).submitted is None
    assert not (root / "files").exists()


def test_dest_cannot_climb_out(runtime):
    run, datasets = runtime
    sid, _ = open_session(run, datasets)
    status, body = run.call(
        "POST", "/v1/convert", {"session_id": sid, "dest": "../../etc", "from": "dbn", "to": "parquet"}
    )
    assert status == 400 and body["error"]["code"] == wire.BAD_REQUEST


def test_an_unknown_session_is_404(runtime):
    run, _ = runtime
    status, body = run.call("POST", "/v1/poll", {"session_id": "nope", "vendor_job_id": "v1"})
    assert status == 404 and body["error"]["code"] == wire.UNKNOWN_SESSION


def test_a_missing_driver_is_409(runtime):
    run, datasets = runtime
    status, body = run.call(
        "POST", "/v1/sessions",
        {
            "protocol": wire.PROTOCOL_VERSION,
            "driver": "not-installed",
            "driver_version": "9",
            "root": str(datasets / "v" / "d" / "req_x"),
        },
    )
    assert status == 409 and body["error"]["code"] == wire.DRIVER_MISSING


def test_a_protocol_mismatch_is_refused(runtime):
    run, datasets = runtime
    status, body = run.call(
        "POST", "/v1/sessions",
        {
            "protocol": wire.PROTOCOL_VERSION + 1,
            "driver": "stub",
            "driver_version": "1",
            "root": str(datasets / "v" / "d" / "req_x"),
        },
    )
    assert status == 400 and "unsupported" in body["error"]["detail"]


def test_a_root_outside_the_dataset_store_is_refused(runtime):
    run, _ = runtime
    status, body = run.call(
        "POST", "/v1/sessions",
        {
            "protocol": wire.PROTOCOL_VERSION,
            "driver": "stub",
            "driver_version": "1",
            "root": "/etc",
        },
    )
    assert status == 400 and "outside" in body["error"]["detail"]


def test_a_hanging_driver_is_killed_at_the_deadline(tmp_path):
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    slow = tmp_path / "drivers" / "slow" / "1"
    slow.mkdir(parents=True)
    (slow / "manifest.json").write_text(
        json.dumps({"name": "slow", "version": "1", "vendor": "Slow", "protocol": 1})
    )
    (slow / "driver.py").write_text(
        "import sys, time\n"
        "sys.stderr.write('slow driver up\\n'); sys.stderr.flush()\n"
        "time.sleep(30)\n"
    )

    run = Runtime(tmp_path / "drivers", datasets, cap=1)
    try:
        status, body = run.call(
            "POST", "/v1/sessions",
            {
                "protocol": wire.PROTOCOL_VERSION,
                "driver": "slow",
                "driver_version": "1",
                "root": str(datasets / "v" / "d" / "req_slow"),
            },
        )
        assert status == 201, body
        sid = body["session_id"]
        session = run.manager.get(sid)

        status, body = run.call(
            "POST", "/v1/poll", {"session_id": sid, "vendor_job_id": "v1"}
        )
        assert status == 504
        assert body["error"]["code"] == wire.TIMEOUT
        # Killed, not left behind: the point of the deadline.
        assert session.closed or session.channel.proc.poll() is not None
    finally:
        run.close()


PROBE_DRIVER = '''
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_download_drivers.child import serve


class Probe:
    """Reports what the child process can see. Never touches a file."""

    def __init__(self, root, options):
        self.root = root

    def _seen(self):
        return sorted(
            key
            for key in ("DRIVER_RUNTIME_TOKEN", "DATABENTO_API_KEY")
            if key in os.environ
        )

    def resolve(self, **kwargs):
        return {"symbols": list(kwargs.get("symbols") or []), "resolved": 1, "seen": self._seen()}

    def cost(self, **kwargs):
        return {"usd": 0.0}

    def submit(self, **kwargs):
        return {"vendor_job_id": "probe_1", "state": "submitted"}

    def poll(self, **kwargs):
        return {"state": "done"}

    def list_files(self, **kwargs):
        return {"files": []}

    def fetch(self, **kwargs):
        return {"written": []}

    def convert(self, **kwargs):
        return {"staged": []}

    def verify(self, **kwargs):
        return {"claimed": [], "manifest": [], "staged": {}, "seen": self._seen()}


def build(root, options):
    return Probe(root, options)


if __name__ == "__main__":
    raise SystemExit(serve(build))
'''


def _install_probe(drivers_dir: Path) -> None:
    probe = drivers_dir / "probe" / "1"
    probe.mkdir(parents=True)
    (probe / "driver.py").write_text(PROBE_DRIVER, encoding="utf-8")
    (probe / "manifest.json").write_text(
        json.dumps(
            {
                "name": "probe",
                "version": "1",
                "vendor": "Probe",
                "protocol": wire.PROTOCOL_VERSION,
                "entrypoint": "driver.py",
                "installed_at": "2026-11-22T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )


def test_the_child_cannot_read_the_runtimes_own_token(tmp_path, monkeypatch):
    """The token protects the host from callers; the child must not become one.

    A driver artefact is untrusted code. If it inherited `DRIVER_RUNTIME_TOKEN`
    it could POST /v1/sessions for any *other* installed vendor and drive that
    vendor's credential -- so the token is scrubbed before spawn. The vendor key
    is the opposite case: scrubbing it would break the only thing that needs it,
    so this test asserts it is still there, which is what stops the fix from
    overshooting.
    """
    monkeypatch.setenv("DRIVER_RUNTIME_TOKEN", "do-not-leak-me")
    monkeypatch.setenv("DATABENTO_API_KEY", "db-probe-key")

    drivers = tmp_path / "drivers"
    datasets = tmp_path / "datasets"
    datasets.mkdir()
    _install_probe(drivers)

    run = Runtime(drivers, datasets)
    try:
        status, body = run.call(
            "POST",
            "/v1/sessions",
            {
                "protocol": wire.PROTOCOL_VERSION,
                "driver": "probe",
                "driver_version": "1",
                "root": str(datasets / "probe" / "GLBX.MDP3" / "req_probe"),
            },
        )
        assert status == 201, body
        sid = body["session_id"]

        status, body = run.call(
            "POST",
            "/v1/resolve",
            {
                "session_id": sid,
                "dataset": "GLBX.MDP3",
                "symbols": ["ES.c.0"],
                "stype_in": "continuous",
            },
        )
        assert status == 200, body
        assert "DRIVER_RUNTIME_TOKEN" not in body["seen"]
        # Scrubbed the token, not the environment: the vendor key is the point.
        assert body["seen"] == ["DATABENTO_API_KEY"]

        # And it is not merely hidden from this verb.
        status, body = run.call("POST", "/v1/verify", {
            "session_id": sid,
            "dest": "files/",
            "claimed": [],
            "staged": [],
        })
        assert status == 200, body
        assert body["seen"] == ["DATABENTO_API_KEY"]
    finally:
        run.close()
