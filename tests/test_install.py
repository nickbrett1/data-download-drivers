"""Installing a driver over HTTP, end to end.

The runtime owns the drivers volume, so the control plane never writes there.
That makes the install route the one hole in a read-only mount, and each test
here is one thing that hole could let through: a tree that escapes its
directory, a manifest that claims a different driver, a silent overwrite, and a
half-written artefact left visible to discovery.

The first test is the point of the whole module: discovery re-globs on every
request, so the driver installed by one HTTP call drives a session opened by the
next one, with no restart and no reload route.
"""

from __future__ import annotations

import base64
import hashlib
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from data_download_drivers import host, install, wire

REPO_DRIVERS = Path(__file__).resolve().parents[1] / "drivers"


class Runtime:
    def __init__(self, drivers_dir: Path, datasets_dir: Path, token: str | None = None):
        self.manager = host.SessionManager(
            drivers_dir=drivers_dir, datasets_dir=datasets_dir
        )
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), host.make_handler(self.manager, token)
        )
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def call(self, method: str, path: str, payload: dict | None = None, token=None):
        data = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers=headers
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
def run(tmp_path):
    runtime = Runtime(tmp_path / "drivers", tmp_path / "datasets")
    yield runtime
    runtime.close()


def payload_for(files: dict[str, bytes], **overrides):
    body = {
        "name": "probe",
        "version": "1",
        "vendor": "Probe",
        "files": {name: base64.b64encode(data).decode() for name, data in files.items()},
    }
    body.update(overrides)
    return body


def manifest(name="probe", version="1", **fields):
    data = {
        "name": name,
        "version": version,
        "vendor": "Probe",
        "protocol": wire.PROTOCOL_VERSION,
        "entrypoint": "driver.py",
        "installed_at": "2026-11-25T00:00:00Z",
    }
    data.update(fields)
    return json.dumps(data).encode()


DRIVER = b"import sys\nsys.stderr.write('probe driver started\\n')\n"


def a_tree(name="probe", version="1", **fields) -> dict[str, bytes]:
    return {
        "manifest.json": manifest(name, version, **fields),
        "driver.py": DRIVER,
    }


def test_an_installed_driver_is_drivable_by_the_very_next_request(run, tmp_path):
    files = {
        name: path.read_bytes()
        for name, path in {
            "manifest.json": REPO_DRIVERS / "stub" / "1" / "manifest.json",
            "driver.py": REPO_DRIVERS / "stub" / "1" / "driver.py",
            "stub.py": REPO_DRIVERS / "stub" / "1" / "stub.py",
        }.items()
    }
    status, body = run.call(
        "POST", "/v1/drivers/install", payload_for(files, name="stub", vendor="Stub")
    )
    assert status == 200, body
    assert body["name"] == "stub" and body["version"] == "1"
    assert body["sha256"] == install.digest_of(files)

    # Discovery is a glob on every request, so no restart is involved.
    status, listed = run.call("GET", "/v1/drivers")
    assert status == 200
    assert [d["name"] for d in listed["drivers"]] == ["stub"]

    # And a session opened against it actually runs the code that landed.
    datasets = tmp_path / "datasets"
    datasets.mkdir(exist_ok=True)
    status, session = run.call(
        "POST",
        "/v1/sessions",
        {
            "protocol": wire.PROTOCOL_VERSION,
            "driver": "stub",
            "driver_version": "1",
            "root": str(datasets / "stub" / "req_1"),
        },
    )
    assert status == 201, session
    status, answer = run.call(
        "POST",
        "/v1/describe",
        {"session_id": session["session_id"], "dataset": "ANY"},
    )
    assert status == 200, answer


def test_installing_over_a_version_that_exists_is_refused(run, tmp_path):
    files = a_tree()
    assert run.call("POST", "/v1/drivers/install", payload_for(files))[0] == 200

    status, body = run.call(
        "POST", "/v1/drivers/install", payload_for({**files, "driver.py": b"different"})
    )
    assert status == 409
    assert body["error"]["code"] == wire.INSTALL_REFUSED
    # The refusal names the digest, so a caller that means it can prove it.
    first = install.digest_of(files)
    assert first in body["error"]["detail"]
    assert (tmp_path / "drivers" / "probe" / "1" / "driver.py").read_bytes() == DRIVER


def test_replace_sha256_is_how_a_caller_says_it_installed_this(run, tmp_path):
    files = a_tree()
    status, body = run.call("POST", "/v1/drivers/install", payload_for(files))
    assert status == 200

    replacement = {**files, "driver.py": b"# second turn\n"}
    status, again = run.call(
        "POST",
        "/v1/drivers/install",
        payload_for(replacement, replace_sha256=body["sha256"]),
    )
    assert status == 200, again
    assert again["replaced"] is True
    assert again["sha256"] == install.digest_of(replacement)
    target = tmp_path / "drivers" / "probe" / "1"
    assert (target / "driver.py").read_bytes() == b"# second turn\n"
    # No temporary directory survived the replacement.
    assert [p.name for p in (tmp_path / "drivers").iterdir() if p.name.startswith(".")] == []


def test_a_replace_sha256_that_does_not_match_is_refused(run):
    files = a_tree()
    assert run.call("POST", "/v1/drivers/install", payload_for(files))[0] == 200
    status, body = run.call(
        "POST", "/v1/drivers/install", payload_for(files, replace_sha256="0" * 64)
    )
    assert status == 409 and body["error"]["code"] == wire.INSTALL_REFUSED


@pytest.mark.parametrize(
    "name",
    ["../evil.py", "/etc/passwd", "a/../../b.py", "", "a\\b.py", "./driver.py"],
)
def test_a_path_that_is_not_plainly_inside_the_artefact_is_refused(name):
    files = a_tree()
    files[name] = b"x"
    with pytest.raises(wire.WireError) as caught:
        install.decode(payload_for(files)["files"])
    assert caught.value.code == wire.BAD_REQUEST


def test_a_manifest_that_names_another_driver_is_refused(run, tmp_path):
    status, body = run.call(
        "POST", "/v1/drivers/install", payload_for(a_tree(name="databento"))
    )
    assert status == 409
    assert "databento" in body["error"]["detail"]
    assert not (tmp_path / "drivers" / "probe").exists()


def test_a_tree_without_a_manifest_is_refused(run):
    files = {"driver.py": DRIVER}
    status, body = run.call("POST", "/v1/drivers/install", payload_for(files))
    assert status == 400
    assert "manifest" in body["error"]["detail"]


def test_an_entrypoint_that_is_not_in_the_tree_is_refused(run):
    files = a_tree(entrypoint="missing.py")
    status, body = run.call("POST", "/v1/drivers/install", payload_for(files))
    assert status == 409
    assert "missing.py" in body["error"]["detail"]


def test_a_driver_for_another_protocol_is_refused(run):
    files = a_tree(protocol=99)
    status, body = run.call("POST", "/v1/drivers/install", payload_for(files))
    assert status == 409
    assert "protocol" in body["error"]["detail"]


def test_a_refusal_leaves_nothing_behind(run, tmp_path):
    files = a_tree(name="databento")
    run.call("POST", "/v1/drivers/install", payload_for(files))
    drivers = tmp_path / "drivers"
    assert list(drivers.glob("*")) == []


def test_uninstall_removes_only_the_install_its_digest_names(run, tmp_path):
    files = a_tree()
    status, body = run.call("POST", "/v1/drivers/install", payload_for(files))
    assert status == 200
    sha = body["sha256"]
    assert [d["name"] for d in run.call("GET", "/v1/drivers")[1]["drivers"]] == [
        "probe"
    ]

    status, removed = run.call("DELETE", f"/v1/drivers/probe/1?sha256={sha}")
    assert status == 200, removed
    assert removed["name"] == "probe"
    assert removed["version"] == "1"
    assert removed["vendor"] == "Probe"
    assert removed["sha256"] == sha
    assert not (tmp_path / "drivers" / "probe" / "1").exists()
    # Discovery re-globs, so the next listing no longer sees it.
    assert run.call("GET", "/v1/drivers")[1]["drivers"] == []


def test_uninstall_accepts_the_digest_in_the_body(run, tmp_path):
    files = a_tree()
    sha = run.call("POST", "/v1/drivers/install", payload_for(files))[1]["sha256"]
    status, removed = run.call(
        "DELETE", "/v1/drivers/probe/1", {"sha256": sha}
    )
    assert status == 200, removed
    assert not (tmp_path / "drivers" / "probe" / "1").exists()


def test_uninstall_refuses_a_digest_that_is_not_the_install(run, tmp_path):
    files = a_tree()
    assert run.call("POST", "/v1/drivers/install", payload_for(files))[0] == 200
    status, body = run.call("DELETE", f"/v1/drivers/probe/1?sha256={'0' * 64}")
    assert status == 409
    assert body["error"]["code"] == wire.INSTALL_REFUSED
    assert "not the install you recorded" in body["error"]["detail"]
    # The tree is untouched.
    assert (tmp_path / "drivers" / "probe" / "1" / "driver.py").read_bytes() == DRIVER


def test_uninstall_of_an_unknown_install_is_404(run):
    status, body = run.call("DELETE", f"/v1/drivers/ghost/9?sha256={'0' * 64}")
    assert status == 404
    assert body["error"]["code"] == wire.NOT_FOUND


def test_uninstall_without_a_digest_is_refused(run, tmp_path):
    files = a_tree()
    assert run.call("POST", "/v1/drivers/install", payload_for(files))[0] == 200
    status, body = run.call("DELETE", "/v1/drivers/probe/1")
    assert status == 400
    assert body["error"]["code"] == wire.BAD_REQUEST
    assert (tmp_path / "drivers" / "probe" / "1").exists()


def test_uninstall_cannot_climb_out_of_the_drivers_root(run, tmp_path):
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "1").mkdir()
    status, _ = run.call(
        "DELETE", f"/v1/drivers/..%2foutside/1?sha256={'0' * 64}"
    )
    assert status in (400, 404)
    assert (tmp_path / "outside" / "1").is_dir()


def test_the_uninstall_route_needs_the_token(tmp_path):
    runtime = Runtime(tmp_path / "drivers", tmp_path / "datasets", token="s3cret")
    try:
        files = a_tree()
        sha = runtime.call(
            "POST", "/v1/drivers/install", payload_for(files), token="s3cret"
        )[1]["sha256"]

        status, _ = runtime.call("DELETE", f"/v1/drivers/probe/1?sha256={sha}")
        assert status == 401
        assert (tmp_path / "drivers" / "probe" / "1").is_dir()

        status, body = runtime.call(
            "DELETE", f"/v1/drivers/probe/1?sha256={sha}", token="s3cret"
        )
        assert status == 200, body
    finally:
        runtime.close()


def test_the_install_route_needs_the_token(tmp_path):
    runtime = Runtime(tmp_path / "drivers", tmp_path / "datasets", token="s3cret")
    try:
        files = a_tree()
        status, body = runtime.call("POST", "/v1/drivers/install", payload_for(files))
        assert status == 401
        assert not (tmp_path / "drivers" / "probe").exists()

        status, body = runtime.call(
            "POST", "/v1/drivers/install", payload_for(files), token="s3cret"
        )
        assert status == 200, body
    finally:
        runtime.close()


def test_the_digest_covers_names_as_well_as_bytes():
    # Byte-compatible with `data_download.driver_build.artifact_digest`: names,
    # a NUL, the bytes, a NUL, in sorted order.
    expected = hashlib.sha256(b"a.py\0one\0b.py\0two\0").hexdigest()
    assert install.digest_of({"b.py": b"two", "a.py": b"one"}) == expected
    assert install.digest_of({"a.py": b"one"}) != install.digest_of({"b.py": b"one"})
