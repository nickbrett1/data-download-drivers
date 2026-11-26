"""Installing a driver: the one write into the drivers volume.

Discovery re-globs `drivers/*/*/manifest.json` on **every** request
(`SessionManager.artifacts()` is literally `discover(self.drivers_dir)`), so an
artefact that lands here is drivable by the next request with no restart and no
reload route. That is the whole reason this module exists in the runtime rather
than in the control plane: the process that runs model-authored code is the
process that owns the directory it runs from, and the control plane can keep its
mount read-only.

The payload is the tree **as data**, not an archive:

    POST /v1/drivers/install
    {"name": "zzz", "version": "1", "vendor": "Zzz",
     "files": {"manifest.json": "<base64>", "driver.py": "<base64>"},
     "replace_sha256": "<digest of the install this replaces>"}

A tarball would need extraction filtering, link handling and mode bits, and it
would put a second parser in front of the one thing we already have to validate
(the manifest). A map of relative paths to bytes has no headers, no links and no
modes: each path is checked against the same containment rule as `wire.dest`,
and the bytes are written exactly once.

Two things this refuses rather than repairs: an artefact whose manifest
disagrees with the name and version it is installed under, and an install over a
version that already exists unless `replace_sha256` proves the caller installed
it. A reinstall is a deliberate act; silently replacing an artefact is how two
different drivers end up claiming one vendor.

Installing is atomic by rename: the tree is written into a temporary directory
*inside* the drivers volume (same filesystem, so the rename cannot degrade to a
copy) and renamed into `drivers/<name>/<version>/`. A half-written artefact is
never visible to discovery.

Removal is the mirror:

    DELETE /v1/drivers/<name>/<version>?sha256=<digest from the install answer>

`remove` refuses unless the digest on disk is the one the caller names, so a
job can delete only the install it made -- the counterpart of
`replace_sha256`. That matters because install happens before the probe: a
driver whose probe stopped the job must be removable, or it stays installed,
unverified, and is what the next request runs.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import wire

#: A whole driver is tens of kilobytes; these bound a body that is meant to be
#: a driver and not a way to fill the volume one request at a time.
MAX_FILES = 2000
MAX_BYTES = 16 * 1024 * 1024

#: name and version become path segments, so they are held to the same rule as
#: a bare file name -- no separators, no `.`/`..`.
SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

#: One install at a time. Discovery is read-only and needs no lock; the staging
#: directory is unique per call, so the lock is only about two renames racing.
_install_lock = threading.Lock()


def digest_of(files: dict[str, bytes]) -> str:
    """One digest over a tree, in a stable order.

    The unit of installation is the directory, so the digest covers names as
    well as bytes: two artefacts with the same code under different names are
    different installations. This is byte-compatible with
    `data_download.driver_build.artifact_digest` on the app side -- the app
    computes it over the checkout it fetched, this computes it over the tree it
    installed, and the two must agree or `replace_sha256` would never match.
    """
    digest = hashlib.sha256()
    for name in sorted(files):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(files[name])
        digest.update(b"\0")
    return digest.hexdigest()


def digest_of_directory(directory: Path) -> str:
    """`digest_of` over a directory on disk (used to judge an existing install)."""
    files: dict[str, bytes] = {}
    for path in sorted(p for p in directory.rglob("*") if p.is_file()):
        files[path.relative_to(directory).as_posix()] = path.read_bytes()
    return digest_of(files)


def _segment_value(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise wire.WireError(wire.BAD_REQUEST, f"{field} is required")
    value = value.strip()
    if not SEGMENT.match(value):
        raise wire.WireError(
            wire.BAD_REQUEST,
            f"{field} {value!r} is not a plain path segment",
        )
    return value


def _segment(payload: dict[str, Any], field: str) -> str:
    return _segment_value(payload.get(field), field)


def _relative(name: Any) -> str:
    """A path inside the artefact, or a refusal.

    Same rule as `wire.dest`: relative, and no component that climbs out. A
    backslash is refused rather than normalised, because it is a separator on a
    filesystem this code does not run on and a literal inside a name on the one
    it does.
    """
    if not isinstance(name, str) or not name.strip():
        raise wire.WireError(wire.BAD_REQUEST, "every file needs a path")
    path = name.strip()
    if "\\" in path:
        raise wire.WireError(wire.BAD_REQUEST, f"path {name!r} contains a backslash")
    if path.startswith("/"):
        raise wire.WireError(wire.BAD_REQUEST, f"path {name!r} is absolute")
    parts = path.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise wire.WireError(
            wire.BAD_REQUEST,
            f"path {name!r} is not a plain relative path",
        )
    return "/".join(parts)


def decode(files_payload: Any) -> dict[str, bytes]:
    if not isinstance(files_payload, dict) or not files_payload:
        raise wire.WireError(wire.BAD_REQUEST, "files must be a non-empty object")
    if len(files_payload) > MAX_FILES:
        raise wire.WireError(wire.BAD_REQUEST, f"more than {MAX_FILES} files")

    files: dict[str, bytes] = {}
    total = 0
    for raw_name, encoded in files_payload.items():
        name = _relative(raw_name)
        if name in files:
            raise wire.WireError(wire.BAD_REQUEST, f"path {name!r} appears twice")
        if not isinstance(encoded, str):
            raise wire.WireError(
                wire.BAD_REQUEST, f"{name}: content must be base64 text"
            )
        try:
            data = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            raise wire.WireError(
                wire.BAD_REQUEST, f"{name}: content is not base64"
            ) from None
        total += len(data)
        if total > MAX_BYTES:
            raise wire.WireError(
                wire.BAD_REQUEST, f"the tree is larger than {MAX_BYTES} bytes"
            )
        files[name] = data
    return files


def read_manifest(files: dict[str, bytes]) -> dict[str, Any]:
    """The manifest at the tree's root, or a refusal.

    It is checked here rather than trusted from the caller because it is what
    discovery reads: an artefact whose manifest names a different driver than
    the directory it sits in would be listed under one name and driven under
    another.
    """
    raw = files.get("manifest.json")
    if raw is None:
        raise wire.WireError(wire.BAD_REQUEST, "the tree has no manifest.json")
    try:
        manifest = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise wire.WireError(
            wire.BAD_REQUEST, f"manifest.json is not JSON: {exc}"
        ) from None
    if not isinstance(manifest, dict):
        raise wire.WireError(wire.BAD_REQUEST, "manifest.json must be an object")
    return manifest


def install(payload: dict[str, Any], drivers_dir: Path) -> dict[str, Any]:
    """Write one artefact into `drivers/<name>/<version>/`.

    Returns what was installed and the digest that identifies it, so a caller
    that installed something can later prove *it* installed it (that is the
    whole point of `replace_sha256`).
    """
    name = _segment(payload, "name")
    version = _segment(payload, "version")
    files = decode(payload.get("files"))
    manifest = read_manifest(files)

    if manifest.get("name") != name:
        raise wire.WireError(
            wire.INSTALL_REFUSED,
            f"manifest names {manifest.get('name')!r}, installed as {name!r}",
        )
    if str(manifest.get("version")) != version:
        raise wire.WireError(
            wire.INSTALL_REFUSED,
            f"manifest is version {manifest.get('version')!r}, installed as {version!r}",
        )
    protocol = manifest.get("protocol")
    if protocol != wire.PROTOCOL_VERSION:
        raise wire.WireError(
            wire.INSTALL_REFUSED,
            f"manifest speaks protocol {protocol!r}; this host speaks "
            f"{wire.PROTOCOL_VERSION}",
        )
    entrypoint = manifest.get("entrypoint") or "driver.py"
    if not isinstance(entrypoint, str) or entrypoint not in files:
        raise wire.WireError(
            wire.INSTALL_REFUSED,
            f"manifest entrypoint {entrypoint!r} is not in the tree",
        )
    vendor = str(manifest.get("vendor") or payload.get("vendor") or "")

    target = drivers_dir / name / version
    replace_sha256 = payload.get("replace_sha256")
    if isinstance(replace_sha256, str):
        replace_sha256 = replace_sha256.strip() or None
    else:
        replace_sha256 = None

    installed_sha = digest_of(files)
    replaced = False

    with _install_lock:
        if target.exists():
            existing = digest_of_directory(target)
            if replace_sha256 != existing:
                raise wire.WireError(
                    wire.INSTALL_REFUSED,
                    f"{target} already exists with digest {existing}; "
                    f"pass replace_sha256={existing} to replace it",
                )
            replaced = True

        drivers_dir.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".install-", dir=drivers_dir))
        old: Path | None = None
        try:
            for relative in sorted(files):
                destination = staging / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(files[relative])
                os.chmod(destination, 0o644)

            target.parent.mkdir(parents=True, exist_ok=True)
            if replaced:
                # `os.rename` onto a non-empty directory is ENOTEMPTY, so the
                # old install is moved aside first. The window between the two
                # renames is the one moment discovery can see no artefact; it
                # is microseconds and the alternative is a copy over live code.
                old = Path(tempfile.mkdtemp(prefix=".old-", dir=drivers_dir))
                os.rename(target, old / version)
            os.rename(staging, target)
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            if old is not None and old.is_dir() and not target.exists():
                try:
                    os.rename(old / version, target)
                except OSError:
                    pass
            raise
        finally:
            if old is not None:
                shutil.rmtree(old, ignore_errors=True)

    return {
        "name": name,
        "version": version,
        "vendor": vendor,
        "path": str(target),
        "sha256": installed_sha,
        "replaced": replaced,
    }


def remove(
    name: str, version: str, expected_sha256: Any, drivers_dir: Path
) -> dict[str, Any]:
    """Remove `drivers/<name>/<version>/`, but only the install the caller names.

    This is the mirror of `replace_sha256`: a caller that installed an artefact
    proved *which* bytes it installed by the digest in the answer, so removal
    takes the same proof. Without it a job could delete an install it did not
    make -- the install route writes before the probe runs, so the one caller
    that most needs to clean up (a driver whose probe stopped the job) is also
    the one that must not be able to remove someone else's.

    404 when there is no such install, 409 when the digest on disk is not the
    one the caller recorded. The directory is gone before the answer is sent,
    so discovery (which re-globs per request) no longer lists it.
    """
    name = _segment_value(name, "name")
    version = _segment_value(version, "version")
    if not isinstance(expected_sha256, str) or not expected_sha256.strip():
        raise wire.WireError(
            wire.BAD_REQUEST,
            "sha256 is required: name the digest of the install you would remove",
        )
    expected = expected_sha256.strip().lower()

    target = drivers_dir / name / version
    root = drivers_dir.resolve()
    with _install_lock:
        # A symlinked directory could point outside the volume; `is_dir`
        # follows the link, so the resolved-parent check is what keeps removal
        # inside the drivers root even if a tree smuggled a link in.
        if target.is_symlink() or not target.is_dir():
            raise wire.WireError(
                wire.NOT_FOUND, f"no install at {name}/{version}"
            )
        resolved = target.resolve()
        if resolved == root or root not in resolved.parents:
            raise wire.WireError(
                wire.NOT_FOUND, f"no install at {name}/{version}"
            )

        on_disk = digest_of_directory(resolved)
        if on_disk != expected:
            raise wire.WireError(
                wire.INSTALL_REFUSED,
                f"not the install you recorded: {name}/{version} on disk has "
                f"digest {on_disk}, not {expected}",
            )

        vendor = ""
        try:
            manifest = json.loads((resolved / "manifest.json").read_text("utf-8"))
            if isinstance(manifest, dict):
                vendor = str(manifest.get("vendor") or "")
        except (OSError, ValueError):
            vendor = ""

        shutil.rmtree(resolved)
        # An empty vendor directory is not an artefact; remove it so the volume
        # does not accumulate one directory per install ever made.
        try:
            resolved.parent.rmdir()
        except OSError:
            pass

    return {
        "name": name,
        "version": version,
        "vendor": vendor,
        "sha256": expected,
        "removed": True,
    }
