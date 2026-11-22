"""The stub artefact's entrypoint: `serve` plus the stub, and nothing else.

A driver artefact is a directory with a `manifest.json` and this file. The host
spawns it with the session root in the environment and speaks JSON lines over
stdin/stdout (`data_download_drivers.child`), so an artefact needs no HTTP
client, no framework and no knowledge of the wire's transport.

`sys.path` is seeded with its own directory because the host spawns it by path
(`python /drivers/<vendor>/<version>/driver.py`), not as an installed package --
the artefact is a mounted directory, and that is the point.
"""

# ruff: noqa: E402, I001 - the path insert must precede the artefact imports

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_download_drivers.child import serve  # noqa: E402
from stub import StubDriver  # noqa: E402


def build(root: str, options: dict) -> StubDriver:
    mode = str(options.get("mode") or "clean")
    return StubDriver(root, mode)


if __name__ == "__main__":
    raise SystemExit(serve(build))
