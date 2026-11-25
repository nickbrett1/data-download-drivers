"""The Databento artefact's entrypoint: `serve` plus the driver, and nothing else.

Same shape as the stub's: the host spawns this file by path with the session
root in `DRIVER_SESSION_ROOT` and speaks JSON lines over stdin/stdout, so an
artefact needs no HTTP client, no framework and no knowledge of the wire's
transport.

`sys.path` is seeded with its own directory because the host spawns it by path
(`python /drivers/databento/1/driver.py`), not as an installed package -- the
artefact is a mounted directory, and that is the point. The module is called
`databento_driver.py`, not `databento.py`, because a file of that name here
would shadow the vendor SDK this driver imports.
"""

# ruff: noqa: E402, I001 - the path insert must precede the artefact imports

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from databento_driver import DatabentoDriver  # noqa: E402
from data_download_drivers.child import serve  # noqa: E402


def build(root: str, options: dict) -> DatabentoDriver:
    return DatabentoDriver(root, options)


if __name__ == "__main__":
    raise SystemExit(serve(build))
