"""Entry point for data_download_drivers — the protocol host.

`--transport`, `--host` and `--port` are kept because the **generated
Dockerfile's CMD passes them** and Dockerfile is genproj-owned: it is rewritten
on regeneration, so a flag removed here comes back as a container that will not
start. `--transport` is therefore vestigial and accepted, not honoured -- both
values serve the host, because the stdio variant answered nothing a client
could use.

The healthcheck the Dockerfile declares (`http:/healthz`) is answered by the
host itself (`host.serve`), which is why this module no longer needs its own
little health server.
"""

from __future__ import annotations

import argparse
import os


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="data_download_drivers",
        description="The driver protocol host: sessions, artefacts, eight verbs.",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="streamable-http",
        help="Vestigial: kept because the generated Dockerfile passes it. "
        "Both values serve the HTTP host.",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind address (default: 0.0.0.0).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DRIVER_HOST_PORT") or 8831),
        help="Port (default: $DRIVER_HOST_PORT, else 8831).",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    from .host import serve

    return serve(args.host, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
