from __future__ import annotations

import argparse
import os

from .dashboard import run_dashboard
from .main import main as run_poller


def cli() -> int:
    default_mode = os.environ.get("PEQUOD_MODE", "dashboard").strip().lower() or "dashboard"
    parser = argparse.ArgumentParser(
        prog="python -m pequod",
        description="Pequod launcher. Defaults to dashboard mode.",
    )
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["dashboard", "poller"],
        default=default_mode if default_mode in {"dashboard", "poller"} else "dashboard",
        help="dashboard = frontend + API + poller, poller = alert service only",
    )
    args = parser.parse_args()

    if args.mode == "dashboard":
        return run_dashboard()
    return run_poller()


if __name__ == "__main__":
    raise SystemExit(cli())

