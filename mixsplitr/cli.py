"""CLI entrypoints for MixSplitR."""

from __future__ import annotations

from .bootstrapping import bootstrap_runtime
from .orchestration import main as orchestration_main


def main() -> None:
    """Launch MixSplitR from the package entrypoint."""
    bootstrap_runtime()
    orchestration_main()


if __name__ == "__main__":
    main()
