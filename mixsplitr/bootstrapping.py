"""Application bootstrapping helpers for MixSplitR."""

from __future__ import annotations

import multiprocessing


def bootstrap_runtime() -> None:
    """Initialize runtime compatibility hooks before launching the app."""
    multiprocessing.freeze_support()
