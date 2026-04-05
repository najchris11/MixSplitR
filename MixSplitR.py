#!/usr/bin/env python3
"""Compatibility launcher for MixSplitR.

`MixSplitR.py` remains as a stable entrypoint for users and packaging tools,
while the application code now lives in the `mixsplitr` package.
"""

from __future__ import annotations

from mixsplitr.cli import main


if __name__ == "__main__":
    main()