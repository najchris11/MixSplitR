#!/usr/bin/env python3
"""mixsplitr_memory.py - lightweight RAM / batching helpers

This module exists to keep MixSplitR usable even when psutil isn't installed.
If psutil is available, we use it to estimate available RAM.
"""

import os
import glob

try:
    import psutil
    _PSUTIL_AVAILABLE = True
except Exception:
    psutil = None
    _PSUTIL_AVAILABLE = False


def is_psutil_available():
    return _PSUTIL_AVAILABLE


def get_available_ram_gb(default_gb: float = 4.0) -> float:
    """Best-effort estimate of available RAM in GB."""
    if _PSUTIL_AVAILABLE:
        try:
            return max(0.5, psutil.virtual_memory().available / (1024 ** 3))
        except Exception:
            pass
    return float(default_gb)


def scan_existing_library(output_folder: str, audio_extensions=None):
    """Return a set of basenames already present in output_folder."""
    if not output_folder or not os.path.isdir(output_folder):
        return set()
    if audio_extensions is None:
        audio_extensions = (".mp3", ".m4a", ".flac", ".wav", ".aiff", ".aac", ".ogg")
    existing = set()
    try:
        for ext in audio_extensions:
            for p in glob.glob(os.path.join(output_folder, f"*{ext}")):
                existing.add(os.path.basename(p))
    except Exception:
        pass
    return existing


def create_file_batches(audio_files, available_ram_gb: float = None, max_batch_size: int = 30):
    """Create simple batches of files.

    Strategy:
      - If available_ram_gb is low, use smaller batches.
      - Otherwise, use up to max_batch_size.
    """
    if available_ram_gb is None:
        available_ram_gb = get_available_ram_gb()

    if not audio_files:
        return []

    # Heuristic batch sizing
    if available_ram_gb < 2.0:
        batch_size = min(8, max_batch_size)
    elif available_ram_gb < 4.0:
        batch_size = min(15, max_batch_size)
    else:
        batch_size = max_batch_size

    batches = []
    for i in range(0, len(audio_files), batch_size):
        batches.append(audio_files[i:i+batch_size])
    return batches
