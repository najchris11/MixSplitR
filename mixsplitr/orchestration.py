#!/usr/bin/env python3
"""
MixSplitR v7.1 - Mix Archival Tool
Main entry point and orchestration

Identification modes (set during first-run setup or via Manage API Keys):
  • acrcloud          – ACRCloud primary + MusicBrainz fallback (original)
  • musicbrainz_only  – AcoustID fingerprint → MusicBrainz only (no account needed)

This is the modular version with functionality split across:
- mixsplitr_core.py       - Configuration, utilities, rate limiting, mode helpers
- mixsplitr_processing.py - Track identification (4 modes + shared helpers)
- mixsplitr_pipeline.py   - Large file streaming, cache application
- mixsplitr_session.py    - Manifest browser, comparison, rollback UI
- mixsplitr_metadata.py   - iTunes, Deezer, Last.fm APIs
- mixsplitr_audio.py      - BPM detection (librosa)
- mixsplitr_identify.py   - AcoustID/MusicBrainz, result merging
- mixsplitr_tagging.py    - FLAC/ALAC embedding
- mixsplitr_memory.py     - RAM management, batching
- mixsplitr_editor.py     - Cache, interactive editor
- splitter_ui.py          - Visual waveform splitter (optional)
"""

import os
import sys
import glob
import json
import time
import shutil
import shlex
import re
import threading
import gc
import base64
import platform
import argparse
import subprocess
from dataclasses import dataclass
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# =============================================================================
# THIRD-PARTY IMPORTS - Must be at top level for PyInstaller to detect
# =============================================================================
from pydub import AudioSegment
from pydub.silence import split_on_silence, detect_silence
from tqdm import tqdm

# These may not be installed - import with fallback
try:
    from acrcloud.recognizer import ACRCloudRecognizer
    ACRCLOUD_AVAILABLE = True
except ImportError:
    ACRCLOUD_AVAILABLE = False
    ACRCloudRecognizer = None

try:
    import acoustid
    import musicbrainzngs
    ACOUSTID_AVAILABLE = True
except ImportError:
    ACOUSTID_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

# =============================================================================
# IMPORT MODULES
# =============================================================================

from .mixsplitr_core import (
    CURRENT_VERSION, GITLAB_REPO, Style,
    AUDIO_EXTENSIONS_GLOB, AUDIO_EXTENSIONS,
    check_for_updates, RateLimiter, resource_path,
    ffmpeg_path, ffprobe_path, setup_ffmpeg, get_audio_duration_fast,
    analyze_files_parallel, close_terminal, get_config, save_config, get_config_path,
    get_cache_path, get_app_data_dir, validate_acrcloud_credentials, get_output_directory,
    # Mode helpers (v7.1)
    MODE_ACRCLOUD, MODE_MB_ONLY, MODE_MANUAL, MODE_DUAL, get_mode,
    # Large file handling
    LARGE_FILE_THRESHOLD, is_large_file, get_file_size_str,
    ffmpeg_detect_silence, ffmpeg_get_split_points_from_silence,
    ffmpeg_split_file, ffmpeg_extract_chunk_for_identification
)

from .mixsplitr_manifest import (
    list_manifests, load_manifest, compare_manifests,
    export_manifest_for_session, rollback_from_manifest, get_manifest_dir
)

from .mixsplitr_metadata import (
    find_art_in_json, get_backup_art, get_all_external_metadata,
    set_lastfm_key
)

from .mixsplitr_audio import detect_bpm_librosa

from .mixsplitr_identify import (
    identify_with_acoustid, identify_with_shazam, get_enhanced_metadata,
    merge_identification_results, batch_download_artwork,
    is_acoustid_available, is_shazam_available, setup_musicbrainz, identify_dual_mode,
    musicbrainz_search_recordings, set_acoustid_api_key, get_acoustid_api_key,
    check_chromaprint_available, is_trace_enabled, print_id_winner
)

from .mixsplitr_tagging import embed_and_sort_flac, embed_and_sort_alac, embed_and_sort_generic, AUDIO_FORMATS

from .mixsplitr_memory import (
    scan_existing_library, get_available_ram_gb, create_file_batches,
    is_psutil_available
)

from .mixsplitr_editor import (
    save_preview_cache, load_preview_cache, interactive_editor,
    display_preview_table
)

# New prompt_toolkit based menus
from .mixsplitr_menus import (
    show_main_menu, show_api_keys_menu, show_mode_switch_menu,
    show_preview_type_menu, show_split_mode_menu,
    show_post_process_menu, show_manifest_menu, show_format_selection_menu,
    show_file_selection_menu, show_exit_menu_with_cache
)
from .mixsplitr_menu import (
    confirm_dialog, input_dialog, wait_for_enter, clear_screen as menu_clear_screen,
    PROMPT_TOOLKIT_AVAILABLE
)

# Split-out modules (v7.1 refactor)
from .mixsplitr_processing import (
    process_single_track,
    process_single_track_manual,
    process_single_track_mb_only,
    process_single_track_dual
)
from .mixsplitr_pipeline import (
    process_large_file_streaming,
    apply_from_cache
)
from .mixsplitr_session import manage_manifests

# Setup ffmpeg paths and configure pydub
# FIXED: Properly get the returned paths from setup_ffmpeg()
ffmpeg_path, ffprobe_path = ffmpeg_path, ffprobe_path = setup_ffmpeg()
AudioSegment.converter = ffmpeg_path
AudioSegment.ffprobe = ffprobe_path
os.environ["FFMPEG_BINARY"] = ffmpeg_path
os.environ["FFPROBE_BINARY"] = ffprobe_path
try:
    import pydub.utils as pydub_utils

    def _mixsplitr_get_prober_name():
        if ffprobe_path and os.path.exists(ffprobe_path):
            return ffprobe_path
        fallback = pydub_utils.which("ffprobe") or pydub_utils.which("avprobe")
        return fallback or "ffprobe"

    pydub_utils.get_prober_name = _mixsplitr_get_prober_name
except Exception:
    pass

# Initialize optional ID backends (MusicBrainz/AcoustID/Shazam) independently
setup_musicbrainz(CURRENT_VERSION, GITLAB_REPO)

if not ACOUSTID_AVAILABLE:
    print("Note: acoustid/musicbrainzngs not found - MusicBrainz/AcoustID disabled")
    print("      Install with: pip install pyacoustid musicbrainzngs")

# If tracing, show whether Shazam is available (useful for EXE builds)
if os.environ.get("MIXSPLITR_TRACE_SHAZAM", "").strip().lower() in ("1", "true", "yes", "y", "on"):
    print(f"Note: Shazam backend is {'available' if is_shazam_available() else 'NOT available'}")
    # Config-level disable flag is applied later when config is loaded

# Check psutil availability (PSUTIL_AVAILABLE set at top of file)
if not PSUTIL_AVAILABLE:
    print("Note: psutil not found - will process files one at a time for safety")

# Visual splitter UI - optional module
SPLITTER_UI_AVAILABLE = False
try:
    from .splitter_ui import get_split_points_visual, split_audio_at_points
    SPLITTER_UI_AVAILABLE = True
except ImportError:
    pass

# NOTE: USE_LOCAL_BPM, SHAZAM_ENABLED, SHOW_ID_SOURCE are now read from
# config at function-call time inside mixsplitr_processing.py.
# CLI --no-bpm-dsp flag is applied to config in main().


# =============================================================================
# SCREEN UTILITIES
# =============================================================================

def clear_screen():
    """Clear terminal screen to reduce clutter"""
    os.system('cls' if os.name == 'nt' else 'clear')


def _set_windows_console_size(cols: int, lines: int) -> bool:
    """
    Best-effort Windows console sizing using WinAPI.
    Returns True if API calls succeeded, False if unavailable/ignored.
    """
    if os.name != 'nt':
        return False
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.windll.kernel32
        STD_OUTPUT_HANDLE = -11
        h_out = kernel32.GetStdHandle(STD_OUTPUT_HANDLE)
        if h_out in (0, -1):
            return False

        class COORD(ctypes.Structure):
            _fields_ = [("X", wintypes.SHORT), ("Y", wintypes.SHORT)]

        class SMALL_RECT(ctypes.Structure):
            _fields_ = [
                ("Left", wintypes.SHORT),
                ("Top", wintypes.SHORT),
                ("Right", wintypes.SHORT),
                ("Bottom", wintypes.SHORT),
            ]

        cols = max(60, int(cols))
        lines = max(20, int(lines))

        # Ensure buffer is large enough before setting window dimensions.
        kernel32.SetConsoleScreenBufferSize(h_out, COORD(cols, max(lines, 300)))
        rect = SMALL_RECT(0, 0, cols - 1, lines - 1)
        ok = bool(kernel32.SetConsoleWindowInfo(h_out, ctypes.c_bool(True), ctypes.byref(rect)))
        # Trim buffer back down to match requested size where possible.
        kernel32.SetConsoleScreenBufferSize(h_out, COORD(cols, lines))
        return ok
    except Exception:
        return False


def _ensure_windows_console_host(cols: int, lines: int):
    """
    Ensure a predictable Windows console window size by relaunching once into
    a dedicated host. This avoids host-specific resize limitations.

    Disabled by default because relaunching causes a visible double-start.
    Set MIXSPLITR_FORCE_RELAUNCH=1 to opt in for troubleshooting.
    """
    if os.name != 'nt':
        return
    if os.environ.get("MIXSPLITR_FORCE_RELAUNCH", "").strip() != "1":
        return
    if os.environ.get("MIXSPLITR_CONHOST", "").strip() == "1":
        return

    try:
        args = subprocess.list2cmdline(sys.argv[1:])
        if getattr(sys, "frozen", False):
            run_cmd = f"\"{sys.executable}\" {args}".strip()
        else:
            # Package layout uses relative imports, so relaunch via module mode.
            run_cmd = f"\"{sys.executable}\" -m mixsplitr {args}".strip()
        inner = (
            f"set MIXSPLITR_CONHOST=1 && "
            f"mode con cols={int(cols)} lines={int(lines)} >nul 2>&1 && "
            f"{run_cmd}"
        )
        child_env = os.environ.copy()
        # Force a fresh onefile extraction in relaunched process.
        child_env["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
        # Clear inherited onefile temp-dir hints that can break child startup.
        child_env.pop("_MEIPASS2", None)
        child_env.pop("_PYI_APPLICATION_HOME_DIR", None)
        child_env.pop("_PYI_ARCHIVE_FILE", None)
        child_env.pop("_PYI_PARENT_PROCESS_LEVEL", None)

        # Preferred path: ask Windows Terminal to open a NEW window at target size.
        wt_bin = shutil.which("wt")
        if wt_bin:
            subprocess.Popen([
                wt_bin,
                "-w", "new",
                "--size", f"{int(cols)},{int(lines)}",
                "cmd", "/k", inner,
            ], env=child_env)
        else:
            # Fallback: separate console window via conhost/cmd.
            subprocess.Popen(
                ["cmd.exe", "/k", inner],
                creationflags=getattr(subprocess, "CREATE_NEW_CONSOLE", 0),
                env=child_env,
            )
        sys.exit(0)
    except Exception:
        # If relaunch fails, keep running in current console.
        return


def set_terminal_window_size(default_cols: int = 81, default_lines: int = 50):
    """
    Try to enforce a consistent terminal character grid across platforms.
    Optional override via MIXSPLITR_TERM_SIZE, e.g. "100x34".
    """
    cols, lines = default_cols, default_lines
    raw = os.environ.get("MIXSPLITR_TERM_SIZE", "").strip().lower()
    if raw and "x" in raw:
        try:
            parsed_cols, parsed_lines = raw.split("x", 1)
            cols = max(60, int(parsed_cols))
            lines = max(20, int(parsed_lines))
        except Exception:
            cols, lines = default_cols, default_lines

    try:
        if os.name == 'nt':
            # Try WinAPI sizing first (works for classic conhost in most cases).
            sized = _set_windows_console_size(cols, lines)
            if not sized:
                # Fallback for shells where WinAPI path is unavailable.
                os.system(f"mode con cols={cols} lines={lines} >nul 2>&1")
            # Also request xterm-style resize for hosts like Windows Terminal.
            try:
                sys.stdout.write(f"\033[8;{lines};{cols}t")
                sys.stdout.flush()
            except Exception:
                pass
        else:
            # xterm-compatible resize request (works in most modern terminals).
            sys.stdout.write(f"\033[8;{lines};{cols}t")
            sys.stdout.flush()
    except Exception:
        pass


# =============================================================================
# OPENING SCREEN
# =============================================================================

def show_opening_screen():
    """Display animated ASCII art opening screen"""
    clear_screen()
    import math
    import time

    term_cols = shutil.get_terminal_size(fallback=(100, 24)).columns

    def _center_text(text: str) -> str:
        text = text.rstrip("\n")
        if not text:
            return ""
        if len(text) >= term_cols:
            return text
        return text.center(term_cols)

    logo_segments = [
        ("███╗   ███╗██╗██╗  ██╗", "███████╗██████╗ ██╗     ██╗████████╗", "██████╗ "),
        ("████╗ ████║██║╚██╗██╔╝", "██╔════╝██╔══██╗██║     ██║╚══██╔══╝", "██╔══██╗"),
        ("██╔████╔██║██║ ╚███╔╝ ", "███████╗██████╔╝██║     ██║   ██║   ", "██████╔╝"),
        ("██║╚██╔╝██║██║ ██╔██╗ ", "╚════██║██╔═══╝ ██║     ██║   ██║   ", "██╔══██╗"),
        ("██║ ╚═╝ ██║██║██╔╝ ██╗", "███████║██║     ███████╗██║   ██║   ", "██║  ██║"),
        ("╚═╝     ╚═╝╚═╝╚═╝  ╚═╝", "╚══════╝╚═╝     ╚══════╝╚═╝   ╚═╝   ", "╚═╝  ╚═╝"),
    ]

    divider = "═══════════════════════════════════════"
    subtitle = "Mix Archival Tool"
    version = f"Version {CURRENT_VERSION}"
    feature_line = "Record • Spilt • Identifty • Archive"
    mix_logo_color = Style.GRAY
    r_logo_color = '\033[38;5;196m'

    # Hide cursor during splash to avoid block artifacts.
    print("\033[?25l", end="", flush=True)
    try:
        for mix_part, split_part, r_part in logo_segments:
            plain_line = f"{mix_part}{split_part}{r_part}"
            pad = " " * max(0, (term_cols - len(plain_line)) // 2)
            print(
                f"{pad}{mix_logo_color}{mix_part}"
                f"{Style.GRAY}{split_part}"
                f"{r_logo_color}{r_part}{Style.RESET}"
            )
            time.sleep(0.04)

        print(f"{Style.GRAY}{_center_text(divider)}")
        print(_center_text(subtitle))
        print(f"{_center_text(divider)}{Style.RESET}\n")
        time.sleep(0.20)

        print(f"{Style.DIM}{_center_text(version)}")
        print(f"{_center_text(feature_line)}{Style.RESET}\n")
        time.sleep(0.20)

        glyphs = "▁▂▃▄▅▆▇█"
        wave_width = 14
        wave_freq = 0.65
        wave_speed = 0.50
        # Match marker color to the red "R" in MIXSPLITR.
        wave_marker_color = r_logo_color
        # Darker waveform body so red marker pops more.
        wave_base_color = '\033[38;5;238m'
        for phase in range(42):
            # Reflection pair that propagates outward from center.
            outward_levels = [
                int((math.sin((distance * wave_freq) - (phase * wave_speed)) + 1.0) * 3.5)
                for distance in range(wave_width)
            ]
            outward_wave = ''.join(glyphs[level] for level in outward_levels)
            left_wave = outward_wave[::-1]
            right_wave = outward_wave
            # Two red scan markers pulse outward from the center.
            # Marker travels outward with waveform flow.
            scan_pos = phase % wave_width
            right_scan_idx = int(scan_pos)
            left_scan_idx = wave_width - 1 - right_scan_idx

            left_colored = ''.join(
                f"{wave_marker_color}{char}" if idx == left_scan_idx else f"{wave_base_color}{char}"
                for idx, char in enumerate(left_wave)
            )
            right_colored = ''.join(
                f"{wave_marker_color}{char}" if idx == right_scan_idx else f"{wave_base_color}{char}"
                for idx, char in enumerate(right_wave)
            )

            wave_plain = f"{left_wave}  │  {right_wave}"
            wave_pad = " " * max(0, (term_cols - len(wave_plain)) // 2)
            wave_line = (
                f"{wave_pad}{left_colored}"
                f"{wave_base_color}  │  "
                f"{right_colored}{Style.RESET}"
            )
            # Clear whole line each frame so no ghost/stuck bar remains.
            print(f"\033[2K\r{wave_line}", end="", flush=True)
            time.sleep(0.06)

        print("\033[2K\r", end="", flush=True)
        print(f"{Style.CYAN}{_center_text('Loading...')}{Style.RESET}")
        time.sleep(0.45)
    finally:
        print("\033[?25h", end="", flush=True)


# =============================================================================
# FILE HELPERS
# =============================================================================

def is_audio_file(path):
    """Return True if *path* has a supported audio extension."""
    return os.path.splitext(path)[1].lower() in AUDIO_EXTENSIONS


@dataclass
class AppState:
    """Mutable runtime state for menu actions."""
    audio_files: list
    base_dir: str
    temp_folder: str
    config: dict
    current_mode: str
    update_info: Optional[dict] = None
    ui_notice: str = ""


def _build_mode_badge(current_mode: str, update_info=None) -> str:
    if current_mode == MODE_MANUAL:
        badge = "[Manual Search]"
    elif current_mode == MODE_DUAL:
        badge = "[Dual Mode]"
    elif current_mode == MODE_MB_ONLY:
        badge = "[MusicBrainz]"
    else:
        badge = "[ACRCloud]"

    return badge


def _get_cached_track_count(cache_path: str) -> int:
    """Return the number of tracks in cache preview data."""
    if not os.path.exists(cache_path):
        return 0
    try:
        with open(cache_path, 'r') as f:
            cache_data = json.load(f)
        return len(cache_data.get('tracks') or [])
    except Exception:
        return 0


def _clear_unsaved_preview_data(cache_path: str, temp_folder: str) -> bool:
    """Remove unsaved preview cache/readable files and temp chunks."""
    removed_any = False
    for path in (cache_path, str(cache_path).replace('.json', '_readable.txt')):
        try:
            if os.path.exists(path):
                os.remove(path)
                removed_any = True
        except Exception:
            pass

    try:
        if temp_folder and os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
            removed_any = True
    except Exception:
        pass

    return removed_any


def _collect_audio_files_from_directory(folder_path: str, deep_scan: bool = False) -> list:
    """Collect supported audio files from a directory."""
    found = []
    try:
        if deep_scan:
            for root, _, files in os.walk(folder_path):
                for filename in files:
                    path = os.path.join(root, filename)
                    if is_audio_file(path):
                        found.append(path)
        else:
            for entry in os.scandir(folder_path):
                if entry.is_file() and is_audio_file(entry.path):
                    found.append(entry.path)
    except Exception:
        return []
    return sorted(set(found))


def split_on_silence_with_loading_bar(recording: AudioSegment, min_silence_len: int = 2000,
                                      silence_thresh: int = -40, keep_silence: int = 200,
                                      progress_label: str = "     Splitting"):
    """Run split_on_silence while showing an animated loading bar for long mixes."""
    result = {}
    error = {}

    def _split_worker():
        try:
            result["chunks"] = split_on_silence(
                recording,
                min_silence_len=min_silence_len,
                silence_thresh=silence_thresh,
                keep_silence=keep_silence,
            )
        except Exception as exc:
            error["exc"] = exc

    worker = threading.Thread(target=_split_worker, daemon=True)
    worker.start()

    # Keep UI responsive with monotonic progress (no 0->100 resets).
    duration_seconds = max(1.0, len(recording) / 1000.0)
    estimated_seconds = max(6.0, duration_seconds * 0.20)
    started_at = time.monotonic()
    last_creep = started_at

    with tqdm(total=100, desc=progress_label, ncols=60, leave=False) as pbar:
        while worker.is_alive():
            time.sleep(0.10)
            elapsed = time.monotonic() - started_at
            target = min(99, int((elapsed / estimated_seconds) * 100))

            # If operation exceeds estimate, keep a slow forward creep up to 99%.
            if target <= pbar.n and pbar.n < 99 and elapsed >= estimated_seconds:
                now = time.monotonic()
                if now - last_creep >= 1.2:
                    target = pbar.n + 1
                    last_creep = now

            if target > pbar.n:
                pbar.update(target - pbar.n)

        worker.join()

        # Complete to 100% once the worker actually finishes.
        if pbar.n < 100:
            pbar.update(100 - pbar.n)

    if "exc" in error:
        raise error["exc"]
    return result.get("chunks", [])


def _normalize_user_path(user_input: str) -> str:
    """Normalize quoted/escaped path text from terminal input."""
    normalized = (user_input or "").strip()
    if not normalized:
        return ""
    if (normalized.startswith('"') and normalized.endswith('"')) or \
       (normalized.startswith("'") and normalized.endswith("'")):
        normalized = normalized[1:-1]
    # macOS terminal drag/drop often emits shell-escaped paths such as:
    # /Volumes/Foo\ Bar/Track\ \(demo\)\,\ v1.flac
    # Decode escaped variants, but do not split plain-space paths.
    if normalized.startswith(('/', '~')) and '\\' in normalized:
        try:
            parsed = shlex.split(normalized, posix=True)
            if len(parsed) == 1:
                normalized = parsed[0]
        except ValueError:
            # Fall through to targeted unescape below.
            pass

    # Unescape common shell-escaped punctuation without harming Windows paths.
    # This only unwraps backslashes before punctuation/space characters.
    normalized = re.sub(r'\\([ !"#$%&\'()*+,;<=>?@\[\]^`{|}~])', r'\1', normalized)
    return os.path.expanduser(normalized)


def _split_user_paths(raw_input: str) -> list[str]:
    """Split drag-drop or pasted text into one or more normalized paths."""
    raw = (raw_input or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return []

    tokens: list[str] = []
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Split only where whitespace is followed by a new absolute/home/drive path.
        # This handles drag-drop payloads even when spaces are not escaped.
        chunks = re.split(r"\s+(?=(?:/|~|[A-Za-z]:[\\/]|\\\\))", line)
        for chunk in chunks:
            chunk = chunk.strip()
            if not chunk:
                continue
            # If quoted bundles are present, use shlex just for that chunk.
            if '"' in chunk or "'" in chunk:
                try:
                    parsed = shlex.split(chunk, posix=(sys.platform != "win32"))
                    if parsed:
                        tokens.extend(parsed)
                        continue
                except ValueError:
                    pass
            tokens.append(chunk)

    normalized: list[str] = []
    seen = set()
    for token in tokens:
        path = _normalize_user_path(token)
        if path and path not in seen:
            seen.add(path)
            normalized.append(path)
    return normalized


def _handle_main_menu_path_input(raw_input: str, state: AppState) -> None:
    """Handle drag-drop/typed path submitted directly in the main menu."""
    user_inputs = _split_user_paths(raw_input)
    if not user_inputs:
        print(f"  {Style.RED}✗{Style.RESET} Path not found")
        wait_for_enter()
        return

    deep_scan_on = bool(get_config().get('deep_scan', False))
    resolved_audio_files: list[str] = []
    missing_paths: list[str] = []
    unsupported_paths: list[str] = []

    for user_input in user_inputs:
        if not os.path.exists(user_input):
            missing_paths.append(user_input)
            continue

        if os.path.isfile(user_input):
            if is_audio_file(user_input):
                resolved_audio_files.append(user_input)
            else:
                unsupported_paths.append(user_input)
            continue

        if os.path.isdir(user_input):
            dir_files = _collect_audio_files_from_directory(user_input, deep_scan=deep_scan_on)
            if dir_files:
                resolved_audio_files.extend(dir_files)
            else:
                unsupported_paths.append(user_input)

    # De-duplicate while preserving order.
    deduped_audio_files: list[str] = []
    seen_files = set()
    for file_path in resolved_audio_files:
        abs_path = os.path.abspath(file_path)
        if abs_path not in seen_files:
            seen_files.add(abs_path)
            deduped_audio_files.append(abs_path)

    if not deduped_audio_files:
        if missing_paths:
            print(f"  {Style.RED}✗{Style.RESET} Path not found: {missing_paths[0]}")
        elif unsupported_paths:
            print(f"  {Style.RED}✗{Style.RESET} No supported audio files in: {unsupported_paths[0]}")
        else:
            print(f"  {Style.YELLOW}⚠️{Style.RESET}  No valid audio files found")
        wait_for_enter()
        return

    if len(deduped_audio_files) == 1:
        new_base_dir = os.path.dirname(deduped_audio_files[0]) or os.getcwd()
    else:
        try:
            common_dir = os.path.commonpath([os.path.dirname(p) for p in deduped_audio_files])
            new_base_dir = common_dir if os.path.isdir(common_dir) else (os.path.dirname(deduped_audio_files[0]) or os.getcwd())
        except Exception:
            new_base_dir = os.path.dirname(deduped_audio_files[0]) or os.getcwd()

    state.audio_files = deduped_audio_files
    state.base_dir = new_base_dir
    state.temp_folder = os.path.join(state.base_dir, "mixsplitr_temp")


def _load_last_saved_recording(state: AppState) -> bool:
    """Load the most recently saved MixSplitR recording into state."""
    from pathlib import Path
    import datetime

    if sys.platform == "win32":
        recordings_dir = Path(os.environ.get("APPDATA", Path.home())) / "MixSplitR" / "recordings"
    else:
        recordings_dir = Path.home() / "Music"

    if not recordings_dir.exists():
        print(f"\n  {Style.YELLOW}⚠️  No recordings directory found{Style.RESET}")
        print(f"  Expected: {recordings_dir}")
        input("\n  Press Enter to continue...")
        return False

    recordings = list(recordings_dir.glob("MixSplitR_recording_*.wav"))
    if not recordings:
        print(f"\n  {Style.YELLOW}⚠️  No saved recordings found{Style.RESET}")
        print(f"  Looked in: {recordings_dir}")
        input("\n  Press Enter to continue...")
        return False

    last_recording = max(recordings, key=lambda p: p.stat().st_mtime)
    print(f"\n  📁 Found last recording:")
    print(f"  {Style.BOLD}{last_recording.name}{Style.RESET}")
    print(f"  Location: {last_recording.parent}")

    file_size = last_recording.stat().st_size / (1024 * 1024)
    mod_time = datetime.datetime.fromtimestamp(last_recording.stat().st_mtime)
    print(f"  Size: {file_size:.1f} MB")
    print(f"  Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")

    confirm = input(f"\n  {Style.BOLD}Load this recording? (y/n) [y]:{Style.RESET} ").strip().lower()
    if confirm not in ('', 'y', 'yes'):
        return False

    state.audio_files = [str(last_recording)]
    state.base_dir = str(last_recording.parent)
    state.temp_folder = os.path.join(state.base_dir, "mixsplitr_temp")
    print(f"  {Style.GREEN}✓ Loaded{Style.RESET}")
    return True


def _handle_load_files_choice(state: AppState) -> None:
    """Interactive file/folder/recording chooser for the load-files action."""
    supported_formats = [ext.lstrip('.') for ext in AUDIO_EXTENSIONS]
    checked_location = state.base_dir

    while True:
        clear_screen()
        print(f"\n{Style.WHITE}{Style.BOLD}{'═'*60}{Style.RESET}")
        print(f"{Style.WHITE}{Style.BOLD}  📁 SELECT AUDIO FILES{Style.RESET}")
        print(f"{Style.WHITE}{Style.BOLD}{'═'*60}{Style.RESET}\n")
        print(f"  Current: {checked_location}\n")
        print(f"  Supported formats:\n  {', '.join([fmt.upper() for fmt in supported_formats])}\n")
        deep_scan_on = bool(get_config().get('deep_scan', False))
        print(f"{Style.BOLD}{Style.WHITE}  Options:{Style.RESET}")
        print(f"  • Drag and drop audio file(s) or folder onto this window")
        print(f"  • Enter or paste a path below")
        if deep_scan_on:
            print(f"  • {Style.GREEN}Deep Scan: ON{Style.RESET} {Style.DIM}(subfolders included — toggle in Settings){Style.RESET}")
        if sys.platform in ("win32", "darwin"):
            print(f"  • Type {Style.CYAN}R{Style.RESET} to record system audio")
            print(f"  • Type {Style.CYAN}L{Style.RESET} to load {Style.BOLD}Last Saved Recording{Style.RESET}")
        print(f"  • Press Enter with no input to cancel")

        user_input = input(f"  {Style.BOLD}Path:{Style.RESET} ").strip()
        if user_input == "":
            print("\n  Cancelled, returning to main menu...")
            return

        if sys.platform in ("win32", "darwin") and user_input.lower() == "r":
            try:
                from .mixsplitr_record import record_system_audio_interactive
            except Exception:
                print("\n  Recording mode requires: pip install soundcard soundfile\n")
                input("Press Enter to continue...")
                continue

            rec_path = record_system_audio_interactive()
            if rec_path and os.path.exists(rec_path) and is_audio_file(rec_path):
                state.audio_files = [rec_path]
                state.base_dir = os.path.dirname(rec_path) or rec_path
                state.temp_folder = os.path.join(state.base_dir, "mixsplitr_temp")
                print(f"  {Style.GREEN}✓ Ready to process{Style.RESET}")
                return
            continue

        if sys.platform in ("win32", "darwin") and user_input.lower() == "l":
            if _load_last_saved_recording(state):
                return
            continue

        user_paths = _split_user_paths(user_input)
        if not user_paths:
            print(f"\n  ✗ Not found")
            input("\nPress Enter to try again...")
            continue

        load_deep = bool(get_config().get('deep_scan', False))
        resolved_audio_files: list[str] = []
        missing_paths: list[str] = []
        unsupported_paths: list[str] = []
        dirs_seen: list[str] = []

        for user_path in user_paths:
            if not os.path.exists(user_path):
                missing_paths.append(user_path)
                continue

            if os.path.isdir(user_path):
                dirs_seen.append(user_path)
                if load_deep:
                    print(f"  🔍 Deep scanning (recursive): {user_path}")
                found_files = _collect_audio_files_from_directory(user_path, deep_scan=load_deep)
                if found_files:
                    resolved_audio_files.extend(found_files)
                else:
                    unsupported_paths.append(user_path)
                continue

            if os.path.isfile(user_path):
                if is_audio_file(user_path):
                    resolved_audio_files.append(user_path)
                else:
                    unsupported_paths.append(user_path)
                continue

            missing_paths.append(user_path)

        deduped_audio_files: list[str] = []
        seen_files = set()
        for file_path in resolved_audio_files:
            abs_path = os.path.abspath(file_path)
            if abs_path not in seen_files:
                seen_files.add(abs_path)
                deduped_audio_files.append(abs_path)

        if deduped_audio_files:
            if len(deduped_audio_files) == 1:
                new_base_dir = os.path.dirname(deduped_audio_files[0]) or os.getcwd()
            else:
                try:
                    common_dir = os.path.commonpath([os.path.dirname(p) for p in deduped_audio_files])
                    new_base_dir = common_dir if os.path.isdir(common_dir) else (os.path.dirname(deduped_audio_files[0]) or os.getcwd())
                except Exception:
                    new_base_dir = os.path.dirname(deduped_audio_files[0]) or os.getcwd()

            state.audio_files = deduped_audio_files
            state.base_dir = new_base_dir
            state.temp_folder = os.path.join(state.base_dir, "mixsplitr_temp")

            if dirs_seen and load_deep:
                folders = set(os.path.dirname(f) for f in deduped_audio_files)
                print(f"  {Style.GREEN}✓ Found {len(deduped_audio_files)} audio file(s) in {len(folders)} folder(s){Style.RESET}")
            else:
                print(f"  {Style.GREEN}✓ Found {len(deduped_audio_files)} audio file(s){Style.RESET}")
            return

        if missing_paths:
            print(f"\n  ✗ Not found: {missing_paths[0]}")
        elif unsupported_paths:
            bad = unsupported_paths[0]
            if os.path.isdir(bad):
                if load_deep:
                    print(f"\n  {Style.YELLOW}⚠️  No audio files found in folder or subfolders{Style.RESET}")
                else:
                    print(f"\n  {Style.YELLOW}⚠️  No audio files in folder{Style.RESET}")
                    print(f"  {Style.DIM}Tip: Enable Auto Deep Scan in Settings to search subfolders{Style.RESET}")
            else:
                print(f"\n  ✗ Not a supported audio file: {bad}")
        else:
            print(f"\n  {Style.YELLOW}⚠️  No valid audio files found{Style.RESET}")
        input("\nPress Enter to try again...")


def _resolve_processing_choice(choice: str, state: AppState) -> Optional[bool]:
    """
    Validate processing action choice.
    Returns preview_mode (bool) when processing should start, else None.
    """
    if choice not in ("preview", "direct"):
        return None

    if not state.audio_files:
        print(f"\n  {Style.YELLOW}⚠️  No audio files loaded!{Style.RESET}")
        print(f"  Please load audio files first.\n")
        input("Press Enter to continue...")
        return None

    if state.current_mode == MODE_MANUAL:
        print(f"\n{Style.YELLOW}{'═'*60}")
        print(f"  ⚠️  Manual Search Only Mode")
        print(f"{'═'*60}{Style.RESET}")
        print(f"\n  No fingerprinting keys configured.")
        print(f"  Use 'Manage API Keys' to add AcoustID (free) or ACRCloud.\n")
        if not confirm_dialog("Continue with manual search only?", default=False):
            return None

    return choice == "preview"


def _save_direct_mode_session_record(
    all_results: list,
    output_files: list,
    current_mode: str,
    direct_output_format: str,
    config: dict,
    session_split_data: dict,
):
    """Persist a Session History manifest for one-click/direct exports."""
    if not output_files:
        return None

    direct_input_files = set()
    for result in all_results:
        if result.get('original_file'):
            direct_input_files.add(result['original_file'])

    direct_pipeline = {}
    if session_split_data:
        methods = list(set(sd.get('method', '?') for sd in session_split_data.values()))
        all_points = {}
        for fpath, split_data in session_split_data.items():
            all_points[fpath] = {
                'method': split_data.get('method'),
                'points_sec': split_data.get('points_sec'),
                'num_segments': split_data.get('num_segments'),
                'params': split_data.get('params', {}),
            }
        direct_pipeline = {'split_methods': methods, 'per_file': all_points}

    try:
        sample_seconds = int(config.get('fingerprint_sample_seconds', 12))
    except Exception:
        sample_seconds = 12
    sample_seconds = max(8, min(45, sample_seconds))

    direct_config = {
        'identification_mode': current_mode,
        'output_format': direct_output_format,
        'shazam_enabled': not bool(config.get('disable_shazam', False)),
        'use_local_bpm': not bool(config.get('disable_local_bpm', False)),
        'show_id_source': bool(config.get('show_id_source', True)),
        'fingerprint_sample_seconds': sample_seconds,
    }

    direct_input = list(direct_input_files)[0] if direct_input_files else "unknown"
    return export_manifest_for_session(
        input_file=direct_input,
        output_files=output_files,
        tracks=all_results,
        mode=current_mode,
        pipeline=direct_pipeline,
        config_snapshot=direct_config,
        input_files=list(direct_input_files) if direct_input_files else None
    )


def _handle_main_menu_utility_choice(choice: str, state: AppState, cache_path: str) -> str:
    """
    Handle utility menu actions that do not enter processing flow.
    Returns: 'unhandled', 'handled', or 'exit_app'
    """
    if choice == "exit":
        track_count = _get_cached_track_count(cache_path)
        if track_count > 0:
            exit_choice = show_exit_menu_with_cache(track_count)
            if exit_choice == "cancel":
                return "handled"
            if exit_choice == "clear_exit":
                if _clear_unsaved_preview_data(cache_path, state.temp_folder):
                    print(f"  {Style.GREEN}✓ Unsaved preview data cleared{Style.RESET}")
        return "exit_app"

    if choice == "record" and sys.platform in ("win32", "darwin"):
        try:
            from .mixsplitr_record import record_system_audio_interactive
        except Exception:
            print(f"\n  {Style.RED}❌ Recording mode not available{Style.RESET}")
            print(f"  Install dependencies: pip install soundcard soundfile\n")
            wait_for_enter()
            return "handled"

        try:
            rec_path = record_system_audio_interactive()
        except KeyboardInterrupt:
            print(f"\n  {Style.YELLOW}↩ Recording canceled. Returning to menu.{Style.RESET}")
            return "handled"
        if rec_path and os.path.exists(rec_path) and is_audio_file(rec_path):
            state.audio_files = [rec_path]
            state.base_dir = os.path.dirname(rec_path) or rec_path
            state.temp_folder = os.path.join(state.base_dir, "mixsplitr_temp")
            print(f"  {Style.GREEN}✓ Recording loaded - ready to process{Style.RESET}\n")
        return "handled"

    if choice == "manifest":
        manage_manifests()
        return "handled"

    if choice == "delete_cache":
        if _clear_unsaved_preview_data(cache_path, state.temp_folder):
            print(f"{Style.GREEN}✅ Unsaved preview data cleared{Style.RESET}")
        else:
            print(f"{Style.DIM}No unsaved preview data found.{Style.RESET}")
        wait_for_enter()
        return "handled"

    if choice == "api_keys":
        while not show_api_keys_menu():
            pass
        state.config = get_config()
        state.current_mode = state.config.get('mode', MODE_ACRCLOUD)
        return "handled"

    if choice == "apply_cache":
        did_apply = apply_from_cache(cache_path, state.temp_folder)
        if did_apply:
            _clear_unsaved_preview_data(cache_path, state.temp_folder)
            wait_for_enter()
            state.ui_notice = ""
        else:
            state.ui_notice = "Export canceled. No files were processed."
        return "handled"

    return "unhandled"


# =============================================================================
# TRACK PROCESSING → moved to mixsplitr_processing.py
# LARGE FILE / CACHE → moved to mixsplitr_pipeline.py
# MANIFEST BROWSER  → moved to mixsplitr_session.py
# =============================================================================


# =============================================================================
# API KEY MANAGEMENT
# =============================================================================

def manage_api_keys():
    """Allow user to view/update API keys and switch identification mode."""
    clear_screen()
    config_path = get_config_path()
    config = json.load(open(config_path)) if os.path.exists(config_path) else {}

    current_mode = config.get('mode', MODE_ACRCLOUD)

    print(f"\n{Style.MAGENTA}{'═'*60}")
    print(f"  {Style.BOLD}🔑 Manage API Keys / Mode{Style.RESET}{Style.MAGENTA}")
    print(f"{'═'*60}{Style.RESET}")
    print(f"\n  📁 Config location:")
    print(f"     {Style.DIM}{config_path}{Style.RESET}\n")

    # Current mode banner
    if current_mode == MODE_MANUAL:
        print(f"  Mode:     {Style.YELLOW}✏️  Manual Search Only{Style.RESET}")
        print(f"            {Style.DIM}(No fingerprinting keys configured){Style.RESET}")
    elif current_mode == MODE_MB_ONLY:
        print(f"  Mode:     {Style.CYAN}🔍 MusicBrainz only{Style.RESET}")
    else:
        print(f"  Mode:     {Style.CYAN}🎵 ACRCloud + MusicBrainz{Style.RESET}")

    has_acr    = bool(config.get('host') and config.get('access_key'))
    has_lastfm = bool(config.get('lastfm_api_key'))
    has_acoustid = bool(config.get('acoustid_api_key'))
    shazam_disabled = bool(config.get('disable_shazam', False))
    # Refresh backend availability (Shazam can be present even if MusicBrainz libs aren't)
    try:
        setup_musicbrainz(CURRENT_VERSION, GITLAB_REPO)
    except Exception:
        pass
    shazam_available = bool(is_shazam_available())

    # Show status
    if current_mode == MODE_ACRCLOUD:
        if has_acr:
            print(f"  ACRCloud: {Style.GREEN}✅ Configured{Style.RESET}")
            print(f"            {Style.DIM}Host: {config.get('host', 'N/A')}")
            print(f"            Key:  {config.get('access_key', '')[:10]}...{Style.RESET}")
        else:
            print(f"  ACRCloud: {Style.RED}❌ Not configured{Style.RESET}")
            print(f"            {Style.DIM}Sign up: https://console.acrcloud.com/signup{Style.RESET}")
    elif current_mode == MODE_MANUAL:
        # In manual mode, show both options
        if has_acr:
            print(f"  ACRCloud: {Style.GREEN}✅ Configured{Style.RESET} {Style.DIM}(not active in manual mode){Style.RESET}")
        else:
            print(f"  ACRCloud: {Style.RED}❌ Not configured{Style.RESET}")
            print(f"            {Style.DIM}Sign up: https://console.acrcloud.com/signup{Style.RESET}")
    else:
        print(f"  ACRCloud: {Style.DIM}— not used in MusicBrainz-only mode{Style.RESET}")

    print(f"  Last.fm:  {Style.GREEN + '✅ Configured' + Style.RESET if has_lastfm else Style.RED + '❌ Not configured' + Style.RESET}")
    # Shazam status (no API key required)
    if shazam_disabled:
        print(f"  Shazam:   {Style.YELLOW}⏸️  Disabled{Style.RESET} {Style.DIM}(toggle to enable){Style.RESET}")
    else:
        if shazam_available:
            print(f"  Shazam:   {Style.GREEN}✅ Enabled{Style.RESET} {Style.DIM}(available){Style.RESET}")
        else:
            print(f"  Shazam:   {Style.YELLOW}⚠️  Enabled{Style.RESET} {Style.DIM}(shazamio not installed / not packaged){Style.RESET}")

    # AcoustID status
    if current_mode == MODE_MB_ONLY:
        if has_acoustid:
            key_preview = config.get('acoustid_api_key', '')[:8] + '...' if config.get('acoustid_api_key') else ''
            print(f"  AcoustID: {Style.GREEN}✅ Configured{Style.RESET}")
            print(f"            {Style.DIM}Key: {key_preview}{Style.RESET}")
        else:
            print(f"  AcoustID: {Style.YELLOW}⚠️  Not configured (fingerprinting disabled){Style.RESET}")
            print(f"            {Style.DIM}Sign up: https://acoustid.org/login (link to MusicBrainz){Style.RESET}")
    elif current_mode == MODE_MANUAL:
        # In manual mode, show AcoustID as an option
        if has_acoustid:
            print(f"  AcoustID: {Style.GREEN}✅ Configured{Style.RESET} {Style.DIM}(not active in manual mode){Style.RESET}")
        else:
            print(f"  AcoustID: {Style.RED}❌ Not configured{Style.RESET}")
            print(f"            {Style.DIM}Sign up: https://acoustid.org/login (link to MusicBrainz){Style.RESET}")
            print(f"            {Style.GREEN}⭐ Recommended: Free + works with MusicBrainz{Style.RESET}")
    else:
        print(f"  AcoustID: {Style.DIM}— not used in ACRCloud mode{Style.RESET}")

    # Info box for manual mode
    if current_mode == MODE_MANUAL:
        print(f"\n  {Style.YELLOW}💡 Tip:{Style.RESET}")
        print(f"     {Style.DIM}In manual mode, you can only search by typing song names.{Style.RESET}")
        print(f"     {Style.DIM}Add AcoustID key (recommended) or ACRCloud for fingerprinting!{Style.RESET}")

    # Menu – ACRCloud items only visible in acrcloud mode
    print(f"\n{Style.MAGENTA}{'─'*60}{Style.RESET}")
    idx = 1

    print(f"  {Style.CYAN}{idx}.{Style.RESET} 🔄 Switch identification mode")
    idx += 1                                                         # 2

    if current_mode == MODE_ACRCLOUD:
        print(f"  {Style.CYAN}{idx}.{Style.RESET} Update ACRCloud credentials")
        acr_update_idx = idx; idx += 1                               # 3
        print(f"  {Style.CYAN}{idx}.{Style.RESET} 🔑 Test ACRCloud credentials")
        acr_test_idx   = idx; idx += 1                               # 4
    else:
        acr_update_idx = None
        acr_test_idx   = None

    print(f"  {Style.CYAN}{idx}.{Style.RESET} Add/Update Last.fm API key")
    lastfm_add_idx = idx; idx += 1

    if has_lastfm:
        print(f"  {Style.CYAN}{idx}.{Style.RESET} Remove Last.fm API key")
        lastfm_rm_idx = idx; idx += 1
    else:
        lastfm_rm_idx = None

    # Shazam toggle (no API key required)
    shazam_toggle_state = 'OFF' if shazam_disabled else 'ON'
    print(f"  {Style.CYAN}{idx}.{Style.RESET} Toggle Shazam ({shazam_toggle_state})")
    shazam_toggle_idx = idx; idx += 1

    # Show ID source toggle (prints which backend identified each track)
    show_id_enabled = config.get('show_id_source', True)
    show_id_state = 'ON' if show_id_enabled else 'OFF'
    print(f"  {Style.CYAN}{idx}.{Style.RESET} Toggle ID Source Output ({show_id_state})")
    show_id_toggle_idx = idx; idx += 1

    # AcoustID menu items - show in all modes to allow dual mode setup
    if has_acoustid:
        print(f"  {Style.CYAN}{idx}.{Style.RESET} Update AcoustID API key")
    else:
        dual_hint = " {Style.GREEN}(enables Dual Mode!){Style.RESET}" if has_acr else " {Style.GREEN}(recommended){Style.RESET}"
        print(f"  {Style.CYAN}{idx}.{Style.RESET} Add AcoustID API key" + dual_hint.format(Style=Style))
    acoustid_add_idx = idx; idx += 1

    if has_acoustid:
        print(f"  {Style.CYAN}{idx}.{Style.RESET} Remove AcoustID API key")
        acoustid_rm_idx = idx; idx += 1
        print(f"  {Style.CYAN}{idx}.{Style.RESET} 🔑 Test AcoustID key")
        acoustid_test_idx = idx; idx += 1
    else:
        acoustid_rm_idx = None
        acoustid_test_idx = None

    print(f"  {Style.CYAN}{idx}.{Style.RESET} Back to main menu")
    back_idx = idx

    print(f"{Style.MAGENTA}{'─'*60}{Style.RESET}")
    choice = input(f"\n  {Style.BOLD}Choice (1-{back_idx}):{Style.RESET} ").strip()

    # ── 1. Switch mode ────────────────────────────────────────────────────
    if choice == '1':
        if current_mode == MODE_MANUAL:
            mode_name = "Manual Search Only"
        elif current_mode == MODE_DUAL:
            mode_name = "Dual Mode (Best of Both)"
        elif current_mode == MODE_ACRCLOUD:
            mode_name = "ACRCloud + MusicBrainz"
        else:
            mode_name = "MusicBrainz only"

        print(f"\n  Current mode: {mode_name}\n")
        print(f"  {Style.CYAN}1.{Style.RESET} ACRCloud + MusicBrainz {Style.DIM}(requires ACRCloud account){Style.RESET}")
        print(f"  {Style.CYAN}2.{Style.RESET} MusicBrainz only {Style.GREEN}⭐ (requires AcoustID - free){Style.RESET}")

        # Show dual mode option if both keys available
        if has_acr and has_acoustid:
            print(f"  {Style.CYAN}3.{Style.RESET} Dual Mode - Best of Both {Style.GREEN}⭐⭐ (requires both keys){Style.RESET}")
            print(f"     {Style.DIM}Runs both methods, picks highest confidence{Style.RESET}")
            max_sub = 3
        else:
            max_sub = 2

        print(f"\n  {Style.DIM}Note: Manual mode activates automatically when no keys are set{Style.RESET}\n")
        sub = input(f"  {Style.BOLD}Choose (1-{max_sub}):{Style.RESET} ").strip()
        if sub == '1':
            # Check if ACRCloud SDK is actually available
            if not ACRCLOUD_AVAILABLE:
                print(f"\n  {Style.RED}❌ Cannot switch to ACRCloud mode{Style.RESET}")
                print(f"  {Style.DIM}ACRCloud SDK is not installed in this build{Style.RESET}")
                print(f"\n  {Style.YELLOW}💡 This executable was built without ACRCloud support.{Style.RESET}")
                print(f"     {Style.DIM}To use ACRCloud mode:{Style.RESET}")
                print(f"     {Style.DIM}• Install SDK: pip install acrcloud-sdk-python{Style.RESET}")
                print(f"     {Style.DIM}• Run from source: python MixSplitR.py{Style.RESET}")
                print(f"     {Style.DIM}• Or rebuild the executable with ACRCloud installed{Style.RESET}")
                input(f"\n  Press Enter to continue...")
                return  # Don't save the config change
            
            config['mode'] = MODE_ACRCLOUD
            print(f"  {Style.GREEN}✅ Switched to ACRCloud + MusicBrainz{Style.RESET}")
            if not has_acr:
                print(f"  {Style.YELLOW}⚠ You will need to enter ACRCloud credentials before processing.{Style.RESET}")
        elif sub == '2':
            config['mode'] = MODE_MB_ONLY
            print(f"  {Style.GREEN}✅ Switched to MusicBrainz only{Style.RESET}")
        elif sub == '3' and has_acr and has_acoustid:
            config['mode'] = MODE_DUAL
            print(f"  {Style.GREEN}✅ Switched to Dual Mode (Best of Both){Style.RESET}")
            print(f"  {Style.DIM}Will run ACRCloud + AcoustID, pick best result{Style.RESET}")
        else:
            return
        save_config(config)
        print(f"  {Style.GREEN}💾 Config saved!{Style.RESET}")
        return

    # ── ACRCloud credential update ────────────────────────────────────────
    if acr_update_idx and choice == str(acr_update_idx):
        print(f"\n  Enter new ACRCloud credentials {Style.DIM}(press Enter to keep existing){Style.RESET}:\n")
        new_host   = input(f"  ACR Host [{config.get('host', '')}]: ").strip()
        new_key    = input(f"  Access Key [{config.get('access_key', '')[:10] + '...' if config.get('access_key') else ''}]: ").strip()
        new_secret = input(f"  Secret Key [{'****' if config.get('access_secret') else ''}]: ").strip()

        if new_host:   config['host']          = new_host
        if new_key:    config['access_key']    = new_key
        if new_secret: config['access_secret'] = new_secret
        config['timeout'] = 10

        print(f"\n  🔑 Validating credentials...", end='', flush=True)
        is_valid, error_msg = validate_acrcloud_credentials(config)
        if is_valid:
            print(f" {Style.GREEN}✅ Valid!{Style.RESET}")
            save_config(config)
            print(f"  {Style.GREEN}💾 Config saved!{Style.RESET}")
        else:
            print(f" {Style.RED}❌{Style.RESET}")
            print(f"     {Style.RED}Error: {error_msg}{Style.RESET}")
            save_anyway = input(f"\n  Save anyway? (y/n): ").strip().lower()
            if save_anyway == 'y':
                save_config(config)
                print(f"  {Style.YELLOW}💾 Config saved (but credentials may not work){Style.RESET}")
        return

    # ── ACRCloud test ─────────────────────────────────────────────────────
    if acr_test_idx and choice == str(acr_test_idx):
        if not has_acr:
            print("\n  ❌ No ACRCloud credentials configured")
        else:
            print(f"\n  🔑 Testing ACRCloud credentials...", end='', flush=True)
            is_valid, error_msg = validate_acrcloud_credentials(config)
            if is_valid:
                print(f" ✅ Valid!")
                print(f"     Your ACRCloud API credentials are working correctly.")
            else:
                print(f" ❌")
                print(f"     Error: {error_msg}")
        return

    # ── Last.fm add/update ────────────────────────────────────────────────
    if choice == str(lastfm_add_idx):
        key = input("\n  Last.fm API Key: ").strip()
        if key:
            config['lastfm_api_key'] = key
            set_lastfm_key(key)
            save_config(config)
            print("  ✅ Last.fm API key saved!")
        return

    # ── Last.fm remove ────────────────────────────────────────────────────
    if lastfm_rm_idx and choice == str(lastfm_rm_idx):
        del config['lastfm_api_key']
        set_lastfm_key(None)
        save_config(config)
        print("  ✅ Last.fm API key removed")
        return

    # ── Shazam toggle ───────────────────────────────────────────────────
    if shazam_toggle_idx and choice == str(shazam_toggle_idx):
        config['disable_shazam'] = not bool(config.get('disable_shazam', False))
        save_config(config)
        if config.get('disable_shazam', False):
            print(f"\n  {Style.YELLOW}⏸️  Shazam disabled{Style.RESET}")
        else:
            print(f"\n  {Style.GREEN}✅ Shazam enabled{Style.RESET}")
        return

    # ── Show ID Source toggle ─────────────────────────────────────────────
    if show_id_toggle_idx and choice == str(show_id_toggle_idx):
        config['show_id_source'] = not bool(config.get('show_id_source', True))
        save_config(config)
        if config['show_id_source']:
            print(f"\n  {Style.GREEN}✅ ID source output enabled{Style.RESET}")
            print(f"  {Style.DIM}   Each track will show: ID: backend → Artist - Title{Style.RESET}")
        else:
            print(f"\n  {Style.YELLOW}⏸️  ID source output disabled{Style.RESET}")
        return

    # ── AcoustID add/update ───────────────────────────────────────────────
    if acoustid_add_idx and choice == str(acoustid_add_idx):
        print(f"\n{Style.MAGENTA}{'─'*60}{Style.RESET}")
        print("  Get a free AcoustID API key")
        print(f"{Style.MAGENTA}{'─'*60}{Style.RESET}")
        print("\n  AcoustID fingerprints audio to identify tracks.")
        print("  Getting your own key ensures reliable identification.\n")
        print("  Steps:")
        print("    1. Visit: https://acoustid.org/api-key")
        print("    2. Register (free, takes 30 seconds)")
        print("    3. Copy your API key")
        print("    4. Paste it below\n")
        
        key = input("  AcoustID API Key: ").strip()
        if key:
            config['acoustid_api_key'] = key
            set_acoustid_api_key(key)
            save_config(config)
            print(f"\n  {Style.GREEN}✅ AcoustID API key saved!{Style.RESET}")
            print(f"  {Style.GREEN}   You now have your own personal rate limits!{Style.RESET}")
        else:
            print(f"\n  {Style.YELLOW}⚠️  No key entered{Style.RESET}")
        return
    
    # ── AcoustID remove ───────────────────────────────────────────────────
    if acoustid_rm_idx and choice == str(acoustid_rm_idx):
        del config['acoustid_api_key']
        set_acoustid_api_key(None)
        save_config(config)
        print(f"\n  {Style.YELLOW}✅ AcoustID API key removed{Style.RESET}")
        print(f"  {Style.YELLOW}⚠️  Audio fingerprinting will not work without a key{Style.RESET}")
        return
    
    # ── AcoustID test ─────────────────────────────────────────────────────
    if acoustid_test_idx and choice == str(acoustid_test_idx):
        if not has_acoustid:
            print("\n  ❌ No AcoustID API key configured")
        else:
            print(f"\n  🔑 Testing AcoustID API key...")
            
            # Simple validation test
            api_key = config.get('acoustid_api_key')
            key_len = len(api_key) if api_key else 0
            
            # Display key info
            if api_key:
                print(f"     Key: {api_key[:8]}...{api_key[-4:]}")
                print(f"     Length: {key_len} characters")
            
            # Basic validation
            if key_len < 8:
                print(f"\n  {Style.RED}❌ Invalid key - too short{Style.RESET}")
                print(f"     AcoustID keys are typically 8+ characters")
            else:
                # Check if acoustid module is available
                try:
                    import acoustid
                    print(f"     {Style.GREEN}✅ Key format looks valid{Style.RESET}")
                    print(f"     {Style.GREEN}✅ acoustid module is available{Style.RESET}")
                    print(f"\n  {Style.GREEN}Key appears to be configured correctly!{Style.RESET}")
                    print(f"  Note: Full testing requires processing an actual audio file")
                except ImportError:
                    print(f"\n  {Style.YELLOW}⚠️  acoustid module not installed{Style.RESET}")
                    print(f"     Install with: pip install pyacoustid")
        return


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='MixSplitR - Mix Archival Tool')
    parser.add_argument('--no-bpm-dsp', action='store_true')
    parser.add_argument('--no-update-check', action='store_true')
    args = parser.parse_args()

    if args.no_bpm_dsp:
        # Persist to config so mixsplitr_processing reads it at function-call time
        _cfg = get_config()
        _cfg['disable_local_bpm'] = True
        save_config(_cfg)

    # Optional Windows relaunch path (opt-in via MIXSPLITR_FORCE_RELAUNCH=1).
    _ensure_windows_console_host(81, 50)

    # Keep startup window proportions predictable across Mac/Windows builds.
    set_terminal_window_size(default_cols=81, default_lines=50)
    
    # Show animated opening screen
    show_opening_screen()
    
    update_info = None
    if not args.no_update_check:
        update_info = check_for_updates()
        if isinstance(update_info, dict):
            print(
                f"  {Style.GREEN}🆕 Update available!{Style.RESET} "
                f"{Style.BOLD}v{update_info['latest']}{Style.RESET} "
                f"{Style.DIM}(current: v{update_info['current']}){Style.RESET}"
            )
            if update_info.get("url"):
                print(f"  {Style.DIM}Download: {update_info['url']}{Style.RESET}\n")

    # Persistent state across loop iterations (preserved by drag-drop / load)
    audio_files = []
    base_dir = ""
    temp_folder = ""
    ui_notice = ""

    # =========================================================================
    # MAIN LOOP – "Cancel" in preview restarts here instead of closing
    # =========================================================================
    while True:
        config = get_config()

        # =========================================================================
        # STEP 1: Find audio files first (before showing menu)
        # Only auto-scan if no files were already loaded (e.g. from drag-drop)
        # =========================================================================

        if not audio_files:
            portable_startup_scan = bool(config.get('portable_mode_local_scan', False))

            if portable_startup_scan:
                # Determine local startup scan directory
                if getattr(sys, 'frozen', False):
                    exe_dir = os.path.dirname(sys.executable)

                    # For macOS .app bundle, check local folder first for portable use
                    if '.app/Contents' in exe_dir and sys.platform == 'darwin':
                        app_bundle_dir = exe_dir
                        while app_bundle_dir and not app_bundle_dir.endswith('.app'):
                            app_bundle_dir = os.path.dirname(app_bundle_dir)
                        app_parent_dir = os.path.dirname(app_bundle_dir) if app_bundle_dir else exe_dir

                        # Check if there are audio files in the app's parent directory (portable mode)
                        local_audio_files = []
                        for ext in AUDIO_EXTENSIONS_GLOB:
                            local_audio_files.extend(glob.glob(os.path.join(app_parent_dir, ext)))

                        if local_audio_files:
                            base_dir = app_parent_dir
                            print(f"  {Style.GREEN}📂 Found {len(local_audio_files)} audio file(s) next to app{Style.RESET}\n")
                        else:
                            base_dir = os.path.expanduser('~/Music')
                    else:
                        base_dir = exe_dir
                else:
                    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            else:
                # Non-portable startup mode avoids scanning the local download/app folder.
                base_dir = os.path.expanduser('~/Music')

            # Scan for audio files
            for ext in AUDIO_EXTENSIONS_GLOB:
                audio_files.extend(glob.glob(os.path.join(base_dir, ext)))

            # Show initial file scan result if files were found
            if audio_files:
                print(f"  {Style.GREEN}✓ Found {len(audio_files)} audio file(s){Style.RESET}")
    
        # =========================================================================
        # STEP 2: Validate API credentials (mode-aware)
        # =========================================================================

        # Globals removed — processing functions now read config directly
        if config.get('lastfm_api_key'):
            set_lastfm_key(config['lastfm_api_key'])
        if config.get('acoustid_api_key'):
            set_acoustid_api_key(config['acoustid_api_key'])
    
        current_mode = get_mode(config)
    
        if current_mode == MODE_ACRCLOUD:
            # ── ACRCloud mode: SDK must be present and creds must be valid ──────
            if not ACRCLOUD_AVAILABLE:
                print(f"\n  {Style.YELLOW}⚠️  ACRCloud SDK not available in this build{Style.RESET}")
                print(f"  {Style.DIM}Automatically switching to MusicBrainz-only mode...{Style.RESET}")
                config['mode'] = MODE_MB_ONLY
                save_config(config)
                current_mode = MODE_MB_ONLY
                # Fall through to MusicBrainz setup below
            else:
                # ACRCloud SDK is available, proceed with validation
                while True:
                    print(f"  🔑 Validating ACRCloud credentials...", end='', flush=True)
                    is_valid, error_msg = validate_acrcloud_credentials(config)
                
                    if is_valid:
                        print(f" {Style.GREEN}✓{Style.RESET}")
                        break
                    else:
                        print(f" {Style.RED}✗{Style.RESET}")
                        print(f"\n  {Style.RED}ACRCloud API Error:{Style.RESET} {error_msg}")
                        print(f"\n  {Style.BOLD}Options:{Style.RESET}")
                        print(f"  {Style.CYAN}1.{Style.RESET} Enter new credentials")
                        print(f"  {Style.CYAN}2.{Style.RESET} Switch to MusicBrainz-only mode")
                        print(f"  {Style.CYAN}3.{Style.RESET} Exit")
                    
                        fix_choice = input(f"\n  {Style.BOLD}Choice (1-3):{Style.RESET} ").strip()
                    
                        if fix_choice == '1':
                            print(f"\n  {Style.BOLD}Enter ACRCloud credentials:{Style.RESET}")
                            print(f"  {Style.DIM}(Get these from https://console.acrcloud.com){Style.RESET}\n")
                        
                            new_host   = input(f"  ACR Host (e.g., identify-us-west-2.acrcloud.com): ").strip()
                            new_key    = input(f"  Access Key: ").strip()
                            new_secret = input(f"  Access Secret: ").strip()
                        
                            if new_host and new_key and new_secret:
                                config['host']          = new_host
                                config['access_key']    = new_key
                                config['access_secret'] = new_secret
                                config['timeout']       = 10
                                save_config(config)
                                print(f"\n  {Style.GREEN}💾 Credentials saved!{Style.RESET}")
                                print(f"  Retrying validation...\n")
                            else:
                                print(f"\n  {Style.YELLOW}⚠ All fields are required{Style.RESET}")
                        elif fix_choice == '2':
                            config['mode'] = MODE_MB_ONLY
                            save_config(config)
                            current_mode = MODE_MB_ONLY
                            print(f"\n  {Style.GREEN}✅ Switched to MusicBrainz-only mode{Style.RESET}")
                            break          # exit validation loop – no ACRCloud check needed
                        else:
                            return close_terminal()
        
        if current_mode == MODE_MB_ONLY:
            # ── MusicBrainz-only mode ──────────────────────────────────────────
            if not ACOUSTID_AVAILABLE:
                print(f"\n  {Style.RED}❌ AcoustID / MusicBrainz libraries not found!{Style.RESET}")
                print(f"  {Style.DIM}Install with: pip install pyacoustid musicbrainzngs{Style.RESET}")
                return close_terminal()
            
            # Check for chromaprint/fpcalc
            has_chromaprint, fpcalc_path = check_chromaprint_available()
            if not has_chromaprint:
                print(f"\n  {Style.YELLOW}⚠️  Warning: chromaprint/fpcalc not found!{Style.RESET}")
                print(f"  {Style.DIM}AcoustID fingerprinting will not work without it.{Style.RESET}")
                print(f"\n  {Style.BOLD}Install chromaprint:{Style.RESET}")
                print(f"   • Windows: Download from https://acoustid.org/chromaprint")
                print(f"   • macOS:   brew install chromaprint")
                print(f"   • Linux:   apt install libchromaprint-tools")
                print(f"\n  {Style.DIM}Without chromaprint, tracks will be marked as unidentified.{Style.RESET}")
                print(f"  {Style.DIM}You can then use the interactive editor to manually identify them.{Style.RESET}\n")
                cont = input(f"  Continue anyway? (y/n) [n]: ").strip().lower()
                if cont != 'y':
                    return close_terminal()
            else:
                print(f"  {Style.CYAN}🔍 MusicBrainz-only mode{Style.RESET} – AcoustID fingerprinting enabled ✓")
                if fpcalc_path:
                    print(f"  {Style.DIM}   chromaprint: {fpcalc_path}{Style.RESET}")
    
        # =========================================================================
        # STEP 3: Show main menu (now that we have files and valid credentials)
        # =========================================================================
    
        # Cache goes in safe app data directory, temp files stay with the music
        cache_path  = get_cache_path("mixsplitr_cache.json")
        temp_folder = os.path.join(base_dir, "mixsplitr_temp")
    
        # Main menu loop
        while True:
            menu_state = AppState(
                audio_files=audio_files,
                base_dir=base_dir,
                temp_folder=temp_folder,
                config=config,
                current_mode=current_mode,
                update_info=update_info,
                ui_notice=ui_notice,
            )
            mode_badge = _build_mode_badge(menu_state.current_mode, menu_state.update_info)
            cached_track_count = _get_cached_track_count(cache_path)

            # Show interactive menu (prompt_toolkit or fallback)
            menu_result = show_main_menu(
                menu_state.audio_files, menu_state.base_dir, menu_state.config, mode_badge,
                has_cached_preview=(cached_track_count > 0),
                update_info=menu_state.update_info,
                ui_notice=menu_state.ui_notice,
            )
            # Show notices once so the menu stays clean.
            ui_notice = ""

            # Handle path input (drag-drop or typed path)
            if menu_result.key == "__path__":
                _handle_main_menu_path_input(menu_result.text_input, menu_state)
                audio_files = menu_state.audio_files
                base_dir = menu_state.base_dir
                temp_folder = menu_state.temp_folder
                continue

            # Handle cancelled/empty selection
            if menu_result.cancelled or not menu_result.key:
                continue

            choice = menu_result.key
            utility_result = _handle_main_menu_utility_choice(choice, menu_state, cache_path)
            if utility_result == "exit_app":
                return close_terminal()
            if utility_result == "handled":
                audio_files = menu_state.audio_files
                base_dir = menu_state.base_dir
                temp_folder = menu_state.temp_folder
                config = menu_state.config
                current_mode = menu_state.current_mode
                ui_notice = menu_state.ui_notice
                continue

            if choice == "load_files":
                _handle_load_files_choice(menu_state)
                audio_files = menu_state.audio_files
                base_dir = menu_state.base_dir
                temp_folder = menu_state.temp_folder
                continue  # Back to main menu after changing directory

            # Processing modes (only available when files are loaded)
            processing_preview_mode = _resolve_processing_choice(choice, menu_state)
            if processing_preview_mode is None:
                continue

            audio_files = menu_state.audio_files
            base_dir = menu_state.base_dir
            temp_folder = menu_state.temp_folder
            config = menu_state.config
            current_mode = menu_state.current_mode
            preview_mode = processing_preview_mode
            break  # Exit menu loop to process files
    
        # File analysis (files already found earlier)
        print(f"\n{Style.CYAN}{'─'*60}{Style.RESET}")
        print(f"\n  Analyzing {len(audio_files)} file(s)...")
        file_analysis = analyze_files_parallel(audio_files)
        file_info = {f['file']: f for f in file_analysis}
        mixes = [f for f in file_analysis if f['is_mix']]
    
        light_preview = False
        if preview_mode:
            # Use new prompt_toolkit menu for preview type
            light_preview = show_preview_type_menu()
            if light_preview is None:
                print(f"   {Style.YELLOW}→ Preview selection canceled. Returning to main menu.{Style.RESET}")
                ui_notice = "Preview selection canceled."
                continue
            print(f"   → {'Light' if light_preview else 'Full'} preview selected")

        use_visual = False
        use_assisted = False
        if mixes:
            print(f"\n  Found {Style.BOLD}{len(mixes)} mix(es){Style.RESET} to split.\n")
            # One-click direct mode should remain truly one-click:
            # skip split-mode prompts and use automatic splitting.
            if preview_mode and SPLITTER_UI_AVAILABLE:
                split_mode = show_split_mode_menu()
                if split_mode == 'manual':
                    use_visual = True
                    print(f"   {Style.GREEN}→ Will use visual editor{Style.RESET}")
                elif split_mode == 'assisted':
                    use_assisted = True
                    print(f"   {Style.GREEN}→ Auto-detect + visual editor review{Style.RESET}")
            elif preview_mode and not SPLITTER_UI_AVAILABLE:
                print(f"   {Style.YELLOW}→ Visual splitter unavailable; using automatic splitting{Style.RESET}")
            else:
                print(f"   {Style.DIM}→ Direct Mode uses automatic splitting{Style.RESET}")
    
        output_folder = get_output_directory(config)
        print(f"  {Style.DIM}📂 Output: {output_folder}{Style.RESET}\n")

        # Recognizer only needed in ACRCloud mode
        recognizer = None
        if current_mode == MODE_ACRCLOUD:
            recognizer = ACRCloudRecognizer(config)
    
        existing_tracks = scan_existing_library(output_folder)
        batches = create_file_batches(audio_files, get_available_ram_gb())
    
        all_results = []
        artwork_cache_global = {}
        file_index = {f: i+1 for i, f in enumerate(audio_files)}
        _session_split_data = {}  # {audio_file: {method, points_sec, params}} for manifest
    
        for batch_num, batch_files in enumerate(batches, 1):
            print(f"\n{Style.BLUE}{'─'*50}")
            print(f"  {Style.BOLD}📦 Batch {batch_num}/{len(batches)}{Style.RESET}{Style.BLUE} ({len(batch_files)} file{'s' if len(batch_files) > 1 else ''})")
            print(f"{'─'*50}{Style.RESET}")
        
            all_chunks = []
            with tqdm(
                total=len(batch_files),
                desc=f"  Preparing batch {batch_num}/{len(batches)}",
                unit="file",
                ncols=72,
                leave=False,
            ) as load_bar:
                for file_idx, audio_file in enumerate(batch_files, 1):
                    fnum = file_index[audio_file]
                    info = file_info.get(audio_file, {})
                    filename = os.path.basename(audio_file)
                    short_name = (filename[:36] + "...") if len(filename) > 39 else filename
                    load_bar.set_postfix_str(short_name)

                    # Check if this is a large file that needs streaming mode
                    if is_large_file(audio_file):
                        # Use FFmpeg streaming mode for large files
                        large_chunks = process_large_file_streaming(
                            audio_file, fnum, output_folder, temp_folder,
                            use_visual=use_visual, use_assisted=use_assisted, preview_mode=preview_mode
                        )
                        all_chunks.extend(large_chunks)
                        # Reconstruct split points from chunk boundaries for manifest
                        if large_chunks:
                            _lf_pts = sorted(set(
                                cd.get('large_file_start', 0) for cd in large_chunks
                                if cd.get('large_file_start', 0) > 0
                            ))
                            _session_split_data[audio_file] = {
                                'method': 'large_file_streaming', 'points_sec': _lf_pts,
                                'params': {'silence_thresh_db': -40, 'min_silence_len_sec': 2.0},
                                'large_file_mode': True
                            }
                        load_bar.update(1)
                        continue

                    if info.get('is_mix'):
                        chunks = None
                        if use_visual:
                            # Pure manual mode - open visual editor with no pre-loaded points
                            pts = get_split_points_visual(audio_file)
                            if pts:
                                chunks = split_audio_at_points(audio_file, pts)
                                _session_split_data[audio_file] = {
                                    'method': 'visual', 'points_sec': sorted(pts), 'params': {}
                                }
                        elif use_assisted:
                            # Assisted mode - detect silence first, then let user review
                            load_bar.set_postfix_str(f"{short_name} (auto-detect)")
                            rec = AudioSegment.from_file(audio_file)
                            # Use detect_silence to find gaps (imported at top of file)
                            silent_ranges = detect_silence(rec, min_silence_len=2000, silence_thresh=-40)

                            # Convert silent ranges to split points (middle of each silence)
                            pre_detected = []
                            for start_ms, end_ms in silent_ranges:
                                mid_point = (start_ms + end_ms) / 2 / 1000  # Convert to seconds
                                # Skip points too close to start or end
                                if mid_point > 5 and mid_point < len(rec) / 1000 - 5:
                                    pre_detected.append(mid_point)

                            del rec

                            # Open visual editor with pre-loaded points
                            pts = get_split_points_visual(audio_file, existing_points=pre_detected)
                            if pts:
                                chunks = split_audio_at_points(audio_file, pts)
                                _session_split_data[audio_file] = {
                                    'method': 'assisted', 'points_sec': sorted(pts),
                                    'params': {'silence_thresh_db': -40, 'min_silence_len_sec': 2.0}
                                }

                        # Fallback to automatic if no chunks yet
                        if chunks is None:
                            rec = AudioSegment.from_file(audio_file)
                            chunks = split_on_silence_with_loading_bar(
                                rec,
                                min_silence_len=2000,
                                silence_thresh=-40,
                                keep_silence=200,
                                progress_label=f"     Splitting {file_idx}/{len(batch_files)}"
                            )
                            _session_split_data[audio_file] = {
                                'method': 'automatic', 'points_sec': None,
                                'num_segments': len(chunks),
                                'params': {'silence_thresh_db': -40, 'min_silence_len_sec': 2.0, 'keep_silence_ms': 200}
                            }
                            del rec
                            load_bar.set_postfix_str(f"{short_name} -> {len(chunks)} tracks")
                        for idx, chunk in enumerate(chunks):
                            all_chunks.append({'chunk': chunk, 'file_num': fnum, 'original_file': audio_file, 'split_index': idx})
                    else:
                        rec = AudioSegment.from_file(audio_file)
                        all_chunks.append({'chunk': rec, 'file_num': fnum, 'original_file': audio_file})
                        _session_split_data[audio_file] = {
                            'method': 'single_track', 'points_sec': [], 'params': {}
                        }

                    load_bar.update(1)
            print(f"  {Style.DIM}✓ Prepared {len(batch_files)} file(s) in batch{Style.RESET}")
        
            # Cache chunks to disk for full preview mode
            if preview_mode and not light_preview:
                print(f"\n  💾 Caching {len(all_chunks)} audio chunks...", end='', flush=True)
                os.makedirs(temp_folder, exist_ok=True)
                for cd in all_chunks:
                    tp = os.path.join(temp_folder, f"chunk_{cd['file_num']}_{cd.get('split_index', 0)}.flac")
                    chunk = cd['chunk']
                    # Convert to stereo if multi-channel (FLAC supports max 8 channels)
                    # Use ffmpeg -ac 2 for proper mixdown (pydub.set_channels doesn't handle >2 to 2)
                    if chunk.channels > 8:
                        chunk.export(tp, format="flac", parameters=["-ac", "2", "-compression_level", "8"])
                    else:
                        chunk.export(tp, format="flac", parameters=["-compression_level", "8"])
                    cd['temp_chunk_path'] = tp
                print(" ✓")
        
            # Identify tracks
            print(f"\n  🎵 Identifying {len(all_chunks)} tracks...")
            lock = threading.Lock()
            results = []

            if current_mode == MODE_MANUAL:
                # Manual mode – no fingerprinting, mark all as unidentified for manual entry
                print(f"  {Style.DIM}(Manual search mode - skipping fingerprinting){Style.RESET}")
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = {executor.submit(process_single_track_manual, cd, i, existing_tracks, output_folder, lock, preview_mode): i for i, cd in enumerate(all_chunks)}
                    for future in tqdm(as_completed(futures), total=len(all_chunks), desc="     Progress", ncols=60):
                        results.append(future.result())
            elif current_mode == MODE_ACRCLOUD:
                # ACRCloud mode – needs rate limiter for the external API
                rate_limiter = RateLimiter(min_interval=1.2)
                with ThreadPoolExecutor(max_workers=4) as executor:
                    futures = {executor.submit(process_single_track, cd, i, recognizer, rate_limiter, existing_tracks, output_folder, lock, preview_mode): i for i, cd in enumerate(all_chunks)}
                    for future in tqdm(as_completed(futures), total=len(all_chunks), desc="     Progress", ncols=60):
                        results.append(future.result())
            elif current_mode == MODE_DUAL:
                # Dual mode – run both ACRCloud AND AcoustID, pick best by confidence
                print(f"  {Style.GREEN}⚡ Dual mode: comparing ACRCloud + AcoustID{Style.RESET}")
                rate_limiter = RateLimiter(min_interval=1.2)  # ACRCloud rate limit
                with ThreadPoolExecutor(max_workers=3) as executor:
                    futures = {executor.submit(process_single_track_dual, cd, i, recognizer, rate_limiter, existing_tracks, output_folder, lock, preview_mode): i for i, cd in enumerate(all_chunks)}
                    for future in tqdm(as_completed(futures), total=len(all_chunks), desc="     Progress", ncols=60):
                        results.append(future.result())
            else:
                # MusicBrainz-only mode – no ACRCloud recognizer
                # AcoustID has a gentler rate limit; we still use a thread pool but
                # with a smaller interval enforced inside identify_with_acoustid.
                rate_limiter = RateLimiter(min_interval=0.5)   # soft throttle
                with ThreadPoolExecutor(max_workers=2) as executor:
                    futures = {executor.submit(process_single_track_mb_only, cd, i, existing_tracks, output_folder, lock, preview_mode): i for i, cd in enumerate(all_chunks)}
                    for future in tqdm(as_completed(futures), total=len(all_chunks), desc="     Progress", ncols=60):
                        results.append(future.result())
        
            all_results.extend(results)
        
            # Show batch summary
            batch_identified = len([r for r in results if r['status'] == 'identified'])
            batch_unidentified = len([r for r in results if r['status'] == 'unidentified'])
            batch_skipped = len([r for r in results if r['status'] == 'skipped'])
            print(f"\n  ✅ {batch_identified} identified  ❓ {batch_unidentified} unidentified  ⏭️ {batch_skipped} skipped")
        
            identified = [r for r in results if r['status'] == 'identified']
            if identified:
                print(f"\n  🖼️  Downloading artwork...", end='', flush=True)
                urls = [r['art_url'] for r in identified if r.get('art_url')]
                if urls:
                    artwork_cache_global.update(batch_download_artwork(urls))
                print(f" ✓")
        
            del all_chunks
            gc.collect()
    
        if preview_mode:
            # Check if all tracks were skipped (nothing to process)
            total_skipped = len([r for r in all_results if r['status'] == 'skipped'])
            total_tracks = len(all_results)
            
            if total_tracks > 0 and total_skipped == total_tracks:
                # All tracks were skipped (already exist in library)
                print(f"\n{Style.YELLOW}⏭️  All tracks skipped!{Style.RESET}")
                print(f"  {Style.DIM}All {total_tracks} track(s) already exist in your library.{Style.RESET}")
                print(f"  {Style.DIM}No new tracks to process.{Style.RESET}")
                input(f"\n📍 Press Enter to return to main menu...")
                continue  # Back to main menu
            
            try:
                sample_seconds = int(config.get('fingerprint_sample_seconds', 12))
            except Exception:
                sample_seconds = 12
            sample_seconds = max(8, min(45, sample_seconds))

            cache_data = {
                'tracks': all_results, 'output_folder': output_folder,
                'artwork_cache': {}, 'light_preview': light_preview,
                'split_data': _session_split_data,
                'config_snapshot': {
                    'identification_mode': current_mode,
                    'shazam_enabled': not bool(config.get('disable_shazam', False)),
                    'use_local_bpm': not bool(config.get('disable_local_bpm', False)),
                    'show_id_source': bool(config.get('show_id_source', True)),
                    'fingerprint_sample_seconds': sample_seconds,
                }
            }
            for url, data in artwork_cache_global.items():
                try:
                    cache_data['artwork_cache'][url] = base64.b64encode(data).decode('utf-8')
                except:
                    pass
        
            if save_preview_cache(cache_data, cache_path):
                display_preview_table(cache_data)

                print(f"\n  {Style.BOLD}What would you like to do next?{Style.RESET}\n")
                print(f"  {Style.CYAN}1.{Style.RESET} {Style.GREEN}✅ Finish Unsaved Preview Now{Style.RESET}")
                print(f"       {Style.DIM}Export tracks from current preview data and save session history{Style.RESET}\n")
                print(f"  {Style.CYAN}2.{Style.RESET} {Style.YELLOW}✏️  Edit Unsaved Preview Tracks{Style.RESET}")
                print(f"       {Style.DIM}Review, fix, or manually identify tracks before exporting")
                print(f"       (Play audio, edit metadata, convert unidentified → identified){Style.RESET}\n")
                print(f"  {Style.CYAN}3.{Style.RESET} {Style.RED}❌ Cancel{Style.RESET}")
                print(f"       {Style.DIM}Discard this preview and return to main menu{Style.RESET}\n")

                next_choice = input(f"  {Style.BOLD}Enter choice (1-3):{Style.RESET} ").strip()
                if next_choice == '1':
                    did_apply = apply_from_cache(cache_path, temp_folder)
                    if did_apply:
                        _clear_unsaved_preview_data(cache_path, temp_folder)
                        input(f"\n📍 Press Enter to return to main menu...")
                    else:
                        ui_notice = "Export canceled. No files were processed."
                    continue
                elif next_choice == '2':
                    result = interactive_editor(cache_data, cache_path)
                    if result == 'apply':
                        did_apply = apply_from_cache(cache_path, temp_folder)
                        if did_apply:
                            _clear_unsaved_preview_data(cache_path, temp_folder)
                            input(f"\n📍 Press Enter to return to main menu...")
                        else:
                            ui_notice = "Export canceled. No files were processed."
                    continue
                else:
                    continue
        else:
            # Direct mode - also check for all-skipped scenario
            total_skipped = len([r for r in all_results if r['status'] == 'skipped'])
            total_tracks = len(all_results)
            
            if total_tracks > 0 and total_skipped == total_tracks:
                # All tracks were skipped (already exist in library)
                print(f"\n{Style.YELLOW}⏭️  All tracks skipped!{Style.RESET}")
                print(f"  {Style.DIM}All {total_tracks} track(s) already exist in your library.{Style.RESET}")
                print(f"  {Style.DIM}No new tracks to process.{Style.RESET}")
                input(f"\n📍 Press Enter to return to main menu...")
                continue  # Back to main menu

            # Ask once at the end of one-click mode which format to export.
            direct_output_format = show_format_selection_menu()
            if not direct_output_format:
                print(f"  {Style.YELLOW}⚠️  Export cancelled. Returning to main menu.{Style.RESET}")
                ui_notice = "Export canceled. No files were processed."
                continue
            if direct_output_format not in AUDIO_FORMATS:
                print(f"  {Style.YELLOW}⚠️  Unknown format '{direct_output_format}', using FLAC{Style.RESET}")
                direct_output_format = "flac"

            direct_identified = [r for r in all_results if r['status'] == 'identified']
            if direct_identified:
                print(
                    f"\n  💾 Exporting {len(direct_identified)} track(s) as "
                    f"{Style.BOLD}{AUDIO_FORMATS[direct_output_format]['name']}{Style.RESET}..."
                )

            _direct_output_files = []
            direct_unidentified_saved = 0
            for idx, r in enumerate(direct_identified, 1):
                print(f"     [{idx}/{len(direct_identified)}] {r['artist'][:20]} - {r['title'][:25]}", end='\r')
                out_path = embed_and_sort_generic(
                    r['temp_flac'],
                    r['artist'],
                    r['title'],
                    r['album'],
                    r.get('art_url'),
                    output_folder,
                    output_format=direct_output_format,
                    artwork_cache=artwork_cache_global,
                    enhanced_metadata=r.get('enhanced_metadata', {})
                )
                if out_path:
                    _direct_output_files.append(out_path)

            # Keep Session History complete even when a run yields only
            # unidentified tracks (no identified exports).
            for result in all_results:
                if result.get('status') != 'unidentified':
                    continue
                unidentified_path = result.get('unidentified_path')
                if unidentified_path and os.path.exists(unidentified_path):
                    _direct_output_files.append(unidentified_path)
                    direct_unidentified_saved += 1

            # Preserve insertion order while removing duplicates.
            _direct_output_files = list(dict.fromkeys(_direct_output_files))

            if direct_identified:
                print(f"     Saved {len(direct_identified)} identified tracks" + " " * 30)
            if direct_unidentified_saved:
                print(f"  📁 Kept {direct_unidentified_saved} unidentified track file(s)")
            
            id_count = len([r for r in all_results if r['status'] == 'identified'])
            print(f"\n{Style.GREEN}✅ Complete!{Style.RESET} {Style.BOLD}{id_count}{Style.RESET} tracks identified and saved to {Style.DIM}{output_folder}{Style.RESET}")

            dm_path = _save_direct_mode_session_record(
                all_results=all_results,
                output_files=_direct_output_files,
                current_mode=current_mode,
                direct_output_format=direct_output_format,
                config=config,
                session_split_data=_session_split_data,
            )
            if dm_path:
                print(f"  📋 Session record saved (manifest): {os.path.basename(dm_path)}")

            # One-click processing is complete; clear any old unsaved preview state.
            _clear_unsaved_preview_data(cache_path, temp_folder)

            input(f"\n📍 Press Enter to return to main menu...")
            continue  # Back to main menu

    close_terminal()


if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
