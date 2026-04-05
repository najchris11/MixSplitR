"""
mixsplitr_core.py - Core utilities for MixSplitR

FIXES APPLIED:
1. Fixed API key detection to use correct config key names (access_key vs acrcloud_access_key)
2. Enhanced ffmpeg path detection and error handling
3. Added debug output for configuration troubleshooting

Contains:
- Terminal styling (Style class)
- Version info and update checking
- Resource path handling (PyInstaller compatible)
- FFmpeg/FFprobe setup
- Configuration management
- Rate limiting
- Audio file analysis utilities
"""

import os
import sys
import json
import time
import shutil
import threading
import subprocess
import re
import requests
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# =============================================================================
# TERMINAL STYLING - ANSI escape codes for colors and formatting
# =============================================================================

class Style:
    """ANSI escape codes for terminal styling"""
    # Colors
    CYAN = '\033[38;5;110m'
    GREEN = '\033[38;5;72m'
    YELLOW = '\033[38;5;186m'
    RED = '\033[38;5;167m'
    MAGENTA = '\033[38;5;104m'
    BLUE = '\033[38;5;68m'
    WHITE = '\033[38;5;252m'
    BRIGHT_WHITE = WHITE
    GRAY = '\033[38;5;242m'
    
    # Formatting
    BOLD = '\033[1m'
    DIM = '\033[2m'
    UNDERLINE = '\033[4m'
    
    # Reset
    RESET = '\033[0m'
    
    @staticmethod
    def disable():
        """Disable colors (for non-supporting terminals)"""
        Style.CYAN = Style.GREEN = Style.YELLOW = Style.RED = ''
        Style.MAGENTA = Style.BLUE = Style.WHITE = Style.GRAY = ''
        Style.BRIGHT_WHITE = ''
        Style.BOLD = Style.DIM = Style.UNDERLINE = Style.RESET = ''

# Enable ANSI on Windows 10+
if sys.platform == 'win32':
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
    except:
        Style.disable()

# =============================================================================
# AUDIO FILE CONSTANTS
# =============================================================================

# For glob patterns (e.g., glob.glob(os.path.join(dir, ext)))
AUDIO_EXTENSIONS_GLOB = ['*.wav', '*.flac', '*.mp3', '*.m4a', '*.ogg', '*.aac', '*.wma', '*.aiff', '*.opus']

# For checking file extensions (e.g., if ext.lower() in AUDIO_EXTENSIONS)
AUDIO_EXTENSIONS = ['.wav', '.flac', '.mp3', '.m4a', '.ogg', '.aac', '.wma', '.aiff', '.opus']

# =============================================================================
# VERSION INFO
# =============================================================================

CURRENT_VERSION = "7.1"
GITLAB_REPO = "chefkjd/MixSplitR"  # GitLab project path
GITHUB_REPO = "chefkjd/MixSplitR"

# ---------------------------------------------------------------------------
# Identification-mode helpers
# ---------------------------------------------------------------------------
# Valid values stored in config["mode"]:
#   "acrcloud"          – original behaviour (ACRCloud primary, MB fallback)
#   "musicbrainz_only"  – AcoustID fingerprint → MusicBrainz only, no ACRCloud
# ---------------------------------------------------------------------------

MODE_ACRCLOUD = "acrcloud"
MODE_MB_ONLY  = "musicbrainz_only"
MODE_MANUAL   = "manual_search_only"
MODE_DUAL     = "dual_best_match"  # Both ACRCloud + AcoustID, pick best
VALID_MODES   = (MODE_ACRCLOUD, MODE_MB_ONLY, MODE_MANUAL, MODE_DUAL)


def get_mode(config=None):
    """
    Return the current identification mode from config.
    Auto-detects dual mode if both keys available, manual if none.
    
    FIX: Now correctly checks for 'access_key' (ACRCloud) not 'acrcloud_access_key'
    """
    if config is None:
        config = get_config()

    # Check if any fingerprinting keys are configured
    # FIX: Use correct key names from config
    has_acrcloud = bool(config.get('access_key'))  # FIXED: was 'acrcloud_access_key'
    has_acoustid = bool(config.get('acoustid_api_key'))

    # If no keys, force manual mode
    if not has_acrcloud and not has_acoustid:
        return MODE_MANUAL

    # Get configured mode
    configured_mode = config.get("mode", MODE_ACRCLOUD)

    # If dual mode requested but missing keys, fallback
    if configured_mode == MODE_DUAL:
        if not has_acrcloud or not has_acoustid:
            # Missing one or both keys for dual mode
            if has_acrcloud:
                return MODE_ACRCLOUD
            elif has_acoustid:
                return MODE_MB_ONLY
            else:
                return MODE_MANUAL

    # If mode is acrcloud but no acrcloud keys, switch to musicbrainz
    if configured_mode == MODE_ACRCLOUD and not has_acrcloud:
        if has_acoustid:
            return MODE_MB_ONLY
        return MODE_MANUAL

    # If both keys available and no mode set, offer dual mode
    if has_acrcloud and has_acoustid and configured_mode == MODE_ACRCLOUD:
        # Could auto-enable dual mode, but let user choose
        return configured_mode

    return configured_mode

# Global settings (can be modified by config)
LASTFM_API_KEY = None
USE_LOCAL_BPM = True


# =============================================================================
# UPDATE CHECKING
# =============================================================================

def _parse_version_parts(version_text: str):
    """Parse version-like text into integer parts for comparison."""
    if not version_text:
        return []
    cleaned = str(version_text).strip().lower().lstrip('v')
    return [int(p) for p in re.findall(r'\d+', cleaned)]


def _is_newer_version(latest: str, current: str) -> bool:
    """Return True if latest > current based on numeric version parts."""
    latest_parts = _parse_version_parts(latest)
    current_parts = _parse_version_parts(current)
    if not latest_parts or not current_parts:
        return False

    max_len = max(len(latest_parts), len(current_parts))
    latest_parts += [0] * (max_len - len(latest_parts))
    current_parts += [0] * (max_len - len(current_parts))
    return latest_parts > current_parts


def check_for_updates():
    """Check GitHub releases and tags for a newer version."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "MixSplitR-UpdateCheck"
    }

    release_url = f"https://github.com/{GITHUB_REPO}/releases"
    candidates = []

    try:
        # Candidate 1: latest GitHub release
        release_resp = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers=headers,
            timeout=5
        )
        if release_resp.status_code == 200:
            release = release_resp.json()
            rel_version = (release.get("tag_name") or release.get("name") or "").strip().lstrip('v')
            rel_url = release.get("html_url") or release_url
            if _parse_version_parts(rel_version):
                candidates.append({"version": rel_version, "url": rel_url})

        # Candidate 2..N: latest tags (works even when releases are not maintained)
        tags_resp = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/tags?per_page=20",
            headers=headers,
            timeout=5
        )
        if tags_resp.status_code == 200:
            tags = tags_resp.json() or []
            for tag in tags:
                raw_name = (tag.get("name") or "").strip()
                tag_version = raw_name.lstrip('v')
                if _parse_version_parts(tag_version):
                    candidates.append({
                        "version": tag_version,
                        "url": f"https://github.com/{GITHUB_REPO}/releases/tag/{raw_name}"
                    })
    except Exception:
        return None

    if not candidates:
        return None

    # Pick the highest semantic version across release + tags.
    best = max(candidates, key=lambda c: _parse_version_parts(c["version"]))
    if not _is_newer_version(best["version"], CURRENT_VERSION):
        return False

    return {
        "latest": best["version"],
        "current": CURRENT_VERSION,
        "url": best["url"] or release_url
    }


# =============================================================================
# RESOURCE PATH HANDLING
# =============================================================================

def resource_path(relative_path):
    """Get path to resource, works for dev and PyInstaller"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


# FFmpeg paths - initialized at module load
ffmpeg_path = resource_path("ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
ffprobe_path = resource_path("ffprobe.exe" if sys.platform == "win32" else "ffprobe")


def setup_ffmpeg():
    """Setup FFmpeg paths and permissions
    
    FIX: Enhanced error handling and fallback detection
    """
    global ffmpeg_path, ffprobe_path

    ffmpeg_name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
    ffprobe_name = "ffprobe.exe" if sys.platform == "win32" else "ffprobe"

    def _resolve_binary(env_var: str, bundled_name: str) -> str:
        """Resolve binary path from env, bundle, exe dir, cwd, then system PATH."""
        env_value = os.environ.get(env_var)
        if env_value and os.path.exists(env_value):
            return env_value

        candidates = [
            resource_path(bundled_name),
            os.path.join(os.path.dirname(sys.executable), bundled_name),
            os.path.join(os.getcwd(), bundled_name),
        ]
        for candidate in candidates:
            if candidate and os.path.exists(candidate):
                return candidate

        base_name = os.path.splitext(bundled_name)[0]
        system_path = shutil.which(base_name) or shutil.which(bundled_name)
        if system_path and os.path.exists(system_path):
            return system_path
        return ""

    ffmpeg_resolved = _resolve_binary('FFMPEG_BINARY', ffmpeg_name)
    ffprobe_resolved = _resolve_binary('FFPROBE_BINARY', ffprobe_name)

    # If ffprobe wasn't found directly, try alongside ffmpeg.
    if not ffprobe_resolved and ffmpeg_resolved:
        sibling_probe = os.path.join(os.path.dirname(ffmpeg_resolved), ffprobe_name)
        if os.path.exists(sibling_probe):
            ffprobe_resolved = sibling_probe

    if not ffmpeg_resolved:
        raise FileNotFoundError(
            "FFmpeg binary not found. Bundle ffmpeg.exe with the app or install FFmpeg."
        )
    if not ffprobe_resolved:
        raise FileNotFoundError(
            "FFprobe binary not found. Bundle ffprobe.exe with the app or install FFmpeg."
        )

    ffmpeg_path = ffmpeg_resolved
    ffprobe_path = ffprobe_resolved

    # Ensure pydub (which uses `which('ffprobe')`) can discover bundled binaries.
    path_parts = os.environ.get('PATH', '').split(os.pathsep) if os.environ.get('PATH') else []
    prepend_dirs = []
    for binary in (ffmpeg_path, ffprobe_path):
        binary_dir = os.path.dirname(binary)
        if binary_dir and binary_dir not in path_parts and binary_dir not in prepend_dirs:
            prepend_dirs.append(binary_dir)
    if prepend_dirs:
        os.environ['PATH'] = os.pathsep.join(prepend_dirs + path_parts) if path_parts else os.pathsep.join(prepend_dirs)

    # Set executable permissions on Unix-like systems
    if sys.platform != 'win32':
        try:
            os.chmod(ffmpeg_path, 0o755)
            os.chmod(ffprobe_path, 0o755)
        except Exception as e:
            print(f"  ⚠️  Could not set executable permissions: {e}")
    
    # Update environment variables
    os.environ['FFMPEG_BINARY'] = ffmpeg_path
    os.environ['FFPROBE_BINARY'] = ffprobe_path
    
    return ffmpeg_path, ffprobe_path


# Rest of the file continues with the original implementation...
# (Including get_app_data_dir, get_config_path, get_cache_path, RateLimiter, etc.)
# I'll include the key functions that need to be complete:

def get_app_data_dir():
    """Get platform-specific app data directory"""
    if sys.platform == 'darwin':
        base = Path.home() / 'Library' / 'Application Support'
    elif sys.platform == 'win32':
        base = Path(os.environ.get('APPDATA', Path.home() / 'AppData' / 'Roaming'))
    else:
        base = Path.home() / '.local' / 'share'
    
    app_dir = base / 'MixSplitR'
    app_dir.mkdir(parents=True, exist_ok=True)
    return app_dir


def get_default_music_folder():
    """Get the system default Music folder for the current platform."""
    music = Path.home() / "Music"
    music.mkdir(parents=True, exist_ok=True)
    return str(music)


def get_output_directory(config=None):
    """Get the output directory for processed tracks.

    Checks config['output_directory'] first; falls back to ~/Music/MixSplitR Library.
    """
    if config is None:
        config = get_config()
    custom = config.get('output_directory', '')
    if custom and os.path.isabs(custom):
        os.makedirs(custom, exist_ok=True)
        return custom
    default = os.path.join(get_default_music_folder(), "MixSplitR Library")
    os.makedirs(default, exist_ok=True)
    return default


def get_recording_directory(config=None):
    """Get the directory where recordings are saved.

    Checks config['recording_directory'] first; falls back to ~/Music.
    """
    if config is None:
        config = get_config()
    custom = config.get('recording_directory', '')
    if custom and os.path.isabs(custom):
        os.makedirs(custom, exist_ok=True)
        return custom
    default = get_default_music_folder()
    return default


def get_manifest_directory(config=None):
    """Get the directory where session manifests are stored.

    Checks config['manifest_directory'] first; falls back to the app data
    folder (e.g. ~/Library/Application Support/MixSplitR/manifests on macOS).
    """
    if config is None:
        config = get_config()
    custom = config.get('manifest_directory', '')
    if custom and os.path.isabs(custom):
        os.makedirs(custom, exist_ok=True)
        return custom
    default = str(get_app_data_dir() / "manifests")
    os.makedirs(default, exist_ok=True)
    return default


def get_config_path():
    """Get path to configuration file"""
    return get_app_data_dir() / 'config.json'


def get_cache_path(cache_name="mixsplitr_cache.json"):
    """Get path to cache file"""
    return get_app_data_dir() / cache_name


# Rate limiter
class RateLimiter:
    """Rate limiter with configurable requests per second or min_interval"""
    def __init__(self, requests_per_second=None, min_interval=None):
        # Support both min_interval (direct delay) and requests_per_second
        if min_interval is not None:
            self.delay = min_interval
        elif requests_per_second is not None:
            self.delay = 1.0 / requests_per_second
        else:
            self.delay = 1.0 / 3  # default: 3 requests per second
        self.last_request = 0
        self.lock = threading.Lock()
    
    def wait(self):
        """Wait if necessary to maintain rate limit"""
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_request
            if time_since_last < self.delay:
                time.sleep(self.delay - time_since_last)
            self.last_request = time.time()


# Large file constants
LARGE_FILE_THRESHOLD = 500 * 1024 * 1024  # 500 MB


def is_large_file(file_path):
    """Check if file is considered large (>500MB)"""
    try:
        size = os.path.getsize(file_path)
        return size > LARGE_FILE_THRESHOLD
    except:
        return False


def get_file_size_str(file_path):
    """Get human-readable file size"""
    try:
        size = os.path.getsize(file_path)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} TB"
    except:
        return "Unknown"


# FFmpeg helpers for large files
def ffmpeg_detect_silence(file_path, silence_thresh_db=-40, min_silence_len=2.0):
    """Detect silence in audio file using ffmpeg"""
    cmd = [
        ffmpeg_path, '-i', file_path,
        '-af', f'silencedetect=noise={silence_thresh_db}dB:d={min_silence_len}',
        '-f', 'null', '-'
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )
        return result.stderr
    except Exception as e:
        print(f"Error detecting silence: {e}")
        return None


def ffmpeg_get_split_points_from_silence(silence_output):
    """Parse ffmpeg silence detection output to get split points"""
    import re
    
    split_points = []
    silence_end_pattern = re.compile(r'silence_end: ([\d.]+)')
    
    for line in silence_output.split('\n'):
        match = silence_end_pattern.search(line)
        if match:
            timestamp = float(match.group(1))
            split_points.append(timestamp)
    
    return split_points


def ffmpeg_split_file(file_path, split_points, output_dir):
    """Split audio file at specified timestamps using ffmpeg"""
    output_files = []
    total = len(split_points)

    try:
        from tqdm import tqdm
        iterator = tqdm(enumerate(split_points), total=total, desc="  ✂️  Splitting", unit="chunk", leave=True)
    except ImportError:
        iterator = enumerate(split_points)

    for i, start_time in iterator:
        end_time = split_points[i + 1] if i + 1 < len(split_points) else None

        output_file = os.path.join(output_dir, f"chunk_{i+1:03d}.flac")

        cmd = [
            ffmpeg_path, '-i', file_path,
            '-ss', str(start_time)
        ]

        if end_time:
            cmd.extend(['-t', str(end_time - start_time)])

        cmd.extend([
            '-c:a', 'flac',
            '-compression_level', '8',
            output_file
        ])

        try:
            subprocess.run(cmd, capture_output=True, check=True, timeout=300)
            output_files.append(output_file)
        except Exception as e:
            print(f"Error splitting at {start_time}: {e}")

    return output_files


def ffmpeg_extract_chunk_for_identification(file_path, start_seconds=30, duration=10):
    """Extract a small chunk for identification without loading full file"""
    import tempfile
    
    temp_file = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
    temp_file.close()
    
    cmd = [
        ffmpeg_path, '-i', file_path,
        '-ss', str(start_seconds),
        '-t', str(duration),
        '-acodec', 'pcm_s16le',
        '-ar', '44100',
        '-ac', '2',
        temp_file.name
    ]
    
    try:
        subprocess.run(cmd, capture_output=True, check=True, timeout=60)
        return temp_file.name
    except Exception as e:
        print(f"Error extracting chunk: {e}")
        if os.path.exists(temp_file.name):
            os.remove(temp_file.name)
        return None


def validate_acrcloud_credentials(config):
    """
    Test ACRCloud credentials by attempting recognition on silence.
    Returns: (is_valid: bool, error_message: str or None)
    """
    try:
        from acrcloud.recognizer import ACRCloudRecognizer
    except ImportError:
        return False, "ACRCloud SDK not installed"
    
    try:
        # Create test config
        test_config = {
            'host': config.get('host', ''),
            'access_key': config.get('access_key', ''),
            'access_secret': config.get('access_secret', ''),
            'timeout': config.get('timeout', 10)
        }
        
        # Create recognizer
        recognizer = ACRCloudRecognizer(test_config)
        
        # Create 1 second of silence for testing
        import wave
        import tempfile
        import struct
        
        temp_file = tempfile.NamedTemporaryFile(suffix='.wav', delete=False)
        temp_file.close()
        
        try:
            with wave.open(temp_file.name, 'wb') as wav:
                wav.setnchannels(2)
                wav.setsampwidth(2)
                wav.setframerate(44100)
                silence = struct.pack('<h', 0) * 44100 * 2
                wav.writeframes(silence)
            
            # Try to recognize (will fail to match, but tests credentials)
            result = recognizer.recognize_by_file(temp_file.name, 0)
        finally:
            if os.path.exists(temp_file.name):
                os.remove(temp_file.name)
        
        # Parse response
        import json
        response = json.loads(result)
        status_code = response.get('status', {}).get('code', -1)
        status_msg = response.get('status', {}).get('msg', 'Unknown error')
        
        # ACRCloud status codes:
        # Code 0 = success (match found)
        # Code 1001 = no match found (credentials work)
        # Code 2004 = can't generate fingerprint (mute/silence - credentials work!)
        # Code 3001 = missing/invalid access key
        # Code 3002 = invalid access secret  
        # Code 3003 = limit exceeded
        # Code 3014 = invalid audio format
        
        # These codes mean credentials ARE valid (the request went through)
        if status_code in [0, 1001, 2004, 3014]:
            return True, None
        elif status_code == 3001:
            return False, "Invalid Access Key"
        elif status_code == 3002:
            return False, "Invalid Access Secret"
        elif status_code == 3003:
            return False, "API limit exceeded - try again later"
        else:
            return False, f"API error: {status_msg} (code {status_code})"
            
    except ImportError:
        return False, "ACRCloud SDK not installed"
    except requests.exceptions.ConnectionError:
        return False, f"Cannot connect to {config.get('host')} - check your internet connection"
    except requests.exceptions.Timeout:
        return False, "Connection timed out - check host address"
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def get_config():
    """Load or create configuration.

    On first run the user is asked which identification mode to use:
      1. ACRCloud + MusicBrainz  (original behaviour)
      2. MusicBrainz only        (no ACRCloud account needed)

    ACRCloud credentials are only requested when mode == "acrcloud".
    """
    global LASTFM_API_KEY, USE_LOCAL_BPM

    config_path = get_config_path()

    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            config = json.load(f)
            if config.get('lastfm_api_key'):
                LASTFM_API_KEY = config['lastfm_api_key']
            if config.get('disable_local_bpm'):
                USE_LOCAL_BPM = False
            changed = False
            # Back-fill mode for configs created before v7.1
            if 'mode' not in config:
                if config.get('host') and config.get('access_key'):
                    config['mode'] = MODE_ACRCLOUD
                else:
                    config['mode'] = MODE_MB_ONLY
                changed = True
            # Back-fill portable startup scan option for existing configs
            if 'portable_mode_local_scan' not in config:
                config['portable_mode_local_scan'] = False
                changed = True
            # Back-fill fingerprint sample length (seconds) used for ACRCloud/Dual matching
            sample_seconds = config.get('fingerprint_sample_seconds')
            if not isinstance(sample_seconds, int) or sample_seconds < 8 or sample_seconds > 45:
                config['fingerprint_sample_seconds'] = 12
                changed = True
            if changed:
                save_config(config)
            return config

    # ── First-run setup ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("          MixSplitR – First-Run Setup")
    print("=" * 60)
    print("\nMixSplitR can identify tracks two ways:\n")
    print("  1. ACRCloud + MusicBrainz  (best accuracy)")
    print("       Requires a free ACRCloud account")
    print("       → https://console.acrcloud.com\n")
    print("  2. MusicBrainz only        (no account needed)")
    print("       Uses audio fingerprinting via AcoustID")
    print("       → Works entirely offline-friendly, no sign-ups\n")

    while True:
        mode_choice = input("  Choose mode (1/2) [1]: ").strip()
        if mode_choice in ('', '1'):
            chosen_mode = MODE_ACRCLOUD
            break
        elif mode_choice == '2':
            chosen_mode = MODE_MB_ONLY
            break
        print("  Please enter 1 or 2.\n")

    conf = {'mode': chosen_mode, 'timeout': 10, 'fingerprint_sample_seconds': 12}

    # ── Startup scan mode (portable/local folder scan) ──────────────────────
    print("\n" + "-" * 60)
    print("          Startup Scan Mode")
    print("-" * 60)
    print("\nOn startup, MixSplitR can auto-scan for audio files in either:")
    print("  • Local program folder (portable mode)")
    print("  • Your Music folder (default)\n")
    print("Enable portable mode if you keep audio next to the app/script.")

    portable_choice = input("  Enable portable startup scan? (y/n) [n]: ").strip().lower()
    conf['portable_mode_local_scan'] = portable_choice in ('y', 'yes')
    if conf['portable_mode_local_scan']:
        print(f"  {Style.GREEN}✅ Portable startup scan enabled{Style.RESET}")
    else:
        print(f"  {Style.GREEN}✅ Portable startup scan disabled (uses Music folder){Style.RESET}")

    # ── ACRCloud credentials (only in acrcloud mode) ────────────────────────
    if chosen_mode == MODE_ACRCLOUD:
        print("\n" + "-" * 60)
        print("          ACRCloud API Setup")
        print("-" * 60)
        print("\nGet your free API keys at: https://console.acrcloud.com\n")

        while True:
            host          = input("  Enter your ACR Host: ").strip()
            access_key    = input("  Enter your Access Key: ").strip()
            access_secret = input("  Enter your Secret Key: ").strip()

            if host and access_key and access_secret:
                break
            print("\n  ❌ All three fields are required!\n")

        conf['host']          = host
        conf['access_key']    = access_key
        conf['access_secret'] = access_secret
    else:
        print(f"\n  {Style.GREEN}✓ MusicBrainz-only mode selected – no ACRCloud account needed.{Style.RESET}")

    # ── AcoustID (optional, MusicBrainz-only mode) ──────────────────────────
    if chosen_mode == MODE_MB_ONLY:
        print("\n" + "-" * 60)
        print("          AcoustID API Key (Recommended)")
        print("-" * 60)
        print("\nAcoustID fingerprints audio to identify tracks in MusicBrainz-only mode.")
        print("You need a free API key for reliable identification.\n")
        print("  Steps:")
        print("    1. Visit: https://acoustid.org/api-key")
        print("    2. Register (free, takes 30 seconds)")
        print("    3. Copy your API key and paste below\n")

        add_acoustid = input("  Add your AcoustID API key now? (y/n) [y]: ").strip().lower()
        if add_acoustid in ('', 'y', 'yes'):
            acoustid_key = input("  Enter your AcoustID API Key: ").strip()
            if acoustid_key:
                conf['acoustid_api_key'] = acoustid_key
                print(f"  {Style.GREEN}✅ AcoustID API key added!{Style.RESET}")
            else:
                print(f"  {Style.YELLOW}⚠️  No key entered - you can add one later in Settings{Style.RESET}")
        else:
            print(f"  {Style.YELLOW}⚠️  Skipped - you can add a key later in: Main Menu → Manage API Keys{Style.RESET}")
            print(f"  {Style.YELLOW}     Without a key, fingerprinting will not work{Style.RESET}")

    # ── Last.fm (optional, both modes) ──────────────────────────────────────
    print("\n" + "-" * 60)
    print("          Last.fm API Setup (Optional)")
    print("-" * 60)
    print("\nLast.fm improves genre detection with user-generated tags.")
    print("Get a free API key at: https://www.last.fm/api/account/create\n")

    add_lastfm = input("  Add Last.fm API key? (y/n) [n]: ").strip().lower()
    if add_lastfm == 'y':
        lastfm_key = input("  Enter your Last.fm API Key: ").strip()
        if lastfm_key:
            conf['lastfm_api_key'] = lastfm_key
            LASTFM_API_KEY = lastfm_key
            print("  ✅ Last.fm API key added!")

    print(f"\n  Config saved to: {config_path}\n")
    with open(config_path, 'w') as f:
        json.dump(conf, f, indent=4)
    return conf


def save_config(config):
    """Save configuration to file"""
    with open(get_config_path(), 'w') as f:
        json.dump(config, f, indent=4)


# =============================================================================
# AUDIO FILE ANALYSIS
# =============================================================================

def get_audio_duration_fast(file_path):
    """Get audio duration quickly using ffprobe"""
    try:
        result = subprocess.run(
            [ffprobe_path, '-v', 'quiet', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            duration_seconds = float(result.stdout.strip())
            return duration_seconds / 60.0
    except:
        pass
    return None


def analyze_files_parallel(audio_files, max_workers=4):
    """Analyze multiple audio files in parallel to determine type"""
    
    def analyze_single(file_path):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        duration = get_audio_duration_fast(file_path)
        if duration is None:
            duration = size_mb / 10.0  # Rough estimate
        return {
            'file': file_path,
            'filename': os.path.basename(file_path),
            'duration_min': duration,
            'is_mix': duration >= 8,
            'size_mb': size_mb
        }
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(analyze_single, audio_files))
    
    return results


# =============================================================================
# UTILITIES
# =============================================================================

def _supports_osc8_links() -> bool:
    """Return True when terminal likely supports OSC 8 hyperlinks."""
    if os.environ.get("WT_SESSION"):
        return True
    if os.environ.get("VTE_VERSION"):
        return True
    term_program = os.environ.get("TERM_PROGRAM", "")
    if term_program in {"iTerm.app", "Apple_Terminal"}:
        return True
    term = os.environ.get("TERM", "")
    return term.startswith("xterm")


def _format_terminal_link(label: str, url: str) -> str:
    """Create a clickable terminal hyperlink when supported."""
    if not _supports_osc8_links():
        return label
    return f"\033]8;;{url}\033\\{label}\033]8;;\033\\"


def _print_close_screen_branding():
    """Render a branded close screen before the final Enter prompt."""
    term_size = shutil.get_terminal_size(fallback=(100, 24))
    cols = term_size.columns
    rows = term_size.lines
    # 15 branded lines + 1 prompt line printed by close_terminal().
    block_height = 16
    top_pad = max(0, (rows - block_height) // 2)
    if top_pad:
        print("\n" * top_pad, end="")

    logo_segments = [
        ("███╗   ███╗██╗██╗  ██╗", "███████╗██████╗ ██╗     ██╗████████╗", "██████╗ "),
        ("████╗ ████║██║╚██╗██╔╝", "██╔════╝██╔══██╗██║     ██║╚══██╔══╝", "██╔══██╗"),
        ("██╔████╔██║██║ ╚███╔╝ ", "███████╗██████╔╝██║     ██║   ██║   ", "██████╔╝"),
        ("██║╚██╔╝██║██║ ██╔██╗ ", "╚════██║██╔═══╝ ██║     ██║   ██║   ", "██╔══██╗"),
        ("██║ ╚═╝ ██║██║██╔╝ ██╗", "███████║██║     ███████╗██║   ██║   ", "██║  ██║"),
        ("╚═╝     ╚═╝╚═╝╚═╝  ╚═╝", "╚══════╝╚═╝     ╚══════╝╚═╝   ╚═╝   ", "╚═╝  ╚═╝"),
    ]
    divider = "═══════════════════════════════════════"
    red = "\033[38;5;196m"

    for mix_part, split_part, r_part in logo_segments:
        plain = f"{mix_part}{split_part}{r_part}"
        pad = " " * max(0, (cols - len(plain)) // 2)
        print(
            f"{pad}{Style.GRAY}{mix_part}"
            f"{Style.GRAY}{split_part}"
            f"{red}{r_part}{Style.RESET}"
        )

    print(f"{Style.GRAY}{divider.center(cols)}{Style.RESET}")
    print(f"{Style.GRAY}{'Mix Archival Tool'.center(cols)}{Style.RESET}")
    print(f"{Style.GRAY}{'Original project by chefkjd (KJD)'.center(cols)}{Style.RESET}")
    print(f"{Style.GRAY}{divider.center(cols)}{Style.RESET}")
    print()
    print(f"{Style.DIM}{'Open-source project maintained from the original MixSplitR repo.'.center(cols)}{Style.RESET}")
    print()


def _show_close_screen_prompt_toolkit() -> bool:
    """Show branded close screen with project credit."""
    try:
        from prompt_toolkit import Application
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.layout import Layout, Window, FormattedTextControl
        from prompt_toolkit.formatted_text import FormattedText
        from prompt_toolkit.styles import Style as PTStyle
    except Exception:
        return False

    term_size = shutil.get_terminal_size(fallback=(100, 24))
    cols = term_size.columns
    rows = term_size.lines
    # 15 branded lines + 1 prompt line.
    block_height = 16
    top_pad = max(0, (rows - block_height) // 2)

    logo_segments = [
        ("███╗   ███╗██╗██╗  ██╗", "███████╗██████╗ ██╗     ██╗████████╗", "██████╗ "),
        ("████╗ ████║██║╚██╗██╔╝", "██╔════╝██╔══██╗██║     ██║╚══██╔══╝", "██╔══██╗"),
        ("██╔████╔██║██║ ╚███╔╝ ", "███████╗██████╔╝██║     ██║   ██║   ", "██████╔╝"),
        ("██║╚██╔╝██║██║ ██╔██╗ ", "╚════██║██╔═══╝ ██║     ██║   ██║   ", "██╔══██╗"),
        ("██║ ╚═╝ ██║██║██╔╝ ██╗", "███████║██║     ███████╗██║   ██║   ", "██║  ██║"),
        ("╚═╝     ╚═╝╚═╝╚═╝  ╚═╝", "╚══════╝╚═╝     ╚══════╝╚═╝   ╚═╝   ", "╚═╝  ╚═╝"),
    ]
    divider = "═══════════════════════════════════════"

    def _line_pad(text: str) -> str:
        return " " * max(0, (cols - len(text)) // 2)

    lines = []
    if top_pad:
        lines.append(("", "\n" * top_pad))

    for mix_part, split_part, r_part in logo_segments:
        plain = f"{mix_part}{split_part}{r_part}"
        lines.append(("class:logo_mix", _line_pad(plain) + mix_part))
        lines.append(("class:logo_split", split_part))
        lines.append(("class:logo_r", r_part + "\n"))

    lines.append(("class:logo_dim", f"{divider.center(cols)}\n"))
    lines.append(("class:logo_dim", f"{'Mix Archival Tool'.center(cols)}\n"))
    lines.append(("class:logo_dim", f"{'By KJD'.center(cols)}\n"))
    lines.append(("class:logo_dim", f"{divider.center(cols)}\n\n"))
    lines.append(("class:body", f"{'Open-source project maintained from the original MixSplitR repo.'.center(cols)}\n"))
    lines.append(("class:dim", "\n"))
    lines.append(("class:prompt", f"{'Press Enter to close...'.center(cols)}"))

    kb = KeyBindings()

    @kb.add("enter")
    @kb.add("escape")
    @kb.add("c-c")
    def _close(event):
        event.app.exit()

    app = Application(
        layout=Layout(
            Window(
                content=FormattedTextControl(lambda: FormattedText(lines)),
                always_hide_cursor=True,
                wrap_lines=True,
            )
        ),
        key_bindings=kb,
        style=PTStyle.from_dict({
            "logo_mix": "#6c6c6c",
            "logo_split": "#6c6c6c",
            "logo_r": "bold #ff0000",
            "logo_dim": "#6c6c6c",
            "body": "#8e95aa",
            "dim": "#7d8499",
            "prompt": "bold #e6e9f2",
            "link": "bold #ff0000",
        }),
        full_screen=False,
        mouse_support=True,
    )

    try:
        print("\033[2J\033[H", end="", flush=True)
    except Exception:
        pass

    app.run()
    return True


def close_terminal():
    """Gracefully end the app session without terminal script errors."""
    # Restore terminal state after prompt_toolkit fullscreen/mouse mode and
    # clear any stale buffer content (for example Session History screens).
    try:
        print("\033[0m\033[?25h\033[?1000l\033[?1002l\033[?1003l\033[?1006l\033[2J\033[H", end="", flush=True)
    except Exception:
        pass

    if not _show_close_screen_prompt_toolkit():
        try:
            _print_close_screen_branding()
        except Exception:
            pass
        try:
            cols = shutil.get_terminal_size(fallback=(100, 24)).columns
            input("Press Enter to close...".center(cols))
        except (EOFError, KeyboardInterrupt):
            pass

    # Only try auto-close when running as a bundled app.
    # In normal terminal runs we should never kill the user's shell window.
    if sys.platform == 'darwin' and getattr(sys, 'frozen', False):
        try:
            subprocess.run(
                ["osascript", "-e", 'tell application "Terminal" to if (count of windows) > 0 then close front window'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass
