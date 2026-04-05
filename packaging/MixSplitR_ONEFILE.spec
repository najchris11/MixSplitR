# -*- mode: python ; coding: utf-8 -*-
"""
MixSplitR PyInstaller Spec File - SINGLE FILE MODE
Bundles everything including chromaprint (fpcalc) and FFmpeg into ONE executable
"""

import sys
import os
import shutil

# =============================================================================
# Helper Function: Find fpcalc Executable
# =============================================================================
def find_fpcalc():
    """
    Find fpcalc executable to bundle with the application.
    Returns list of tuples: [(source_path, destination_in_exe), ...]
    """
    # Try to find in PATH first
    fpcalc = shutil.which('fpcalc')
    if fpcalc:
        print(f"✓ Found fpcalc in PATH: {fpcalc}")
        return [(fpcalc, '.')]

    # Windows: Check common locations
    if sys.platform == 'win32':
        common_paths = [
            'fpcalc.exe',  # Current directory
            r'C:\Program Files\Chromaprint\fpcalc.exe',
            r'C:\Program Files (x86)\Chromaprint\fpcalc.exe',
        ]
        for path in common_paths:
            if os.path.exists(path):
                print(f"✓ Found fpcalc at: {path}")
                return [(path, '.')]

    # macOS: Check Homebrew location
    elif sys.platform == 'darwin':
        homebrew_paths = [
            '/usr/local/bin/fpcalc',  # Intel Mac
            '/opt/homebrew/bin/fpcalc',  # Apple Silicon Mac
        ]
        for path in homebrew_paths:
            if os.path.exists(path):
                print(f"✓ Found fpcalc at: {path}")
                return [(path, '.')]

    # Linux: Check common locations
    else:
        linux_paths = [
            '/usr/bin/fpcalc',
            '/usr/local/bin/fpcalc',
        ]
        for path in linux_paths:
            if os.path.exists(path):
                print(f"✓ Found fpcalc at: {path}")
                return [(path, '.')]

    print("⚠️  Warning: fpcalc not found! MusicBrainz mode will require manual installation.")
    return []


# =============================================================================
# Helper Function: Find FFmpeg Executables (ffmpeg + ffprobe)
# =============================================================================
def find_ffmpeg():
    """
    Find ffmpeg AND ffprobe executables to bundle with the application.
    Returns list of tuples: [(source_path, destination_in_exe), ...]
    """
    binaries = []
    
    # --- Find ffmpeg ---
    ffmpeg = shutil.which('ffmpeg')
    if ffmpeg:
        print(f"✓ Found ffmpeg in PATH: {ffmpeg}")
        binaries.append((ffmpeg, '.'))
    elif sys.platform == 'win32':
        common_paths = [
            'ffmpeg.exe',  # Current directory
            r'C:\Program Files\ffmpeg\bin\ffmpeg.exe',
            r'C:\ffmpeg\bin\ffmpeg.exe',
        ]
        for path in common_paths:
            if os.path.exists(path):
                print(f"✓ Found ffmpeg at: {path}")
                binaries.append((path, '.'))
                break
    
    # --- Find ffprobe (CRITICAL - pydub needs this!) ---
    ffprobe = shutil.which('ffprobe')
    if ffprobe:
        print(f"✓ Found ffprobe in PATH: {ffprobe}")
        binaries.append((ffprobe, '.'))
    elif sys.platform == 'win32':
        common_paths = [
            'ffprobe.exe',  # Current directory
            r'C:\Program Files\ffmpeg\bin\ffprobe.exe',
            r'C:\ffmpeg\bin\ffprobe.exe',
        ]
        for path in common_paths:
            if os.path.exists(path):
                print(f"✓ Found ffprobe at: {path}")
                binaries.append((path, '.'))
                break
    
    if len(binaries) < 2:
        print("⚠️  Warning: ffmpeg or ffprobe not found! Audio processing will fail.")
        print("    Make sure both ffmpeg.exe AND ffprobe.exe are in the project folder.")
    
    return binaries

# =============================================================================
# Analysis - Collect Scripts and Dependencies
# =============================================================================

from PyInstaller.utils.hooks import collect_all, collect_submodules

# Collect complex packages that have data files, binaries, or dynamic imports.
# Each collect_all returns (datas, binaries, hiddenimports).
# We accumulate them all into combined lists.
_extra_datas = []
_extra_binaries = []
_extra_hiddenimports = []

_packages_to_collect = [
    'acrcloud',         # ACRCloud SDK - has native binaries
    'librosa',          # Audio analysis - has data files and many submodules
    'shazamio',         # Shazam async client - has internal data/protos
    'prompt_toolkit',   # Interactive terminal UI - many submodules
    'soundcard',        # System audio capture - platform-specific backends
    'soundfile',        # WAV/FLAC I/O - wraps libsndfile
    'sounddevice',      # Audio I/O fallback - wraps PortAudio
]

for pkg in _packages_to_collect:
    try:
        d, b, h = collect_all(pkg)
        print(f"✓ Collected {pkg}: {len(d)} data, {len(b)} binaries, {len(h)} hidden imports")
        _extra_datas.extend(d)
        _extra_binaries.extend(b)
        _extra_hiddenimports.extend(h)
    except Exception as e:
        print(f"ℹ {pkg} not installed (optional) - {e}")

a = Analysis(
    ['MixSplitR.py'],
    pathex=[],
    binaries=[
        # External binaries (ffmpeg, ffprobe, fpcalc)
        *find_fpcalc(),
        *find_ffmpeg(),
        # Binaries discovered by collect_all above
        *_extra_binaries,
    ],
    datas=[
        # Data files discovered by collect_all above
        *_extra_datas,
    ],
    hiddenimports=[
        # ==== Core (always required) ====
        'pydub',
        'pydub.silence',
        'pydub.utils',
        'requests',
        'requests.adapters',
        'urllib3',
        'tqdm',

        # ==== Audio tagging - mutagen (dynamic imports in tagging functions) ====
        'mutagen',
        'mutagen.flac',
        'mutagen.mp4',
        'mutagen.id3',
        'mutagen.id3._frames',
        'mutagen.oggvorbis',
        'mutagen.oggopus',
        'mutagen.wave',
        'mutagen.aiff',
        'mutagen.mp3',

        # ==== ACRCloud (optional) ====
        'acrcloud',
        'acrcloud.recognizer',
        'hmac',
        'hashlib',
        'base64',

        # ==== MusicBrainz / AcoustID (optional) ====
        'acoustid',
        'musicbrainzngs',

        # ==== Shazam (optional) - async library with many deps ====
        'shazamio',
        'aiohttp',
        'aiohttp.connector',
        'aiohttp.client',
        'aiohttp.client_reqrep',
        'aiohttp.formdata',
        'aiohttp.multipart',
        'aiohttp.payload',
        'aiohttp.resolver',
        'aiohttp.tracing',
        'aiosignal',
        'frozenlist',
        'multidict',
        'yarl',
        'async_timeout',
        'attrs',
        'charset_normalizer',

        # ==== Audio analysis (optional) ====
        'librosa',
        'librosa.beat',
        'librosa.onset',
        'librosa.core',
        'librosa.util',
        'numpy',
        'numpy.fft',
        'scipy',
        'scipy.signal',
        'scipy.fft',
        'numba',
        'soundfile',
        'soundcard',
        'sounddevice',

        # ==== Interactive UI (optional) ====
        'prompt_toolkit',
        'prompt_toolkit.application',
        'prompt_toolkit.key_binding',
        'prompt_toolkit.key_binding.key_bindings',
        'prompt_toolkit.layout',
        'prompt_toolkit.layout.containers',
        'prompt_toolkit.layout.controls',
        'prompt_toolkit.formatted_text',
        'prompt_toolkit.formatted_text.html',
        'prompt_toolkit.styles',
        'prompt_toolkit.widgets',
        'wcwidth',

        # ==== System utilities (optional) ====
        'psutil',

        # ==== Stdlib that PyInstaller sometimes misses ====
        'concurrent.futures',
        'asyncio',
        'asyncio.events',
        'asyncio.base_events',

        # Hidden imports from collect_all
        *_extra_hiddenimports,
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=['mixsplitr/rthook_ffmpeg.py'] if os.path.exists('mixsplitr/rthook_ffmpeg.py') else [],
    excludes=[
        # Exclude unnecessary packages to reduce size
        'matplotlib',
        'matplotlib.pyplot',
        'PIL',
        'tkinter',
        'IPython',
        'jupyter',
        'pytest',
        'sphinx',
        'setuptools',
    ],
    noarchive=False,
)

# =============================================================================
# PYZ - Python Archive
# =============================================================================
pyz = PYZ(a.pure)

# =============================================================================
# EXE - SINGLE FILE EXECUTABLE
# =============================================================================
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='MixSplitR',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,  # Compress with UPX if available
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep console for terminal UI
    disable_windowed_traceback=False,
    onefile=True,  # ← SINGLE FILE MODE - Everything in one .exe!
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

# =============================================================================
# Build Summary
# =============================================================================
print("\n" + "="*60)
print("PyInstaller Build Configuration - SINGLE FILE MODE")
print("="*60)
print(f"Target: {sys.platform}")
print(f"Mode: Single executable (onefile=True)")
fpcalc_binaries = find_fpcalc()
ffmpeg_binaries = find_ffmpeg()
print(f"Bundled chromaprint: {'Yes' if fpcalc_binaries else 'No (WARNING!)'}")
print(f"Bundled ffmpeg+ffprobe: {'Yes (' + str(len(ffmpeg_binaries)) + ' files)' if ffmpeg_binaries else 'No (WARNING!)'}")
print(f"Collected packages: {', '.join(p for p in _packages_to_collect)}")
print(f"Total hidden imports: {len(a.hiddenimports)}")
print(f"Total extra datas: {len(_extra_datas)}")
print(f"Total extra binaries: {len(_extra_binaries) + len(fpcalc_binaries) + len(ffmpeg_binaries)}")
print("Output: dist/MixSplitR.exe (Windows) or dist/MixSplitR (Mac/Linux)")
print("="*60 + "\n")
