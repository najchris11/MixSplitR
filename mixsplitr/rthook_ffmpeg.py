"""
Runtime Hook for FFmpeg in PyInstaller Bundle

Save this file as: rthook_ffmpeg.py
Place it in: D:\My Drive\MixSplitR\7.1 teset windows\

This code runs BEFORE your main script when the exe starts.
It configures pydub to use the bundled FFmpeg binaries.
"""

import os
import sys

print("="*60)
print("Runtime Hook: Configuring bundled FFmpeg...")
print("="*60)

# Detect if running as PyInstaller bundle
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    bundle_dir = sys._MEIPASS
    print(f"✓ Detected PyInstaller bundle at: {bundle_dir}")
    
    # Find bundled binaries
    if os.name == 'nt':  # Windows
        ffmpeg_exe = os.path.join(bundle_dir, 'ffmpeg.exe')
        ffprobe_exe = os.path.join(bundle_dir, 'ffprobe.exe')
        fpcalc_exe = os.path.join(bundle_dir, 'fpcalc.exe')
    else:  # Mac/Linux
        ffmpeg_exe = os.path.join(bundle_dir, 'ffmpeg')
        ffprobe_exe = os.path.join(bundle_dir, 'ffprobe')
        fpcalc_exe = os.path.join(bundle_dir, 'fpcalc')
    
    # Configure ffmpeg
    if os.path.exists(ffmpeg_exe):
        print(f"✓ Found bundled ffmpeg: {ffmpeg_exe}")
        os.environ['FFMPEG_BINARY'] = ffmpeg_exe
    else:
        print(f"✗ WARNING: ffmpeg not found at {ffmpeg_exe}")
    
    # Configure ffprobe
    if os.path.exists(ffprobe_exe):
        print(f"✓ Found bundled ffprobe: {ffprobe_exe}")
        os.environ['FFPROBE_BINARY'] = ffprobe_exe
    else:
        print(f"✗ WARNING: ffprobe not found at {ffprobe_exe}")
    
    # Configure chromaprint
    if os.path.exists(fpcalc_exe):
        print(f"✓ Found bundled fpcalc: {fpcalc_exe}")
        os.environ['FPCALC'] = fpcalc_exe
    else:
        print(f"✗ WARNING: fpcalc not found at {fpcalc_exe}")
    
    # CRITICAL: Configure pydub directly
    # This must happen before pydub.AudioSegment is imported
    try:
        from pydub import AudioSegment
        if os.path.exists(ffmpeg_exe):
            AudioSegment.converter = ffmpeg_exe
            AudioSegment.ffmpeg = ffmpeg_exe
        if os.path.exists(ffprobe_exe):
            AudioSegment.ffprobe = ffprobe_exe
        print("✓ pydub configured to use bundled FFmpeg")
    except ImportError:
        print("⚠ pydub not yet imported, will use environment variables")
    
    print("="*60)
else:
    print("Running as normal Python script (not bundled)")
    print("="*60)
