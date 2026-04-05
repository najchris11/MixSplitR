#!/usr/bin/env python3
"""
MixSplitR v7.1 - Pipeline Module

Large file streaming and cache application logic.
Extracted from mixsplitr.py for maintainability.
"""

import os
import base64

from pydub import AudioSegment
from pydub.silence import split_on_silence

from .mixsplitr_core import (
    Style, get_config, get_cache_path, get_audio_duration_fast,
    get_file_size_str, get_output_directory,
    ffmpeg_detect_silence, ffmpeg_get_split_points_from_silence,
    ffmpeg_split_file, ffmpeg_extract_chunk_for_identification
)
from .mixsplitr_editor import load_preview_cache
from .mixsplitr_tagging import embed_and_sort_generic, AUDIO_FORMATS
from .mixsplitr_manifest import export_manifest_for_session

# Visual splitter UI - optional module
SPLITTER_UI_AVAILABLE = False
try:
    from .splitter_ui import get_split_points_visual, split_audio_at_points
    SPLITTER_UI_AVAILABLE = True
except ImportError:
    pass


# =============================================================================
# LARGE FILE PROCESSING (FFmpeg streaming mode)
# =============================================================================

def process_large_file_streaming(audio_file, file_num, output_folder, temp_folder,
                                  use_visual=False, use_assisted=False, preview_mode=False):
    """
    Process a large file using FFmpeg streaming mode (no full file RAM load).

    This function:
    1. Uses FFmpeg to detect silence (streaming - no RAM)
    2. Uses FFmpeg to split at silence points (streaming - no RAM)
    3. Only loads small chunks into RAM for identification

    Returns:
        List of chunk data dictionaries compatible with the normal processing flow
    """
    filename = os.path.basename(audio_file)
    file_size = get_file_size_str(audio_file)

    print(f"\n  {Style.YELLOW}⚠ Large file detected: {file_size}{Style.RESET}")
    print(f"  {Style.DIM}Using streaming mode (FFmpeg) to avoid memory issues{Style.RESET}")

    # Get duration without loading file
    duration = get_audio_duration_fast(audio_file)
    if not duration:
        print(f"  {Style.RED}✗ Could not determine file duration{Style.RESET}")
        return []

    split_points = []

    if use_visual and SPLITTER_UI_AVAILABLE:
        # Visual mode - still works because splitter_ui downsamples
        print(f"  🎛️ Opening visual editor...")
        pts = get_split_points_visual(audio_file)
        if pts:
            split_points = pts
    elif use_assisted and SPLITTER_UI_AVAILABLE:
        # Assisted mode - detect silence with FFmpeg, then visual review
        print(f"  🔍 Detecting silence with FFmpeg (streaming)...", end='', flush=True)
        silences = ffmpeg_detect_silence(audio_file, silence_thresh_db=-40, min_silence_len_sec=2.0)
        print(f" found {len(silences)} silent regions")

        # Convert to split points
        pre_detected = ffmpeg_get_split_points_from_silence(silences, duration)
        print(f"  📍 {len(pre_detected)} potential split points")

        # Open visual editor with pre-loaded points
        pts = get_split_points_visual(audio_file, existing_points=pre_detected)
        if pts:
            split_points = pts

    # Fallback to automatic silence detection
    if not split_points:
        print(f"  🔍 Detecting silence with FFmpeg (streaming)...", end='', flush=True)
        silences = ffmpeg_detect_silence(audio_file, silence_thresh_db=-40, min_silence_len_sec=2.0)
        print(f" found {len(silences)} silent regions")
        split_points = ffmpeg_get_split_points_from_silence(silences, duration)
        print(f"  📍 {len(split_points)} split points identified")

    if not split_points:
        print(f"  {Style.YELLOW}⚠ No split points found - treating as single track{Style.RESET}")
        # For single track, we still need to extract a chunk for identification
        chunk_path = ffmpeg_extract_chunk_for_identification(
            audio_file,
            start_time=duration / 2 - 7.5,
            duration_sec=15,
            output_path=os.path.join(temp_folder, f"large_{file_num}_sample.wav")
        )

        if chunk_path:
            sample_chunk = AudioSegment.from_file(chunk_path)
            return [{
                'chunk': sample_chunk,
                'file_num': file_num,
                'original_file': audio_file,
                'split_index': 0,
                'is_large_file': True,
                'large_file_start': 0,
                'large_file_end': duration,
                'temp_chunk_path': chunk_path
            }]
        return []

    # Split the file using FFmpeg (streaming - no RAM needed)
    print(f"  ✂️ Splitting file with FFmpeg (streaming)...")
    os.makedirs(temp_folder, exist_ok=True)
    chunk_paths = ffmpeg_split_file(
        audio_file,
        split_points,
        temp_folder,
        output_prefix=f"large_{file_num}"
    )

    print(f"  {Style.GREEN}✓ Created {len(chunk_paths)} chunks{Style.RESET}")

    # Create chunk data compatible with normal flow
    all_chunks = []
    boundaries = [0] + sorted(split_points) + [duration]

    for idx, chunk_path in enumerate(chunk_paths):
        try:
            chunk = AudioSegment.from_file(chunk_path)

            all_chunks.append({
                'chunk': chunk,
                'file_num': file_num,
                'original_file': audio_file,
                'split_index': idx,
                'temp_chunk_path': chunk_path,
                'is_large_file': True,
                'large_file_start': boundaries[idx] if idx < len(boundaries) else 0,
                'large_file_end': boundaries[idx + 1] if idx + 1 < len(boundaries) else duration
            })
        except Exception as e:
            print(f"  {Style.YELLOW}⚠ Could not load chunk {idx+1}: {e}{Style.RESET}")
            continue

    return all_chunks


# =============================================================================
# APPLY FROM CACHE
# =============================================================================

def apply_from_cache(cache_path=None, temp_audio_folder=None):
    """Apply cached processing results"""
    # Default to safe cache location
    if cache_path is None:
        cache_path = get_cache_path("mixsplitr_cache.json")

    print(f"\n{Style.GREEN}{'═'*50}")
    print(f"  {Style.BOLD}💾 APPLY MODE{Style.RESET}{Style.GREEN} - Creating Files")
    print(f"{'═'*50}{Style.RESET}\n")

    # Format selection menu
    print(f"{Style.BOLD}Select output format:{Style.RESET}\n")
    print(f"{Style.GREEN}Lossless formats (bit-perfect quality):{Style.RESET}")
    print(f"  1. FLAC (recommended)")
    print(f"  2. ALAC (M4A) - Apple Lossless")
    print(f"  3. WAV - Uncompressed")
    print(f"  4. AIFF - Uncompressed (macOS)\n")
    print(f"{Style.YELLOW}Lossy formats (smaller file size):{Style.RESET}")
    print(f"  5. MP3 320kbps - High quality")
    print(f"  6. MP3 256kbps")
    print(f"  7. MP3 192kbps - Standard quality")
    print(f"  8. AAC 256kbps - Apple format")
    print(f"  9. OGG Vorbis Q10 (~500kbps) - High quality")
    print(f" 10. OGG Vorbis Q8 (~320kbps)")
    print(f" 11. OPUS 256kbps - Modern format\n")

    format_choice = input(f"{Style.BOLD}Enter choice (1-11) [1]: {Style.RESET}").strip()

    format_map = {
        '1': 'flac', '2': 'alac', '3': 'wav', '4': 'aiff',
        '5': 'mp3_320', '6': 'mp3_256', '7': 'mp3_192', '8': 'aac_256',
        '9': 'ogg_500', '10': 'ogg_320', '11': 'opus'
    }

    output_format = format_map.get(format_choice, 'flac')
    fmt_info = AUDIO_FORMATS.get(output_format, AUDIO_FORMATS['flac'])
    quality_text = "Lossless" if fmt_info['lossless'] else "Lossy"
    print(f"   {Style.GREEN}→ Using {fmt_info['name']} ({quality_text}){Style.RESET}\n")

    cache_data = load_preview_cache(cache_path)
    if not cache_data:
        return False

    tracks = cache_data.get('tracks', [])
    artwork_cache_b64 = cache_data.get('artwork_cache', {})
    output_folder = cache_data.get('output_folder') or get_output_directory()
    os.makedirs(output_folder, exist_ok=True)

    if temp_audio_folder is None:
        temp_audio_folder = os.path.join(os.path.dirname(cache_path), "mixsplitr_temp")

    has_temp_files = os.path.exists(temp_audio_folder) and len(os.listdir(temp_audio_folder)) > 0

    # Count tracks to process
    to_process = [t for t in tracks if t['status'] in ['identified', 'unidentified']]
    total_to_process = len(to_process)

    print(f"\n{Style.CYAN}{'═'*50}")
    print(f"  {Style.BOLD}APPLYING CACHED PREVIEW{Style.RESET}{Style.CYAN}")
    print(f"{'═'*50}{Style.RESET}")
    print(f"  📊 {Style.BOLD}{total_to_process}{Style.RESET} tracks to process")
    print(f"  📁 Output: {Style.DIM}{output_folder}{Style.RESET}")
    print(f"{Style.CYAN}{'─'*50}{Style.RESET}")

    print(f"\n  🎨 Decoding {len(artwork_cache_b64)} cached artworks...", end='', flush=True)
    artwork_cache = {url: base64.b64decode(b64) for url, b64 in artwork_cache_b64.items()}
    print(f" {Style.GREEN}✓{Style.RESET}")

    identified_count = unidentified_count = skipped_count = 0
    output_files_created = []
    input_files_used = set()

    if has_temp_files:
        print(f"\n  💾 Saving tracks from cache...")
        for track_idx, track in enumerate(tracks, 1):
            if track['status'] == 'skipped':
                skipped_count += 1
                continue

            temp_path = track.get('temp_chunk_path')
            if not temp_path or not os.path.exists(temp_path):
                continue

            if track['status'] == 'identified':
                artist = track.get('artist', 'Unknown')[:20]
                title = track.get('title', 'Unknown')[:25]
                print(f"     [{track_idx}/{len(tracks)}] {artist} - {title}", end='\r', flush=True)

            chunk = AudioSegment.from_file(temp_path)

            if track['status'] == 'identified':
                temp_flac = os.path.join(output_folder, f"temp_apply_{track['file_num']}_{track['index']}.flac")
                chunk.export(temp_flac, format="flac")

                embed_and_sort_generic(temp_flac, track['artist'], track['title'], track['album'],
                                      track.get('art_url'), output_folder, output_format, artwork_cache,
                                      track.get('enhanced_metadata', {}))
                ext = AUDIO_FORMATS.get(output_format, AUDIO_FORMATS['flac'])['ext']
                safe_artist = track['artist'].translate(str.maketrans('', '', '<>:"/\\|?*'))
                out_name = f"{track['artist']} - {track['title']}{ext}".translate(str.maketrans('', '', '<>:"/\\|?*'))
                output_files_created.append(os.path.join(output_folder, safe_artist, out_name))
                if track.get('original_file'):
                    input_files_used.add(track['original_file'])
                identified_count += 1
            elif track['status'] == 'unidentified':
                unidentified_dir = os.path.join(output_folder, "Unidentified")
                os.makedirs(unidentified_dir, exist_ok=True)
                unidentified_name = os.path.basename(
                    track.get('unidentified_filename') or f"File{track.get('file_num', 0)}_Track_{track.get('index', 0)+1}_Unidentified.flac"
                )
                unidentified_path = os.path.join(unidentified_dir, unidentified_name)
                track['unidentified_path'] = unidentified_path
                track['unidentified_filename'] = unidentified_name
                chunk.export(unidentified_path, format="flac")
                output_files_created.append(unidentified_path)
                if track.get('original_file'):
                    input_files_used.add(track['original_file'])
                unidentified_count += 1
            del chunk
        print(" " * 60)  # Clear progress line
    else:
        print(f"\n  📂 Re-splitting original files (light preview mode)...")
        files_to_process = {}
        for track in tracks:
            orig = track.get('original_file')
            if orig:
                files_to_process.setdefault(orig, []).append(track)

        for file_idx, (orig_file, file_tracks) in enumerate(files_to_process.items(), 1):
            if not os.path.exists(orig_file):
                continue

            filename = os.path.basename(orig_file)[:40]
            print(f"     [{file_idx}/{len(files_to_process)}] Loading {filename}...", end='', flush=True)
            recording = AudioSegment.from_file(orig_file)
            print(" splitting...", end='', flush=True)
            chunks = split_on_silence(recording, min_silence_len=2000, silence_thresh=-40, keep_silence=200)
            print(f" saving {len([t for t in file_tracks if t['status'] == 'identified'])} tracks")

            for track in file_tracks:
                if track['status'] == 'skipped':
                    skipped_count += 1
                    continue
                chunk_idx = track.get('chunk_index', 0)
                if chunk_idx >= len(chunks):
                    continue
                chunk = chunks[chunk_idx]

                if track['status'] == 'identified':
                    temp_flac = os.path.join(output_folder, f"temp_apply_{track['file_num']}_{track['index']}.flac")
                    chunk.export(temp_flac, format="flac")
                    embed_and_sort_generic(temp_flac, track['artist'], track['title'], track['album'],
                                          track.get('art_url'), output_folder, output_format, artwork_cache,
                                          track.get('enhanced_metadata', {}))
                    ext = AUDIO_FORMATS.get(output_format, AUDIO_FORMATS['flac'])['ext']
                    safe_artist = track['artist'].translate(str.maketrans('', '', '<>:"/\\|?*'))
                    out_name = f"{track['artist']} - {track['title']}{ext}".translate(str.maketrans('', '', '<>:"/\\|?*'))
                    output_files_created.append(os.path.join(output_folder, safe_artist, out_name))
                    input_files_used.add(orig_file)
                    identified_count += 1
                elif track['status'] == 'unidentified':
                    unidentified_dir = os.path.join(output_folder, "Unidentified")
                    os.makedirs(unidentified_dir, exist_ok=True)
                    unidentified_name = os.path.basename(
                        track.get('unidentified_filename') or f"File{track.get('file_num', 0)}_Track_{track.get('index', 0)+1}_Unidentified.flac"
                    )
                    unidentified_path = os.path.join(unidentified_dir, unidentified_name)
                    track['unidentified_path'] = unidentified_path
                    track['unidentified_filename'] = unidentified_name
                    chunk.export(unidentified_path, format="flac")
                    output_files_created.append(unidentified_path)
                    input_files_used.add(orig_file)
                    unidentified_count += 1
            del recording, chunks

    print(f"\n{Style.GREEN}{'═'*50}")
    print(f"  {Style.BOLD}✅ APPLY COMPLETE!{Style.RESET}{Style.GREEN}")
    print(f"{'─'*50}{Style.RESET}")
    print(f"  {Style.GREEN}✅ Saved:{Style.RESET}        {Style.BOLD}{identified_count}{Style.RESET} tracks")
    print(f"  {Style.YELLOW}❓ Unidentified:{Style.RESET} {unidentified_count} tracks")
    print(f"  {Style.DIM}⏭️  Skipped:{Style.RESET}      {skipped_count} tracks")
    print(f"{Style.GREEN}{'═'*50}{Style.RESET}\n")

    # Save manifest for history/rollback
    if output_files_created:
        input_file = list(input_files_used)[0] if input_files_used else "unknown"

        # Build pipeline data from cached split_data (v2.0)
        _split_data = cache_data.get('split_data', {})
        _pipeline = {}
        if _split_data:
            _methods = list(set(sd.get('method', '?') for sd in _split_data.values()))
            _all_points = {}
            for fpath, sd in _split_data.items():
                _all_points[fpath] = {
                    'method': sd.get('method'),
                    'points_sec': sd.get('points_sec'),
                    'num_segments': sd.get('num_segments'),
                    'params': sd.get('params', {}),
                }
            _pipeline = {
                'split_methods': _methods,
                'per_file': _all_points,
            }

        _config_snap = cache_data.get('config_snapshot', {})
        _config_snap['output_format'] = output_format

        manifest_path = export_manifest_for_session(
            input_file=input_file,
            output_files=output_files_created,
            tracks=tracks,
            mode=_config_snap.get('identification_mode', 'preview'),
            pipeline=_pipeline,
            config_snapshot=_config_snap,
            input_files=list(input_files_used) if input_files_used else None
        )
        if manifest_path:
            print(f"  📋 Manifest saved: {os.path.basename(manifest_path)}")

    return bool(output_files_created)
