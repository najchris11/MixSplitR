#!/usr/bin/env python3
"""
MixSplitR v7.1 - Track Processing Module

Contains the four process_single_track_* functions and their shared helpers.
Extracted from mixsplitr.py for maintainability.

Each function follows the same pipeline shape:
  1. Export sample → 2. Identify → 3. Enrich metadata → 4. BPM → 5. Merge → 6. Dedup → 7. Return
"""

import os
import json
import threading

from pydub import AudioSegment

# Optional third-party imports with fallback
try:
    import acoustid
    ACOUSTID_AVAILABLE = True
except ImportError:
    ACOUSTID_AVAILABLE = False

# =============================================================================
# LOCAL MODULE IMPORTS
# =============================================================================

from .mixsplitr_core import get_config, Style

from .mixsplitr_metadata import find_art_in_json, get_backup_art, get_all_external_metadata

from .mixsplitr_audio import detect_bpm_librosa

from .mixsplitr_identify import (
    identify_with_acoustid, identify_with_shazam, get_enhanced_metadata,
    merge_identification_results, is_shazam_available, is_trace_enabled,
    musicbrainz_search_recordings, print_id_winner
)


# =============================================================================
# SHARED HELPERS for process_single_track_* functions
# =============================================================================

def _export_id_sample(chunk, file_num, i, prefix="temp_id"):
    """Export a configurable WAV sample from the middle of the chunk.

    Used by ACRCloud and Dual modes for fingerprint submission.
    Returns the temporary file path.
    """
    config = get_config()
    try:
        sample_seconds = int(config.get('fingerprint_sample_seconds', 12))
    except Exception:
        sample_seconds = 12
    sample_seconds = max(8, min(45, sample_seconds))
    sample_ms = sample_seconds * 1000

    chunk_len = len(chunk)
    if chunk_len <= sample_ms:
        sample = chunk
    else:
        middle = chunk_len // 2
        half_window = sample_ms // 2
        start = max(0, middle - half_window)
        end = min(chunk_len, start + sample_ms)
        start = max(0, end - sample_ms)
        sample = chunk[start:end]

    temp_name = f"{prefix}_{file_num}_{i}_{threading.current_thread().ident}.wav"
    sample.export(temp_name, format="wav")
    return temp_name


def _detect_bpm_if_needed(chunk, external_meta=None):
    """Run local BPM detection if enabled and Deezer didn't already provide one.

    If *external_meta* is provided, checks its ``deezer.bpm`` field first and
    stores the result back into ``external_meta['local_bpm']``.

    Returns the local_bpm dict (or None).
    """
    config = get_config()
    use_local_bpm = not bool(config.get('disable_local_bpm', False))

    local_bpm = None
    if use_local_bpm:
        skip = False
        if external_meta is not None:
            deezer_data = (external_meta or {}).get('deezer') or {}
            if isinstance(deezer_data, dict) and deezer_data.get('bpm'):
                skip = True
        if not skip and chunk is not None:
            try:
                local_bpm = detect_bpm_librosa(chunk)
            except:
                pass
    if external_meta is not None:
        external_meta['local_bpm'] = local_bpm
    return local_bpm


def _resolve_artwork(art_url, artist, title):
    """Fall back to backup artwork sources if *art_url* is not already set."""
    if not art_url and artist and title:
        return get_backup_art(artist, title)
    return art_url


def _build_enhanced_metadata(merged):
    """Extract enhanced metadata fields from a merge-result dict."""
    enhanced = {}
    if merged['genres']['value']:
        enhanced['genres'] = merged['genres']['value']
    if merged['release_date']['value']:
        enhanced['release_date'] = merged['release_date']['value']
    if merged['label']['value']:
        enhanced['label'] = merged['label']['value']
    if merged['isrc']['value']:
        enhanced['isrc'] = merged['isrc']['value']
    if merged.get('bpm', {}).get('value'):
        enhanced['bpm'] = merged['bpm']['value']
    return enhanced


def _build_readable_metadata(merged, artist, title, album):
    """Build the human-readable metadata dict shown in the UI and manifest."""
    readable = {
        'artist':      {'value': artist, 'source': merged['artist']['source']},
        'title':       {'value': title,  'source': merged['title']['source']},
        'album':       {'value': album,  'source': merged['album']['source']},
        'confidence':  merged['confidence'],
        'agreement':   merged['agreement'],
        'sources_used': merged['sources_used']
    }
    if merged['label']['value']:
        readable['label'] = {'value': merged['label']['value'], 'source': merged['label']['source']}
    if merged['genres']['value']:
        readable['genres'] = {'value': merged['genres']['value'], 'source': merged['genres']['source']}
    if merged['release_date']['value']:
        year_val = merged['release_date']['value'][:4] if len(merged['release_date']['value']) >= 4 else merged['release_date']['value']
        readable['year'] = {'value': year_val, 'source': merged['release_date']['source']}
    if merged['isrc']['value']:
        readable['isrc'] = {'value': merged['isrc']['value'], 'source': merged['isrc']['source']}
    if merged.get('bpm', {}).get('value'):
        readable['bpm'] = {'value': merged['bpm']['value'], 'source': merged['bpm']['source']}
    return readable


# =============================================================================
# TRACK PROCESSING – ACRCloud mode (primary)
# =============================================================================

def process_single_track(chunk_data, i, recognizer, rate_limiter, existing_tracks,
                         output_folder, existing_tracks_lock, preview_mode=False):
    """Process a single track - designed for parallel execution with merged identification"""
    chunk = chunk_data.get('chunk')
    file_num = chunk_data.get('file_num', 0)

    if chunk is None:
        return {'status': 'skipped', 'reason': 'no_chunk', 'index': i, 'file_num': file_num}

    if len(chunk) < 10000:
        return {'status': 'skipped', 'reason': 'too_short', 'index': i, 'file_num': file_num}

    temp_name = _export_id_sample(chunk, file_num, i)

    rate_limiter.wait()
    res = json.loads(recognizer.recognize_by_file(temp_name, 0))

    if os.path.exists(temp_name):
        os.remove(temp_name)

    # Read config for runtime flags
    config = get_config()
    shazam_enabled = not bool(config.get('disable_shazam', False))

    acr_result = None
    mb_result = None
    mb_enhanced = {}
    art_url = None
    recording_id = None
    _shazam_raw = None
    _acoustid_raw = None

    if res.get("status", {}).get("code") == 0 and res.get("metadata", {}).get("music"):
        music = res["metadata"]["music"][0]
        acr_result = {
            'artist': music["artists"][0]["name"],
            'title': music["title"],
            'album': music.get("album", {}).get("name", "Unknown Album")
        }
        art_url = find_art_in_json(music)

        if ACOUSTID_AVAILABLE:
            mb_enhanced = get_enhanced_metadata(acr_result['artist'], acr_result['title'])
            acoustid_result = identify_with_acoustid(chunk)
            if acoustid_result:
                mb_result = {'artist': acoustid_result['artist'], 'title': acoustid_result['title']}
                recording_id = acoustid_result.get('recording_id')
                _acoustid_raw = {'artist': acoustid_result['artist'], 'title': acoustid_result['title'],
                                 'score': acoustid_result.get('score'), 'recording_id': recording_id}
                if recording_id:
                    mb_enhanced = get_enhanced_metadata(acr_result['artist'], acr_result['title'], recording_id)
    else:
        # Try Shazam first (best for underground tracks)
        shazam_result = None
        if shazam_enabled and is_shazam_available():
            shazam_result = identify_with_shazam(chunk)
            if shazam_result:
                mb_result = {'artist': shazam_result['artist'], 'title': shazam_result['title']}
                mb_enhanced = get_enhanced_metadata(shazam_result['artist'], shazam_result['title'])
                _shazam_raw = {'artist': shazam_result['artist'], 'title': shazam_result['title']}

        # Fall back to AcoustID if Shazam didn't find it
        if not shazam_result and ACOUSTID_AVAILABLE:
            acoustid_result = identify_with_acoustid(chunk)
            if acoustid_result:
                mb_result = {'artist': acoustid_result['artist'], 'title': acoustid_result['title']}
                recording_id = acoustid_result.get('recording_id')
                mb_enhanced = get_enhanced_metadata(mb_result['artist'], mb_result['title'], recording_id)
                _acoustid_raw = {'artist': acoustid_result['artist'], 'title': acoustid_result['title'],
                                 'score': acoustid_result.get('score'), 'recording_id': recording_id}

    final_artist = (acr_result or mb_result or {}).get('artist')
    final_title = (acr_result or mb_result or {}).get('title')
    art_url = _resolve_artwork(art_url, final_artist, final_title)

    external_meta = get_all_external_metadata(final_artist, final_title) if final_artist and final_title else {}
    _detect_bpm_if_needed(chunk, external_meta)

    merged = merge_identification_results(acr_result, mb_result, mb_enhanced, external_meta)

    if merged['artist']['value'] and merged['title']['value']:
        artist = merged['artist']['value']
        title = merged['title']['value']
        album = merged['album']['value'] or 'Unknown Album'

        expected_filename = f"{artist} - {title}.flac".translate(str.maketrans('', '', '<>:"/\\|?*'))
        with existing_tracks_lock:
            if expected_filename in existing_tracks:
                return {
                    'status': 'skipped', 'reason': 'already_exists', 'index': i, 'file_num': file_num,
                    'artist': artist, 'title': title, 'album': album,
                    'original_file': chunk_data.get('original_file'),
                    'chunk_index': chunk_data.get('split_index', 0),
                    'temp_chunk_path': chunk_data.get('temp_chunk_path')
                }

        readable_metadata = _build_readable_metadata(merged, artist, title, album)
        enhanced_metadata = _build_enhanced_metadata(merged)

        result = {
            'status': 'identified', 'index': i, 'file_num': file_num,
            'artist': artist, 'title': title, 'album': album, 'art_url': art_url,
            'expected_filename': expected_filename,
            'identification_source': merged['sources_used'][0].lower() if merged['sources_used'] else 'unknown',
            'enhanced_metadata': enhanced_metadata, 'readable_metadata': readable_metadata,
            'backend_candidates': {
                'acrcloud': acr_result, 'shazam': _shazam_raw,
                'acoustid': _acoustid_raw, 'recording_id': recording_id
            },
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if not preview_mode:
            temp_flac = os.path.join(output_folder, f"temp_{file_num}_{i}_{threading.current_thread().ident}.flac")
            chunk.export(temp_flac, format="flac")
            result['temp_flac'] = temp_flac

        with existing_tracks_lock:
            existing_tracks.add(expected_filename)

        return result
    else:
        unidentified_filename = f"File{file_num}_Track_{i+1}_Unidentified.flac"
        unidentified_path = os.path.join(output_folder, unidentified_filename)

        if os.path.exists(unidentified_path):
            return {
                'status': 'skipped', 'reason': 'already_exists', 'index': i, 'file_num': file_num,
                'unidentified_filename': unidentified_filename,
                'original_file': chunk_data.get('original_file'),
                'chunk_index': chunk_data.get('split_index', 0),
                'temp_chunk_path': chunk_data.get('temp_chunk_path')
            }

        local_bpm = _detect_bpm_if_needed(chunk)

        result = {
            'status': 'unidentified', 'index': i, 'file_num': file_num,
            'unidentified_filename': unidentified_filename, 'unidentified_path': unidentified_path,
            'backend_candidates': {
                'acrcloud': acr_result, 'shazam': _shazam_raw,
                'acoustid': _acoustid_raw, 'recording_id': recording_id
            },
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if local_bpm and local_bpm.get('bpm'):
            result['detected_bpm'] = local_bpm['bpm']
            result['bpm_confidence'] = local_bpm.get('confidence', 0)

        if not preview_mode:
            chunk.export(unidentified_path, format="flac")

        return result


# =============================================================================
# TRACK PROCESSING – Manual mode (no fingerprinting)
# =============================================================================

def process_single_track_manual(chunk_data, i, existing_tracks,
                                 output_folder, existing_tracks_lock, preview_mode=False):
    """Mark track as unidentified for manual entry (no fingerprinting).

    Used when MODE_MANUAL is active (no API keys configured).
    All tracks are marked as unidentified for manual metadata entry in the editor.
    """
    chunk = chunk_data.get('chunk')
    file_num = chunk_data.get('file_num', 0)

    if chunk is None:
        return {'status': 'skipped', 'reason': 'no_chunk', 'index': i, 'file_num': file_num}

    if len(chunk) < 10000:
        return {'status': 'skipped', 'reason': 'too_short', 'index': i, 'file_num': file_num}

    # Skip duplicate check in manual mode
    with existing_tracks_lock:
        existing_tracks.append({'status': 'unidentified'})

    # Export temp FLAC for manual editing
    temp_flac = chunk_data.get('temp_chunk_path')
    if not temp_flac:
        temp_flac = os.path.join(output_folder, f"temp_track_{i}_{file_num}.flac")
        # Convert to stereo if multi-channel (FLAC supports max 8 channels)
        # Use ffmpeg -ac 2 for proper mixdown (pydub.set_channels doesn't handle >2 to 2)
        if chunk.channels > 8:
            chunk.export(temp_flac, format="flac", parameters=["-ac", "2", "-compression_level", "8"])
        else:
            chunk.export(temp_flac, format="flac", parameters=["-compression_level", "8"])

    return {
        'status': 'unidentified',
        'index': i,
        'file_num': file_num,
        'temp_flac': temp_flac,
        'artist': '',
        'title': '',
        'album': '',
        'art_url': None,
        'enhanced_metadata': {}
    }

# =============================================================================
# TRACK PROCESSING – MusicBrainz-only mode  (v7.1)
# =============================================================================

def process_single_track_mb_only(chunk_data, i, existing_tracks,
                                  output_folder, existing_tracks_lock, preview_mode=False):
    """Identify a single track using AcoustID + MusicBrainz only (no ACRCloud).

    Workflow
    --------
    1. Export a 12-second sample from the middle of the chunk.
    2. Submit to AcoustID for fingerprint lookup -> recording_id (if AcoustID is available).
    3. If AcoustID is NOT available, track will be marked as unidentified for manual handling.
       If AcoustID IS available but fingerprint failed, fall back to MusicBrainz text-search
       using the sanitised source filename as a free-text query (only if filename looks valid).
    4. Fetch full metadata from MusicBrainz via recording_id.
    5. Enrich with iTunes / Deezer / Last.fm (same as normal mode).
    6. Run local BPM detection if Deezer didn't return one.
    7. Merge everything through the standard merge_identification_results().
    """
    chunk = chunk_data.get('chunk')
    file_num = chunk_data.get('file_num', 0)

    if chunk is None:
        return {'status': 'skipped', 'reason': 'no_chunk', 'index': i, 'file_num': file_num}

    if len(chunk) < 10000:
        return {'status': 'skipped', 'reason': 'too_short', 'index': i, 'file_num': file_num}

    # Read config for runtime flags
    config = get_config()
    shazam_enabled = not bool(config.get('disable_shazam', False))

    # -- 1. Shazam + AcoustID fingerprint lookup --
    mb_result = None
    mb_enhanced = {}
    recording_id = None
    art_url = None
    acoustid_attempted = False
    shazam_result = None
    _shazam_raw = None
    _acoustid_raw = None
    _mb_search_candidates = None

    # Try Shazam first (best for underground)
    if shazam_enabled and is_shazam_available():
        shazam_result = identify_with_shazam(chunk)
        if shazam_result:
            mb_result = {
                'artist': shazam_result['artist'],
                'title':  shazam_result['title']
            }
            mb_enhanced = get_enhanced_metadata(shazam_result['artist'], shazam_result['title'])
            _shazam_raw = {'artist': shazam_result['artist'], 'title': shazam_result['title']}

    # Fall back to AcoustID if Shazam didn't find it
    if not shazam_result and ACOUSTID_AVAILABLE:
        acoustid_attempted = True
        acoustid_result = identify_with_acoustid(chunk)
        if acoustid_result:
            mb_result = {
                'artist': acoustid_result['artist'],
                'title':  acoustid_result['title']
            }
            recording_id = acoustid_result.get('recording_id')
            mb_enhanced  = get_enhanced_metadata(
                mb_result['artist'], mb_result['title'], recording_id
            )
            _acoustid_raw = {'artist': acoustid_result['artist'], 'title': acoustid_result['title'],
                             'score': acoustid_result.get('score'), 'recording_id': recording_id}

    # -- 2. MusicBrainz text-search fallback --
    if not mb_result and acoustid_attempted:
        original_file = chunk_data.get('original_file', '')
        if original_file:
            import re as _re
            query = os.path.splitext(os.path.basename(original_file))[0]
            query = _re.sub(r'[_\-]+', ' ', query).strip()

            has_separator = ' - ' in query or '_-_' in os.path.basename(original_file) or '-' in query
            is_long_enough = len(query) > 10
            looks_like_generic = query.lower().startswith(('file', 'track', 'audio', 'recording'))

            if query and has_separator and is_long_enough and not looks_like_generic:
                candidates = musicbrainz_search_recordings(query=query, limit=3)
                _mb_search_candidates = candidates
                if candidates and candidates[0].get('artist') and candidates[0].get('title'):
                    best = candidates[0]
                    mb_result = {
                        'artist': best['artist'],
                        'title':  best['title']
                    }
                    recording_id = best.get('recording_id')
                    mb_enhanced  = get_enhanced_metadata(
                        best['artist'], best['title'], recording_id
                    )

    # -- 3. Resolve final artist/title and artwork --
    final_artist = (mb_result or {}).get('artist')
    final_title  = (mb_result or {}).get('title')
    art_url = _resolve_artwork(art_url, final_artist, final_title)

    # -- 4. External metadata + BPM --
    external_meta = (
        get_all_external_metadata(final_artist, final_title)
        if final_artist and final_title else {}
    )
    _detect_bpm_if_needed(chunk, external_meta)

    # -- 5. Merge (acr_result=None because we have no ACRCloud result) --
    merged = merge_identification_results(None, mb_result, mb_enhanced, external_meta)

    # -- 7. Build result --
    if merged['artist']['value'] and merged['title']['value']:
        artist = merged['artist']['value']
        title  = merged['title']['value']
        album  = merged['album']['value'] or 'Unknown Album'

        expected_filename = f"{artist} - {title}.flac".translate(
            str.maketrans('', '', '<>:"/\\|?*')
        )
        with existing_tracks_lock:
            if expected_filename in existing_tracks:
                return {
                    'status': 'skipped', 'reason': 'already_exists',
                    'index': i, 'file_num': file_num,
                    'artist': artist, 'title': title, 'album': album,
                    'original_file': chunk_data.get('original_file'),
                    'chunk_index': chunk_data.get('split_index', 0),
                    'temp_chunk_path': chunk_data.get('temp_chunk_path')
                }

        readable_metadata = _build_readable_metadata(merged, artist, title, album)
        enhanced_metadata = _build_enhanced_metadata(merged)

        result = {
            'status': 'identified', 'index': i, 'file_num': file_num,
            'artist': artist, 'title': title, 'album': album, 'art_url': art_url,
            'expected_filename': expected_filename,
            'identification_source': merged['sources_used'][0].lower() if merged['sources_used'] else 'musicbrainz',
            'enhanced_metadata': enhanced_metadata,
            'readable_metadata': readable_metadata,
            'backend_candidates': {
                'shazam': _shazam_raw, 'acoustid': _acoustid_raw,
                'mb_text_search': _mb_search_candidates, 'recording_id': recording_id
            },
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if not preview_mode:
            temp_flac = os.path.join(output_folder, f"temp_{file_num}_{i}_{threading.current_thread().ident}.flac")
            chunk.export(temp_flac, format="flac")
            result['temp_flac'] = temp_flac

        with existing_tracks_lock:
            existing_tracks.add(expected_filename)

        return result

    else:
        unidentified_filename = f"File{file_num}_Track_{i+1}_Unidentified.flac"
        unidentified_path     = os.path.join(output_folder, unidentified_filename)

        if os.path.exists(unidentified_path):
            return {
                'status': 'skipped', 'reason': 'already_exists',
                'index': i, 'file_num': file_num,
                'unidentified_filename': unidentified_filename,
                'original_file': chunk_data.get('original_file'),
                'chunk_index': chunk_data.get('split_index', 0),
                'temp_chunk_path': chunk_data.get('temp_chunk_path')
            }

        local_bpm = _detect_bpm_if_needed(chunk)

        result = {
            'status': 'unidentified', 'index': i, 'file_num': file_num,
            'unidentified_filename': unidentified_filename,
            'unidentified_path':     unidentified_path,
            'backend_candidates': {
                'shazam': _shazam_raw, 'acoustid': _acoustid_raw,
                'mb_text_search': _mb_search_candidates, 'recording_id': recording_id
            },
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if local_bpm and local_bpm.get('bpm'):
            result['detected_bpm']    = local_bpm['bpm']
            result['bpm_confidence']  = local_bpm.get('confidence', 0)

        if not preview_mode:
            chunk.export(unidentified_path, format="flac")

        return result


# =============================================================================
# TRACK PROCESSING – Dual mode (Best of Both)  (v7.1)
# =============================================================================

def process_single_track_dual(chunk_data, i, recognizer, rate_limiter, existing_tracks,
                              output_folder, existing_tracks_lock, preview_mode=False):
    """
    Identify track using BOTH ACRCloud and AcoustID, pick best result by confidence.

    This provides the most comprehensive identification by:
    1. Running ACRCloud fingerprint lookup
    2. Running AcoustID fingerprint lookup (in parallel)
    3. Comparing confidence scores
    4. Picking the winner and enriching with metadata
    """
    chunk = chunk_data.get('chunk')
    file_num = chunk_data.get('file_num', 0)

    if chunk is None:
        return {'status': 'skipped', 'reason': 'no_chunk', 'index': i, 'file_num': file_num}

    if len(chunk) < 10000:
        return {'status': 'skipped', 'reason': 'too_short', 'index': i, 'file_num': file_num}

    # Read config for runtime flags
    config = get_config()
    shazam_enabled = not bool(config.get('disable_shazam', False))
    show_id_source = bool(config.get('show_id_source', True))

    # Export sample for identification
    temp_name = _export_id_sample(chunk, file_num, i, prefix="temp_dual")

    # Run both identifications
    acr_result = None
    acr_confidence = 0
    aid_result = None
    aid_confidence = 0
    recording_id = None

    # ACRCloud identification
    rate_limiter.wait()
    _trace = is_trace_enabled()
    if _trace:
        print(f"  [ACRCloud] attempted...")
    try:
        res = json.loads(recognizer.recognize_by_file(temp_name, 0))
        if res.get("status", {}).get("code") == 0 and res.get("metadata", {}).get("music"):
            music = res["metadata"]["music"][0]
            acr_result = {
                'artist': music["artists"][0]["name"],
                'title': music["title"],
                'album': music.get("album", {}).get("name", "Unknown Album"),
                'art_url': find_art_in_json(music)
            }
            acr_confidence = res.get("status", {}).get("score", 85)
            if _trace:
                print(f"  [ACRCloud] -> hit: {acr_result['artist']} - {acr_result['title']}")
        elif _trace:
            print(f"  [ACRCloud] -> miss")
    except Exception:
        if _trace:
            print(f"  [ACRCloud] -> miss (error)")
        pass

    # Shazam identification (try first for underground tracks)
    shazam_result = None
    shazam_confidence = 0
    if shazam_enabled and is_shazam_available():
        try:
            shazam_result_data = identify_with_shazam(chunk)
            if shazam_result_data and shazam_result_data.get('title'):
                shazam_result = {
                    'artist': shazam_result_data['artist'],
                    'title': shazam_result_data['title'],
                    'album': shazam_result_data.get('album', 'Unknown Album')
                }
                shazam_confidence = 85
        except Exception:
            pass

    # AcoustID identification
    if ACOUSTID_AVAILABLE:
        try:
            acoustid_result = identify_with_acoustid(chunk)
            if acoustid_result and acoustid_result.get('title'):
                aid_result = {
                    'artist': acoustid_result['artist'],
                    'title': acoustid_result['title'],
                    'album': acoustid_result.get('album', 'Unknown Album')
                }
                recording_id = acoustid_result.get('recording_id')
                aid_confidence = acoustid_result.get('score', 0.8) * 100
        except Exception:
            pass

    # Clean up temp file
    if os.path.exists(temp_name):
        os.remove(temp_name)

    # Pick winner by confidence across Shazam / ACRCloud / AcoustID
    winner = None
    winner_source = None
    art_url = None
    winner_recording_id = None

    candidates = []
    if shazam_result:
        candidates.append({
            "src": "shazam", "data": shazam_result, "confidence": shazam_confidence,
            "priority": 3, "art_url": None, "recording_id": None,
        })
    if acr_result:
        candidates.append({
            "src": "acrcloud", "data": acr_result, "confidence": acr_confidence,
            "priority": 2, "art_url": acr_result.get("art_url"), "recording_id": None,
        })
    if aid_result:
        candidates.append({
            "src": "acoustid", "data": aid_result, "confidence": aid_confidence,
            "priority": 1, "art_url": None, "recording_id": recording_id,
        })

    if candidates:
        best = max(candidates, key=lambda c: (c["confidence"], c["priority"]))
        winner = best["data"]
        art_url = best["art_url"]
        winner_recording_id = best["recording_id"]
        recording_id = winner_recording_id
        winner_confidence = best["confidence"]

        src_label = {"shazam": "Shazam", "acrcloud": "ACRCloud", "acoustid": "AcoustID"}.get(best["src"], best["src"])
        winner_source = f"{src_label} ({best['confidence']:.0f}%)"

        # Print winner line if enabled
        if show_id_source:
            print_id_winner(i + 1, best["src"], winner["artist"], winner["title"])
    else:
        winner_confidence = 0
        if show_id_source:
            print_id_winner(i + 1, "none")

    if winner:
        artist = winner['artist']
        title = winner['title']
        album = winner['album']

        mb_enhanced = get_enhanced_metadata(artist, title, recording_id) if recording_id else {}
        art_url = _resolve_artwork(art_url, artist, title)

        external_meta = get_all_external_metadata(artist, title) or {}
        _detect_bpm_if_needed(chunk, external_meta)

        mb_result = {'artist': artist, 'title': title, 'album': album}
        acr_for_merge = acr_result if winner_source and 'ACRCloud' in str(winner_source) else None
        merged = merge_identification_results(acr_for_merge, mb_result, mb_enhanced, external_meta)

        expected_filename = f"{artist} - {title}.flac".translate(str.maketrans('', '', '<>:"/\\|?*'))
        with existing_tracks_lock:
            if expected_filename in existing_tracks:
                return {'status': 'skipped', 'reason': 'duplicate', 'index': i, 'file_num': file_num,
                        'artist': artist, 'title': title}
            existing_tracks.add(expected_filename)

        enhanced_metadata = _build_enhanced_metadata(merged)
        confidence = winner_confidence / 100.0

        _serializable_candidates = [
            {'src': c['src'], 'confidence': c['confidence'], 'priority': c['priority'],
             'artist': c['data'].get('artist'), 'title': c['data'].get('title'),
             'album': c['data'].get('album'), 'recording_id': c.get('recording_id')}
            for c in candidates
        ]

        result = {
            'status': 'identified', 'index': i, 'file_num': file_num,
            'artist': artist, 'title': title, 'album': album, 'art_url': art_url,
            'expected_filename': expected_filename,
            'identification_source': 'dual',
            'dual_comparison': winner_source,
            'confidence': confidence,
            'enhanced_metadata': enhanced_metadata,
            'backend_candidates': {
                'dual_candidates': _serializable_candidates,
                'winner_src': best['src'],
                'recording_id': recording_id
            },
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if not preview_mode:
            temp_flac = os.path.join(output_folder, f"temp_dual_{file_num}_{i}_{threading.current_thread().ident}.flac")
            chunk.export(temp_flac, format="flac")
            result['temp_flac'] = temp_flac

        return result

    else:
        unidentified_filename = f"File{file_num}_Track_{i+1}_Unidentified.flac"
        unidentified_path = os.path.join(output_folder, unidentified_filename)

        result = {
            'status': 'unidentified', 'index': i, 'file_num': file_num,
            'unidentified_filename': unidentified_filename,
            'unidentified_path': unidentified_path,
            'backend_candidates': {'dual_candidates': [], 'recording_id': None},
            'original_file': chunk_data.get('original_file'),
            'chunk_index': chunk_data.get('split_index', 0),
            'temp_chunk_path': chunk_data.get('temp_chunk_path')
        }

        if not preview_mode:
            chunk.export(unidentified_path, format="flac")

        return result
