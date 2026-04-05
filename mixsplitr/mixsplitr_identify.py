"""
mixsplitr_identify.py - Track identification for MixSplitR

Contains:
- AcoustID/MusicBrainz identification
- Enhanced metadata lookup from MusicBrainz
- Result merging from multiple sources
- Artwork batch downloading
"""

import os
import re
import threading
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# MusicBrainz/AcoustID availability flags
_ACOUSTID_AVAILABLE = False
_MB_AVAILABLE = False
_acoustid = None
_musicbrainzngs = None
_ACOUSTID_API_KEY = None
_ACOUSTID_DEBUG = os.environ.get("MIXSPLITR_DEBUG_ACOUSTID", "").strip().lower() in ("1", "true", "yes", "y", "on")
_ACOUSTID_TRACE = os.environ.get("MIXSPLITR_TRACE_ACOUSTID", "").strip().lower() in ("1", "true", "yes", "y", "on")
_default_key_warning_shown = False

# Shazam availability flags
_SHAZAM_AVAILABLE = False
_shazamio = None
_SHAZAM_DEBUG = os.environ.get("MIXSPLITR_DEBUG_SHAZAM", "").strip().lower() in ("1", "true", "yes", "y", "on")
_SHAZAM_TRACE = os.environ.get("MIXSPLITR_TRACE_SHAZAM", "").strip().lower() in ("1", "true", "yes", "y", "on")
_SHAZAM_MISSING_WARNED = False

# ACRCloud trace flag
_ACRCLOUD_TRACE = os.environ.get("MIXSPLITR_TRACE_ACRCLOUD", "").strip().lower() in ("1", "true", "yes", "y", "on")

# Unified trace flag (enables trace for ALL backends)
_TRACE_ALL = os.environ.get("MIXSPLITR_TRACE", "").strip().lower() in ("1", "true", "yes", "y", "on")

def is_trace_enabled():
    """Check if any trace mode is enabled (unified or per-backend)"""
    return _TRACE_ALL or _SHAZAM_TRACE or _ACOUSTID_TRACE or _ACRCLOUD_TRACE


def setup_musicbrainz(version, repo):
    """Initialize MusicBrainz client (no account required).

    This sets a proper User-Agent and enables text-search lookups for metadata.
    AcoustID fingerprinting is optional and only enabled if pyacoustid is installed.
    Shazam fingerprinting is optional and only enabled if shazamio is installed.
    """
    global _ACOUSTID_AVAILABLE, _MB_AVAILABLE, _acoustid, _musicbrainzngs
    global _SHAZAM_AVAILABLE, _shazamio

    _ACOUSTID_AVAILABLE = False
    _MB_AVAILABLE = False
    _SHAZAM_AVAILABLE = False
    _acoustid = None
    _musicbrainzngs = None
    _shazamio = None

    try:
        import musicbrainzngs
        _musicbrainzngs = musicbrainzngs
        _MB_AVAILABLE = True

        # Use GitLab repo URL for User-Agent contact info
        _musicbrainzngs.set_useragent("MixSplitR", version, f"https://gitlab.com/{repo}")
    except ImportError:
        _MB_AVAILABLE = False
        _musicbrainzngs = None
        # IMPORTANT: do NOT return here; Shazam can still be initialized below.

    # Optional: AcoustID (fingerprinting) support if installed.
    try:
        import acoustid
        _acoustid = acoustid
        _ACOUSTID_AVAILABLE = True
    except ImportError:
        _ACOUSTID_AVAILABLE = False

    # Optional: Shazam (fingerprinting) support if installed.
    try:
        from shazamio import Shazam
        _shazamio = Shazam
        _SHAZAM_AVAILABLE = True
    except ImportError:
        _SHAZAM_AVAILABLE = False

    return _MB_AVAILABLE or _ACOUSTID_AVAILABLE or _SHAZAM_AVAILABLE


def set_acoustid_api_key(key):
    """Set a custom AcoustID API key"""
    global _ACOUSTID_API_KEY
    _ACOUSTID_API_KEY = key


def get_acoustid_api_key():
    """Get the current AcoustID API key
    
    Returns the user-configured key, or None if not set.
    NO DEFAULT KEY - users must provide their own.
    """
    global _ACOUSTID_API_KEY
    return _ACOUSTID_API_KEY


def check_chromaprint_available():
    """
    Check if chromaprint/fpcalc is available.
    Handles PyInstaller onefile mode where fpcalc is extracted to _MEIPASS temp folder.
    """
    import shutil
    import sys
    
    # PRIORITY 1: Check PyInstaller temporary extraction folder (onefile mode)
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Running as PyInstaller onefile - files extracted to _MEIPASS temp folder
        temp_dir = sys._MEIPASS
        
        if sys.platform == 'win32':
            fpcalc_name = 'fpcalc.exe'
        else:
            fpcalc_name = 'fpcalc'
        
        temp_fpcalc = os.path.join(temp_dir, fpcalc_name)
        
        if os.path.exists(temp_fpcalc):
            # Found in PyInstaller temp extraction folder
            os.environ['FPCALC'] = temp_fpcalc
            return (True, temp_fpcalc)
    
    # PRIORITY 2: Check if running as compiled executable (onefolder mode)
    if getattr(sys, 'frozen', False):
        # Running as compiled executable (but not onefile, or _MEIPASS not set)
        exe_dir = os.path.dirname(sys.executable)
        
        if sys.platform == 'win32':
            bundled_fpcalc = os.path.join(exe_dir, 'fpcalc.exe')
        else:
            bundled_fpcalc = os.path.join(exe_dir, 'fpcalc')
        
        if os.path.exists(bundled_fpcalc):
            # Found next to executable
            os.environ['FPCALC'] = bundled_fpcalc
            return (True, bundled_fpcalc)
    
    # PRIORITY 3: Check system PATH
    fpcalc_path = shutil.which('fpcalc')
    if fpcalc_path:
        return (True, fpcalc_path)
    
    # PRIORITY 4: Try common Windows locations
    if os.name == 'nt':
        common_paths = [
            r'C:\Program Files\Chromaprint\fpcalc.exe',
            r'C:\Program Files (x86)\Chromaprint\fpcalc.exe',
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fpcalc.exe'),
            os.path.join(os.getcwd(), 'fpcalc.exe'),
        ]
        for path in common_paths:
            if os.path.exists(path):
                os.environ['FPCALC'] = path
                return (True, path)
    
    # Not found anywhere
    return (False, None)


def is_acoustid_available():
    """Check if AcoustID/MusicBrainz is available"""
    return _ACOUSTID_AVAILABLE


def is_musicbrainz_available():
    """Check if MusicBrainz (musicbrainzngs) is available"""
    return _MB_AVAILABLE


def is_shazam_available():
    """Check if Shazam (shazamio) is available"""
    return _SHAZAM_AVAILABLE


def print_id_winner(track_num, winner_backend, artist=None, title=None):
    """Print one-line summary of which backend won identification.

    Args:
        track_num: Track number/index for display
        winner_backend: 'acrcloud' | 'shazam' | 'acoustid' | 'none'
        artist: Artist name (optional, for display)
        title: Track title (optional, for display)
    """
    backend_display = winner_backend.lower() if winner_backend else 'none'
    if artist and title:
        print(f"  Track {track_num}: ID: {backend_display} → {artist} - {title}")
    else:
        print(f"  Track {track_num}: ID: {backend_display}")


# =============================================================================
# ACOUSTID IDENTIFICATION
# =============================================================================

def identify_with_acoustid(audio_chunk):
    """Fallback identification using AcoustID/MusicBrainz
    
    Returns:
        dict with artist, title, recording_id, score if found
        None if not found or error occurred
    """
    if not _ACOUSTID_AVAILABLE:
        return None
    
    # Check if API key is configured
    api_key = get_acoustid_api_key()
    if not api_key:
        # No key configured - show error once per session
        global _default_key_warning_shown
        if not _default_key_warning_shown:
            print(f"  [AcoustID] ⚠️  No API key configured")
            print(f"             AcoustID fingerprinting requires a free API key.")
            print(f"             Get one at: https://acoustid.org/api-key")
            print(f"             Add it via: Main Menu → Option 5 → Manage API Keys")
            _default_key_warning_shown = True
        return None
    
    temp_file = None
    try:
        temp_file = f"temp_acoustid_{threading.current_thread().ident}.wav"
        audio_chunk.export(temp_file, format="wav")
        
        if _ACOUSTID_DEBUG:
            print(f"  [AcoustID] Fingerprinting audio chunk (API key: {api_key[:4]}...)")
        if _ACOUSTID_TRACE or _TRACE_ALL:
            print(f"  [AcoustID] attempted...")

        # Use acoustid.match which handles fingerprinting + lookup
        results = _acoustid.match(api_key, temp_file)
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
            temp_file = None
        
        # Process results
        result_count = 0
        for score, recording_id, title, artist in results:
            result_count += 1
            if _ACOUSTID_DEBUG:
                print(f"  [AcoustID] Match #{result_count}: {artist} - {title} (score: {score:.2f})")
            
            # Accept matches with score > 0.5
            if score > 0.5:
                if _ACOUSTID_DEBUG:
                    print(f"  [AcoustID] ✓ Accepted match (score > 0.5)")
                if _ACOUSTID_TRACE or _TRACE_ALL:
                    print(f"  [AcoustID] → hit: {artist} - {title}")
                return {
                    'artist': artist,
                    'title': title,
                    'recording_id': recording_id,
                    'score': score,
                    'source': 'acoustid'
                }
        
        if _ACOUSTID_DEBUG:
            if result_count == 0:
                print(f"  [AcoustID] No matches found")
            else:
                print(f"  [AcoustID] No matches with score > 0.5")
        if _ACOUSTID_TRACE or _TRACE_ALL:
            print(f"  [AcoustID] → miss")

        return None
        
    except _acoustid.WebServiceError as e:
        # Handle AcoustID API-specific errors
        error_msg = str(e)
        error_lower = error_msg.lower()
        
        # Clean up temp file first
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        
        # Parse and display appropriate error message
        if "invalid" in error_lower and "key" in error_lower:
            print(f"  [AcoustID] ⚠️  Invalid API key")
            print(f"             Get a free key at: https://acoustid.org/api-key")
            print(f"             Update it in: Main Menu → Option 5 → Manage API Keys")
        elif "rate limit" in error_lower:
            print(f"  [AcoustID] ⚠️  Rate limit exceeded")
            print(f"             Your API key has hit its rate limit")
            print(f"             Wait a bit or contact AcoustID for higher limits")
        elif "status: error" in error_lower or "status:error" in error_lower:
            # Generic error from AcoustID - usually means service issue
            print(f"  [AcoustID] ⚠️  API returned generic error")
            print(f"             Common causes:")
            print(f"             • Internet connection issue")
            print(f"             • AcoustID service temporarily unavailable")
            print(f"             • Your API key may have issues")
            print(f"             Try again later or check: https://status.acoustid.org/")
        elif "fingerprint" in error_lower:
            print(f"  [AcoustID] ⚠️  Fingerprinting failed")
            print(f"             Audio may be too short or corrupted")
        elif "timeout" in error_lower or "timed out" in error_lower:
            print(f"  [AcoustID] ⚠️  Request timed out")
            print(f"             Check your internet connection")
        else:
            # Unknown error - show details
            print(f"  [AcoustID] API Error: {error_msg}")
            if _ACOUSTID_DEBUG:
                print(f"             Full error details:")
                import traceback
                traceback.print_exc()
        
        return None
        
    except Exception as e:
        # Handle other errors (fpcalc missing, file issues, etc.)
        error_msg = str(e)
        
        # Clean up temp file
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        
        error_lower = error_msg.lower()
        
        if "chromaprint" in error_lower or "fpcalc" in error_lower:
            print(f"  [AcoustID] ⚠️  chromaprint/fpcalc not found")
            print(f"             Install chromaprint:")
            print(f"             • Windows: Download from https://acoustid.org/chromaprint")
            print(f"             • macOS: brew install chromaprint")
            print(f"             • Linux: apt install libchromaprint-tools")
        elif "no such file" in error_lower or "cannot find" in error_lower:
            print(f"  [AcoustID] ⚠️  fpcalc executable not found in PATH")
            print(f"             Make sure chromaprint is installed (see above)")
        elif "command" in error_lower and "not found" in error_lower:
            print(f"  [AcoustID] ⚠️  fpcalc command failed")
            print(f"             Install or reinstall chromaprint")
        else:
            # Unknown error
            print(f"  [AcoustID] Error: {error_msg}")
            if _ACOUSTID_DEBUG:
                import traceback
                print(f"  [AcoustID] Traceback:")
                traceback.print_exc()
        
        return None


# =============================================================================
# SHAZAM IDENTIFICATION
# =============================================================================

def identify_with_shazam(audio_chunk):
    """Identify track using Shazam (ShazamIO library)
    
    Shazam advantages:
    - NO API KEY REQUIRED (completely free!)
    - Better coverage of underground electronic/dance music
    - Recognizes DJ edits, bootlegs, and SoundCloud uploads
    - Fast recognition (usually < 5 seconds)
    - Includes extended metadata (Apple Music IDs, genres, etc.)
    
    Returns:
        dict with artist, title, album, genres, etc. if found
        None if not found or error occurred
    """
    global _SHAZAM_MISSING_WARNED
    if not _SHAZAM_AVAILABLE:
        if _SHAZAM_TRACE and not _SHAZAM_MISSING_WARNED:
            print("  [Shazam] ✗ Not available (shazamio not installed / not packaged)")
            _SHAZAM_MISSING_WARNED = True
        return None
    
    import asyncio
    temp_file = None
    
    try:
        temp_file = f"temp_shazam_{threading.current_thread().ident}.wav"
        
        # Sample from the middle of the track where energy is usually highest.
        # This is configurable from Settings -> Identification Mode.
        duration_ms = len(audio_chunk)

        sample_seconds = 12
        try:
            from .mixsplitr_core import get_config  # Local import to avoid hard import cycles.
            config = get_config() or {}
            sample_seconds = int(config.get('fingerprint_sample_seconds', 12))
        except Exception:
            sample_seconds = 12
        sample_seconds = max(8, min(45, sample_seconds))
        sample_ms = sample_seconds * 1000

        if duration_ms > sample_ms:
            start = max(0, (duration_ms // 2) - (sample_ms // 2))
            end = min(duration_ms, start + sample_ms)
            start = max(0, end - sample_ms)
            sample = audio_chunk[start:end]
        else:
            # Track is short, use entire chunk
            sample = audio_chunk
        
        # Export sample
        sample.export(temp_file, format="wav")
        
        if _SHAZAM_DEBUG or _SHAZAM_TRACE or _TRACE_ALL:
            print(f"  [Shazam] attempted...")
        
        # Create Shazam instance and recognize
        shazam = _shazamio()
        
        # Run async recognition in a new event loop
        # (This handles the case where we're already in an async context)
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # We're in an async context, create a new loop in a thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    result = pool.submit(
                        lambda: asyncio.run(shazam.recognize(temp_file))
                    ).result(timeout=30)
            else:
                # Not in async context, use the loop directly
                result = loop.run_until_complete(shazam.recognize(temp_file))
        except RuntimeError:
            # No event loop, create a new one
            result = asyncio.run(shazam.recognize(temp_file))
        
        # Clean up temp file
        if os.path.exists(temp_file):
            os.remove(temp_file)
            temp_file = None
        
        # Parse Shazam result
        if result and 'track' in result:
            track = result['track']
            
            # Extract basic info
            title = track.get('title')
            artist = track.get('subtitle')  # Shazam uses 'subtitle' for artist name
            
            if _SHAZAM_DEBUG or _SHAZAM_TRACE or _TRACE_ALL:
                print(f"  [Shazam] → hit: {artist} - {title}")
            
            if not title or not artist:
                if _SHAZAM_DEBUG or _SHAZAM_TRACE or _TRACE_ALL:
                    print(f"  [Shazam] → miss (incomplete data)")
                return None
            
            # Build result dict
            shazam_result = {
                'artist': artist,
                'title': title,
                'source': 'shazam',
                'id_method': 'shazam',
                'shazam_key': track.get('key'),  # Unique Shazam track ID
            }
            
            # Extract metadata from sections
            sections = track.get('sections', [])
            for section in sections:
                section_type = section.get('type', '')
                metadata_items = section.get('metadata', [])
                
                if section_type == 'SONG':
                    # Main song metadata
                    for item in metadata_items:
                        title_key = item.get('title', '').lower()
                        text = item.get('text', '')
                        
                        if 'album' in title_key and text:
                            shazam_result['album'] = text
                        elif 'released' in title_key or 'release' in title_key:
                            # Parse release year
                            import re
                            year_match = re.search(r'\b(19|20)\d{2}\b', text)
                            if year_match:
                                shazam_result['release_date'] = year_match.group(0)
                        elif 'label' in title_key and text:
                            shazam_result['label'] = text
                
            # Extract genres from hub (if available)
            hub = track.get('hub', {})
            if hub:
                # Some tracks have genre info in hub
                actions = hub.get('actions', [])
                for action in actions:
                    if action.get('type') == 'applemusicplay':
                        # Apple Music integration may have genre
                        pass
            
            # Extract genre from URL hints (Shazam encodes genre in URLs)
            share_url = track.get('share', {}).get('subject', '')
            if 'genre=' in share_url:
                import re
                genre_match = re.search(r'genre=([^&]+)', share_url)
                if genre_match:
                    genre = genre_match.group(1).replace('+', ' ').replace('%20', ' ')
                    shazam_result['genres'] = [genre]
            
            # Get Apple Music ID if available (useful for further metadata lookup)
            apple_music = track.get('hub', {}).get('providers', [])
            for provider in apple_music:
                if provider.get('type') == 'APPLEMUSIC':
                    actions = provider.get('actions', [])
                    for action in actions:
                        if 'uri' in action:
                            uri = action['uri']
                            # Extract Apple Music ID from URI
                            if 'song/' in uri:
                                am_id = uri.split('song/')[-1].split('?')[0]
                                shazam_result['apple_music_id'] = am_id
            
            if _SHAZAM_DEBUG:
                print(f"  [Shazam] Extracted metadata: {shazam_result}")
            
            return shazam_result
        
        if _SHAZAM_DEBUG or _SHAZAM_TRACE or _TRACE_ALL:
            print(f"  [Shazam] → miss")

        return None

    except Exception as e:
        # Clean up temp file
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass
        
        error_msg = str(e)
        error_lower = error_msg.lower()
        
        if "timeout" in error_lower:
            print(f"  [Shazam] ⚠️  Request timed out")
            print(f"            Check your internet connection")
        elif "connection" in error_lower or "network" in error_lower:
            print(f"  [Shazam] ⚠️  Network error")
            print(f"            Unable to reach Shazam servers")
        else:
            print(f"  [Shazam] Error: {error_msg}")
            if _SHAZAM_DEBUG:
                import traceback
                print(f"  [Shazam] Traceback:")
                traceback.print_exc()
        
        return None


# =============================================================================
# MUSICBRAINZ ENHANCED METADATA
# =============================================================================

def get_enhanced_metadata(artist, title, recording_id=None):
    """Get enhanced metadata from MusicBrainz including genres"""
    if not _MB_AVAILABLE:
        return {}
    
    try:
        enhanced = {}
        all_genres = []
        artist_id = None
        
        # If we have a recording ID, use it directly
        if recording_id:
            try:
                recording = _musicbrainzngs.get_recording_by_id(
                    recording_id, 
                    includes=['artists', 'releases', 'tags', 'isrcs']
                )
                rec = recording.get('recording', {})
                
                if 'tag-list' in rec:
                    rec_genres = [tag['name'] for tag in rec['tag-list'] if tag.get('count', 0) >= 0]
                    all_genres.extend(rec_genres)
                
                if 'isrc-list' in rec:
                    enhanced['isrc'] = rec['isrc-list'][0] if rec['isrc-list'] else None
                
                if 'artist-credit' in rec:
                    for credit in rec['artist-credit']:
                        if isinstance(credit, dict) and 'artist' in credit:
                            artist_id = credit['artist'].get('id')
                            break
                
                if 'release-list' in rec:
                    release = rec['release-list'][0]
                    enhanced['album'] = release.get('title', '')
                    enhanced['release_date'] = release.get('date', '')
                    
                    try:
                        release_id = release.get('id')
                        if release_id:
                            full_release = _musicbrainzngs.get_release_by_id(
                                release_id, 
                                includes=['labels', 'tags', 'release-groups']
                            )
                            rel = full_release.get('release', {})
                            
                            if 'label-info-list' in rel:
                                label_info = rel['label-info-list']
                                if label_info and 'label' in label_info[0]:
                                    enhanced['label'] = label_info[0]['label'].get('name', '')
                            
                            if 'tag-list' in rel:
                                rel_genres = [tag['name'] for tag in rel['tag-list']]
                                all_genres.extend(rel_genres)
                            
                            if 'release-group' in rel:
                                rg = rel['release-group']
                                if 'tag-list' in rg:
                                    rg_genres = [tag['name'] for tag in rg['tag-list']]
                                    all_genres.extend(rg_genres)
                    except:
                        pass
                        
            except:
                pass
        
        # Search by artist and title if no recording ID
        if not recording_id or not enhanced:
            try:
                results = _musicbrainzngs.search_recordings(
                    artist=artist,
                    recording=title,
                    limit=1
                )
                
                if results.get('recording-list'):
                    rec = results['recording-list'][0]
                    rec_id = rec.get('id')
                    
                    if 'tag-list' in rec:
                        rec_genres = [tag['name'] for tag in rec['tag-list']]
                        all_genres.extend(rec_genres)
                    
                    if 'artist-credit' in rec:
                        for credit in rec['artist-credit']:
                            if isinstance(credit, dict) and 'artist' in credit:
                                artist_id = credit['artist'].get('id')
                                break
                    
                    if rec_id and not enhanced.get('album'):
                        try:
                            full_rec = _musicbrainzngs.get_recording_by_id(
                                rec_id,
                                includes=['releases', 'tags']
                            )
                            rec_data = full_rec.get('recording', {})
                            
                            if 'tag-list' in rec_data:
                                full_genres = [tag['name'] for tag in rec_data['tag-list']]
                                all_genres.extend(full_genres)
                            
                            if 'release-list' in rec_data and not enhanced.get('album'):
                                release = rec_data['release-list'][0]
                                enhanced['album'] = release.get('title', '')
                                enhanced['release_date'] = release.get('date', '')
                        except:
                            pass
                    
                    if 'release-list' in rec and not enhanced.get('album'):
                        release = rec['release-list'][0]
                        enhanced['album'] = release.get('title', '')
                        enhanced['release_date'] = release.get('date', '')
                        
                        try:
                            release_id = release.get('id')
                            if release_id and not enhanced.get('label'):
                                full_release = _musicbrainzngs.get_release_by_id(release_id, includes=['labels', 'tags'])
                                rel = full_release.get('release', {})
                                if 'label-info-list' in rel:
                                    label_info = rel['label-info-list']
                                    if label_info and 'label' in label_info[0]:
                                        enhanced['label'] = label_info[0]['label'].get('name', '')
                                if 'tag-list' in rel:
                                    rel_genres = [tag['name'] for tag in rel['tag-list']]
                                    all_genres.extend(rel_genres)
                        except:
                            pass
            except:
                pass
        
        # Get artist tags
        if artist_id:
            try:
                artist_data = _musicbrainzngs.get_artist_by_id(artist_id, includes=['tags'])
                if 'tag-list' in artist_data.get('artist', {}):
                    artist_genres = [tag['name'] for tag in artist_data['artist']['tag-list']]
                    all_genres.extend(artist_genres)
            except:
                pass
        
        # Search for artist directly if no genres yet
        if not all_genres and artist:
            try:
                artist_results = _musicbrainzngs.search_artists(artist=artist, limit=1)
                if artist_results.get('artist-list'):
                    found_artist = artist_results['artist-list'][0]
                    aid = found_artist.get('id')
                    if aid:
                        artist_data = _musicbrainzngs.get_artist_by_id(aid, includes=['tags'])
                        if 'tag-list' in artist_data.get('artist', {}):
                            artist_genres = [tag['name'] for tag in artist_data['artist']['tag-list']]
                            all_genres.extend(artist_genres)
            except:
                pass
        
        # Deduplicate genres
        if all_genres:
            seen = set()
            unique_genres = []
            for g in all_genres:
                g_lower = g.lower().strip()
                if g_lower not in seen and len(g_lower) > 1:
                    seen.add(g_lower)
                    unique_genres.append(g.strip())
            enhanced['genres'] = unique_genres[:5]
        
        return enhanced
    except Exception:
        return {}

# =============================================================================
# MUSICBRAINZ TEXT SEARCH (no account required)
# =============================================================================

_DEBUG_MB = os.environ.get("MIXSPLITR_DEBUG_MB", "").strip().lower() in ("1", "true", "yes", "y", "on")
_mb_lock = threading.Lock()
_mb_last_call = 0.0

def _dbg_mb(msg):
    if _DEBUG_MB:
        print(f"  ℹ️  {msg}")

def _mb_rate_limit(min_interval=1.1):
    """Polite MusicBrainz throttling (~1 request/sec)."""
    global _mb_last_call
    with _mb_lock:
        now = time.time()
        wait = (_mb_last_call + min_interval) - now
        if wait > 0:
            time.sleep(wait)
        _mb_last_call = time.time()

def musicbrainz_search_recordings(query=None, artist=None, title=None, limit=5):
    """Search MusicBrainz recordings by free-text or artist/title."""
    if not _MB_AVAILABLE:
        return []
    
    try:
        _mb_rate_limit()
        
        if query:
            _dbg_mb(f"[MB] Searching with free-text query: {query}")
            results = _musicbrainzngs.search_recordings(query=query, limit=limit)
        else:
            _dbg_mb(f"[MB] Searching recordings: artist={artist}, title={title}")
            results = _musicbrainzngs.search_recordings(artist=artist, recording=title, limit=limit)
        
        recordings = results.get('recording-list', [])
        _dbg_mb(f"[MB] Found {len(recordings)} recording(s)")
        
        processed = []
        for rec in recordings:
            artist_name = rec.get('artist-credit', [{}])[0].get('artist', {}).get('name', 'Unknown Artist')
            if isinstance(rec.get('artist-credit', []), list) and len(rec.get('artist-credit', [])) > 0:
                credit_obj = rec['artist-credit'][0]
                if isinstance(credit_obj, dict) and 'artist' in credit_obj:
                    artist_name = credit_obj['artist'].get('name', 'Unknown Artist')
            
            album_name = 'Unknown Album'
            if 'release-list' in rec and len(rec['release-list']) > 0:
                album_name = rec['release-list'][0].get('title', 'Unknown Album')
            
            processed.append({
                'artist': artist_name,
                'title': rec.get('title', 'Unknown Title'),
                'album': album_name,
                'recording_id': rec.get('id'),
                'score': int(rec.get('ext:score', '0'))
            })
        
        return processed
    except Exception as e:
        _dbg_mb(f"[MB] Search error: {e}")
        return []


def musicbrainz_search_releases(query=None, limit=10):
    """Search MusicBrainz releases (albums) by name.

    Args:
        query: Album name to search
        limit: Max number of results (default 10)

    Returns:
        List of dicts with keys: release_id, title, date, artists, score
    """
    if not _MB_AVAILABLE:
        return []

    try:
        _mb_rate_limit()
        _dbg_mb(f"[MB] Searching releases: query={query}")
        results = _musicbrainzngs.search_releases(query=query, limit=limit)

        releases = results.get('release-list', [])
        _dbg_mb(f"[MB] Found {len(releases)} release(s)")

        processed = []
        for rel in releases:
            artists = []
            for credit in rel.get('artist-credit', []):
                if isinstance(credit, dict) and 'artist' in credit:
                    artists.append(credit['artist'].get('name', 'Unknown'))

            processed.append({
                'release_id': rel.get('id', ''),
                'title': rel.get('title', 'Unknown Album'),
                'date': rel.get('date', ''),
                'artists': artists if artists else ['Unknown Artist'],
                'score': int(rel.get('ext:score', '0')),
            })

        return processed
    except Exception as e:
        _dbg_mb(f"[MB] Release search error: {e}")
        return []


def musicbrainz_get_release_tracklist(release_id):
    """Fetch full tracklist for a release (album) by release ID.

    Args:
        release_id: MusicBrainz release UUID

    Returns:
        Dict with keys: title, tracks (list of {position, title, artist, duration_ms, recording_id})
        Returns None on failure.
    """
    if not _MB_AVAILABLE or not release_id:
        return None

    try:
        _mb_rate_limit()
        _dbg_mb(f"[MB] Fetching release tracklist: {release_id}")
        result = _musicbrainzngs.get_release_by_id(
            release_id, includes=['recordings', 'artist-credits']
        )

        release = result.get('release', {})
        album_title = release.get('title', 'Unknown Album')

        tracks = []
        for medium in release.get('medium-list', []):
            for trk in medium.get('track-list', []):
                recording = trk.get('recording', {})

                # Extract artist from recording credits
                artist_name = 'Unknown Artist'
                for credit in recording.get('artist-credit', []):
                    if isinstance(credit, dict) and 'artist' in credit:
                        artist_name = credit['artist'].get('name', 'Unknown Artist')
                        break

                duration_ms = 0
                if trk.get('length'):
                    try:
                        duration_ms = int(trk['length'])
                    except (ValueError, TypeError):
                        pass
                elif recording.get('length'):
                    try:
                        duration_ms = int(recording['length'])
                    except (ValueError, TypeError):
                        pass

                tracks.append({
                    'position': int(trk.get('position', 0)),
                    'title': recording.get('title', trk.get('title', 'Unknown')),
                    'artist': artist_name,
                    'duration_ms': duration_ms,
                    'recording_id': recording.get('id', ''),
                })

        return {'title': album_title, 'tracks': tracks}
    except Exception as e:
        _dbg_mb(f"[MB] Release tracklist error: {e}")
        return None


def group_recordings_by_album(results):
    """Group flat recording search results by album.

    Args:
        results: List of dicts from musicbrainz_search_recordings()

    Returns:
        OrderedDict keyed by album name, each value is {album, tracks: [...]}.
        Preserves order of first appearance (highest score first).
    """
    from collections import OrderedDict
    grouped = OrderedDict()

    for r in results:
        album = r.get('album', 'Unknown Album')
        if album not in grouped:
            grouped[album] = {
                'album': album,
                'tracks': [],
            }
        grouped[album]['tracks'].append(r)

    return grouped


# =============================================================================
# RESULT MERGING
# =============================================================================

def strings_match(s1, s2):
    """Check if two strings match (case/punctuation insensitive)"""
    if not s1 or not s2:
        return False
    
    def normalize(s):
        return re.sub(r'[^a-z0-9]', '', str(s).lower())
    
    return normalize(s1) == normalize(s2)


def merge_identification_results(acr_result, mb_result, mb_enhanced, external_meta=None):
    """Merge ACRCloud, MusicBrainz, iTunes, Deezer, Last.fm results"""
    merged = {
        'artist': {'value': None, 'source': None},
        'title': {'value': None, 'source': None},
        'album': {'value': None, 'source': None},
        'label': {'value': None, 'source': None},
        'genres': {'value': [], 'source': None},
        'release_date': {'value': None, 'source': None},
        'isrc': {'value': None, 'source': None},
        'bpm': {'value': None, 'source': None},
        'confidence': 0.0,
        'agreement': 'none',
        'sources_used': [],
        'sources_checked': 0
    }
    
    # Extract data from all sources
    acr_artist = acr_result.get('artist') if acr_result else None
    acr_title = acr_result.get('title') if acr_result else None
    acr_album = acr_result.get('album') if acr_result else None
    
    mb_artist = mb_result.get('artist') if mb_result else None
    mb_title = mb_result.get('title') if mb_result else None
    mb_album = mb_enhanced.get('album') if mb_enhanced else None
    mb_label = mb_enhanced.get('label') if mb_enhanced else None
    mb_genres = mb_enhanced.get('genres', []) if mb_enhanced else []
    mb_date = mb_enhanced.get('release_date') if mb_enhanced else None
    mb_isrc = mb_enhanced.get('isrc') if mb_enhanced else None
    
    # External sources
    external_meta = external_meta or {}
    itunes = external_meta.get('itunes') or {}
    deezer = external_meta.get('deezer') or {}
    lastfm = external_meta.get('lastfm') or {}
    
    itunes_genre = itunes.get('genre')
    itunes_year = itunes.get('year')
    itunes_album = itunes.get('album')
    
    deezer_genre = deezer.get('genre')
    deezer_year = deezer.get('year')
    deezer_album = deezer.get('album')
    deezer_bpm = deezer.get('bpm')
    
    lastfm_tags = lastfm.get('tags', [])
    
    # Track sources
    has_acr = bool(acr_artist and acr_title)
    has_mb = bool(mb_artist and mb_title)
    has_itunes = bool(itunes)
    has_deezer = bool(deezer)
    has_lastfm = bool(lastfm)
    
    sources_count = int(has_acr) + int(has_mb) + int(has_itunes) + int(has_deezer) + int(has_lastfm)
    merged['sources_checked'] = sources_count
    
    if has_acr:
        merged['sources_used'].append('ACRCloud')
    if has_mb:
        merged['sources_used'].append('MusicBrainz')
    if has_itunes:
        merged['sources_used'].append('iTunes')
    if has_deezer:
        merged['sources_used'].append('Deezer')
    if has_lastfm:
        merged['sources_used'].append('Last.fm')
    
    # Calculate agreement and confidence
    if has_acr and has_mb:
        artist_match = strings_match(acr_artist, mb_artist)
        title_match = strings_match(acr_title, mb_title)
        
        if artist_match and title_match:
            merged['agreement'] = 'full'
            merged['confidence'] = 0.90 + (0.025 * (sources_count - 2))
        elif artist_match or title_match:
            merged['agreement'] = 'partial'
            merged['confidence'] = 0.75 + (0.025 * (sources_count - 2))
        else:
            merged['agreement'] = 'conflict'
            merged['confidence'] = 0.60
    elif has_acr:
        merged['agreement'] = 'acr_only'
        merged['confidence'] = 0.75 + (0.05 * (sources_count - 1))
    elif has_mb:
        merged['agreement'] = 'mb_only'
        merged['confidence'] = 0.70 + (0.05 * (sources_count - 1))
    else:
        merged['agreement'] = 'none'
        merged['confidence'] = 0.0
        return merged
    
    merged['confidence'] = min(0.99, merged['confidence'])
    
    # Merge artist - prefer ACRCloud
    if acr_artist:
        merged['artist'] = {'value': acr_artist, 'source': 'ACRCloud'}
    elif mb_artist:
        merged['artist'] = {'value': mb_artist, 'source': 'MusicBrainz'}
    
    # Merge title - prefer ACRCloud
    if acr_title:
        merged['title'] = {'value': acr_title, 'source': 'ACRCloud'}
    elif mb_title:
        merged['title'] = {'value': mb_title, 'source': 'MusicBrainz'}
    
    # Merge album
    if mb_album and mb_album != 'Unknown Album':
        merged['album'] = {'value': mb_album, 'source': 'MusicBrainz'}
    elif acr_album and acr_album != 'Unknown Album':
        merged['album'] = {'value': acr_album, 'source': 'ACRCloud'}
    elif itunes_album:
        merged['album'] = {'value': itunes_album, 'source': 'iTunes'}
    elif deezer_album:
        merged['album'] = {'value': deezer_album, 'source': 'Deezer'}
    else:
        merged['album'] = {'value': 'Unknown Album', 'source': None}
    
    # Label
    if mb_label:
        merged['label'] = {'value': mb_label, 'source': 'MusicBrainz'}
    
    # Genres - collect from all sources
    all_genres = []
    genre_source = None
    
    if mb_genres:
        all_genres.extend(mb_genres)
        genre_source = 'MusicBrainz'
    
    if itunes_genre and itunes_genre.lower() not in [g.lower() for g in all_genres]:
        all_genres.append(itunes_genre)
        if not genre_source:
            genre_source = 'iTunes'
    
    if deezer_genre and deezer_genre.lower() not in [g.lower() for g in all_genres]:
        all_genres.append(deezer_genre)
        if not genre_source:
            genre_source = 'Deezer'
    
    for tag in lastfm_tags:
        if tag and tag.lower() not in [g.lower() for g in all_genres]:
            all_genres.append(tag)
            if not genre_source:
                genre_source = 'Last.fm'
    
    if all_genres:
        seen = set()
        unique = []
        for g in all_genres:
            if g and str(g).lower() not in seen:
                seen.add(str(g).lower())
                unique.append(str(g))
        
        source_str = str(genre_source) if genre_source else 'Unknown'
        if len(merged['sources_used']) > 1 and len(unique) > 1:
            source_str += '+'
        
        merged['genres'] = {'value': unique[:5], 'source': source_str}
    
    # Release date
    if mb_date:
        merged['release_date'] = {'value': mb_date, 'source': 'MusicBrainz'}
    elif itunes_year:
        merged['release_date'] = {'value': itunes_year, 'source': 'iTunes'}
    elif deezer_year:
        merged['release_date'] = {'value': deezer_year, 'source': 'Deezer'}
    
    # ISRC
    if mb_isrc:
        merged['isrc'] = {'value': mb_isrc, 'source': 'MusicBrainz'}
    
    # BPM - prefer Deezer
    if deezer_bpm is not None:
        try:
            bpm_val = int(deezer_bpm)
            if bpm_val > 0:
                merged['bpm'] = {'value': bpm_val, 'source': 'Deezer'}
        except (ValueError, TypeError):
            pass
    
    # Fallback to local BPM
    if not merged['bpm']['value']:
        local_bpm = external_meta.get('local_bpm') if external_meta else None
        if local_bpm and local_bpm.get('bpm'):
            merged['bpm'] = {
                'value': local_bpm['bpm'],
                'source': 'librosa',
                'confidence': local_bpm.get('confidence', 0.7)
            }
    
    return merged


# =============================================================================
# ARTWORK
# =============================================================================

def batch_download_artwork(artwork_urls):
    """Download multiple artworks in parallel"""
    artwork_cache = {}
    
    def download_single(url):
        if not url or "{w}x{h}" in url:
            url = url.replace("{w}x{h}", "600x600") if url else None
        if not url:
            return None, None
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return url, response.content
        except:
            pass
        return url, None
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(download_single, url): url for url in artwork_urls if url}
        for future in as_completed(futures):
            url, img_data = future.result()
            if img_data:
                artwork_cache[url] = img_data
    
    return artwork_cache


def identify_dual_mode(audio_chunk, acrcloud_recognizer=None, acoustid_key=None):
    """
    Run both ACRCloud and AcoustID, return best result based on confidence.
    
    Args:
        audio_chunk: Audio file path or data
        acrcloud_recognizer: ACRCloud recognizer instance
        acoustid_key: AcoustID API key
    
    Returns:
        Best result dict with 'id_method' = 'dual_best'
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results = {}
    
    def run_acrcloud():
        try:
            if acrcloud_recognizer:
                result = acrcloud_recognizer.recognize_by_file(audio_chunk, 0)
                # Parse ACRCloud response (simplified)
                import json
                data = json.loads(result)
                if data.get('status', {}).get('code') == 0:
                    music = data['metadata']['music'][0]
                    return {
                        'title': music.get('title'),
                        'artist': music['artists'][0]['name'] if music.get('artists') else None,
                        'album': music.get('album', {}).get('name'),
                        'confidence': data['status'].get('score', 0),
                        'id_method': 'acrcloud',
                        'success': True
                    }
        except:
            pass
        return {'success': False, 'confidence': 0, 'id_method': 'acrcloud'}
    
    def run_acoustid():
        try:
            if acoustid_key:
                result = identify_with_acoustid(audio_chunk)
                if result.get('title'):
                    result['id_method'] = 'acoustid'
                    result['success'] = True
                    # AcoustID returns score 0-1, convert to percentage
                    if 'confidence' not in result:
                        result['confidence'] = 80  # Default if no score
                    return result
        except:
            pass
        return {'success': False, 'confidence': 0, 'id_method': 'acoustid'}
    
    # Run both in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_acr = executor.submit(run_acrcloud)
        future_aid = executor.submit(run_acoustid)
        
        acr_result = future_acr.result()
        aid_result = future_aid.result()
    
    results['acrcloud'] = acr_result
    results['acoustid'] = aid_result
    
    # Compare and pick best
    acr_conf = acr_result.get('confidence', 0)
    aid_conf = aid_result.get('confidence', 0)
    
    if not acr_result.get('success') and not aid_result.get('success'):
        # Both failed
        return {
            'title': None,
            'artist': None,
            'confidence': 0,
            'id_method': 'dual_both_failed',
            'comparison': f"ACR: failed, AID: failed"
        }
    
    # Pick winner
    if acr_conf >= aid_conf and acr_result.get('success'):
        winner = acr_result.copy()
        winner['id_method'] = 'dual_best_acrcloud'
        winner['comparison'] = f"ACRCloud {acr_conf}% > AcoustID {aid_conf}%"
        winner['runner_up'] = aid_result
    elif aid_result.get('success'):
        winner = aid_result.copy()
        winner['id_method'] = 'dual_best_acoustid'
        winner['comparison'] = f"AcoustID {aid_conf}% > ACRCloud {acr_conf}%"
        winner['runner_up'] = acr_result
    else:
        # Use whichever succeeded
        winner = acr_result if acr_result.get('success') else aid_result
        winner['id_method'] = 'dual_fallback'
        winner['comparison'] = f"One method succeeded"
    
    return winner
