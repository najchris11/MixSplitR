"""
mixsplitr_tagging.py - Audio file tagging for MixSplitR

Contains:
- Artist normalization (feat/collab → title, single artist folder)
- FLAC metadata embedding
- ALAC conversion and tagging (macOS compatibility)
- Multi-format support (MP3, OGG, OPUS, WAV, AIFF, etc.)
- File organization (artist folders)
"""

import os
import re
import shutil
import platform
import requests


# ─── Artist normalization ────────────────────────────────────────────────────
# Splits collaboration credits into a primary artist and a featured string
# that gets appended to the title, so all tracks by the same primary artist
# end up in one folder instead of "Artist & Guest1", "Artist feat. Guest2", etc.

# Patterns that indicate a featured/guest credit (case-insensitive)
_FEAT_PATTERNS = [
    r'\s+feat\.?\s+',     # feat. / feat
    r'\s+ft\.?\s+',       # ft. / ft
    r'\s+featuring\s+',   # featuring
    r'\s+with\s+',        # with
    r'\s+vs\.?\s+',       # vs. / vs
    r'\s+x\s+',           # x (common in electronic: "Artist x Artist")
]

# Separator characters that indicate multiple co-artists
_COLLAB_SEPARATORS = [
    r'\s*&\s+',           # & (with space after, to avoid matching "Simon & Garfunkel"-style names)
    r'\s*,\s+',           # , followed by space
]

def normalize_artist(artist, title):
    """Split collaboration credits: return (primary_artist, updated_title).

    When enabled, transforms:
        artist="Catching Flies & Hot Chip"  title="Sunne"
    into:
        ("Catching Flies", "Sunne (feat. Hot Chip)")

    Also handles stacked credits:
        artist="Catching Flies, Erick The Architect, Lord Apex"  title="Dive"
    into:
        ("Catching Flies", "Dive (feat. Erick The Architect, Lord Apex)")

    If 'feat.' is already in the title, the additional artists are appended
    to the existing parenthetical rather than creating a duplicate.
    """
    if not artist:
        return artist, title

    # ── Step 1: Check for explicit feat/with/vs patterns first ────────────
    for pat in _FEAT_PATTERNS:
        match = re.split(pat, artist, maxsplit=1, flags=re.IGNORECASE)
        if len(match) == 2:
            primary = match[0].strip()
            featured = match[1].strip()
            if primary and featured:
                title = _append_featured(title, featured)
                return primary, title

    # ── Step 2: Check for separator-based collaborations (& , ) ───────────
    for sep in _COLLAB_SEPARATORS:
        parts = re.split(sep, artist)
        if len(parts) >= 2:
            primary = parts[0].strip()
            featured = ", ".join(p.strip() for p in parts[1:] if p.strip())
            if primary and featured:
                title = _append_featured(title, featured)
                return primary, title

    # ── No collaboration detected — return as-is ─────────────────────────
    return artist, title


def _append_featured(title, featured):
    """Append featured artist(s) to title, merging with existing feat. if present."""
    # Check if title already has a feat/ft parenthetical
    existing = re.search(r'\((?:feat\.?|ft\.?|featuring)\s+([^)]+)\)', title, re.IGNORECASE)
    if existing:
        # Merge: "Song (feat. A)" + "B" → "Song (feat. A, B)"
        old_feat = existing.group(0)
        merged = old_feat[:-1] + ", " + featured + ")"
        return title.replace(old_feat, merged)

    return f"{title} (feat. {featured})"


def _maybe_normalize(artist, title):
    """Apply normalize_artist only if the config toggle is enabled."""
    try:
        from .mixsplitr_core import get_config
        config = get_config()
        if config.get('normalize_artists', True):
            return normalize_artist(artist, title)
    except Exception:
        pass
    return artist, title


def retag_file(filepath, artist, title):
    """Update only the artist and title tags in an existing audio file.

    Detects format from file extension and uses the appropriate mutagen
    class.  Does NOT re-encode audio — this is a metadata-only update.
    """
    ext = os.path.splitext(filepath)[1].lower()

    try:
        if ext == '.flac':
            from mutagen.flac import FLAC
            audio = FLAC(filepath)
            audio["artist"] = artist
            audio["title"] = title
            audio.save()

        elif ext in ('.m4a', '.mp4', '.aac'):
            from mutagen.mp4 import MP4
            mp4 = MP4(filepath)
            mp4["\xa9ART"] = [artist]
            mp4["\xa9nam"] = [title]
            mp4.save()

        elif ext == '.mp3':
            from mutagen.id3 import ID3, TIT2, TPE1
            try:
                tags = ID3(filepath)
            except Exception:
                tags = ID3()
            tags.add(TPE1(encoding=3, text=artist))
            tags.add(TIT2(encoding=3, text=title))
            tags.save(filepath)

        elif ext == '.ogg':
            from mutagen.oggvorbis import OggVorbis
            ogg = OggVorbis(filepath)
            ogg["artist"] = [artist]
            ogg["title"] = [title]
            ogg.save()

        elif ext == '.opus':
            from mutagen.oggopus import OggOpus
            opus = OggOpus(filepath)
            opus["artist"] = [artist]
            opus["title"] = [title]
            opus.save()

        elif ext in ('.wav', '.aiff'):
            # WAV/AIFF have limited ID3 support — best-effort, don't crash
            try:
                from mutagen.id3 import ID3, TIT2, TPE1
                tags = ID3(filepath)
                tags.add(TPE1(encoding=3, text=artist))
                tags.add(TIT2(encoding=3, text=title))
                tags.save(filepath)
            except Exception:
                pass  # silently skip — these formats often lack tag support

    except Exception as e:
        raise RuntimeError(f"retag failed for {os.path.basename(filepath)}: {e}")


# Audio format definitions
AUDIO_FORMATS = {
    'flac': {'name': 'FLAC', 'ext': '.flac', 'lossless': True, 'codec': None, 'mutagen': 'flac'},
    'alac': {'name': 'ALAC (M4A)', 'ext': '.m4a', 'lossless': True, 'codec': 'alac', 'mutagen': 'mp4'},
    'wav': {'name': 'WAV', 'ext': '.wav', 'lossless': True, 'codec': 'pcm_s16le', 'mutagen': 'wave'},
    'aiff': {'name': 'AIFF', 'ext': '.aiff', 'lossless': True, 'codec': 'pcm_s16be', 'mutagen': 'aiff'},
    'mp3_320': {'name': 'MP3 320kbps', 'ext': '.mp3', 'lossless': False, 'codec': 'libmp3lame', 'bitrate': '320k', 'mutagen': 'mp3'},
    'mp3_256': {'name': 'MP3 256kbps', 'ext': '.mp3', 'lossless': False, 'codec': 'libmp3lame', 'bitrate': '256k', 'mutagen': 'mp3'},
    'mp3_192': {'name': 'MP3 192kbps', 'ext': '.mp3', 'lossless': False, 'codec': 'libmp3lame', 'bitrate': '192k', 'mutagen': 'mp3'},
    'aac_256': {'name': 'AAC 256kbps', 'ext': '.m4a', 'lossless': False, 'codec': 'aac', 'bitrate': '256k', 'mutagen': 'mp4'},
    'ogg_500': {'name': 'OGG Vorbis Q10 (~500kbps)', 'ext': '.ogg', 'lossless': False, 'codec': 'libvorbis', 'quality': '10', 'mutagen': 'ogg'},
    'ogg_320': {'name': 'OGG Vorbis Q8 (~320kbps)', 'ext': '.ogg', 'lossless': False, 'codec': 'libvorbis', 'quality': '8', 'mutagen': 'ogg'},
    'opus': {'name': 'OPUS 256kbps', 'ext': '.opus', 'lossless': False, 'codec': 'libopus', 'bitrate': '256k', 'mutagen': 'opus'},
}


def embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache=None, enhanced_metadata=None):
    """Embed metadata in FLAC file and move to artist folder"""
    from mutagen.flac import FLAC, Picture

    # Apply artist normalization if enabled
    artist, title = _maybe_normalize(artist, title)

    try:
        audio = FLAC(file_path)
        audio["artist"], audio["title"], audio["album"] = artist, title, album
        
        # Add enhanced metadata if available
        if enhanced_metadata:
            if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                date = enhanced_metadata['release_date']
                year = date[:4] if len(date) >= 4 else date
                audio["date"] = year
            
            if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                audio["genre"] = ", ".join(enhanced_metadata['genres'])
            
            if 'label' in enhanced_metadata and enhanced_metadata['label']:
                audio["label"] = enhanced_metadata['label']
            
            if 'isrc' in enhanced_metadata and enhanced_metadata['isrc']:
                audio["isrc"] = enhanced_metadata['isrc']
            
            if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                audio["bpm"] = str(enhanced_metadata['bpm'])
        
                # Handle artwork (prefer embedded artwork if present)
        img_data = None

        # If the file already has embedded artwork, keep it and skip online fetching
        try:
            if getattr(audio, "pictures", None) and len(audio.pictures) > 0:
                img_data = audio.pictures[0].data
        except Exception:
            pass

        # Only fetch/download artwork if nothing is embedded already
        if img_data is None and cover_url:
            if "{w}x{h}" in cover_url:
                cover_url = cover_url.replace("{w}x{h}", "600x600")

            # Try cache first
            if artwork_cache and cover_url in artwork_cache:
                img_data = artwork_cache[cover_url]
            else:
                try:
                    img_res = requests.get(cover_url, timeout=10)
                    if img_res.status_code == 200:
                        img_data = img_res.content
                except Exception:
                    pass

            if img_data:
                pic = Picture()
                pic.data, pic.type, pic.mime = img_data, 3, u"image/jpeg"
                audio.add_picture(pic)

        audio.save()
        
        # Move file to artist folder
        safe_artist = artist.translate(str.maketrans('', '', '<>:"/\\|?*'))
        dest_dir = os.path.join(base_output_folder, safe_artist)
        os.makedirs(dest_dir, exist_ok=True)
        
        # On macOS, create folder.jpg for Finder compatibility
        if platform.system() == 'Darwin' and img_data:
            art_path = os.path.join(dest_dir, "folder.jpg")
            if not os.path.exists(art_path):
                with open(art_path, "wb") as f:
                    f.write(img_data)

        new_name = f"{artist} - {title}.flac".translate(str.maketrans('', '', '<>:"/\\|?*'))
        shutil.move(file_path, os.path.join(dest_dir, new_name))
    except Exception as e: 
        print(f"   [!] Tag Error: {e}")


def embed_and_sort_alac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache=None, enhanced_metadata=None):
    """Convert FLAC to ALAC and embed metadata for macOS compatibility"""
    from pydub import AudioSegment

    # Apply artist normalization if enabled
    artist, title = _maybe_normalize(artist, title)

    try:
        from mutagen.mp4 import MP4, MP4Cover
    except ImportError:
        print("   [!] mutagen.mp4 not available, falling back to FLAC")
        return embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)

    try:
        # Setup output path
        safe_artist = artist.translate(str.maketrans('', '', '<>:"/\\|?*'))
        dest_dir = os.path.join(base_output_folder, safe_artist)
        os.makedirs(dest_dir, exist_ok=True)
        
        new_name = f"{artist} - {title}.m4a".translate(str.maketrans('', '', '<>:"/\\|?*'))
        output_path = os.path.join(dest_dir, new_name)
        
        # Prefer embedded artwork from the source FLAC before conversion
        # (the conversion step does not preserve embedded pictures automatically)
        img_data = None
        try:
            from mutagen.flac import FLAC as _FLAC
            src_flac = _FLAC(file_path)
            if getattr(src_flac, "pictures", None) and len(src_flac.pictures) > 0:
                img_data = src_flac.pictures[0].data
        except Exception:
            img_data = None

        # Load and convert to ALAC
        audio = AudioSegment.from_file(file_path)
        audio.export(output_path, format="ipod", codec="alac")
        
        # Remove original FLAC
        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Add metadata using mutagen MP4
        mp4 = MP4(output_path)
        mp4["\xa9nam"] = title      # Title
        mp4["\xa9ART"] = artist     # Artist  
        mp4["\xa9alb"] = album      # Album
        
        # Add enhanced metadata
        if enhanced_metadata:
            if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                date = enhanced_metadata['release_date']
                year = date[:4] if len(date) >= 4 else date
                mp4["\xa9day"] = year
            
            if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                mp4["\xa9gen"] = ", ".join(enhanced_metadata['genres'])
            
            if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                try:
                    mp4["tmpo"] = [int(enhanced_metadata['bpm'])]
                except:
                    pass
        
                # Embed artwork (prefer embedded art from source FLAC)
        # If img_data was captured from the source FLAC, use it.
        # Otherwise, fall back to cached/downloaded cover_url.
        if img_data is None and cover_url:
            if "{w}x{h}" in cover_url:
                cover_url = cover_url.replace("{w}x{h}", "600x600")

            if artwork_cache and cover_url in artwork_cache:
                img_data = artwork_cache[cover_url]
            else:
                try:
                    img_res = requests.get(cover_url, timeout=10)
                    if img_res.status_code == 200:
                        img_data = img_res.content
                except Exception:
                    pass

        if img_data:
            mp4["covr"] = [MP4Cover(img_data, imageformat=MP4Cover.FORMAT_JPEG)]

        mp4.save()
        
    except Exception as e:
        print(f"   [!] ALAC Conversion Error: {e}")
        # Fall back to FLAC if ALAC fails
        embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)


def embed_and_sort_generic(file_path, artist, title, album, cover_url, base_output_folder, output_format='flac', artwork_cache=None, enhanced_metadata=None):
    """Generic function to convert and tag audio to any supported format"""
    from pydub import AudioSegment

    # Apply artist normalization if enabled (do it here so the FLAC/ALAC
    # fast-paths below receive already-normalized values)
    artist, title = _maybe_normalize(artist, title)

    # Handle legacy format names
    if output_format == 'alac':
        return embed_and_sort_alac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)
    if output_format == 'flac':
        return embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)

    # Get format info
    if output_format not in AUDIO_FORMATS:
        print(f"   [!] Unknown format {output_format}, falling back to FLAC")
        return embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)

    fmt = AUDIO_FORMATS[output_format]

    try:
        # Setup output path
        safe_artist = artist.translate(str.maketrans('', '', '<>:"/\\|?*'))
        dest_dir = os.path.join(base_output_folder, safe_artist)
        os.makedirs(dest_dir, exist_ok=True)

        new_name = f"{artist} - {title}{fmt['ext']}".translate(str.maketrans('', '', '<>:"/\\|?*'))
        output_path = os.path.join(dest_dir, new_name)

        # Extract embedded artwork from source FLAC
        img_data = None
        try:
            from mutagen.flac import FLAC as _FLAC
            src_flac = _FLAC(file_path)
            if getattr(src_flac, "pictures", None) and len(src_flac.pictures) > 0:
                img_data = src_flac.pictures[0].data
        except Exception:
            pass

        # Fetch artwork if not embedded
        if img_data is None and cover_url:
            if "{w}x{h}" in cover_url:
                cover_url = cover_url.replace("{w}x{h}", "600x600")
            if artwork_cache and cover_url in artwork_cache:
                img_data = artwork_cache[cover_url]
            else:
                try:
                    img_res = requests.get(cover_url, timeout=10)
                    if img_res.status_code == 200:
                        img_data = img_res.content
                except Exception:
                    pass

        # Load audio
        audio = AudioSegment.from_file(file_path)

        # Export with format-specific parameters
        export_params = {}
        if fmt['codec']:
            export_params['codec'] = fmt['codec']
        if 'bitrate' in fmt:
            export_params['bitrate'] = fmt['bitrate']
        if 'quality' in fmt:
            export_params['parameters'] = ['-q:a', fmt['quality']]

        # Determine pydub format
        pydub_format = output_format.split('_')[0]  # mp3_320 -> mp3
        if output_format == 'alac':
            pydub_format = 'ipod'

        audio.export(output_path, format=pydub_format, **export_params)

        # Remove original FLAC
        if os.path.exists(file_path):
            os.remove(file_path)

        # Tag the file using mutagen
        mutagen_type = fmt['mutagen']

        if mutagen_type == 'mp3':
            from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, TCON, APIC, TBPM
            try:
                audio_file = ID3(output_path)
            except:
                from mutagen.id3 import ID3NoHeaderError
                audio_file = ID3()

            audio_file.add(TIT2(encoding=3, text=title))
            audio_file.add(TPE1(encoding=3, text=artist))
            audio_file.add(TALB(encoding=3, text=album))

            if enhanced_metadata:
                if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                    year = enhanced_metadata['release_date'][:4]
                    audio_file.add(TDRC(encoding=3, text=year))
                if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                    audio_file.add(TCON(encoding=3, text=", ".join(enhanced_metadata['genres'])))
                if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                    audio_file.add(TBPM(encoding=3, text=str(enhanced_metadata['bpm'])))

            if img_data:
                audio_file.add(APIC(encoding=3, mime='image/jpeg', type=3, desc='Cover', data=img_data))

            audio_file.save(output_path)

        elif mutagen_type == 'mp4':
            from mutagen.mp4 import MP4, MP4Cover
            mp4 = MP4(output_path)
            mp4["\xa9nam"] = title
            mp4["\xa9ART"] = artist
            mp4["\xa9alb"] = album

            if enhanced_metadata:
                if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                    mp4["\xa9day"] = enhanced_metadata['release_date'][:4]
                if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                    mp4["\xa9gen"] = ", ".join(enhanced_metadata['genres'])
                if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                    try:
                        mp4["tmpo"] = [int(enhanced_metadata['bpm'])]
                    except:
                        pass

            if img_data:
                mp4["covr"] = [MP4Cover(img_data, imageformat=MP4Cover.FORMAT_JPEG)]

            mp4.save()

        elif mutagen_type == 'ogg':
            from mutagen.oggvorbis import OggVorbis
            from mutagen.flac import Picture
            import base64

            ogg = OggVorbis(output_path)
            ogg["title"] = title
            ogg["artist"] = artist
            ogg["album"] = album

            if enhanced_metadata:
                if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                    ogg["date"] = enhanced_metadata['release_date'][:4]
                if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                    ogg["genre"] = ", ".join(enhanced_metadata['genres'])
                if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                    ogg["bpm"] = str(enhanced_metadata['bpm'])

            if img_data:
                pic = Picture()
                pic.data = img_data
                pic.type = 3
                pic.mime = u"image/jpeg"
                pic.width = 600
                pic.height = 600
                ogg["metadata_block_picture"] = [base64.b64encode(pic.write()).decode('ascii')]

            ogg.save()

        elif mutagen_type == 'opus':
            from mutagen.oggopus import OggOpus
            from mutagen.flac import Picture
            import base64

            opus = OggOpus(output_path)
            opus["title"] = title
            opus["artist"] = artist
            opus["album"] = album

            if enhanced_metadata:
                if 'release_date' in enhanced_metadata and enhanced_metadata['release_date']:
                    opus["date"] = enhanced_metadata['release_date'][:4]
                if 'genres' in enhanced_metadata and enhanced_metadata['genres']:
                    opus["genre"] = ", ".join(enhanced_metadata['genres'])
                if 'bpm' in enhanced_metadata and enhanced_metadata['bpm']:
                    opus["bpm"] = str(enhanced_metadata['bpm'])

            if img_data:
                pic = Picture()
                pic.data = img_data
                pic.type = 3
                pic.mime = u"image/jpeg"
                pic.width = 600
                pic.height = 600
                opus["metadata_block_picture"] = [base64.b64encode(pic.write()).decode('ascii')]

            opus.save()

        elif mutagen_type in ['wave', 'aiff']:
            # WAV and AIFF have limited tagging support via ID3
            try:
                from mutagen.id3 import ID3, TIT2, TPE1, TALB
                audio_file = ID3(output_path)
                audio_file.add(TIT2(encoding=3, text=title))
                audio_file.add(TPE1(encoding=3, text=artist))
                audio_file.add(TALB(encoding=3, text=album))
                audio_file.save(output_path)
            except:
                pass  # WAV/AIFF tagging is optional

        # On macOS, create folder.jpg
        if platform.system() == 'Darwin' and img_data:
            art_path = os.path.join(dest_dir, "folder.jpg")
            if not os.path.exists(art_path):
                with open(art_path, "wb") as f:
                    f.write(img_data)

    except Exception as e:
        print(f"   [!] Format Error ({output_format}): {e}")
        # Fall back to FLAC
        embed_and_sort_flac(file_path, artist, title, album, cover_url, base_output_folder, artwork_cache, enhanced_metadata)
