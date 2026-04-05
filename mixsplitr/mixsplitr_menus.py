"""
MixSplitR Interactive Menus - prompt_toolkit implementation
Replaces print/input() loops with arrow-key navigation
"""

import os
import sys
import json
import glob
import shlex
import webbrowser
import shutil

from .mixsplitr_menu import (
    MenuItem, MenuResult, select_menu, confirm_dialog,
    input_dialog, wait_for_enter, clear_screen, PROMPT_TOOLKIT_AVAILABLE
)

from .mixsplitr_core import (
    Style, AUDIO_EXTENSIONS_GLOB, AUDIO_EXTENSIONS,
    get_config, save_config, get_config_path,
    MODE_ACRCLOUD, MODE_MB_ONLY, MODE_MANUAL, MODE_DUAL,
    validate_acrcloud_credentials
)

from .mixsplitr_identify import (
    is_shazam_available, setup_musicbrainz,
    set_acoustid_api_key, get_acoustid_api_key,
    check_chromaprint_available
)

from .mixsplitr_metadata import set_lastfm_key
from .mixsplitr_tagging import AUDIO_FORMATS

# Try to import version info
try:
    from .mixsplitr_core import CURRENT_VERSION, GITLAB_REPO
except ImportError:
    CURRENT_VERSION = "7.1"
    GITLAB_REPO = ""

ISSUES_URL = "https://github.com/chefkjd/MixSplitR/issues"
PROJECT_URL = "https://github.com/chefkjd/MixSplitR"

# Check ACRCloud availability
try:
    from acrcloud.recognizer import ACRCloudRecognizer
    ACRCLOUD_AVAILABLE = True
except ImportError:
    ACRCLOUD_AVAILABLE = False


def build_main_menu_items(audio_files: list, config: dict, has_cached_preview: bool = False) -> list:
    """Build main menu items based on current state"""
    items = []
    has_files = bool(audio_files)
    can_record = sys.platform in ("win32", "darwin")

    # Loaded-files flow: prioritize processing actions first.
    if has_files:
        items.append(MenuItem(
            "preview", "👁️", "Start Preview Session",
            "Analyze files and review results before export"
        ))
        items.append(MenuItem(
            "direct", "⚡", "Direct Mode (One-Click)",
                "Process everything immediately, save as you go"
            ))

    if has_files and has_cached_preview:
        items.append(MenuItem(
            "apply_cache", "📦", "Finish Unsaved Preview",
            "Export unsaved preview results without re-analyzing"
        ))

    if has_files:
        if can_record:
            items.append(MenuItem(
                "record", "🎙️", "Record Audio",
                "Record system audio"
            ))

        items.append(MenuItem(
            "load_files", "📁", "Load Different Directory",
            "Choose another folder or file set"
        ))
        items.append(MenuItem(
            "manifest", "📋", "Session History (Beta)",
            "Manage session history and rollback"
        ))
        items.append(MenuItem(
            "api_keys", "⚙️", "Settings",
            "Identification mode, directories, API keys, and preferences"
        ))
        items.append(MenuItem(
            "exit", "🚪", "Exit",
            "Close the program"
        ))
        return items

    # No-files flow: start with inputs to load or record.
    if can_record:
        items.append(MenuItem(
            "record", "🎙️", "Record Audio",
            "Record system audio"
        ))

    items.append(MenuItem(
        "load_files", "📁", "Load Audio Files",
        "Select a folder or audio file to process"
    ))

    if has_cached_preview:
        items.append(MenuItem(
            "apply_cache", "📦", "Finish Unsaved Preview",
            "Export unsaved preview results without re-analyzing"
        ))

    items.append(MenuItem(
        "manifest", "📋", "Session History (Beta)",
        "View, compare, import/export, restore, and apply session records. Use with caution."
    ))
    items.append(MenuItem(
        "api_keys", "⚙️", "Settings",
        "Identification mode, directories, API keys, and preferences"
    ))
    items.append(MenuItem(
        "exit", "🚪", "Exit",
        "Close the program"
    ))

    return items


def _build_main_menu_logo(config: dict, has_cached_preview: bool, mode_badge: str = "", update_info: dict = None):
    """Build the static logo for the main menu header.
    Returns (header_lines, fallback_header) where header_lines is for
    prompt_toolkit and fallback_header is ANSI-coded for basic terminals.
    """
    logo_segments = [
        ('    ███╗   ███╗██╗██╗  ██╗', '███████╗██████╗ ██╗     ██╗████████╗', '██████╗ '),
        ('    ████╗ ████║██║╚██╗██╔╝', '██╔════╝██╔══██╗██║     ██║╚══██╔══╝', '██╔══██╗'),
        ('    ██╔████╔██║██║ ╚███╔╝ ', '███████╗██████╔╝██║     ██║   ██║   ', '██████╔╝'),
        ('    ██║╚██╔╝██║██║ ██╔██╗ ', '╚════██║██╔═══╝ ██║     ██║   ██║   ', '██╔══██╗'),
        ('    ██║ ╚═╝ ██║██║██╔╝ ██╗', '███████║██║     ███████╗██║   ██║   ', '██║  ██║'),
        ('    ╚═╝     ╚═╝╚═╝╚═╝  ╚═╝', '╚══════╝╚═╝     ╚══════╝╚═╝   ╚═╝   ', '╚═╝  ╚═╝'),
    ]
    logo_width = max(len(mix + split + r) for mix, split, r in logo_segments)
    divider_core = '             ═══════════════════════════════════════'.strip()
    divider = (' ' * max(0, (logo_width - len(divider_core)) // 2)) + divider_core
    tagline_text = 'Mix Archival Tool'
    tagline = (' ' * max(0, (logo_width - len(tagline_text)) // 2)) + tagline_text
    project_prefix = ' ' * max(0, (logo_width - len(PROJECT_URL)) // 2)
    deep_scan = "ON" if config.get('deep_scan', False) else "OFF"
    album_search = "ON" if config.get('enable_album_search', True) else "OFF"
    preview_state = "UNSAVED" if has_cached_preview else "NONE"
    mode_value = (mode_badge or "").strip()
    if mode_value.startswith("[") and mode_value.endswith("]"):
        mode_value = mode_value[1:-1]
    if not mode_value:
        mode_value = "Unknown"
    mix_logo_color = Style.GRAY
    r_logo_color = '\033[38;5;196m'

    # prompt_toolkit FormattedText tuples
    header_lines = []
    for mix_part, split_part, r_part in logo_segments:
        header_lines.append(('class:logo_mix', mix_part))
        header_lines.append(('class:logo_split', split_part))
        header_lines.append(('class:logo_r', r_part + '\n'))
    header_lines.append(('class:logo_accent', divider + '\n'))
    header_lines.append(('class:logo_accent', tagline + '\n'))
    def _open_project_page(mouse_event):
        try:
            from prompt_toolkit.mouse_events import MouseEventType
            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                webbrowser.open(PROJECT_URL, new=2)
        except Exception:
            pass
    header_lines.append(('class:logo_dim', project_prefix))
    header_lines.append(('class:link', PROJECT_URL, _open_project_page))
    header_lines.append(('class:logo_dim', '\n'))
    if isinstance(update_info, dict):
        release_url = update_info.get("url") or f"https://github.com/{GITLAB_REPO}/releases"

        def _open_release_page(mouse_event):
            try:
                from prompt_toolkit.mouse_events import MouseEventType
                if mouse_event.event_type == MouseEventType.MOUSE_UP:
                    webbrowser.open(release_url, new=2)
            except Exception:
                pass

        header_lines.append(('class:logo_accent', '                New version available! '))
        header_lines.append(('class:link', '(click here)', _open_release_page))
        header_lines.append(('class:logo_accent', '\n'))
    header_lines.append(('class:logo_accent', divider + '\n'))
    header_lines.append(('class:logo_dim', '    Status • '))
    header_lines.append(('class:logo_keyword', 'Deep Scan'))
    header_lines.append(('class:logo_dim', ': '))
    header_lines.append(('class:logo_r' if deep_scan == "ON" else 'class:logo_dim', deep_scan))
    header_lines.append(('class:logo_dim', '   '))
    header_lines.append(('class:logo_keyword', 'Album Search'))
    header_lines.append(('class:logo_dim', ': '))
    header_lines.append(('class:logo_r' if album_search == "ON" else 'class:logo_dim', album_search))
    header_lines.append(('class:logo_dim', '   '))
    header_lines.append(('class:logo_keyword', 'Unsaved Preview'))
    header_lines.append(('class:logo_dim', ': '))
    header_lines.append(('class:logo_r' if preview_state == "UNSAVED" else 'class:logo_dim', preview_state))
    header_lines.append(('class:logo_dim', '\n'))
    header_lines.append(('class:logo_dim', '    Mode: '))
    header_lines.append(('class:logo_r', f'{mode_value}\n'))
    header_lines.append(('class:logo_dim', '    Controls • '))
    header_lines.append(('class:logo_keyword', '↑/↓ Navigate'))
    header_lines.append(('class:logo_dim', '   '))
    header_lines.append(('class:logo_keyword', 'Enter Select'))
    header_lines.append(('class:logo_dim', '   '))
    header_lines.append(('class:logo_keyword', 'Type/Paste Path'))
    header_lines.append(('class:logo_dim', '   '))
    header_lines.append(('class:logo_keyword', 'Drag/Drop Path'))
    header_lines.append(('class:logo_dim', '\n'))
    header_lines.append(('class:logo_dim', '\n'))

    # ANSI fallback string
    fb = ""
    for mix_part, split_part, r_part in logo_segments:
        fb += (
            f"{mix_logo_color}{mix_part}"
            f"{Style.GRAY}{split_part}"
            f"{r_logo_color}{r_part}{Style.RESET}\n"
        )
    fb += f"{Style.GRAY}{divider}\n{tagline}{Style.RESET}\n"
    fb += f"{Style.GRAY}{project_prefix}{PROJECT_URL}{Style.RESET}\n"
    if isinstance(update_info, dict):
        fb += (
            f"{Style.GRAY}                New version available! "
            f"{Style.MAGENTA}(click here){Style.RESET}\n"
        )
    fb += f"{Style.GRAY}{divider}{Style.RESET}\n"
    deep_scan_color = r_logo_color if deep_scan == "ON" else Style.GRAY
    album_search_color = r_logo_color if album_search == "ON" else Style.GRAY
    preview_color = r_logo_color if preview_state == "UNSAVED" else Style.GRAY
    fb += (
        f"{Style.DIM}    Status • {Style.MAGENTA}Deep Scan{Style.DIM}: {deep_scan_color}{deep_scan}{Style.DIM}   "
        f"{Style.MAGENTA}Album Search{Style.DIM}: {album_search_color}{album_search}{Style.DIM}   "
        f"{Style.MAGENTA}Unsaved Preview{Style.DIM}: {preview_color}{preview_state}{Style.RESET}\n"
    )
    fb += f"{Style.DIM}    Mode: {r_logo_color}{mode_value}{Style.RESET}\n"
    fb += (
        f"{Style.DIM}    Controls • {Style.MAGENTA}↑/↓ Navigate{Style.DIM}   "
        f"{Style.MAGENTA}Enter Select{Style.DIM}   "
        f"{Style.MAGENTA}Type/Paste Path{Style.DIM}   "
        f"{Style.MAGENTA}Drag/Drop Path{Style.RESET}\n\n"
    )

    return header_lines, fb


def _build_exit_menu_logo() -> tuple:
    """Build logo + donation callout for the exit confirmation menu."""
    logo_segments = [
        ('    ███╗   ███╗██╗██╗  ██╗', '███████╗██████╗ ██╗     ██╗████████╗', '██████╗ '),
        ('    ████╗ ████║██║╚██╗██╔╝', '██╔════╝██╔══██╗██║     ██║╚══██╔══╝', '██╔══██╗'),
        ('    ██╔████╔██║██║ ╚███╔╝ ', '███████╗██████╔╝██║     ██║   ██║   ', '██████╔╝'),
        ('    ██║╚██╔╝██║██║ ██╔██╗ ', '╚════██║██╔═══╝ ██║     ██║   ██║   ', '██╔══██╗'),
        ('    ██║ ╚═╝ ██║██║██╔╝ ██╗', '███████║██║     ███████╗██║   ██║   ', '██║  ██║'),
        ('    ╚═╝     ╚═╝╚═╝╚═╝  ╚═╝', '╚══════╝╚═╝     ╚══════╝╚═╝   ╚═╝   ', '╚═╝  ╚═╝'),
    ]
    logo_width = max(len(mix + split + r) for mix, split, r in logo_segments)
    divider_core = '             ═══════════════════════════════════════'.strip()
    divider = (' ' * max(0, (logo_width - len(divider_core)) // 2)) + divider_core
    tagline_text = 'Mix Archival Tool'
    tagline = (' ' * max(0, (logo_width - len(tagline_text)) // 2)) + tagline_text
    mix_logo_color = Style.GRAY
    r_logo_color = '\033[38;5;196m'

    header_lines = []
    for mix_part, split_part, r_part in logo_segments:
        header_lines.append(('class:logo_mix', mix_part))
        header_lines.append(('class:logo_split', split_part))
        header_lines.append(('class:logo_r', r_part + '\n'))
    header_lines.append(('class:logo_accent', divider + '\n'))
    header_lines.append(('class:logo_accent', tagline + '\n'))
    header_lines.append(('class:logo_accent', divider + '\n'))
    header_lines.append(('class:logo_dim', '    Open-source project maintained from the original MixSplitR repo.\n'))

    fb = ""
    for mix_part, split_part, r_part in logo_segments:
        fb += (
            f"{mix_logo_color}{mix_part}"
            f"{Style.GRAY}{split_part}"
            f"{r_logo_color}{r_part}{Style.RESET}\n"
        )
    fb += f"{Style.GRAY}{divider}\n{tagline}\n{divider}{Style.RESET}\n"
    fb += f"{Style.DIM}    Open-source project maintained from the original MixSplitR repo.{Style.RESET}\n"
    return header_lines, fb


def show_main_menu(audio_files: list, base_dir: str, config: dict, mode_badge: str,
                   has_cached_preview: bool = False, update_info: dict = None,
                   ui_notice: str = "") -> MenuResult:
    """Display main menu and return selection"""

    # Build file status line
    if audio_files:
        if len(audio_files) == 1:
            display_path = audio_files[0]
            if len(display_path) > 50:
                display_path = "..." + display_path[-47:]
            file_line = f"Loaded: {display_path}"
        else:
            file_line = f"{len(audio_files)} audio file(s) loaded"
    else:
        file_line = "No audio files loaded (drag files here or select below)"

    subtitle_lines = []
    if ui_notice:
        subtitle_lines.append(f"Notice: {ui_notice}")
    subtitle_lines.append(file_line)
    subtitle = "\n".join(subtitle_lines)

    items = build_main_menu_items(audio_files, config, has_cached_preview=has_cached_preview)
    header_lines, fallback_header = _build_main_menu_logo(config, has_cached_preview, mode_badge=mode_badge, update_info=update_info)
    def _open_issues_page(mouse_event):
        try:
            from prompt_toolkit.mouse_events import MouseEventType
            if mouse_event.event_type == MouseEventType.MOUSE_UP:
                webbrowser.open(ISSUES_URL, new=2)
        except Exception:
            pass
    footer_lines = [
        ('class:help', '  Issues/bugs? report '),
        ('class:link_red', 'here', _open_issues_page),
        ('class:help', '\n'),
    ]
    fallback_footer = (
        f"{Style.DIM}  Issues/bugs? report "
        f"\033[38;5;196mhere{Style.RESET}{Style.DIM} ({ISSUES_URL}){Style.RESET}"
    )
    current_dir = (base_dir or os.getcwd()).strip() or os.getcwd()
    if len(current_dir) > 92:
        current_dir = "..." + current_dir[-89:]
    hint_text = (
        "__hint_red__Drag in your folders/files anywhere\n\n"
        "__hint_divider__\n\n"
        f"Current directory: {current_dir}"
    )

    return select_menu(
        "",
        items,
        subtitle=subtitle,
        allow_text_input=True,
        text_input_hint=hint_text,
        header_lines=header_lines,
        footer_lines=footer_lines,
        fallback_header=fallback_header,
        fallback_footer=fallback_footer,
        show_item_divider=True,
        animate_item_divider=False
    )


def show_mode_switch_menu(config: dict, has_acr: bool, has_acoustid: bool) -> str:
    """Show mode selection submenu, return new mode or empty string"""
    current_mode = config.get('mode', MODE_ACRCLOUD)

    items = [
        MenuItem(
            MODE_ACRCLOUD, "🎵", "ACRCloud + MusicBrainz",
            "Requires ACRCloud account",
            enabled=ACRCLOUD_AVAILABLE
        ),
        MenuItem(
            MODE_MB_ONLY, "🔍", "MusicBrainz only",
            "Uses AcoustID fingerprinting — without it, falls back to manual search"
        ),
    ]

    # Dual mode option if both keys available
    if has_acr and has_acoustid:
        items.append(MenuItem(
            MODE_DUAL, "⭐", "Dual Mode - Best of Both",
            "Runs both methods, picks highest confidence"
        ))

    items.append(MenuItem("cancel", "←", "Cancel", "Return without changing"))

    result = select_menu(
        "Switch Identification Mode",
        items,
        subtitle=f"Current: {_mode_name(current_mode)}"
    )

    if result.cancelled or result.key == "cancel":
        return ""
    return result.key


def _mode_name(mode: str) -> str:
    """Get human readable mode name"""
    names = {
        MODE_MANUAL: "Manual Search Only",
        MODE_MB_ONLY: "MusicBrainz only",
        MODE_ACRCLOUD: "ACRCloud + MusicBrainz",
        MODE_DUAL: "Dual Mode (Best of Both)"
    }
    return names.get(mode, mode)


def show_api_keys_menu() -> bool:
    """
    Settings menu — top-level hub for Identification Mode and API Keys.
    Returns True to return to main menu, False to show again.
    """
    clear_screen()
    config_path = get_config_path()
    config = json.load(open(config_path)) if os.path.exists(config_path) else {}

    current_mode = config.get('mode', MODE_ACRCLOUD)
    has_acr = bool(config.get('host') and config.get('access_key'))
    has_lastfm = bool(config.get('lastfm_api_key'))
    has_acoustid = bool(config.get('acoustid_api_key'))
    album_search_enabled = config.get('enable_album_search', True)
    album_search_state = "ON" if album_search_enabled else "OFF"
    normalize_artists_enabled = config.get('normalize_artists', True)
    normalize_state = "ON" if normalize_artists_enabled else "OFF"
    deep_scan_enabled = config.get('deep_scan', False)
    deep_scan_state = "ON" if deep_scan_enabled else "OFF"
    portable_scan_enabled = bool(config.get('portable_mode_local_scan', False))
    portable_scan_state = "ON" if portable_scan_enabled else "OFF"
    shazam_disabled = bool(config.get('disable_shazam', False))

    # Refresh backend availability
    try:
        setup_musicbrainz(CURRENT_VERSION, GITLAB_REPO)
    except Exception:
        pass
    shazam_available = bool(is_shazam_available())

    # Fallback mode still prints status before the numbered menu.
    if not PROMPT_TOOLKIT_AVAILABLE:
        print(f"\n{Style.MAGENTA}{'═'*60}{Style.RESET}")
        print(f"  {Style.BOLD}⚙️  Settings{Style.RESET}")
        print(f"{Style.MAGENTA}{'═'*60}{Style.RESET}")
        print(f"\n  📁 Config: {Style.DIM}{config_path}{Style.RESET}\n")
        _print_api_status(config, current_mode, has_acr, has_lastfm, has_acoustid,
                          shazam_disabled, shazam_available)

    items = [
        MenuItem(
            "id_mode", "🔄", "Identification Mode",
            f"Current: {_mode_name(current_mode)} — switch mode, toggle backends"
        ),
        MenuItem(
            "album_search_toggle", "💿", f"Album Search ({album_search_state})",
            "Search by album and group results by release"
        ),
        MenuItem(
            "normalize_toggle", "👤", f"Normalize Artists ({normalize_state})",
            "Move feat/collab credits to title, keep one folder per primary artist"
        ),
        MenuItem(
            "deep_scan_toggle", "🔍", f"Auto Deep Scan ({deep_scan_state})",
            "Automatically scan subfolders when loading directories"
        ),
        MenuItem(
            "portable_scan_toggle", "💼", f"Portable Startup Scan ({portable_scan_state})",
            "Auto-scan audio in local app/script folder on startup"
        ),
        MenuItem(
            "delete_cache", "🗑️", "Clear Unsaved Preview Data",
            "Delete unsaved preview data and temporary chunks"
        ),
        MenuItem(
            "dir_settings", "📂", "Directory Settings",
            "Output, recording, and session history folders"
        ),
        MenuItem(
            "api_keys_sub", "🔑", "API Key Settings",
            "Add, update, remove, or test API keys"
        ),
        MenuItem("back", "←", "Back to main menu"),
    ]

    result = select_menu(
        "Settings",
        items,
        show_item_divider=True,
        wrap_selected_description=True,
    )

    if result.cancelled or result.key == "back":
        return True  # Return to main

    if result.key == "id_mode":
        _show_identification_mode_menu(config, has_acr, has_acoustid, shazam_available)
        return False

    if result.key == "album_search_toggle":
        config['enable_album_search'] = not config.get('enable_album_search', True)
        save_config(config)
        state = "enabled" if config['enable_album_search'] else "disabled"
        print(f"\n  {Style.GREEN}✅ Album search features {state}{Style.RESET}")
        wait_for_enter()
        return False

    if result.key == "normalize_toggle":
        config['normalize_artists'] = not config.get('normalize_artists', True)
        save_config(config)
        if config['normalize_artists']:
            print(f"\n  {Style.GREEN}✅ Artist normalization enabled{Style.RESET}")
            print(f"  {Style.DIM}  Collabs moved to title: \"Artist & Guest - Song\" → \"Artist - Song (feat. Guest)\"{Style.RESET}")
        else:
            print(f"\n  {Style.GREEN}✅ Artist normalization disabled{Style.RESET}")
            print(f"  {Style.DIM}  Full collaboration credits kept in artist tag{Style.RESET}")
        wait_for_enter()
        return False

    if result.key == "deep_scan_toggle":
        config['deep_scan'] = not config.get('deep_scan', False)
        save_config(config)
        if config['deep_scan']:
            print(f"\n  {Style.GREEN}✅ Auto Deep Scan enabled{Style.RESET}")
            print(f"  {Style.DIM}  Folders will be scanned recursively (including subfolders){Style.RESET}")
        else:
            print(f"\n  {Style.GREEN}✅ Auto Deep Scan disabled{Style.RESET}")
            print(f"  {Style.DIM}  Only top-level folder contents will be scanned{Style.RESET}")
        wait_for_enter()
        return False

    if result.key == "portable_scan_toggle":
        config['portable_mode_local_scan'] = not bool(config.get('portable_mode_local_scan', False))
        save_config(config)
        if config['portable_mode_local_scan']:
            print(f"\n  {Style.GREEN}✅ Portable startup scan enabled{Style.RESET}")
            print(f"  {Style.DIM}  Startup will scan audio next to the app/script{Style.RESET}")
        else:
            print(f"\n  {Style.GREEN}✅ Portable startup scan disabled{Style.RESET}")
            print(f"  {Style.DIM}  Startup scan uses your Music folder instead{Style.RESET}")
        wait_for_enter()
        return False

    if result.key == "delete_cache":
        _clear_preview_cache_from_settings()
        return False

    if result.key == "dir_settings":
        _show_directory_settings_menu(config)
        return False

    if result.key == "api_keys_sub":
        _show_api_key_settings_menu(config)
        return False

    return True


def _clear_preview_cache_from_settings():
    """Clear preview cache and known temporary chunk folders."""
    from .mixsplitr_core import get_cache_path

    cache_path = get_cache_path("mixsplitr_cache.json")
    readable_path = str(cache_path).replace('.json', '_readable.txt')

    temp_dirs = {os.path.join(os.path.dirname(cache_path), "mixsplitr_temp")}

    # Also clear temp chunk directories referenced in cache, if present.
    try:
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            for track in cache_data.get('tracks', []):
                tp = track.get('temp_chunk_path')
                if tp:
                    temp_dirs.add(os.path.dirname(tp))
    except Exception:
        pass

    removed_files = []
    for path in (cache_path, readable_path):
        try:
            if os.path.exists(path):
                os.remove(path)
                removed_files.append(path)
        except Exception as e:
            print(f"  {Style.YELLOW}⚠️  Could not delete {path}: {e}{Style.RESET}")

    removed_dirs = []
    for d in sorted(temp_dirs):
        # Safety: only remove known temp folder names.
        if os.path.basename(os.path.normpath(d)) != "mixsplitr_temp":
            continue
        try:
            if os.path.exists(d):
                shutil.rmtree(d)
                removed_dirs.append(d)
        except Exception as e:
            print(f"  {Style.YELLOW}⚠️  Could not delete {d}: {e}{Style.RESET}")

    if removed_files or removed_dirs:
        print(f"\n  {Style.GREEN}✅ Unsaved preview data cleared{Style.RESET}")
        for p in removed_files:
            print(f"     {Style.DIM}Deleted file: {p}{Style.RESET}")
        for d in removed_dirs:
            print(f"     {Style.DIM}Deleted folder: {d}{Style.RESET}")
    else:
        print(f"\n  {Style.DIM}No unsaved preview data found.{Style.RESET}")

    wait_for_enter()


def _show_directory_settings_menu(config: dict):
    """Submenu for configuring output, recording, and manifest directories."""
    from .mixsplitr_core import get_app_data_dir

    default_app_data = str(get_app_data_dir() / "manifests")

    while True:
        out_val = config.get('output_directory', '')
        rec_val = config.get('recording_directory', '')
        man_val = config.get('manifest_directory', '')

        items = [
            MenuItem(
                "output", "📂", "Output Folder",
                out_val or "Default (Music/MixSplitR Library)"
            ),
            MenuItem(
                "recording", "🎙️", "Recording Folder",
                rec_val or "Default (Music)"
            ),
            MenuItem(
                "manifest", "📋", "Session History Folder",
                man_val or f"Default ({default_app_data})"
            ),
            MenuItem("back", "←", "Back to Settings"),
        ]

        result = select_menu("Directory Settings", items, show_item_divider=True)

        if result.cancelled or result.key == "back":
            return

        if result.key == "output":
            _change_directory_setting(config, 'output_directory',
                                      "Output Folder",
                                      "Where processed tracks are saved",
                                      "Music/MixSplitR Library")

        elif result.key == "recording":
            _change_directory_setting(config, 'recording_directory',
                                      "Recording Folder",
                                      "Where recordings are saved",
                                      "Music")

        elif result.key == "manifest":
            _change_directory_setting(config, 'manifest_directory',
                                      "Session History Folder",
                                      "Where session history files are stored",
                                      default_app_data)


def _change_directory_setting(config: dict, config_key: str,
                              label: str, description: str,
                              default_hint: str):
    """Prompt the user to change a directory setting, or reset to default."""
    from .mixsplitr_core import get_default_music_folder

    current = config.get(config_key, '')
    default_path = get_default_music_folder()

    print(f"\n{Style.CYAN}{'═'*60}{Style.RESET}")
    print(f"  {Style.BOLD}📂 {label}{Style.RESET}")
    print(f"  {Style.DIM}{description}{Style.RESET}")
    print(f"{Style.CYAN}{'═'*60}{Style.RESET}")

    if current:
        print(f"\n  Current:  {Style.GREEN}{current}{Style.RESET}")
    else:
        print(f"\n  Current:  {Style.DIM}Default ({default_hint}){Style.RESET}")

    items = [
        MenuItem("change", "📝", "Set a custom folder",
                 "Type or paste a folder path"),
        MenuItem("reset", "🔄", "Reset to default",
                 f"Uses your Music folder ({default_path})"),
        MenuItem("back", "←", "Cancel"),
    ]

    result = select_menu("", items)

    if result.cancelled or result.key == "back":
        return

    if result.key == "reset":
        config.pop(config_key, None)
        save_config(config)
        print(f"\n  {Style.GREEN}✅ {label} reset to default ({default_hint}){Style.RESET}")
        wait_for_enter()
        return

    if result.key == "change":
        print(f"\n  Enter the full folder path (or press Enter to cancel):")
        new_path = input(f"  → ").strip().strip('"').strip("'")

        if not new_path:
            print(f"  {Style.DIM}Cancelled.{Style.RESET}")
            wait_for_enter()
            return

        # Expand ~ to home directory
        new_path = os.path.expanduser(new_path)

        if not os.path.isabs(new_path):
            print(f"\n  {Style.RED}✗ Please use a full path (e.g. /Users/you/Music or C:\\Users\\you\\Music){Style.RESET}")
            wait_for_enter()
            return

        # Create the folder if it doesn't exist
        try:
            os.makedirs(new_path, exist_ok=True)
        except Exception as e:
            print(f"\n  {Style.RED}✗ Could not create folder: {e}{Style.RESET}")
            wait_for_enter()
            return

        config[config_key] = new_path
        save_config(config)
        print(f"\n  {Style.GREEN}✅ {label} set to:{Style.RESET}")
        print(f"     {new_path}")
        wait_for_enter()


def _show_identification_mode_menu(config: dict, has_acr: bool, has_acoustid: bool,
                                    shazam_available: bool):
    """Submenu for switching identification mode and toggling backends."""
    while True:
        clear_screen()
        # Re-read toggles each loop
        current_mode = config.get('mode', MODE_ACRCLOUD)
        shazam_disabled = bool(config.get('disable_shazam', False))
        show_id_enabled = config.get('show_id_source', True)
        try:
            sample_seconds = int(config.get('fingerprint_sample_seconds', 12))
        except Exception:
            sample_seconds = 12
        sample_seconds = max(8, min(45, sample_seconds))

        shazam_state = "OFF" if shazam_disabled else "ON"
        shazam_desc = "Available" if shazam_available else "shazamio not installed"
        id_state = "ON" if show_id_enabled else "OFF"

        items = [
            MenuItem(
                "switch_mode", "🔄", "Switch identification mode",
                f"Current: {_mode_name(current_mode)}"
            ),
            MenuItem(
                "fingerprint_sample", "🎚️", f"Fingerprint Sample Size ({sample_seconds}s)",
                "Longer sample can improve matching accuracy on hard tracks (8-45s)"
            ),
            MenuItem(
                "shazam_toggle", "🎤", f"Toggle Shazam ({shazam_state})",
                shazam_desc
            ),
            MenuItem(
                "id_source_toggle", "📊", f"Toggle ID Source Output ({id_state})",
                "Console-only toggle — backend source is always saved in session records"
            ),
            MenuItem("back", "←", "Back to Settings"),
        ]

        result = select_menu(
            "Identification Mode",
            items,
            show_item_divider=True,
            wrap_selected_description=True,
        )

        if result.cancelled or result.key == "back":
            return

        if result.key == "switch_mode":
            new_mode = show_mode_switch_menu(config, has_acr, has_acoustid)
            if new_mode:
                if new_mode == MODE_ACRCLOUD and not ACRCLOUD_AVAILABLE:
                    print(f"\n  {Style.RED}❌ ACRCloud SDK not available in this build{Style.RESET}")
                    wait_for_enter()
                    continue
                config['mode'] = new_mode
                save_config(config)
                print(f"\n  {Style.GREEN}✅ Switched to {_mode_name(new_mode)}{Style.RESET}")
                wait_for_enter()

        elif result.key == "fingerprint_sample":
            entered = input_dialog(
                "Fingerprint sample length in seconds (8-45)",
                default=str(sample_seconds)
            )
            if entered is None:
                continue
            entered = entered.strip()
            if not entered:
                continue
            try:
                new_seconds = int(float(entered))
            except ValueError:
                print(f"\n  {Style.RED}❌ Please enter a number between 8 and 45.{Style.RESET}")
                wait_for_enter()
                continue
            new_seconds = max(8, min(45, new_seconds))
            config['fingerprint_sample_seconds'] = new_seconds
            save_config(config)
            print(f"\n  {Style.GREEN}✅ Fingerprint sample size set to {new_seconds}s{Style.RESET}")
            if new_seconds >= 20:
                print(f"  {Style.DIM}Longer samples are slower but can improve ID accuracy.{Style.RESET}")
            wait_for_enter()

        elif result.key == "shazam_toggle":
            config['disable_shazam'] = not config.get('disable_shazam', False)
            save_config(config)
            state = "disabled" if config['disable_shazam'] else "enabled"
            print(f"\n  {Style.GREEN}✅ Shazam {state}{Style.RESET}")
            wait_for_enter()

        elif result.key == "id_source_toggle":
            config['show_id_source'] = not config.get('show_id_source', True)
            save_config(config)
            state = "enabled" if config['show_id_source'] else "disabled"
            print(f"\n  {Style.GREEN}✅ ID source output {state}{Style.RESET}")
            wait_for_enter()


def _show_api_key_settings_menu(config: dict):
    """Submenu for managing all API keys (ACRCloud, Last.fm, AcoustID)."""
    while True:
        clear_screen()
        # Re-read config state each loop
        has_acr = bool(config.get('host') and config.get('access_key'))
        has_lastfm = bool(config.get('lastfm_api_key'))
        has_acoustid = bool(config.get('acoustid_api_key'))

        items = []

        # ACRCloud
        items.append(MenuItem(
            "acr_update", "📝", "Update ACRCloud credentials",
            "Configured" if has_acr else "Not configured"
        ))
        if has_acr:
            items.append(MenuItem("acr_test", "🔑", "Test ACRCloud credentials"))

        # Last.fm
        items.append(MenuItem(
            "lastfm_add", "🎸", "Add/Update Last.fm API key",
            "Configured" if has_lastfm else "Not configured"
        ))
        if has_lastfm:
            items.append(MenuItem("lastfm_remove", "🗑️", "Remove Last.fm API key"))

        # AcoustID
        acoustid_label = "Update AcoustID API key" if has_acoustid else "Add AcoustID API key"
        acoustid_desc = "Configured" if has_acoustid else "Enables fingerprinting!"
        items.append(MenuItem("acoustid_add", "🎵", acoustid_label, acoustid_desc))
        if has_acoustid:
            items.append(MenuItem("acoustid_remove", "🗑️", "Remove AcoustID API key"))
            items.append(MenuItem("acoustid_test", "🔑", "Test AcoustID key"))

        items.append(MenuItem("back", "←", "Back to Settings"))

        result = select_menu("API Key Settings", items, show_item_divider=True)

        if result.cancelled or result.key == "back":
            return

        if result.key == "acr_update":
            _update_acrcloud_credentials(config)
        elif result.key == "acr_test":
            _test_acrcloud_credentials(config)
        elif result.key == "lastfm_add":
            _add_lastfm_key(config)
        elif result.key == "lastfm_remove":
            if confirm_dialog("Remove Last.fm API key?"):
                config.pop('lastfm_api_key', None)
                save_config(config)
                set_lastfm_key(None)
                print(f"  {Style.GREEN}✅ Last.fm key removed{Style.RESET}")
                wait_for_enter()
        elif result.key == "acoustid_add":
            _add_acoustid_key(config)
        elif result.key == "acoustid_remove":
            if confirm_dialog("Remove AcoustID API key?"):
                config.pop('acoustid_api_key', None)
                save_config(config)
                set_acoustid_api_key(None)
                print(f"  {Style.GREEN}✅ AcoustID key removed{Style.RESET}")
                wait_for_enter()
        elif result.key == "acoustid_test":
            _test_acoustid_key(config)


def _print_api_status(config, mode, has_acr, has_lastfm, has_acoustid,
                      shazam_disabled, shazam_available):
    """Print API configuration status with decision tree"""
    # Mode
    print(f"  Mode:     {Style.CYAN}{_mode_name(mode)}{Style.RESET}")

    # ACRCloud
    if mode == MODE_ACRCLOUD:
        if has_acr:
            print(f"  ACRCloud: {Style.GREEN}✅ Configured{Style.RESET}")
        else:
            print(f"  ACRCloud: {Style.RED}❌ Not configured{Style.RESET}")
    else:
        print(f"  ACRCloud: {Style.DIM}— not used{Style.RESET}")

    # Last.fm
    status = f"{Style.GREEN}✅ Configured{Style.RESET}" if has_lastfm else f"{Style.RED}❌ Not configured{Style.RESET}"
    print(f"  Last.fm:  {status}")

    # Shazam
    if shazam_disabled:
        print(f"  Shazam:   {Style.YELLOW}⏸️  Disabled{Style.RESET}")
    elif shazam_available:
        print(f"  Shazam:   {Style.GREEN}✅ Enabled{Style.RESET}")
    else:
        print(f"  Shazam:   {Style.YELLOW}⚠️  Unavailable{Style.RESET}")

    # AcoustID
    if has_acoustid:
        print(f"  AcoustID: {Style.GREEN}✅ Configured{Style.RESET}")
    else:
        print(f"  AcoustID: {Style.RED}❌ Not configured{Style.RESET}")

    # Decision tree showing how identification works
    print(f"\n{Style.DIM}{'─'*58}{Style.RESET}")
    print(f"  {Style.BOLD}How Identification Works:{Style.RESET}")
    print(f"{Style.DIM}{'─'*58}{Style.RESET}")

    shazam_on = shazam_available and not shazam_disabled

    if mode == MODE_DUAL:
        print(f"  {Style.CYAN}┌─ Audio Chunk ─────────────────────────────────┐{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}  Run ALL backends in parallel:               {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}    ├─ ACRCloud    {'✓' if has_acr else '✗'}                         {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}    ├─ AcoustID    {'✓' if has_acoustid else '✗'} → MusicBrainz       {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}    └─ Shazam      {'✓' if shazam_on else '✗'}                         {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}                                               {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}  Pick winner by {Style.GREEN}highest confidence{Style.RESET}          {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}  Tie-break: Shazam > ACRCloud > AcoustID      {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}└───────────────────────────────────────────────┘{Style.RESET}")
    elif mode == MODE_ACRCLOUD:
        print(f"  {Style.CYAN}┌─ Audio Chunk ─────────────────────────────────┐{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}  1. Try ACRCloud  {'✓' if has_acr else '✗ (needs setup)'}                    {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}     └─ if fail → 2. Try Shazam {'✓' if shazam_on else '✗'}             {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}                   └─ if fail → 3. Try AcoustID {'✓' if has_acoustid else '✗'}  {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}└───────────────────────────────────────────────┘{Style.RESET}")
    elif mode == MODE_MB_ONLY:
        print(f"  {Style.CYAN}┌─ Audio Chunk ─────────────────────────────────┐{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}  1. Try Shazam    {'✓' if shazam_on else '✗'}                         {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}     └─ if fail → 2. Try AcoustID {'✓' if has_acoustid else '✗'}          {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}│{Style.RESET}                      └─ query MusicBrainz      {Style.CYAN}│{Style.RESET}")
        print(f"  {Style.CYAN}└───────────────────────────────────────────────┘{Style.RESET}")
    else:  # Manual
        print(f"  {Style.YELLOW}┌─ Manual Mode ─────────────────────────────────┐{Style.RESET}")
        print(f"  {Style.YELLOW}│{Style.RESET}  No auto-identification (no keys set)         {Style.YELLOW}│{Style.RESET}")
        print(f"  {Style.YELLOW}│{Style.RESET}  You search & tag tracks manually in editor   {Style.YELLOW}│{Style.RESET}")
        print(f"  {Style.YELLOW}└───────────────────────────────────────────────┘{Style.RESET}")

    # Enrichment explanation
    print(f"\n  {Style.BOLD}After ID → Enrichment:{Style.RESET}")
    print(f"  {Style.DIM}  Once a track is identified, we fetch extra metadata:{Style.RESET}")
    enrichments = []
    enrichments.append(f"MusicBrainz (genres, dates)")
    if has_lastfm:
        enrichments.append(f"Last.fm (tags, popularity)")
    enrichments.append(f"iTunes/Deezer (artwork, BPM)")
    print(f"  {Style.DIM}  {' → '.join(enrichments)}{Style.RESET}")
    print()




def _update_acrcloud_credentials(config: dict):
    """Update ACRCloud credentials"""
    print(f"\n  Enter new ACRCloud credentials (Enter to keep existing):\n")

    current_host = config.get('host', '')
    current_key = config.get('access_key', '')

    new_host = input_dialog(f"ACR Host", current_host)
    new_key = input_dialog(f"Access Key", current_key[:10] + '...' if current_key else '')
    new_secret = input_dialog("Secret Key", password=True)

    if new_host and new_host != current_host:
        config['host'] = new_host
    if new_key and not new_key.endswith('...'):
        config['access_key'] = new_key
    if new_secret:
        config['access_secret'] = new_secret
    config['timeout'] = 10

    print(f"\n  🔑 Validating...", end='', flush=True)
    is_valid, error_msg = validate_acrcloud_credentials(config)

    if is_valid:
        print(f" {Style.GREEN}✅ Valid!{Style.RESET}")
        save_config(config)
        print(f"  {Style.GREEN}💾 Config saved!{Style.RESET}")
    else:
        print(f" {Style.RED}❌ {error_msg}{Style.RESET}")
        if confirm_dialog("Save anyway?", default=False):
            save_config(config)
            print(f"  {Style.YELLOW}💾 Saved (may not work){Style.RESET}")

    wait_for_enter()


def _test_acrcloud_credentials(config: dict):
    """Test ACRCloud credentials"""
    print(f"\n  🔑 Testing ACRCloud credentials...", end='', flush=True)
    is_valid, error_msg = validate_acrcloud_credentials(config)

    if is_valid:
        print(f" {Style.GREEN}✅ Valid!{Style.RESET}")
    else:
        print(f" {Style.RED}❌ {error_msg}{Style.RESET}")

    wait_for_enter()


def _add_lastfm_key(config: dict):
    """Add or update Last.fm API key"""
    print(f"\n  Get your free API key at: https://www.last.fm/api/account/create\n")

    key = input_dialog("Last.fm API Key")
    if key:
        config['lastfm_api_key'] = key
        save_config(config)
        set_lastfm_key(key)
        print(f"\n  {Style.GREEN}✅ Last.fm key saved!{Style.RESET}")
    else:
        print(f"\n  {Style.YELLOW}Cancelled{Style.RESET}")

    wait_for_enter()


def _add_acoustid_key(config: dict):
    """Add or update AcoustID API key"""
    # Check chromaprint first
    if not check_chromaprint_available():
        print(f"\n  {Style.YELLOW}⚠️  fpcalc (Chromaprint) not found{Style.RESET}")
        print(f"  {Style.DIM}AcoustID requires fpcalc for fingerprinting{Style.RESET}")
        print(f"  {Style.DIM}Download from: https://acoustid.org/chromaprint{Style.RESET}")
        wait_for_enter()
        return

    print(f"\n  Get your free API key at: https://acoustid.org/login\n")

    key = input_dialog("AcoustID API Key")
    if key:
        config['acoustid_api_key'] = key
        save_config(config)
        set_acoustid_api_key(key)
        print(f"\n  {Style.GREEN}✅ AcoustID key saved!{Style.RESET}")

        # Suggest dual mode if ACRCloud also configured
        if config.get('host') and config.get('access_key'):
            print(f"\n  {Style.CYAN}💡 Both ACRCloud and AcoustID configured!{Style.RESET}")
            print(f"  {Style.DIM}You can now use Dual Mode for best results.{Style.RESET}")
    else:
        print(f"\n  {Style.YELLOW}Cancelled{Style.RESET}")

    wait_for_enter()


def _test_acoustid_key(config: dict):
    """Test AcoustID API key"""
    from .mixsplitr_identify import is_acoustid_available

    print(f"\n  🔑 Testing AcoustID configuration...")

    if not check_chromaprint_available():
        print(f"  {Style.RED}❌ fpcalc not found{Style.RESET}")
        wait_for_enter()
        return

    key = config.get('acoustid_api_key')
    if not key:
        print(f"  {Style.RED}❌ No API key configured{Style.RESET}")
        wait_for_enter()
        return

    # Try to set key and check availability
    set_acoustid_api_key(key)
    if is_acoustid_available():
        print(f"  {Style.GREEN}✅ AcoustID ready!{Style.RESET}")
    else:
        print(f"  {Style.YELLOW}⚠️  Configuration issue{Style.RESET}")

    wait_for_enter()


def show_preview_type_menu():
    """Show preview type selection.
    Returns:
      - True for light preview
      - False for full preview
      - None if cancelled (Esc)
    """
    items = [
        MenuItem("full", "🔍", "Full Preview (Recommended)",
                 "Best for editing/reliability: saves split chunks, faster export, uses more disk"),
        MenuItem("light", "⚡", "Light Preview",
                 "Best for speed/storage: lower disk use, export re-splits from source"),
    ]

    result = select_menu(
        "Choose Preview Type",
        items,
        show_item_divider=True,
        wrap_selected_description=True,
    )
    if result.cancelled:
        return None
    return result.key == "light"


def show_split_mode_menu() -> str:
    """Show splitting mode selection. Returns 'silence', 'manual', or 'assisted'"""
    items = [
        MenuItem("silence", "🔇", "Automatic (Recommended)",
                 "Silence detection - works for most mixes"),
        MenuItem("manual", "✂️", "Manual (Visual Editor)",
                 "Set split points on waveform - for seamless mixes"),
        MenuItem("assisted", "🎯", "Assisted (Auto + Review)",
                 "Auto-detect then review in visual editor"),
    ]

    result = select_menu("Choose Splitting Mode", items)

    if result.cancelled:
        return "silence"
    return result.key


def show_exit_menu_with_cache(track_count: int) -> str:
    """
    Show exit options when an unsaved preview exists.
    Returns: 'exit', 'clear_exit', 'cancel'
    """
    items = [
        MenuItem("cancel", "←", "Go Back",
                 "Return to main menu"),
        MenuItem("clear_exit", "🗑️", "Clear Unsaved Preview & Exit",
                 f"Delete {track_count} unsaved track(s) and exit"),
        MenuItem("exit", "🚪", "Exit and Keep Unsaved Preview",
                 "Keep unsaved preview data for next launch"),
    ]

    header_lines, fallback_header = _build_exit_menu_logo()

    result = select_menu(
        f"Unsaved Preview ({track_count} tracks)",
        items,
        subtitle="You can finish this from the main menu via 'Finish Unsaved Preview'",
        header_lines=header_lines,
        fallback_header=fallback_header,
    )

    if result.cancelled:
        return "cancel"
    return result.key


def show_post_process_menu() -> str:
    """Show post-processing options. Returns 'main', 'another', or 'edit'"""
    items = [
        MenuItem("main", "🏠", "Return to Main Menu"),
        MenuItem("another", "📁", "Process Another File"),
        MenuItem("edit", "✏️", "Edit Track Metadata"),
    ]

    result = select_menu("Processing Complete!", items)

    if result.cancelled:
        return "main"
    return result.key


def show_manifest_menu(manifests: list) -> tuple:
    """
    Show manifest management menu (session-first flow)
    Returns (action, manifest_idx or None)
    Actions: 'view', 'compare', 'edit_session', 'reorganize', 'rollback',
             'apply_session', 'delete', 'export', 'import', 'back', 'cancel'
    """
    # Step 1: Pick a session first (or import/back).
    session_items = []
    for i, m in enumerate(manifests[:10]):
        timestamp = (m.get('timestamp') or '')[:19].replace('T', ' ')
        label = m.get('session_name', 'Unknown Session')[:48]
        desc = f"{timestamp} • {m.get('total_tracks', '?')} tracks"
        session_items.append(MenuItem(str(i), "📋", label, desc))
    session_items.append(MenuItem("import", "📥", "Import session record",
                                  "Import a shared/exported .json session record (drag/drop path)"))
    session_items.append(MenuItem("back", "←", "Back to main menu"))

    pick = select_menu(
        "Session History",
        session_items,
        subtitle=f"{len(manifests)} session(s) available • select a session first"
    )

    if pick.cancelled or pick.key == "back":
        return ("back", None)
    if pick.key == "import":
        return ("import", None)

    try:
        selected_idx = int(pick.key)
    except ValueError:
        return ("cancel", None)

    selected = manifests[selected_idx]
    selected_label = selected.get('session_name', 'Unknown Session')[:48]
    selected_stamp = (selected.get('timestamp') or '')[:19].replace('T', ' ')
    selected_tracks = selected.get('total_tracks', '?')

    # Step 2: Action menu for selected session.
    action_items = [
        MenuItem("view", "👁️", "View session details"),
        MenuItem("compare", "🔄", "Compare with another session"),
        MenuItem("edit_session", "✏️", "Session editor",
                 "Revise track metadata/filenames for already-exported files"),
        MenuItem("reorganize", "🗂️", "Reorganize files",
                 "Rename/re-folder using current settings"),
        MenuItem("rollback", "⏪", "Undo / restore previous results"),
        MenuItem("apply_session", "▶️", "Apply Session Record (Safe)",
                 "Copy session outputs to a target folder with strict safety checks"),
        MenuItem("delete", "🗑️", "Delete session record",
                 "Remove selected session record from history"),
        MenuItem("export", "📤", "Export session data"),
        MenuItem("back", "←", "Choose another session"),
    ]

    result = select_menu(
        f"Session: {selected_label}",
        action_items,
        subtitle=f"{selected_stamp} • {selected_tracks} tracks"
    )

    if result.cancelled or result.key == "back":
        return ("cancel", None)

    if result.key == "compare":
        # Compare selected session against another one.
        manifest_items = []
        for i, m in enumerate(manifests[:10]):
            timestamp = (m.get('timestamp') or '')[:19].replace('T', ' ')
            label = m.get('session_name', 'Unknown Session')[:48]
            manifest_items.append(MenuItem(str(i), "📋", label, timestamp))
        manifest_items.append(MenuItem("cancel", "←", "Cancel"))
        m2_result = select_menu("Select session to compare against", manifest_items)
        if m2_result.cancelled or m2_result.key == "cancel":
            return ("cancel", None)
        try:
            idx2 = int(m2_result.key)
            return ("compare", (selected_idx, idx2))
        except ValueError:
            return ("cancel", None)

    return (result.key, selected_idx)


def show_format_selection_menu() -> str | None:
    """Show output format selection"""
    descriptions = {
        "flac": ("🎵", "Lossless compression (recommended)"),
        "alac": ("🍎", "Apple Lossless (M4A)"),
        "wav": ("📼", "Uncompressed PCM (large files)"),
        "aiff": ("🎹", "AIFF lossless"),
        "mp3_320": ("🎧", "High quality compressed"),
        "mp3_256": ("🎧", "Good quality compressed"),
        "mp3_192": ("🎧", "Standard quality"),
        "aac_256": ("📱", "High quality AAC"),
        "ogg_500": ("🐧", "OGG Vorbis Q10"),
        "ogg_320": ("🐧", "OGG Vorbis Q8"),
        "opus": ("📡", "OPUS 256kbps"),
    }
    preferred_order = [
        "flac", "alac", "wav", "aiff",
        "mp3_320", "mp3_256", "mp3_192",
        "aac_256", "ogg_500", "ogg_320", "opus"
    ]

    items = []
    for key in preferred_order:
        if key in AUDIO_FORMATS:
            icon, desc = descriptions.get(key, ("🎵", "Audio export format"))
            items.append(MenuItem(key, icon, AUDIO_FORMATS[key]["name"], desc))

    result = select_menu("Select Output Format", items)

    if result.cancelled:
        return None
    return result.key


def show_file_selection_menu(current_dir: str) -> tuple:
    """
    Show file/folder selection menu
    Returns (action, path_or_none)
    Actions: 'path', 'record', 'last_recording', 'cancel'
    """
    from .mixsplitr_core import get_config
    _cfg = get_config()
    _deep = _cfg.get('deep_scan', False)
    _deep_label = "Deep Scan: ON (toggle in Settings)" if _deep else "Deep Scan: OFF (toggle in Settings)"

    items = []

    if sys.platform in ("win32", "darwin"):
        items.append(MenuItem("record", "🎙️", "Record system audio"))
        items.append(MenuItem("last_rec", "📼", "Load last saved recording"))

    items.append(MenuItem("cancel", "←", "Cancel"))

    result = select_menu(
        "Select Audio Files",
        items,
        subtitle=f"Current: {current_dir}  |  🔍 {_deep_label}",
        allow_text_input=True,
        text_input_hint="Drag files/folder or paste path"
    )

    if result.cancelled or result.key == "cancel":
        return ("cancel", None)

    if result.key == "__path__":
        return ("path", result.text_input)

    return (result.key, None)
