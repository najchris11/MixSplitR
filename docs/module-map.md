# MixSplitR Module Map

Primary launch paths:

- `MixSplitR.py` (compatibility launcher)
- `python -m mixsplitr` (package entrypoint)
- `mixsplitr` (console script from `pyproject.toml`)

Application code now lives under the `mixsplitr/` package.

## Core Modules

- `mixsplitr/orchestration.py`: primary runtime orchestration loop
- `mixsplitr/cli.py`: CLI entrypoint
- `mixsplitr/bootstrapping.py`: runtime bootstrap helpers (freeze support)
- `mixsplitr/mixsplitr_core.py`: configuration, update checks, ffmpeg setup, shared constants
- `mixsplitr/mixsplitr_processing.py`: per-track identification workflows and orchestration helpers
- `mixsplitr/mixsplitr_pipeline.py`: larger pipeline stages (streaming, apply-from-cache)
- `mixsplitr/mixsplitr_session.py`: session history, manifests, rollback tooling

## Identification + Metadata

- `mixsplitr/mixsplitr_identify.py`: backend IDs (AcoustID, MusicBrainz, Shazam) and merge logic
- `mixsplitr/mixsplitr_metadata.py`: artwork + external metadata providers

## Audio + Tagging

- `mixsplitr/mixsplitr_audio.py`: audio analysis helpers (for example BPM detection)
- `mixsplitr/mixsplitr_tagging.py`: tagging/embedding and export format handling
- `mixsplitr/mixsplitr_tracklist.py`: tracklist-related support logic

## UI + Interaction

- `mixsplitr/mixsplitr_menu.py`: prompt primitives and shared menu helpers
- `mixsplitr/mixsplitr_menus.py`: prompt-toolkit menu flows
- `mixsplitr/splitter_ui.py`: optional visual waveform splitter
- `mixsplitr/mixsplitr_editor.py`: preview cache display/editor flow

## Runtime Support

- `mixsplitr/mixsplitr_memory.py`: RAM awareness + batching helpers
- `mixsplitr/mixsplitr_record.py`: session record helpers
- `mixsplitr/mixsplitr_manifest.py`: manifest persistence and comparisons

## Build Support

- `mixsplitr/rthook_ffmpeg.py`: PyInstaller runtime hook for ffmpeg binaries
- `packaging/MixSplitR_ONEFILE.spec`: onefile packaging spec
