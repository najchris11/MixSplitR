# MixSplitR Development Guide

This repository is organized as a standard Python package.

## Layout

- `mixsplitr/`: application package
- `MixSplitR.py`: compatibility launcher
- `pyproject.toml`: packaging metadata and console script definition
- `packaging/`: PyInstaller spec and related packaging assets
- `docs/`: project and development documentation

## Local Setup

Create and activate a virtual environment on macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the project in editable mode:

```bash
pip install -e .
```

If you need optional audio or UI features, install the extra dependencies used by the app build you are testing.

## Running Locally

Use either of these entrypoints:

```bash
python MixSplitR.py
python -m mixsplitr
```

The console script declared in `pyproject.toml` is also available as `mixsplitr` after editable install.

## Validation

Useful sanity checks during development:

```bash
python3 -m compileall MixSplitR.py mixsplitr
python3 -m py_compile MixSplitR.py
```

## Packaging

The PyInstaller spec lives in `packaging/MixSplitR_ONEFILE.spec`.

It is configured for the package layout and should be kept in sync with any new runtime assets placed under `mixsplitr/`.

## Building

You can build the packaged app with PyInstaller using the spec file:

```bash
pyinstaller packaging/MixSplitR_ONEFILE.spec
```

That produces a one-file executable in `dist/` on the current platform.

If you are iterating on packaging changes, it is usually enough to:

```bash
python3 -m compileall MixSplitR.py mixsplitr
pyinstaller packaging/MixSplitR_ONEFILE.spec --clean
```

When build assets change, keep these in sync:

- `mixsplitr/` for runtime Python modules and helpers
- `packaging/MixSplitR_ONEFILE.spec` for packaging configuration
- `pyproject.toml` for console-script metadata

## Code Organization Notes

- Put application logic inside `mixsplitr/`.
- Keep `MixSplitR.py` as a thin compatibility wrapper.
- Prefer package-relative imports inside the package.
- Put architecture notes and workflow docs under `docs/`.