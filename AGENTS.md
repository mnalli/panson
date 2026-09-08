# AGENTS.md

For now, **ignore everything under example/**.

## Project

Panson is an interactive sonification framework built on top of [`sc3nb`](https://github.com/interactive-sonification/sc3nb). It is designed to support usage inside Jupyter notebooks: framework objects render themselves as `ipywidgets` (via `_ipython_display_`) so users can tweak sonification parameters live while data is being sonified.

## Commands

- Install (editable, with dev deps): `uv sync` (uses `uv.lock`), or `pip install -e .`
- Run a command in the project environment: `uv run <command>`
- Lint: `uv run ruff check .`
- Format: `uv run ruff format .`
- Run all pre-commit hooks: `uv run pre-commit run --all-files`

Packaging uses the `uv_build` backend (no setuptools) via `[build-system]` in `pyproject.toml`.

## Architecture

The package (`src/panson/`) is organized around a real-time data pipeline: **Stream → Preprocessor → Sonification → SuperCollider server**, with optional live plotting and video playback synced alongside it. Every submodule defines `__all__`, and `panson/__init__.py` re-exports everything with `from .module import *`, so new public classes must be added to both the module's `__all__` and the aggregation in `__init__.py`.

- **`sonification.py`** — the core abstraction. `Sonification` is an ABC with a fixed lifecycle: `init_parameters()` (set defaults), `init_server()` (returns a `Bundler` of OSC setup messages — SynthDefs, buffers, busses), `start()`/`stop()` (per-run OSC messages), and `_process(row)` (per-data-row OSC messages, called through the public, lock-guarded `process()`). All lifecycle methods return `sc3nb.osc.osc_communication.Bundler` objects rather than sending directly; callers are responsible for sending them to the server. `Parameter` is a descriptor for plain (non-widget) sonification parameters that serializes access through an `RLock` so a parameter can't be reassigned mid-computation. `GroupSonification` composes several `Sonification` instances (which must share the same server) so they can be started/stopped/processed as one unit.
- **`widget_parameters.py`** — `WidgetParameter` and its subclasses (`IntSliderParameter`, `FloatSliderParameter`, `DbSliderParameter`, `SelectParameter`, `CheckboxParameter`, etc.) are descriptors, declared as class attributes on `Sonification` subclasses, that lazily create and back themselves with an `ipywidgets` control. Reading/writing the attribute reads/writes the underlying widget's `.value`, under the same instance lock as `Parameter`. `Sonification._ipython_display_` scans `self.__dict__` for the `__*_widget` private attributes these descriptors create and displays them as a `VBox`, which is how the auto-generated GUI panel is built.
- **`streams.py`** — `Stream` wraps a generator function (`datagen`) that must yield a header array first, then equally-shaped numpy rows forever. A `Stream` can optionally carry a `Preprocessor` type; `open()` wraps the raw generator so each row is preprocessed (as a named `pandas.Series`) before being handed to the caller. `test()` samples the generator to validate shape/dtype consistency and measure FPS. `CsvFifo`, `DummySin`, `DummySinCos`, `NoneStream` are concrete example streams.
- **`preprocessors.py`** — `Preprocessor` is a minimal ABC (`preprocess(row: pd.Series) -> None`, mutates in place) instantiated fresh per stream `open()` call, so it can hold per-run state.
- **`data_players.py`** (the largest module) — orchestrates streams + sonification + optional `RTFeatureDisplay`/video player into a playable session:
  - `DataPlayer` — plays back a pre-recorded (offline) pandas DataFrame at a controllable rate.
  - `RTDataPlayer` — single real-time `Stream`, pumped by a worker `Thread`; on each row it sends `sonification.process(row)` to the server, and feeds `DataLogger`/`RTFeatureDisplay` if attached.
  - `RTDataPlayerMT` — joins multiple real-time streams with different frame rates using threads, taking the latest sample from each stream when computing sonification. Subject to GIL contention.
  - `RTDataPlayerMP` — same multi-stream joining but backed by `multiprocessing`, for when per-stream processing is CPU-heavy enough that the GIL would bottleneck `RTDataPlayerMT`.
  - `DataLogger` — CSV logging of stream rows, attachable to any real-time player.
- **`feature_displays.py`** — `RTFeatureDisplay` is a live matplotlib (`FuncAnimation`, `fivethirtyeight` style) plot fed row-by-row alongside sonification, for visually inspecting the data being sonified in real time.
- **`video_players.py`** — `VideoPlayer`/`RTVideoPlayer` play back video frames in sync with a data/sonification session. Playback runs in a separate process (`VideoPlayerServer`/`RTVideoPlayerServer`, using `cv2`) communicating over an `mp.connection.Connection` pipe, deliberately bypassing the GIL — same rationale as `RTDataPlayerMP`. This code originates from external prototypes (`videoviewer.py`, `vcr_gui_pipe.py` from the "physioson" project), noted in the module docstring.
- **`views.py`** — `ipywidgets`-based GUI chrome (`DataPlayerWidgetView`, `RTDataPlayerWidgetView`, `RTDataPlayerMultiWidgetView`) that the data player classes display via `_ipython_display_`; these hold transport controls (play/pause/record/log) and reference their data player via a `weakref.proxy` to avoid reference cycles.

### Concurrency model

Because everything is meant to run live in a Jupyter kernel, two concurrency strategies coexist:
1. **Threads + locks** for I/O-bound or GIL-friendly work (stream reading in `RTDataPlayer`, parameter updates from widgets) — correctness relies on the `RLock` in `Sonification`/`WidgetParameter` to keep a parameter write from tearing a `process()` call.
2. **Separate processes** for CPU-bound or blocking work that would otherwise stall the GIL (`RTDataPlayerMP`, all of `video_players.py`) — these communicate over `multiprocessing` pipes/connections rather than shared memory.

New real-time components should fit into one of these two patterns rather than inventing a third.

## Examples

`examples/facial-feature-sonification/` demonstrates driving Panson from [OpenFace](https://github.com/TadasBaltrusaitis/OpenFace) facial-feature extraction running in Docker (`docker compose up -d`, then `docker compose exec openface build/bin/FeatureExtraction ...`), streaming features into Panson via a named pipe (see `CsvFifo` in `streams.py`). This is currently how interactive behavior is tested.
