"""Adapt command-line interface.

Entry point: ``adapt``

Usage::

    adapt run-nexrad [config.yaml] --radar KLOT --mode realtime
    adapt run-nexrad --radar KDIX --base-dir /data/radar --mode historical \\
        --start-time 2025-03-05T15:00:00Z --end-time 2025-03-05T18:00:00Z

    adapt config [output_path]          # generate config.yaml template
    adapt dashboard [--repo /path]      # open GUI dashboard

The config file is optional. When omitted, ParamConfig expert defaults are
used. Any value from the config file can be overridden with CLI flags.
"""

import argparse
import os
import signal
import sys
import threading
import time
from pathlib import Path


# ---------------------------------------------------------------------------
# Single-instance enforcement
# ---------------------------------------------------------------------------

_PID_FILE = Path.home() / '.adapt' / 'pipeline.pid'


def _check_single_instance() -> None:
    """Exit with an error if another adapt run-nexrad is already running."""
    if not _PID_FILE.exists():
        return
    try:
        pid = int(_PID_FILE.read_text().strip())
        os.kill(pid, 0)  # signal 0 = existence check only
        print(f'[adapt] Error: A pipeline is already running (PID {pid}).')
        print(f'[adapt] Stop it first, or delete {_PID_FILE} if it is stale.')
        sys.exit(1)
    except (ProcessLookupError, PermissionError):
        pass  # process gone — stale PID file
    except ValueError:
        pass  # malformed PID file — ignore


def _write_pid() -> None:
    _PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _PID_FILE.write_text(str(os.getpid()))


def _remove_pid() -> None:
    try:
        _PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Sub-command: run-nexrad
# ---------------------------------------------------------------------------

def _build_run_nexrad_parser(sub: argparse.ArgumentParser) -> None:
    """Add arguments for the run-nexrad sub-command."""
    sub.add_argument(
        'config',
        nargs='?',
        default=None,
        help='Path to config file (.yaml or .py with CONFIG dict). '
             'Optional — falls back to expert defaults if omitted.',
    )
    sub.add_argument('--radar', help='Radar ID (e.g. KLOT, KDIX)')
    sub.add_argument(
        '--mode',
        choices=['realtime', 'historical'],
        help='Processing mode',
    )
    sub.add_argument('--start-time', dest='start_time', help='Start time (ISO 8601)')
    sub.add_argument('--end-time',   dest='end_time',   help='End time (ISO 8601)')
    sub.add_argument('--base-dir',   dest='base_dir',   help='Repository output directory')
    sub.add_argument('--run-id',     dest='run_id',     help='Resume a previous run by ID')
    sub.add_argument(
        '--max-runtime', dest='max_runtime', type=int,
        help='Max runtime in minutes (realtime mode only)',
    )
    sub.add_argument(
        '--rerun', action='store_true',
        help='Delete output directories before running',
    )
    sub.add_argument(
        '--no-plot', dest='no_plot', action='store_true',
        help='Disable plot consumer thread',
    )
    sub.add_argument(
        '--plot-interval', dest='plot_interval', type=float, default=2.0,
        help='Plot polling interval in seconds (default: 2.0)',
    )
    sub.add_argument(
        '--show-plots', dest='show_plots', action='store_true',
        help='Display plots in a live window',
    )
    sub.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable DEBUG logging',
    )


def _run_nexrad(args: argparse.Namespace) -> None:
    """Execute the NEXRAD processing pipeline."""
    _check_single_instance()
    _write_pid()

    from adapt.configuration.schemas import init_runtime_config
    from adapt.runtime.orchestrator import PipelineOrchestrator

    config = init_runtime_config(args)
    orchestrator = PipelineOrchestrator(config)

    stop_event = threading.Event()

    def _safe_stop(orch: PipelineOrchestrator) -> None:
        try:
            orch.stop()
        except Exception as exc:
            print(f'[adapt] Stop cleanup error (ignored): {exc}')

    def _run_orchestrator(orch: PipelineOrchestrator,
                          max_runtime: int,
                          done: threading.Event) -> None:
        try:
            orch.start(max_runtime=max_runtime)
        finally:
            done.set()

    def _handle_sigterm(signum, frame) -> None:
        print('\n[adapt] SIGTERM received — stopping pipeline...')
        stop_event.set()
        threading.Thread(
            target=_safe_stop, args=(orchestrator,), daemon=True
        ).start()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    if args.no_plot:
        print('Starting pipeline (plotting disabled)...')

    orchestrator_thread = threading.Thread(
        target=_run_orchestrator,
        args=(orchestrator, args.max_runtime, stop_event),
        name='OrchestratorRunner',
        daemon=False,
    )
    orchestrator_thread.start()

    # Give the orchestrator a moment to initialise the repository
    time.sleep(2)

    plot_consumer = None
    if not args.no_plot and orchestrator.repository is not None:
        from adapt.visualization.plotter import PlotConsumer
        radar = args.radar or config.downloader.radar
        plot_output_dir = Path(config.output_dirs['base']) / radar / 'plots'
        plot_consumer = PlotConsumer(
            repository=orchestrator.repository,
            stop_event=stop_event,
            output_dir=plot_output_dir,
            config=config,
            poll_interval=args.plot_interval,
            show_live=args.show_plots,
            name='PlotConsumer',
        )
        plot_consumer.start()

    try:
        orchestrator_thread.join()
    except KeyboardInterrupt:
        print('\nShutdown signal received — stopping pipeline...')
        # orchestrator runs in a non-main thread so it never sees KeyboardInterrupt;
        # call stop() explicitly so _main_loop breaks on the next iteration.
        threading.Thread(target=_safe_stop, args=(orchestrator,), daemon=True).start()
        orchestrator_thread.join(timeout=20)
        if orchestrator_thread.is_alive():
            print('Warning: orchestrator did not stop within 20 s')
    finally:
        stop_event.set()
        if plot_consumer is not None and plot_consumer.is_alive():
            print('Waiting for plot consumer to finish...')
            plot_consumer.join(timeout=10)
            if plot_consumer.is_alive():
                print('Warning: Plot consumer did not stop cleanly')
        _remove_pid()
        print('Pipeline shutdown complete.')


# ---------------------------------------------------------------------------
# Sub-command: config  (generate config.yaml template)
# ---------------------------------------------------------------------------

_CONFIG_TEMPLATE = """\
# Adapt Pipeline Configuration
# Generated: {timestamp}
#
# Usage:
#   adapt run-nexrad config.yaml --radar KLOT --mode realtime
#   adapt run-nexrad config.yaml --radar KDIX --mode historical \\
#       --start-time 2025-03-05T15:00:00Z --end-time 2025-03-05T18:00:00Z
#
# All settings below override the built-in defaults for given run. 
# CLI options (e.g. --radar, --mode) take precedence over config file values.

# --------------------------------------------------------------------------
# PIPELINE TARGET
# --------------------------------------------------------------------------
radar: KHTX                 # NWS NEXRAD radar ID (e.g. KLOT, KHTX, KDIX)
mode: realtime              # "realtime" or "historical"
base_dir: ""                # Output directory (override with --base-dir)

# --------------------------------------------------------------------------
# REALTIME MODE
# --------------------------------------------------------------------------
latest_files: 5             # Number of latest AWS files to keep
latest_minutes: 60          # Rolling window for file selection (minutes)
poll_interval_sec: 30       # Seconds between AWS polls

# --------------------------------------------------------------------------
# HISTORICAL MODE  (uncomment start_time / end_time to activate)
# --------------------------------------------------------------------------
# start_time: "2025-03-05T15:00:00Z"
# end_time:   "2025-03-05T18:00:00Z"

# --------------------------------------------------------------------------
# GRID  (Cartesian regridding)
# --------------------------------------------------------------------------
grid_shape: [41, 301, 301]  # [z, y, x] number of grid points
grid_limits:
  - [0, 20000]              # z: 0-20 km altitude (metres)
  - [-150000, 150000]       # y: +-150 km south-north (metres)
  - [-150000, 150000]       # x: +-150 km west-east  (metres)

# --------------------------------------------------------------------------
# CELL DETECTION
# --------------------------------------------------------------------------
z_level: 2000               # Analysis altitude in metres above radar
threshold: 40               # dBZ threshold for cell detection
segmentation_method: threshold
min_cellsize_gridpoint: 5   # Minimum cell area (grid points)
# max_cellsize_gridpoint: null   # No upper limit (comment out to disable)

# --------------------------------------------------------------------------
# CELL PROJECTION  (optical-flow motion tracking)
# --------------------------------------------------------------------------
projection_method: adapt_default   # Optical-flow forward-projected hull
max_projection_steps: 5
"""


def _build_config_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        'output',
        nargs='?',
        default=None,
        help='Path where config.yaml will be written. '
             'Defaults to ./config.yaml in the current directory.',
    )


def _config_cmd(args: argparse.Namespace) -> None:
    """Write a config.yaml template to the specified path."""
    from datetime import datetime

    out = Path(args.output) if args.output else Path.cwd() / 'config.yaml'

    if out.is_dir():
        out = out / 'config.yaml'

    if out.exists():
        print(f'[adapt config] File already exists: {out}')
        answer = input('Overwrite? [y/N] ').strip().lower()
        if answer != 'y':
            print('Aborted.')
            return

    out.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    out.write_text(_CONFIG_TEMPLATE.format(timestamp=timestamp))
    print(f'Config written: {out}')
    print(f'Edit it, then run:  adapt run-nexrad {out} --radar KLOT')


# ---------------------------------------------------------------------------
# Sub-command: dashboard  (GUI)
# ---------------------------------------------------------------------------

def _build_dashboard_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        '--repo',
        default=None,
        help='Path to the Adapt output repository (pre-populates the repo field).',
    )


def _dashboard_cmd(args: argparse.Namespace) -> None:
    """Launch the Adapt GUI dashboard."""
    import os
    try:
        os.getcwd()
    except FileNotFoundError:
        os.chdir(os.path.expanduser("~"))
    from adapt.gui import main
    main(repo=args.repo)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Top-level CLI dispatcher."""
    parser = argparse.ArgumentParser(
        prog='adapt',
        description='Adapt - Real-Time data processing for informed adaptive scanning of ARM weather radars.',
    )
    subparsers = parser.add_subparsers(dest='command', metavar='COMMAND')
    subparsers.required = True

    run_nexrad_parser = subparsers.add_parser(
        'run-nexrad',
        help='Run the NEXRAD processing pipeline.',
        description='Download and process NEXRAD Level-II data.',
    )
    _build_run_nexrad_parser(run_nexrad_parser)
    run_nexrad_parser.set_defaults(func=_run_nexrad)

    config_parser = subparsers.add_parser(
        'config',
        help='Generate a config.yaml template.',
        description='Write a commented YAML configuration template.',
    )
    _build_config_parser(config_parser)
    config_parser.set_defaults(func=_config_cmd)

    dashboard_parser = subparsers.add_parser(
        'dashboard',
        help='Open the GUI dashboard.',
        description='Launch the Adapt radar dashboard (read-only consumer).',
    )
    _build_dashboard_parser(dashboard_parser)
    dashboard_parser.set_defaults(func=_dashboard_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
