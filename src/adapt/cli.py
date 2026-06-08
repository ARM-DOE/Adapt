# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

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
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path

from adapt import __version__

# ---------------------------------------------------------------------------
# Single-instance enforcement
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

_PID_FILE = Path.home() / ".adapt" / "pipeline.pid"


def _check_single_instance() -> None:
    """Exit with an error if another adapt run-nexrad is already running."""
    if not _PID_FILE.exists():
        return
    try:
        pid = int(_PID_FILE.read_text().strip())
        os.kill(pid, 0)  # signal 0 = existence check only
        print(f"[adapt] Error: A pipeline is already running (PID {pid}).")
        print(f"[adapt] Stop it first, or delete {_PID_FILE} if it is stale.")
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
    except OSError as exc:
        logger.warning("Could not remove PID file %s: %s", _PID_FILE, exc)


# ---------------------------------------------------------------------------
# Sub-command: run-nexrad
# ---------------------------------------------------------------------------


def _build_run_nexrad_parser(sub: argparse.ArgumentParser) -> None:
    """Add arguments for the run-nexrad sub-command."""
    sub.add_argument(
        "config",
        nargs="?",
        default=None,
        help="Path to config file (.yaml or .py with CONFIG dict). "
        "Optional — falls back to expert defaults if omitted.",
    )
    sub.add_argument("--radar", help="Radar ID (e.g. KLOT, KDIX)")
    sub.add_argument(
        "--mode",
        choices=["realtime", "historical"],
        help="Processing mode",
    )
    sub.add_argument("--start-time", dest="start_time", help="Start time (ISO 8601)")
    sub.add_argument("--end-time", dest="end_time", help="End time (ISO 8601)")
    sub.add_argument("--base-dir", dest="base_dir", help="Repository output directory")
    sub.add_argument(
        "--run-id",
        dest="run_id",
        help="Continue with a run ID (format: YYYYMONDD-HHMM-RADAR, requires --base-dir)",
    )
    sub.add_argument(
        "--max-runtime",
        dest="max_runtime",
        type=int,
        help="Max runtime in minutes (realtime mode only)",
    )
    sub.add_argument(
        "--rerun",
        action="store_true",
        help="Delete output directories before running",
    )
    sub.add_argument(
        "--no-plot",
        dest="no_plot",
        action="store_true",
        help="Disable plot consumer thread",
    )
    sub.add_argument(
        "--plot-interval",
        dest="plot_interval",
        type=float,
        default=2.0,
        help="Plot polling interval in seconds (default: 2.0)",
    )
    sub.add_argument(
        "--show-plots",
        dest="show_plots",
        action="store_true",
        help="Display plots in a live window",
    )
    sub.add_argument(
        "--only",
        dest="only_modules",
        default=None,
        help="Run only these pipeline modules (comma-separated node names). "
        "Mutually exclusive with --not.",
    )
    sub.add_argument(
        "--not",
        dest="exclude_modules",
        default=None,
        help="Run all modules except these (comma-separated node names). "
        "Mutually exclusive with --only.",
    )
    sub.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )


def _run_nexrad(args: argparse.Namespace) -> None:
    """Execute the NEXRAD processing pipeline."""
    if getattr(args, "only_modules", None) and getattr(args, "exclude_modules", None):
        raise SystemExit("error: --only and --not are mutually exclusive")
    _check_single_instance()
    _write_pid()

    from adapt.configuration.schemas import init_runtime_config
    from adapt.runtime.orchestrator import PipelineOrchestrator

    config = init_runtime_config(args)
    orchestrator = PipelineOrchestrator(
        config,
        close_repository_on_stop=bool(args.no_plot),
    )

    stop_event = threading.Event()

    def _safe_stop(orch: PipelineOrchestrator) -> None:
        try:
            orch.stop()
        except Exception as exc:
            print(f"[adapt] Stop cleanup error (ignored): {exc}")

    def _run_orchestrator(
        orch: PipelineOrchestrator, max_runtime: int, done: threading.Event
    ) -> None:
        try:
            orch.start(max_runtime=max_runtime)
        finally:
            done.set()

    def _handle_sigterm(signum, frame) -> None:
        print("\n[adapt] SIGTERM received — stopping pipeline...")
        orchestrator._interrupted = True
        stop_event.set()
        threading.Thread(target=_safe_stop, args=(orchestrator,), daemon=True).start()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    if args.no_plot:
        print("Starting pipeline (plotting disabled)...")

    orchestrator_thread = threading.Thread(
        target=_run_orchestrator,
        args=(orchestrator, args.max_runtime, stop_event),
        name="OrchestratorRunner",
        daemon=False,
    )
    orchestrator_thread.start()

    plot_consumer = None
    try:
        # Inside the try so a Ctrl+C during startup still reaches the finally
        # block (PID file removal, repository close).
        time.sleep(2)

        if not args.no_plot and orchestrator.repository is not None:
            from adapt.visualization.plotter import PlotConsumer

            radar = args.radar or config.downloader.radar
            assert config.output_dirs is not None
            plot_output_dir = Path(config.output_dirs["base"]) / radar / "plots"
            plot_consumer = PlotConsumer(
                repository=orchestrator.repository,
                stop_event=stop_event,
                output_dir=plot_output_dir,
                config=config,
                poll_interval=args.plot_interval,
                show_live=args.show_plots,
                name="PlotConsumer",
            )
            plot_consumer.start()

        orchestrator_thread.join()

    except KeyboardInterrupt:
        print("\nShutdown signal received — stopping pipeline...")
        # Mark interrupted so the run is finalised as "cancelled" not "completed".
        orchestrator._interrupted = True
        stop_event.set()
        # The orchestrator runs in a worker thread and never receives
        # KeyboardInterrupt; set its stop flag explicitly.
        threading.Thread(target=_safe_stop, args=(orchestrator,), daemon=True).start()
        try:
            orchestrator_thread.join(timeout=20)
        except KeyboardInterrupt:
            print("[adapt] Forcing shutdown...")
        if orchestrator_thread.is_alive():
            print("Warning: orchestrator did not stop within 20 s")

    finally:
        stop_event.set()
        if plot_consumer is not None and plot_consumer.is_alive():
            print("Waiting for plot consumer to finish...")
            plot_consumer.join(timeout=10)
            if plot_consumer.is_alive():
                print("Warning: Plot consumer did not stop cleanly")
        orchestrator.close_repository()
        _remove_pid()
        print("Pipeline shutdown complete.")


# ---------------------------------------------------------------------------
# Sub-command: config  (generate config.yaml template)
# ---------------------------------------------------------------------------


def _build_config_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        "output",
        nargs="?",
        default=None,
        help="Path where config.yaml will be written. "
        "Defaults to ./config.yaml in the current directory.",
    )
    sub.add_argument(
        "--pipeline",
        default="nexrad",
        help="Pipeline whose modules' parameters are written (default: nexrad).",
    )
    sub.add_argument(
        "--extensions",
        default=None,
        help="Comma-separated extension module import paths to include "
        "(e.g. adapt.execution.nodes.cell_volume_stats).",
    )


def _config_cmd(args: argparse.Namespace) -> None:
    """Write a config.yaml template to the specified path."""
    from adapt.configuration.schemas.initialization import write_default_config

    try:
        cwd = Path.cwd()
        cwd_missing = False
    except FileNotFoundError:
        cwd = None
        cwd_missing = True

    if args.output:
        out = Path(args.output)
        if cwd_missing and not out.is_absolute():
            raise ValueError(
                "Current working directory no longer exists. "
                "Pass an absolute output path, e.g. `adapt config /path/to/config.yaml`."
            )
    else:
        if cwd_missing:
            raise FileNotFoundError(
                "Current working directory no longer exists. "
                "Run `cd` into an existing directory, or pass an absolute output path."
            )
        assert cwd is not None
        out = cwd / "config.yaml"

    if out.is_dir():
        out = out / "config.yaml"

    if out.exists():
        print(f"[adapt config] File already exists: {out}")
        answer = input("Overwrite? [y/N] ").strip().lower()
        if answer != "y":
            print("Aborted.")
            return

    pipeline = getattr(args, "pipeline", "nexrad")
    if pipeline != "nexrad":
        raise ValueError(f"Unknown pipeline '{pipeline}'. Only 'nexrad' is defined.")
    extensions_arg = getattr(args, "extensions", None)
    extensions = (
        [p.strip() for p in extensions_arg.split(",") if p.strip()] if extensions_arg else None
    )

    write_default_config(out, extensions=extensions)
    print(f"Config written: {out}")
    print(f"Edit it, then run:  adapt run-nexrad {out} --radar KLOT")


# ---------------------------------------------------------------------------
# Sub-command: dashboard  (GUI)
# ---------------------------------------------------------------------------


def _build_dashboard_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        "--repo",
        default=None,
        help="Path to the Adapt output repository (pre-populates the repo field).",
    )


def _dashboard_cmd(args: argparse.Namespace) -> None:
    """Launch the Adapt GUI dashboard."""
    import os

    try:
        os.getcwd()
    except FileNotFoundError:
        os.chdir(os.path.expanduser("~"))
    from adapt.consumers.live import main

    main(repo=args.repo)


# ---------------------------------------------------------------------------
# Sub-command: postprocess  (repository enrichment)
# ---------------------------------------------------------------------------


def _build_postprocess_parser(sub: argparse.ArgumentParser) -> None:
    sub.add_argument(
        "--repository",
        default=".",
        help="Path to an existing Adapt repository (default: current directory).",
    )
    sub.add_argument(
        "--module",
        nargs="+",
        default=None,
        help="Post-process module name(s) to run, e.g. --module lma. Accepts a list.",
    )
    sub.add_argument(
        "--input-dir",
        dest="input_dir",
        default=None,
        help="Input directory for the selected module(s) (e.g. LMA ASCII files).",
    )
    sub.add_argument("--config", default=None, help="Optional YAML config file.")
    sub.add_argument("-v", "--verbose", action="store_true", help="Verbose logging.")


def _open_repository(repo_root: str, config_path: str | None):
    """Discover the latest run in an existing repository and load its config.

    Returns ``(DataRepository, InternalConfig)``. Reuses the existing root
    registry for run/radar discovery and the single ``init_runtime_config``
    entrypoint (continuation fast-path) to reload the saved runtime config.
    """
    from types import SimpleNamespace

    from adapt.configuration.schemas.initialization import init_runtime_config
    from adapt.persistence import DataRepository
    from adapt.persistence.registry import RepositoryRegistry

    root = Path(repo_root).resolve()
    run = RepositoryRegistry.get_instance(root).get_latest_run()
    if run is None:
        raise ValueError(f"No runs found in repository: {root}")
    run_id, radar = run["run_id"], run["radar"]

    config = init_runtime_config(
        SimpleNamespace(run_id=run_id, base_dir=str(root), config=config_path)
    )
    repository = DataRepository(run_id=run_id, base_dir=root, radar=radar, config=config)
    return repository, config


def _postprocess_cmd(args: argparse.Namespace) -> None:
    """Run post-process extension modules over an existing repository."""
    from adapt.runtime.postprocessor import PostProcessor

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    if not args.module:
        raise ValueError("Specify at least one module to run, e.g. --module lma.")

    repository, config = _open_repository(args.repository, args.config)

    if args.input_dir:
        merged = dict(config.module_params)
        for name in args.module:
            merged[name] = {**merged.get(name, {}), "input_dir": args.input_dir}
        config = config.model_copy(update={"module_params": merged})

    PostProcessor(repository, config).run(modules=args.module)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Top-level CLI dispatcher."""
    parser = argparse.ArgumentParser(
        prog="adapt",
        description=(
            "Adapt - Real-Time data processing for informed adaptive scanning "
            "of ARM weather radars."
        ),
    )

    # Add version argument
    adapt_module_path = Path(__file__).parent
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}\nInstalled at: {adapt_module_path}",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    run_nexrad_parser = subparsers.add_parser(
        "run-nexrad",
        help="Run the NEXRAD processing pipeline.",
        description="Download and process NEXRAD Level-II data.",
    )
    _build_run_nexrad_parser(run_nexrad_parser)
    run_nexrad_parser.set_defaults(func=_run_nexrad)

    config_parser = subparsers.add_parser(
        "config",
        help="Generate a config.yaml template.",
        description="Write a commented YAML configuration template.",
    )
    _build_config_parser(config_parser)
    config_parser.set_defaults(func=_config_cmd)

    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="Open the GUI dashboard.",
        description="Launch the Adapt radar dashboard (read-only consumer).",
    )
    _build_dashboard_parser(dashboard_parser)
    dashboard_parser.set_defaults(func=_dashboard_cmd)

    postprocess_parser = subparsers.add_parser(
        "postprocess",
        help="Enrich an existing repository with extension tables.",
        description="Run post-process extension modules (e.g. lma) over a repository.",
    )
    _build_postprocess_parser(postprocess_parser)
    postprocess_parser.set_defaults(func=_postprocess_cmd)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
