"""CLI entry points: specimux-suite and specimux-replay."""

import argparse
import logging
import sys
from pathlib import Path

from .config import PipelineConfig


def main():
    parser = argparse.ArgumentParser(
        prog="specimux-suite",
        description="Orchestrate the Mycomap fungal DNA barcoding pipeline",
    )
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- batch ---
    batch_parser = subparsers.add_parser("batch", help="Run pipeline in batch mode")
    batch_parser.add_argument("primers", type=Path, help="Primers FASTA file")
    batch_parser.add_argument("specimens", type=Path, help="Specimens TSV file")
    batch_parser.add_argument("reads", type=Path, help="Input FASTQ file")
    _add_common_args(batch_parser)

    # --- live ---
    live_parser = subparsers.add_parser("live", help="Run pipeline in live watch mode")
    live_parser.add_argument("primers", type=Path, help="Primers FASTA file")
    live_parser.add_argument("specimens", type=Path, help="Specimens TSV file")
    live_parser.add_argument("watch_dir", type=Path, help="Directory to watch for FASTQ files")
    _add_common_args(live_parser)
    live_parser.add_argument("--settle-time", type=float, default=30.0,
                             help="Seconds to wait for file to stabilize (default: 30)")
    live_parser.add_argument("--presample", type=int, default=100,
                             help="Subsample reads for incremental consensus; 0 = no limit (default: 100)")

    # Handle --list-profiles early (before subparser dispatch)
    if '--list-profiles' in sys.argv:
        from .profiles import print_profiles_list
        print_profiles_list()
        sys.exit(0)

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    # Load suite profile if specified
    profile = None
    specimux_profile = None
    speconsense_profile = None
    summarize_profile = None
    specimux_overrides = {}
    speconsense_overrides = {}
    summarize_overrides = {}
    if args.profile:
        from .profiles import SuiteProfile
        profile = SuiteProfile.load(args.profile)

        # Apply suite section defaults (CLI args override)
        _suite_key_map = {
            "min-reads": "min_reads",
            "reprocess-ratio": "reprocess_ratio",
            "workers": "workers",
            "live-presample": "presample",
        }
        for yaml_key, attr_name in _suite_key_map.items():
            if yaml_key in profile.suite:
                # Only apply if user didn't explicitly pass the CLI flag
                if not _arg_was_explicit(args, attr_name):
                    setattr(args, attr_name, profile.suite[yaml_key])

        # Extract specimux profile and overrides
        specimux_section = dict(profile.specimux)
        specimux_profile = specimux_section.pop("profile", None)
        specimux_overrides = specimux_section

        # Extract speconsense profile and overrides
        speconsense_section = dict(profile.speconsense)
        speconsense_profile = speconsense_section.pop("profile", None)
        speconsense_overrides = speconsense_section

        # Extract summarize profile and overrides
        summarize_section = dict(profile.summarize)
        summarize_profile = summarize_section.pop("profile", None)
        summarize_overrides = summarize_section

        # Apply identify section
        _identify_key_map = {
            "vsearch-min-identity": "vsearch_min_identity",
            "vsearch-max-accepts": "vsearch_max_accepts",
            "identify-min-coverage": "identify_min_coverage",
        }
        for yaml_key, attr_name in _identify_key_map.items():
            if yaml_key in profile.identify:
                if not _arg_was_explicit(args, attr_name):
                    setattr(args, attr_name, profile.identify[yaml_key])

    # Handle --share: bind to all interfaces and detect LAN IP
    web_host = args.web_host
    share_url = None
    share_max_clients = 0
    if args.share is not None:
        web_host = "0.0.0.0"
        share_max_clients = args.share
        lan_ip = _detect_lan_ip()
        if lan_ip:
            share_url = f"http://{lan_ip}:{args.web_port}"
            logging.getLogger(__name__).info(f"Sharing dashboard at {share_url} (max {share_max_clients} clients)")
        else:
            logging.getLogger(__name__).warning("Could not detect LAN IP; QR code will not be shown")

    config = PipelineConfig(
        primers_file=args.primers,
        specimens_file=args.specimens,
        reads_file=getattr(args, "reads", None),
        watch_dir=getattr(args, "watch_dir", None),
        output_dir=args.output_dir,
        reference_db=args.reference_db,
        min_reads=args.min_reads,
        reprocess_ratio=args.reprocess_ratio,
        workers=args.workers,
        specimux_profile=specimux_profile,
        speconsense_profile=speconsense_profile,
        specimux_overrides=specimux_overrides,
        speconsense_overrides=speconsense_overrides,
        specimux_args=args.specimux_args if args.specimux_args else [],
        speconsense_args=args.speconsense_args if args.speconsense_args else [],
        summarize_profile=summarize_profile,
        summarize_overrides=summarize_overrides,
        summarize_args=args.summarize_args if args.summarize_args else [],
        identify_min_coverage=args.identify_min_coverage,
        web_host=web_host,
        web_port=args.web_port,
        share_url=share_url,
        share_max_clients=share_max_clients,
        settle_time=getattr(args, "settle_time", 30.0),
        live_presample=getattr(args, "presample", 100),
    )

    from .pipeline import Pipeline
    pipeline = Pipeline(config)

    # Start web server in background (both modes)
    if not args.no_web:
        from .web.server import start_web_server
        start_web_server(pipeline.event_log, pipeline.state, config)
        if not args.no_open:
            url = f"http://localhost:{config.web_port}"
            # Only auto-open in a graphical session, and never on the main
            # thread: on headless Linux webbrowser falls back to text-mode
            # browsers (www-browser/lynx) whose open() blocks until the user
            # quits — and that browser would own the TTY the console needs.
            import os
            if sys.platform == "darwin" or os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
                import threading
                import webbrowser
                threading.Thread(
                    target=webbrowser.open, args=(url,),
                    name="browser-open", daemon=True,
                ).start()
            else:
                logging.getLogger(__name__).info(f"Dashboard available at {url}")

    if args.command == "batch":
        pipeline.run_batch()
        # Show the results screen only on natural completion — a user who
        # quit with [Q] wants out, not another keypress prompt.
        if not pipeline.was_shutdown and sys.stdin.isatty() and sys.stderr.isatty():
            import queue
            from .console import ConsoleUI
            cmd_queue = queue.Queue()
            with ConsoleUI("batch_complete", pipeline.state, cmd_queue) as console:
                try:
                    while True:
                        try:
                            cmd = cmd_queue.get(timeout=1.0)
                            if cmd == "enter":
                                break
                        except queue.Empty:
                            console.redraw()
                except (KeyboardInterrupt, EOFError):
                    pass
    elif args.command == "live":
        pipeline.run_live()


def _add_common_args(parser: argparse.ArgumentParser):
    """Add arguments common to batch and live modes."""
    parser.add_argument("-p", "--profile", type=str, default=None,
                        help="Load a suite profile preset")
    parser.add_argument("--list-profiles", action="store_true",
                        help="List available suite profiles and exit")
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("specimux-suite-output"),
                        help="Output directory (default: specimux-suite-output)")
    parser.add_argument("--reference-db", type=Path, default=None,
                        help="Reference FASTA for identification")
    parser.add_argument("--min-reads", type=int, default=30,
                        help="Minimum reads before consensus (default: 30)")
    parser.add_argument("--reprocess-ratio", type=float, default=0.5,
                        help="New/old reads ratio to trigger reprocessing (default: 0.5)")
    parser.add_argument("--workers", type=int, default=0,
                        help="Total processing cores — specimux uses all, consensus uses 1 each (default: half of CPU cores)")
    parser.add_argument("--specimux-args", nargs=argparse.REMAINDER, default=[],
                        help="Extra arguments passed through to specimux")
    parser.add_argument("--speconsense-args", nargs=argparse.REMAINDER, default=[],
                        help="Extra arguments passed through to speconsense")
    parser.add_argument("--summarize-args", nargs=argparse.REMAINDER, default=[],
                        help="Extra arguments passed through to speconsense-summarize")
    parser.add_argument("--web-host", default="127.0.0.1",
                        help="Web server host (default: 127.0.0.1)")
    parser.add_argument("--web-port", type=int, default=8077,
                        help="Web server port (default: 8077)")
    parser.add_argument("--share", nargs="?", type=int, const=20, default=None, metavar="MAX_CLIENTS",
                        help="Share dashboard on LAN with QR code (default max: 20 clients)")
    parser.add_argument("--no-web", action="store_true",
                        help="Disable the web dashboard")
    parser.add_argument("--identify-min-coverage", type=float, default=0.5,
                        help="Minimum coverage (max of query/target) for identification hits (default: 0.5)")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't auto-open the dashboard in a browser")


def _arg_was_explicit(args: argparse.Namespace, attr_name: str) -> bool:
    """Check if a CLI argument was explicitly provided by the user.

    Uses argparse's internal tracking: if an attribute has its default value
    and wasn't in the original argv, it wasn't explicit. We rely on checking
    sys.argv for the flag form.
    """
    # Map attribute names to CLI flag forms
    flag = f"--{attr_name.replace('_', '-')}"
    short_flags = {
        "profile": "-p",
        "output_dir": "-o",
    }
    short = short_flags.get(attr_name)
    for arg in sys.argv:
        if arg == flag or (short and arg == short):
            return True
        if arg.startswith(f"{flag}="):
            return True
    return False


def _detect_lan_ip() -> str | None:
    """Detect the machine's LAN IP address by connecting to a public DNS server."""
    import socket
    try:
        # Connect to a public IP (doesn't actually send data) to find our route
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return None


def replay_main():
    parser = argparse.ArgumentParser(
        prog="specimux-replay",
        description="Replay a FASTQ file as incremental MinKNOW-style chunks",
    )
    parser.add_argument("source", type=Path, help="Source FASTQ file to split")
    parser.add_argument("output_dir", type=Path, help="Output directory for chunks")
    parser.add_argument("--reads-per-file", type=int, default=4000,
                        help="Reads per output file (default: 4000)")
    parser.add_argument("--delay", type=float, default=30.0,
                        help="Delay in seconds between files (default: 30)")
    parser.add_argument("--gzip", action="store_true",
                        help="Compress output files (.fastq.gz)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    from .replay import simulate
    simulate(args.source, args.output_dir, args.reads_per_file, args.delay, compress=args.gzip)
