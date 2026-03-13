"""CLI entry points: specimux-suite and specimux-simulate."""

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
    live_parser.add_argument("--watch-pattern", default="*.fastq",
                             help="Glob pattern for FASTQ files (default: *.fastq)")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

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
        specimux_args=args.specimux_args if args.specimux_args else [],
        speconsense_args=args.speconsense_args if args.speconsense_args else [],
        web_host=args.web_host,
        web_port=args.web_port,
        settle_time=getattr(args, "settle_time", 30.0),
        watch_pattern=getattr(args, "watch_pattern", "*.fastq"),
    )

    from .pipeline import Pipeline
    pipeline = Pipeline(config)

    if args.command == "batch":
        pipeline.run_batch()
    elif args.command == "live":
        # Start web server in background
        if not args.no_web:
            from .web.server import start_web_server
            start_web_server(pipeline.event_log, pipeline.state, config)
            if not args.no_open:
                import webbrowser
                url = f"http://localhost:{config.web_port}"
                webbrowser.open(url)
        pipeline.run_live()


def _add_common_args(parser: argparse.ArgumentParser):
    """Add arguments common to batch and live modes."""
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
    parser.add_argument("--web-host", default="0.0.0.0",
                        help="Web server host (default: 0.0.0.0)")
    parser.add_argument("--web-port", type=int, default=8077,
                        help="Web server port (default: 8077)")
    parser.add_argument("--no-web", action="store_true",
                        help="Disable the web dashboard")
    parser.add_argument("--no-open", action="store_true",
                        help="Don't auto-open the dashboard in a browser")


def simulate_main():
    parser = argparse.ArgumentParser(
        prog="specimux-simulate",
        description="Simulate MinION FASTQ output for testing",
    )
    parser.add_argument("source", type=Path, help="Source FASTQ file to split")
    parser.add_argument("output_dir", type=Path, help="Output directory for chunks")
    parser.add_argument("--reads-per-file", type=int, default=4000,
                        help="Reads per output file (default: 4000)")
    parser.add_argument("--delay", type=float, default=30.0,
                        help="Delay in seconds between files (default: 30)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    from .simulator import simulate
    simulate(args.source, args.output_dir, args.reads_per_file, args.delay)
