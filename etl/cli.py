"""
cli.py — single operational entry point.

    python -m etl.cli info                 # show resolved config (no secrets)
    python -m etl.cli validate             # run the setup smoke test
    python -m etl.cli run                  # full batch load + analytics + export
    python -m etl.cli run --incremental    # incremental (merge) demo cycle

An ops team schedules ONE command; strategy/destination come from config, so the
same invocation works in dev and prod. Exit codes are meaningful for CI/cron.
"""

from __future__ import annotations

import argparse
import sys

from etl.config import load_config
from etl.logging_setup import configure_from


def cmd_info(cfg) -> int:
    print(f"profile        : {cfg.profile}")
    print(f"pipeline       : {cfg.pipeline_name}")
    print(f"load strategy  : {cfg.load_strategy}")
    print(f"source         : {cfg.source['type']} ({cfg.source.get('path', 'n/a')})")
    print(f"destination    : {cfg.destination['type']}")
    print(f"fq table       : {cfg.fq_table}")
    print(f"quality gate   : {'on' if cfg.quality['enabled'] else 'off'} "
          f"(fail_on_error={cfg.quality['fail_on_error']})")
    print(f"log            : {cfg.runtime['log_level']} / {cfg.runtime['log_format']}")
    return 0


def cmd_validate() -> int:
    from scripts import smoke_test
    return smoke_test.main()


def cmd_run(cfg, incremental: bool) -> int:
    if incremental or cfg.load_strategy == "incremental":
        from demos import incremental_demo
        incremental_demo.main()
        return 0
    from demos import pipeline
    pipeline.main()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="etl.cli", description="Data pipeline operations")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("info", help="print resolved configuration")
    sub.add_parser("validate", help="run environment smoke test")
    run = sub.add_parser("run", help="execute the pipeline")
    run.add_argument("--incremental", action="store_true",
                     help="force incremental (merge) strategy for this run")
    sub.add_parser("run-multi", help="run the multi-source demo (SQL+SQL+CSV -> Parquet)")
    sub.add_parser("run-etl", help="run the declarative ETL framework (-> Delta Lake -> BI)")
    sub.add_parser("run-oracle", help="run the Oracle source example")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config()
    configure_from(cfg)

    if args.command == "info":
        return cmd_info(cfg)
    if args.command == "validate":
        return cmd_validate()
    if args.command == "run":
        return cmd_run(cfg, args.incremental)
    if args.command == "run-multi":
        from demos import multi_source_pipeline
        from demos.seeds import seed_sources
        seed_sources.main()
        report = multi_source_pipeline.run_all()
        print("\nBI report (from Parquet):\n")
        print(report.to_string(index=False))
        return 0
    if args.command == "run-etl":
        from demos.run_local_demo import run_seeded
        results = run_seeded()
        print("\nETL results (rows written per node):")
        for name, rows in results.items():
            print(f"  {name:<16} {rows}")
        return 0
    if args.command == "run-oracle":
        from demos.seeds import seed_oracle
        seed_oracle.main()
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
