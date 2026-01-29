"""
Maryland Viability Atlas - V2 Pipeline Orchestration

Runs the multi-year evidence pipeline and GeoJSON export.

Pipeline stages (V2):
1. Timeseries features (level, momentum, stability)
2. Multi-year layer scoring
3. Multi-year classification (final synthesis)
4. GeoJSON export

Usage:
    python src/run_pipeline.py --year 2025
    python src/run_pipeline.py --export-only
"""

import argparse
import sys
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.database import test_connection
from src.utils.logging import setup_logging
from src.run_multiyear_pipeline import run_pipeline as run_multiyear_pipeline
from src.export.geojson_export import run_geojson_export

logger = setup_logging("pipeline")


def check_prerequisites() -> bool:
    """
    Check that prerequisites are met before running pipeline.

    Returns:
        True if all checks pass, False otherwise
    """
    logger.info("Checking prerequisites")

    if not test_connection():
        logger.error("Database connection failed")
        return False

    logger.info("Prerequisites check passed")
    return True


def main():
    """Main pipeline orchestration"""

    parser = argparse.ArgumentParser(
        description="Maryland Viability Atlas - V2 Pipeline Orchestration"
    )

    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="As-of year for multi-year synthesis (default: 2025)"
    )

    parser.add_argument(
        "--skip-timeseries",
        action="store_true",
        help="Skip timeseries computation (use existing features)"
    )

    parser.add_argument(
        "--skip-scoring",
        action="store_true",
        help="Skip multi-year scoring (use existing scores)"
    )

    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Only run export (skip timeseries + scoring + classification)"
    )

    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Skip GeoJSON export after pipeline"
    )

    args = parser.parse_args()

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Maryland Viability Atlas - V2 Pipeline Start")
    logger.info(f"Time: {start_time.isoformat()}")
    logger.info(f"Arguments: {vars(args)}")
    logger.info("=" * 60)

    if not check_prerequisites():
        logger.error("Prerequisites check failed, exiting")
        sys.exit(1)

    try:
        if not args.export_only:
            logger.info("\n" + "=" * 60)
            logger.info("STAGE 1-3: MULTI-YEAR PIPELINE")
            logger.info("=" * 60)

            success = run_multiyear_pipeline(
                as_of_year=args.year,
                skip_timeseries=args.skip_timeseries,
                skip_scoring=args.skip_scoring
            )

            if not success:
                logger.error("Multi-year pipeline failed")
                sys.exit(1)

        if not args.no_export:
            logger.info("\n" + "=" * 60)
            logger.info("STAGE 4: GEOJSON EXPORT")
            logger.info("=" * 60)

            result = run_geojson_export(level="county", versioned=True)
            logger.info(
                f"Export complete: {result['record_count']} features, "
                f"output: {result['latest_path']}"
            )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"End time: {end_time.isoformat()}")
        logger.info("=" * 60)

        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(130)

    except Exception as e:
        logger.error(f"Pipeline failed with unhandled exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
