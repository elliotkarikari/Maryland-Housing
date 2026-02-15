"""
Maryland Viability Atlas - Multi-Year Evidence Pipeline
Orchestrates the complete pipeline from timeseries features to final synthesis

Usage:
    python src/run_multiyear_pipeline.py [--as-of-year 2021] [--skip-timeseries] [--skip-scoring]

Steps:
    1. Compute timeseries features (level, momentum, stability)
    2. Compute layer summary scores (normalized 0-1)
    3. Classify counties (directional + confidence)
    4. Store final synthesis

All steps use multi-year evidence when available.
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path when running as a script
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.processing.timeseries_features import compute_all_timeseries_features
from src.processing.multiyear_scoring import compute_all_layer_scores
from src.processing.multiyear_classification import classify_all_counties, store_final_synthesis
from src.utils.logging import get_logger
from src.utils.year_policy import pipeline_default_year

logger = get_logger(__name__)
DEFAULT_YEAR = pipeline_default_year()


def run_pipeline(
    as_of_year: int = DEFAULT_YEAR,
    skip_timeseries: bool = False,
    skip_scoring: bool = False
):
    """
    Run the complete multi-year evidence pipeline.

    Args:
        as_of_year: Reference year for analysis
        skip_timeseries: Skip timeseries computation (use existing)
        skip_scoring: Skip scoring computation (use existing)
    """
    logger.info("=" * 80)
    logger.info("MARYLAND VIABILITY ATLAS - MULTI-YEAR EVIDENCE PIPELINE")
    logger.info("=" * 80)
    logger.info(f"As of year: {as_of_year}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    try:
        # STEP 1: Timeseries Features
        if not skip_timeseries:
            logger.info("─" * 80)
            logger.info("STEP 1/3: Computing Timeseries Features")
            logger.info("         (Level, Momentum, Stability)")
            logger.info("─" * 80)
            feature_count = compute_all_timeseries_features(
                window_size=5,
                as_of_year=as_of_year
            )
            logger.info(f"✓ Step 1 complete: {feature_count} feature records computed\n")
        else:
            logger.info("⏭  Skipping timeseries computation (using existing)\n")

        # STEP 2: Layer Scoring
        if not skip_scoring:
            logger.info("─" * 80)
            logger.info("STEP 2/3: Computing Layer Summary Scores")
            logger.info("         (Normalized 0-1 with Composition)")
            logger.info("─" * 80)
            scores_df = compute_all_layer_scores(as_of_year=as_of_year)
            logger.info(f"✓ Step 2 complete: {len(scores_df)} layer scores computed\n")
        else:
            logger.info("⏭  Skipping scoring computation (using existing)\n")

        # STEP 3: Classification & Final Synthesis
        logger.info("─" * 80)
        logger.info("STEP 3/3: Classifying Counties & Computing Final Synthesis")
        logger.info("         (Directional + Confidence + Grouping)")
        logger.info("─" * 80)
        classifications_df = classify_all_counties(as_of_year=as_of_year)

        if not classifications_df.empty:
            store_final_synthesis(classifications_df)
            logger.info(f"✓ Step 3 complete: {len(classifications_df)} counties classified\n")
        else:
            logger.error("✗ Step 3 failed: No classifications generated\n")
            return False

        # SUMMARY
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETE")
        logger.info("=" * 80)

        # Log final distribution
        grouping_dist = classifications_df['final_grouping'].value_counts()
        logger.info("\nFinal Synthesis Grouping Distribution:")
        for grouping, count in grouping_dist.items():
            logger.info(f"  {grouping}: {count} counties")

        directional_dist = classifications_df['directional_status'].value_counts()
        logger.info("\nDirectional Status Distribution:")
        for status, count in directional_dist.items():
            logger.info(f"  {status}: {count} counties")

        confidence_dist = classifications_df['confidence_level'].value_counts()
        logger.info("\nConfidence Level Distribution:")
        for conf, count in confidence_dist.items():
            logger.info(f"  {conf}: {count} counties")

        logger.info("\n" + "=" * 80)
        logger.info(f"✓ ALL STEPS COMPLETED SUCCESSFULLY")
        logger.info(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"✗ PIPELINE FAILED: {e}")
        logger.error("=" * 80)
        raise


def main():
    """Main execution with CLI argument parsing"""
    parser = argparse.ArgumentParser(
        description='Run the multi-year evidence pipeline for Maryland Viability Atlas'
    )
    parser.add_argument(
        '--year',
        type=int,
        default=DEFAULT_YEAR,
        help=f'Reference year for analysis (default: {DEFAULT_YEAR})'
    )
    parser.add_argument(
        '--skip-timeseries',
        action='store_true',
        help='Skip timeseries computation (use existing features)'
    )
    parser.add_argument(
        '--skip-scoring',
        action='store_true',
        help='Skip scoring computation (use existing scores)'
    )

    args = parser.parse_args()

    success = run_pipeline(
        as_of_year=args.year,
        skip_timeseries=args.skip_timeseries,
        skip_scoring=args.skip_scoring
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
