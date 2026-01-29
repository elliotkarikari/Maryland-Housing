"""
Maryland Viability Atlas - Main Pipeline Orchestration

Runs the complete analytics pipeline from data ingestion through export.

Pipeline stages:
1. Data ingestion (6 layers + policy persistence)
2. AI extraction (optional, if enabled)
3. Normalization
4. Scoring
5. Classification
6. GeoJSON export

Usage:
    python src/run_pipeline.py --level county --run-ai true
    python src/run_pipeline.py --export-only
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, List

from config.settings import get_settings
from config.database import test_connection
from src.utils.logging import setup_logging, get_logger

logger = setup_logging("pipeline")
settings = get_settings()


def check_prerequisites() -> bool:
    """
    Check that all prerequisites are met before running pipeline.

    Returns:
        True if all checks pass, False otherwise
    """
    logger.info("Checking prerequisites")

    # Check database connection
    if not test_connection():
        logger.error("Database connection failed")
        return False

    # Check required environment variables
    required_vars = ['DATABASE_URL', 'CENSUS_API_KEY']

    for var in required_vars:
        if not getattr(settings, var, None):
            logger.error(f"Required environment variable not set: {var}")
            return False

    logger.info("Prerequisites check passed")
    return True


def run_data_ingestion(
    data_year: int = None,
    layers: List[str] = None
) -> Dict[str, bool]:
    """
    Run data ingestion for specified layers.

    Args:
        data_year: Year to ingest (default: latest)
        layers: List of layer names to ingest (default: all)

    Returns:
        Dict mapping layer_name -> success/failure
    """
    logger.info(f"Starting data ingestion (data_year={data_year})")

    results = {}

    # Layer 1: Employment Gravity
    if not layers or 'employment' in layers:
        try:
            from src.ingest.layer1_employment import run_layer1_ingestion
            run_layer1_ingestion(data_year=data_year)
            results['employment_gravity'] = True
        except Exception as e:
            logger.error(f"Layer 1 ingestion failed: {e}", exc_info=True)
            results['employment_gravity'] = False

    # Policy Persistence (deterministic part)
    if not layers or 'policy' in layers:
        try:
            from src.ingest.policy_persistence import run_policy_persistence_ingestion
            run_policy_persistence_ingestion(
                data_year=data_year or 2025,
                include_ai=False  # AI runs separately
            )
            results['policy_persistence'] = True
        except Exception as e:
            logger.error(f"Policy persistence ingestion failed: {e}", exc_info=True)
            results['policy_persistence'] = False

    # Note: Layers 2-6 would be implemented similarly
    # For V1, we focus on Layer 1 + Policy Persistence as proof of concept

    logger.info(f"Data ingestion complete: {sum(results.values())}/{len(results)} successful")

    return results


def run_ai_extraction(cost_limit: float = 5.0) -> Dict:
    """
    Run AI extraction for CIP documents.

    Args:
        cost_limit: Maximum cost in USD

    Returns:
        Dict with extraction summary
    """
    logger.info(f"Starting AI extraction (cost_limit=${cost_limit})")

    if not settings.AI_ENABLED:
        logger.warning("AI extraction disabled (AI_ENABLED=false)")
        return {"status": "skipped", "reason": "AI_ENABLED=false"}

    try:
        from src.ai.pipeline.cip_extractor import run_cip_extraction_all

        summary = run_cip_extraction_all(
            force_refresh=False,
            cost_limit=cost_limit
        )

        logger.info(
            f"AI extraction complete: {summary['valid']}/{summary['processed']} valid, "
            f"cost ${summary['total_cost']:.4f}"
        )

        # Update policy persistence with AI data
        from src.ingest.policy_persistence import run_policy_persistence_ingestion
        run_policy_persistence_ingestion(data_year=2025, include_ai=True)

        return summary

    except Exception as e:
        logger.error(f"AI extraction failed: {e}", exc_info=True)
        return {"status": "failed", "error": str(e)}


def run_processing(
    data_year: int = None,
    skip_ai_features: bool = False
) -> bool:
    """
    Run normalization, scoring, and classification.

    Args:
        data_year: Year to process (default: latest)
        skip_ai_features: If True, skip AI-dependent features

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting processing pipeline (skip_ai={skip_ai_features})")

    try:
        # Normalization
        from src.processing.normalization import run_normalization

        logger.info("Running normalization")
        run_normalization(data_year=data_year, skip_ai_features=skip_ai_features)

        # Scoring
        from src.processing.normalization import normalize_all_layers
        from src.processing.scoring import run_scoring

        logger.info("Running scoring")
        normalized = normalize_all_layers(data_year=data_year, skip_ai_features=skip_ai_features)

        # Determine data year if not specified
        if data_year is None:
            for layer_df in normalized.values():
                if not layer_df.empty and 'data_year' in layer_df.columns:
                    data_year = layer_df['data_year'].iloc[0]
                    break

        layer_scores = run_scoring(normalized, data_year)

        # Classification
        from src.processing.classification import run_classification

        logger.info("Running classification")
        run_classification(layer_scores, data_year)

        logger.info("Processing pipeline complete")
        return True

    except Exception as e:
        logger.error(f"Processing pipeline failed: {e}", exc_info=True)
        return False


def run_export(level: str = "county") -> bool:
    """
    Run GeoJSON export.

    Args:
        level: Geography level ('county' for V1)

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting export (level={level})")

    try:
        from src.export.geojson_export import run_geojson_export

        result = run_geojson_export(level=level, versioned=True)

        logger.info(
            f"Export complete: {result['record_count']} features, "
            f"output: {result['latest_path']}"
        )

        return True

    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        return False


def main():
    """Main pipeline orchestration"""

    parser = argparse.ArgumentParser(
        description="Maryland Viability Atlas - Pipeline Orchestration"
    )

    parser.add_argument(
        "--level",
        type=str,
        default="county",
        choices=["county"],
        help="Geography level (default: county)"
    )

    parser.add_argument(
        "--run-ai",
        type=str,
        default="auto",
        choices=["true", "false", "auto"],
        help="Run AI extraction (auto=only if AI_ENABLED)"
    )

    parser.add_argument(
        "--ai-cost-limit",
        type=float,
        default=5.0,
        help="Maximum AI cost in USD (default: 5.0)"
    )

    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Only run export (skip ingestion and processing)"
    )

    parser.add_argument(
        "--data-year",
        type=int,
        help="Data year to process (default: latest available)"
    )

    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip data ingestion (use existing data)"
    )

    parser.add_argument(
        "--layers",
        type=str,
        nargs='+',
        help="Specific layers to ingest (default: all)"
    )

    args = parser.parse_args()

    # Pipeline start
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("Maryland Viability Atlas - Pipeline Start")
    logger.info(f"Time: {start_time.isoformat()}")
    logger.info(f"Arguments: {vars(args)}")
    logger.info("=" * 60)

    # Check prerequisites
    if not check_prerequisites():
        logger.error("Prerequisites check failed, exiting")
        sys.exit(1)

    # Determine AI execution
    run_ai = args.run_ai
    if run_ai == "auto":
        run_ai = "true" if settings.AI_ENABLED else "false"

    try:
        # Stage 1: Data Ingestion
        if not args.export_only and not args.skip_ingestion:
            logger.info("\n" + "=" * 60)
            logger.info("STAGE 1: DATA INGESTION")
            logger.info("=" * 60)

            ingestion_results = run_data_ingestion(
                data_year=args.data_year,
                layers=args.layers
            )

            if not any(ingestion_results.values()):
                logger.error("All ingestion layers failed, cannot continue")
                sys.exit(1)

        # Stage 2: AI Extraction (optional)
        if not args.export_only and run_ai == "true":
            logger.info("\n" + "=" * 60)
            logger.info("STAGE 2: AI EXTRACTION")
            logger.info("=" * 60)

            ai_results = run_ai_extraction(cost_limit=args.ai_cost_limit)

            if ai_results.get('status') == 'failed':
                logger.warning("AI extraction failed, continuing with degraded confidence")

        # Stage 3: Processing
        if not args.export_only:
            logger.info("\n" + "=" * 60)
            logger.info("STAGE 3: PROCESSING (Normalize, Score, Classify)")
            logger.info("=" * 60)

            skip_ai_features = (run_ai == "false")

            processing_success = run_processing(
                data_year=args.data_year,
                skip_ai_features=skip_ai_features
            )

            if not processing_success:
                logger.error("Processing failed, cannot export")
                sys.exit(1)

        # Stage 4: Export
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 4: GEOJSON EXPORT")
        logger.info("=" * 60)

        export_success = run_export(level=args.level)

        if not export_success:
            logger.error("Export failed")
            sys.exit(1)

        # Pipeline complete
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
