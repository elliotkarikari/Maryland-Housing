"""
Maryland Viability Atlas - Classification Logic
Converts layer scores into directional classifications and confidence overlays

Directional Status (describes structural tailwinds):
- Improving: ≥3 layers above 0.6 AND none below 0.3
- At Risk: ≥2 layers below 0.3 OR (severe risk drag AND ≥1 layer below 0.4)
- Stable: all else

Confidence Overlay (from policy persistence):
- Strong: persistence score ≥ 0.67
- Conditional: 0.34 ≤ persistence < 0.67
- Fragile: persistence < 0.34

NO PREDICTIONS. Only directional pressure classification.
"""

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

from config.database import get_db, log_refresh, table_name
from config.settings import get_settings
from src.utils.db_bulk import execute_batch
from src.utils.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


def classify_directional_status(layer_scores: pd.Series, risk_drag_score: float) -> str:
    """
    Classify directional status based on layer scores.

    Args:
        layer_scores: Series of layer scores (excluding risk_drag)
        risk_drag_score: Risk drag score (0-1, higher = more risk)

    Returns:
        Classification: 'improving', 'stable', or 'at_risk'
    """
    # Filter out NaN scores
    valid_scores = layer_scores.dropna()

    if len(valid_scores) == 0:
        logger.warning("No valid layer scores for classification")
        return "stable"  # Default when no data

    # Count layers above/below thresholds
    high_performers = (valid_scores >= settings.THRESHOLD_IMPROVING_HIGH).sum()
    low_performers = (valid_scores < settings.THRESHOLD_IMPROVING_LOW).sum()

    # Check for severe risk drag
    severe_risk_drag = (
        pd.notna(risk_drag_score) and risk_drag_score >= 0.7  # High risk drag threshold
    )

    # IMPROVING: ≥3 layers above 0.6 AND none below 0.3
    if high_performers >= settings.THRESHOLD_IMPROVING_MIN_LAYERS and low_performers == 0:
        return "improving"

    # AT RISK: ≥2 layers below 0.3 OR (severe risk drag AND ≥1 layer below 0.4)
    if low_performers >= settings.THRESHOLD_AT_RISK_COUNT:
        return "at_risk"

    if severe_risk_drag:
        # With severe risk drag, lower the threshold
        at_risk_with_drag = (valid_scores < settings.THRESHOLD_AT_RISK_WITH_DRAG).sum()
        if at_risk_with_drag >= 1:
            return "at_risk"

    # STABLE: everything else
    return "stable"


def classify_confidence(policy_persistence_score: float) -> str:
    """
    Classify confidence based on policy persistence score.

    Args:
        policy_persistence_score: Policy persistence score (0-1)

    Returns:
        Classification: 'strong', 'conditional', or 'fragile'
    """
    if pd.isna(policy_persistence_score):
        # No policy data = default to conditional
        return "conditional"

    if policy_persistence_score >= settings.CONFIDENCE_STRONG_MIN:
        return "strong"
    elif policy_persistence_score >= settings.CONFIDENCE_CONDITIONAL_MIN:
        return "conditional"
    else:
        return "fragile"


def calculate_final_synthesis_grouping(
    directional_class: str,
    confidence_class: str,
    risk_drag_score: float,
    policy_persistence_score: float,
    layer_scores: pd.Series,
    classification_contested: bool = False,
) -> str:
    """
    Calculate final synthesis grouping for map display.

    This is the PRIMARY output for the interactive map, answering:
    "What is the structural trajectory of this place — and how reliable is that assessment?"

    Groupings:
    - emerging_tailwinds: Improving + Strong confidence + low-medium uncertainty
    - conditional_growth: Improving + Conditional confidence OR high uncertainty
    - stable_constrained: Stable + no severe risk drag
    - at_risk_headwinds: At Risk OR severe risk + weak policy
    - high_uncertainty: Fragile confidence OR contested classification OR sparse data

    Args:
        directional_class: Directional status (improving/stable/at_risk)
        confidence_class: Confidence level (strong/conditional/fragile)
        risk_drag_score: Risk drag score (0-1, higher = more risk)
        policy_persistence_score: Policy persistence score (0-1)
        layer_scores: Series of layer scores for data completeness check
        classification_contested: Whether classification is contested by claims

    Returns:
        Final synthesis grouping name
    """
    # Check data completeness
    valid_layers = layer_scores.dropna()
    data_sparse = len(valid_layers) < 2  # Less than 2 layers = sparse

    # Severe risk drag threshold
    severe_risk_drag = pd.notna(risk_drag_score) and risk_drag_score >= 0.5

    # Weak policy persistence
    weak_policy = pd.notna(policy_persistence_score) and policy_persistence_score < 0.3

    # HIGH UNCERTAINTY / CONTESTED
    # Takes precedence - if we can't trust the assessment, say so explicitly
    if confidence_class == "fragile" or classification_contested or data_sparse:
        return "high_uncertainty"

    # AT RISK / HEADWINDS
    # Structural headwinds dominate
    if directional_class == "at_risk" or (severe_risk_drag and weak_policy):
        return "at_risk_headwinds"

    # EMERGING TAILWINDS
    # Stacked tailwinds with high confidence
    if directional_class == "improving" and confidence_class == "strong":
        return "emerging_tailwinds"

    # CONDITIONAL GROWTH
    # Upside exists but execution matters
    if directional_class == "improving":
        # Either conditional confidence or other uncertainty factors
        return "conditional_growth"

    # STABLE BUT CONSTRAINED
    # Default for stable situations without severe headwinds
    return "stable_constrained"


def identify_top_strengths(layer_scores: Dict[str, float], top_n: int = 2) -> List[str]:
    """
    Identify top performing layers.

    Args:
        layer_scores: Dict of layer_name -> score
        top_n: Number of top layers to return

    Returns:
        List of layer names (top performers)
    """
    # Filter out NaN
    valid_scores = {k: v for k, v in layer_scores.items() if pd.notna(v)}

    if not valid_scores:
        return []

    # Sort by score (descending)
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1], reverse=True)

    return [name for name, score in sorted_layers[:top_n]]


def identify_top_weaknesses(layer_scores: Dict[str, float], top_n: int = 2) -> List[str]:
    """
    Identify weakest performing layers.

    Args:
        layer_scores: Dict of layer_name -> score
        top_n: Number of bottom layers to return

    Returns:
        List of layer names (weak performers)
    """
    # Filter out NaN
    valid_scores = {k: v for k, v in layer_scores.items() if pd.notna(v)}

    if not valid_scores:
        return []

    # Sort by score (ascending)
    sorted_layers = sorted(valid_scores.items(), key=lambda x: x[1], reverse=False)

    return [name for name, score in sorted_layers[:top_n]]


def generate_explainability_payload(row: pd.Series) -> Dict:
    """
    Generate explainability payload for a county.

    Args:
        row: DataFrame row with layer scores and classification

    Returns:
        Dict with explanation fields
    """
    # Extract layer scores
    layer_scores = {
        "employment_gravity": row.get("employment_gravity_score"),
        "mobility_optionality": row.get("mobility_optionality_score"),
        "school_trajectory": row.get("school_trajectory_score"),
        "housing_elasticity": row.get("housing_elasticity_score"),
        "demographic_momentum": row.get("demographic_momentum_score"),
    }

    # Identify strengths and weaknesses
    strengths = identify_top_strengths(layer_scores, top_n=2)
    weaknesses = identify_top_weaknesses(layer_scores, top_n=2)

    # Generate human-readable explanations
    layer_names = {
        "employment_gravity": "Employment Gravity",
        "mobility_optionality": "Mobility Optionality",
        "school_trajectory": "School System Trajectory",
        "housing_elasticity": "Housing Elasticity",
        "demographic_momentum": "Demographic Momentum",
    }

    primary_strengths = [layer_names.get(s, s) for s in strengths]
    primary_weaknesses = [layer_names.get(w, w) for w in weaknesses]

    # Key trends (based on classification)
    directional_class = row.get("directional_class", "stable")
    confidence_class = row.get("confidence_class", "conditional")

    key_trends = []

    if directional_class == "improving":
        key_trends.append("Multiple reinforcing structural tailwinds present")
    elif directional_class == "at_risk":
        key_trends.append("Structural headwinds constraining growth capacity")
    else:
        key_trends.append("Balanced signals, mixed pressure directions")

    if confidence_class == "strong":
        key_trends.append("High policy delivery reliability")
    elif confidence_class == "fragile":
        key_trends.append("Low policy follow-through, high uncertainty")

    # Risk drag note
    risk_drag = row.get("risk_drag_score")
    if pd.notna(risk_drag) and risk_drag >= 0.5:
        key_trends.append("Elevated environmental or infrastructure risk")

    return {
        "primary_strengths": primary_strengths,
        "primary_weaknesses": primary_weaknesses,
        "key_trends": key_trends,
    }


def classify_all_counties(layer_scores_df: pd.DataFrame, data_year: int) -> pd.DataFrame:
    """
    Classify all counties and generate explainability payloads.

    Args:
        layer_scores_df: DataFrame with layer scores
        data_year: Data year

    Returns:
        DataFrame with classifications and explanations
    """
    logger.info(f"Classifying {len(layer_scores_df)} counties")

    # Fetch policy persistence scores
    with get_db() as db:
        policy_query = text(
            f"""
            SELECT fips_code, confidence_score
            FROM {table_name('policy_persistence')}
            WHERE data_year = :data_year
        """
        )

        policy_df = pd.read_sql(policy_query, db.connection(), params={"data_year": int(data_year)})

    # Merge policy scores
    if not policy_df.empty:
        layer_scores_df = layer_scores_df.merge(policy_df, on="fips_code", how="left")
    else:
        logger.warning("No policy persistence scores found")
        layer_scores_df["confidence_score"] = np.nan

    # Classify each county
    classifications = []

    for _, row in layer_scores_df.iterrows():
        # Extract layer scores (excluding risk drag)
        layer_scores = pd.Series(
            {
                "employment_gravity": row.get("employment_gravity_score"),
                "mobility_optionality": row.get("mobility_optionality_score"),
                "school_trajectory": row.get("school_trajectory_score"),
                "housing_elasticity": row.get("housing_elasticity_score"),
                "demographic_momentum": row.get("demographic_momentum_score"),
            }
        )

        risk_drag = row.get("risk_drag_score")
        policy_score = row.get("confidence_score")

        # Classify directional status
        directional_class = classify_directional_status(layer_scores, risk_drag)

        # Classify confidence
        confidence_class = classify_confidence(policy_score)

        # Calculate final synthesis grouping (PRIMARY MAP OUTPUT)
        synthesis_grouping = calculate_final_synthesis_grouping(
            directional_class=directional_class,
            confidence_class=confidence_class,
            risk_drag_score=risk_drag,
            policy_persistence_score=policy_score,
            layer_scores=layer_scores,
            classification_contested=False,  # TODO: Implement claims system
        )

        # Generate explainability
        row_with_classes = row.copy()
        row_with_classes["directional_class"] = directional_class
        row_with_classes["confidence_class"] = confidence_class
        row_with_classes["synthesis_grouping"] = synthesis_grouping

        explainability = generate_explainability_payload(row_with_classes)

        classifications.append(
            {
                "fips_code": row["fips_code"],
                "data_year": data_year,
                "directional_class": directional_class,
                "composite_score": row.get("composite_normalized"),
                "confidence_class": confidence_class,
                "synthesis_grouping": synthesis_grouping,
                "primary_strengths": explainability["primary_strengths"],
                "primary_weaknesses": explainability["primary_weaknesses"],
                "key_trends": explainability["key_trends"],
                "classification_method": "rule_based_v1",
                "version": "v1.0",
            }
        )

    classifications_df = pd.DataFrame(classifications)

    logger.info(
        f"Classification distribution: "
        f"improving={len(classifications_df[classifications_df['directional_class']=='improving'])}, "
        f"stable={len(classifications_df[classifications_df['directional_class']=='stable'])}, "
        f"at_risk={len(classifications_df[classifications_df['directional_class']=='at_risk'])}"
    )

    logger.info(
        f"Confidence distribution: "
        f"strong={len(classifications_df[classifications_df['confidence_class']=='strong'])}, "
        f"conditional={len(classifications_df[classifications_df['confidence_class']=='conditional'])}, "
        f"fragile={len(classifications_df[classifications_df['confidence_class']=='fragile'])}"
    )

    logger.info(
        f"Final Synthesis Grouping (PRIMARY MAP LAYER) distribution: "
        f"emerging_tailwinds={len(classifications_df[classifications_df['synthesis_grouping']=='emerging_tailwinds'])}, "
        f"conditional_growth={len(classifications_df[classifications_df['synthesis_grouping']=='conditional_growth'])}, "
        f"stable_constrained={len(classifications_df[classifications_df['synthesis_grouping']=='stable_constrained'])}, "
        f"at_risk_headwinds={len(classifications_df[classifications_df['synthesis_grouping']=='at_risk_headwinds'])}, "
        f"high_uncertainty={len(classifications_df[classifications_df['synthesis_grouping']=='high_uncertainty'])}"
    )

    return classifications_df


def store_classifications(classifications_df: pd.DataFrame):
    """
    Store classifications in database.

    Args:
        classifications_df: DataFrame with classifications
    """
    logger.info(f"Storing classifications for {len(classifications_df)} counties")

    sql = text(
        f"""
        INSERT INTO {table_name('county_classifications')} (
            fips_code, data_year, directional_class, composite_score,
            confidence_class, synthesis_grouping, primary_strengths, primary_weaknesses,
            key_trends, classification_method, version
        ) VALUES (
            :fips_code, :data_year, :directional_class, :composite_score,
            :confidence_class, :synthesis_grouping, :primary_strengths, :primary_weaknesses,
            :key_trends, :classification_method, :version
        )
        ON CONFLICT (fips_code, data_year)
        DO UPDATE SET
            directional_class = EXCLUDED.directional_class,
            composite_score = EXCLUDED.composite_score,
            confidence_class = EXCLUDED.confidence_class,
            synthesis_grouping = EXCLUDED.synthesis_grouping,
            primary_strengths = EXCLUDED.primary_strengths,
            primary_weaknesses = EXCLUDED.primary_weaknesses,
            key_trends = EXCLUDED.key_trends,
            classification_method = EXCLUDED.classification_method,
            version = EXCLUDED.version,
            updated_at = CURRENT_TIMESTAMP
    """
    )

    rows = []
    for row in classifications_df.to_dict(orient="records"):
        fips_code = row.get("fips_code")
        rows.append(
            {
                "fips_code": (
                    None
                    if pd.isna(fips_code)
                    else fips_code if isinstance(fips_code, str) else str(int(fips_code))
                ),
                "data_year": int(row.get("data_year")),
                "directional_class": row.get("directional_class"),
                "composite_score": row.get("composite_score"),
                "confidence_class": row.get("confidence_class"),
                "synthesis_grouping": row.get("synthesis_grouping"),
                "primary_strengths": row.get("primary_strengths"),
                "primary_weaknesses": row.get("primary_weaknesses"),
                "key_trends": row.get("key_trends"),
                "classification_method": row.get("classification_method"),
                "version": row.get("version"),
            }
        )

    with get_db() as db:
        execute_batch(db, sql, rows, chunk_size=1000)

        db.commit()

    logger.info("Classifications stored successfully")


def run_classification(layer_scores_df: pd.DataFrame, data_year: int) -> pd.DataFrame:
    """
    Main entry point for classification pipeline.

    Args:
        layer_scores_df: DataFrame with layer scores
        data_year: Data year

    Returns:
        DataFrame with classifications
    """
    logger.info("Starting classification pipeline")

    try:
        # Classify all counties
        classifications_df = classify_all_counties(layer_scores_df, data_year)

        # Store in database
        store_classifications(classifications_df)

        # Log success
        log_refresh(
            layer_name="classification",
            data_source="Layer scores",
            status="success",
            records_processed=len(classifications_df),
            records_inserted=len(classifications_df),
            metadata={"data_year": data_year},
        )

        logger.info("Classification pipeline completed successfully")

        return classifications_df

    except Exception as e:
        logger.error(f"Classification pipeline failed: {e}", exc_info=True)

        log_refresh(
            layer_name="classification",
            data_source="Layer scores",
            status="failed",
            error_message=str(e),
        )

        raise


if __name__ == "__main__":
    import sys

    from src.processing.normalization import normalize_all_layers
    from src.processing.scoring import run_scoring
    from src.utils.logging import setup_logging

    setup_logging("classification")

    year = int(sys.argv[1]) if len(sys.argv) > 1 else None

    # Run full pipeline
    normalized = normalize_all_layers(data_year=year)

    if year is None:
        for layer_df in normalized.values():
            if not layer_df.empty:
                year = layer_df["data_year"].iloc[0]
                break

    if year is None:
        raise ValueError("Unable to determine data year from normalized layer outputs")

    resolved_year = int(year)
    layer_scores = run_scoring(normalized, resolved_year)
    run_classification(layer_scores, resolved_year)
