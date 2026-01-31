"""
Maryland Viability Atlas - Feature Registry
Single source of truth for all analytical features across all layers

This registry defines:
- Canonical feature names
- Source tables and columns
- Directionality (higher=better or higher=worse)
- Normalization method (percentile or robust z-score)
- Units and descriptions

NO feature should be scored without being registered here.
"""

from typing import Literal, Dict, List
from dataclasses import dataclass
from enum import Enum


class Directionality(str, Enum):
    """Feature directionality for scoring"""
    POSITIVE = "positive"  # Higher values are better
    NEGATIVE = "negative"  # Lower values are better (e.g., risk, cost)


class NormMethod(str, Enum):
    """Normalization method"""
    PERCENTILE = "percentile"  # Rank-based (0-1), robust to outliers
    ROBUST_ZSCORE = "robust_zscore"  # (x - median) / IQR, for heavy-tailed distributions
    MINMAX = "minmax"  # (x - min) / (max - min), use sparingly


@dataclass
class FeatureDefinition:
    """
    Definition of a single analytical feature
    """
    name: str  # Canonical feature name
    layer: str  # Layer name (e.g., 'employment_gravity')
    source_table: str  # PostgreSQL table name
    source_column: str  # Column name in source table
    directionality: Directionality  # Interpretation for scoring
    norm_method: NormMethod  # Normalization approach
    unit: str  # Human-readable unit
    description: str  # What this feature measures
    weight: float = 1.0  # Default weight within layer (can override)
    requires_ai: bool = False  # True if depends on AI extraction


# ============================================================================
# LAYER 1: EMPLOYMENT GRAVITY
# ============================================================================

LAYER1_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="employment_sector_diversity",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="sector_diversity_entropy",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Shannon entropy (bits)",
        description="Employment diversification across 20 NAICS sectors (higher = more resilient)",
        weight=1.5  # Emphasize diversity
    ),
    FeatureDefinition(
        name="stable_sector_employment",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="stable_sector_share",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Share (0-1)",
        description="Share of employment in education, health, and public administration",
        weight=1.0
    ),
    FeatureDefinition(
        name="federal_spending_stability",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="federal_awards_volatility",
        directionality=Directionality.NEGATIVE,  # Lower volatility is better
        norm_method=NormMethod.ROBUST_ZSCORE,
        unit="Coefficient of variation",
        description="Federal spending consistency (lower CV = more persistent funding)",
        weight=0.8
    ),
    FeatureDefinition(
        name="employment_diversification_composite",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="employment_diversification_score",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score (0-1)",
        description="Legacy local-strength composite (sector diversity + stability)",
        weight=0.8
    ),
    FeatureDefinition(
        name="economic_opportunity_index",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="economic_opportunity_index",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score (0-1)",
        description="Composite of local strength (v1) + regional accessibility (v2)",
        weight=2.2  # Primary Layer 1 signal
    ),
    FeatureDefinition(
        name="qwi_net_job_growth_rate",
        layer="employment_gravity",
        source_table="layer1_employment_gravity",
        source_column="qwi_net_job_growth_rate",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Rate (0-1)",
        description="Net job growth rate from Census QWI (hires minus separations)",
        weight=0.7
    )
]

# ============================================================================
# LAYER 2: MOBILITY OPTIONALITY
# ============================================================================

LAYER2_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="highway_access",
        layer="mobility_optionality",
        source_table="layer2_mobility_optionality",
        source_column="interstate_exits",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Count of interstate exits",
        description="Highway network access points",
        weight=1.0
    ),
    FeatureDefinition(
        name="transit_mode_count",
        layer="mobility_optionality",
        source_table="layer2_mobility_optionality",
        source_column="mode_count",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Count of distinct modes",
        description="Number of transportation modes available (highway, rail, bus)",
        weight=1.5
    ),
    FeatureDefinition(
        name="mobility_optionality_index",
        layer="mobility_optionality",
        source_table="layer2_mobility_optionality",
        source_column="mobility_optionality_index",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Index (0-1)",
        description="Composite mobility redundancy score",
        weight=2.0
    )
]

# ============================================================================
# LAYER 3: SCHOOL SYSTEM TRAJECTORY
# ============================================================================

LAYER3_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="enrollment_momentum",
        layer="school_trajectory",
        source_table="layer3_school_trajectory",
        source_column="enrollment_3yr_change_pct",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.ROBUST_ZSCORE,
        unit="Percent change (3-year)",
        description="Enrollment trend direction (growing = positive family demand signal)",
        weight=1.5
    ),
    FeatureDefinition(
        name="capital_per_student",
        layer="school_trajectory",
        source_table="layer3_school_trajectory",
        source_column="capital_per_student",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Dollars per student",
        description="School capital investment per student (if CIP data available)",
        weight=1.0
    ),
    FeatureDefinition(
        name="enrollment_momentum_score",
        layer="school_trajectory",
        source_table="layer3_school_trajectory",
        source_column="enrollment_momentum_score",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score",
        description="Pre-calculated enrollment trajectory signal",
        weight=2.0
    )
]

# ============================================================================
# LAYER 4: HOUSING ELASTICITY
# ============================================================================

LAYER4_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="permits_per_1000_households",
        layer="housing_elasticity",
        source_table="layer4_housing_elasticity",
        source_column="permits_per_1000_households",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Permits per 1000 households",
        description="Building permit intensity (supply responsiveness proxy)",
        weight=1.5
    ),
    FeatureDefinition(
        name="price_to_income_trend",
        layer="housing_elasticity",
        source_table="layer4_housing_elasticity",
        source_column="price_to_income_5yr_change",
        directionality=Directionality.NEGATIVE,  # Rising P/I ratio is a constraint
        norm_method=NormMethod.ROBUST_ZSCORE,
        unit="Percentage point change",
        description="5-year change in price-to-income ratio (rising = less affordable)",
        weight=1.0
    ),
    FeatureDefinition(
        name="supply_responsiveness",
        layer="housing_elasticity",
        source_table="layer4_housing_elasticity",
        source_column="supply_responsiveness_score",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score",
        description="Housing supply elasticity composite",
        weight=2.0
    ),
    FeatureDefinition(
        name="fmr_2br_to_income",
        layer="housing_elasticity",
        source_table="layer4_housing_elasticity",
        source_column="fmr_2br_to_income",
        directionality=Directionality.NEGATIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Ratio (annual rent / income)",
        description="HUD Fair Market Rent (2BR) as share of median income",
        weight=0.6
    ),
    FeatureDefinition(
        name="lihtc_units_per_1000_households",
        layer="housing_elasticity",
        source_table="layer4_housing_elasticity",
        source_column="lihtc_units_per_1000_households",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Units per 1000 households",
        description="LIHTC affordable unit supply intensity",
        weight=0.6
    )
]

# ============================================================================
# LAYER 5: DEMOGRAPHIC MOMENTUM
# ============================================================================

LAYER5_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="net_migration_households",
        layer="demographic_momentum",
        source_table="layer5_demographic_momentum",
        source_column="net_migration_households",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.ROBUST_ZSCORE,
        unit="Net household inflow",
        description="IRS county-to-county migration net flow",
        weight=1.5
    ),
    FeatureDefinition(
        name="working_age_share",
        layer="demographic_momentum",
        source_table="layer5_demographic_momentum",
        source_column="pop_age_25_44_pct",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Share (0-1)",
        description="Share of population age 25-44 (prime working age)",
        weight=1.0
    ),
    FeatureDefinition(
        name="household_formation_change",
        layer="demographic_momentum",
        source_table="layer5_demographic_momentum",
        source_column="household_formation_change",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.ROBUST_ZSCORE,
        unit="Percent change",
        description="Year-over-year household formation rate change",
        weight=1.2
    ),
    FeatureDefinition(
        name="demographic_momentum_composite",
        layer="demographic_momentum",
        source_table="layer5_demographic_momentum",
        source_column="demographic_momentum_score",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score",
        description="Pre-calculated demographic pressure composite",
        weight=2.0
    )
]

# ============================================================================
# LAYER 6: RISK DRAG (PENALTY LAYER - ALL NEGATIVE)
# ============================================================================

LAYER6_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="flood_hazard_exposure",
        layer="risk_drag",
        source_table="layer6_risk_drag",
        source_column="sfha_pct_of_county",
        directionality=Directionality.NEGATIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Percent of county area",
        description="FEMA Special Flood Hazard Area (100-year floodplain)",
        weight=1.5
    ),
    FeatureDefinition(
        name="air_quality_burden",
        layer="risk_drag",
        source_table="layer6_risk_drag",
        source_column="pm25_avg",
        directionality=Directionality.NEGATIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="PM2.5 μg/m³",
        description="Particulate matter exposure (EPA EJScreen)",
        weight=0.8
    ),
    FeatureDefinition(
        name="infrastructure_deficiency",
        layer="risk_drag",
        source_table="layer6_risk_drag",
        source_column="bridges_deficient_pct",
        directionality=Directionality.NEGATIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Percent of bridges",
        description="Share of structurally deficient bridges",
        weight=1.0
    ),
    FeatureDefinition(
        name="risk_drag_composite",
        layer="risk_drag",
        source_table="layer6_risk_drag",
        source_column="risk_drag_index",
        directionality=Directionality.NEGATIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Risk index (0-1)",
        description="Pre-calculated composite risk drag (subtractive only)",
        weight=2.0
    )
]

# ============================================================================
# POLICY PERSISTENCE LAYER (CONFIDENCE MODIFIER)
# ============================================================================

POLICY_FEATURES: List[FeatureDefinition] = [
    FeatureDefinition(
        name="federal_spending_consistency",
        layer="policy_persistence",
        source_table="policy_persistence",
        source_column="federal_awards_yoy_consistency",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Consistency score (0-1)",
        description="1 - CV of federal awards across 5 years",
        weight=1.0
    ),
    FeatureDefinition(
        name="cip_follow_through_rate",
        layer="policy_persistence",
        source_table="policy_persistence",
        source_column="cip_follow_through_rate",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Completion rate (0-1)",
        description="CIP projects completed / planned (AI-extracted)",
        weight=1.5,
        requires_ai=True  # Depends on AI document extraction
    ),
    FeatureDefinition(
        name="confidence_score",
        layer="policy_persistence",
        source_table="policy_persistence",
        source_column="confidence_score",
        directionality=Directionality.POSITIVE,
        norm_method=NormMethod.PERCENTILE,
        unit="Composite score (0-1)",
        description="Policy delivery reliability composite",
        weight=2.0
    )
]

# ============================================================================
# FEATURE REGISTRY (ALL FEATURES)
# ============================================================================

ALL_FEATURES: List[FeatureDefinition] = (
    LAYER1_FEATURES +
    LAYER2_FEATURES +
    LAYER3_FEATURES +
    LAYER4_FEATURES +
    LAYER5_FEATURES +
    LAYER6_FEATURES +
    POLICY_FEATURES
)

# Build lookup dictionaries
FEATURES_BY_NAME: Dict[str, FeatureDefinition] = {
    f.name: f for f in ALL_FEATURES
}

FEATURES_BY_LAYER: Dict[str, List[FeatureDefinition]] = {
    "employment_gravity": LAYER1_FEATURES,
    "mobility_optionality": LAYER2_FEATURES,
    "school_trajectory": LAYER3_FEATURES,
    "housing_elasticity": LAYER4_FEATURES,
    "demographic_momentum": LAYER5_FEATURES,
    "risk_drag": LAYER6_FEATURES,
    "policy_persistence": POLICY_FEATURES
}


def get_features_for_layer(layer_name: str) -> List[FeatureDefinition]:
    """Get all features for a specific layer"""
    return FEATURES_BY_LAYER.get(layer_name, [])


def get_feature(feature_name: str) -> FeatureDefinition:
    """Get feature definition by canonical name"""
    if feature_name not in FEATURES_BY_NAME:
        raise ValueError(f"Unknown feature: {feature_name}")
    return FEATURES_BY_NAME[feature_name]


def get_primary_features() -> List[FeatureDefinition]:
    """
    Get primary features (composite scores) for layer scoring.
    These are the features with highest weights that represent
    pre-calculated composites from each layer.
    """
    primary = []
    for layer_features in FEATURES_BY_LAYER.values():
        # Get feature with highest weight in each layer
        if layer_features:
            primary_feature = max(layer_features, key=lambda f: f.weight)
            primary.append(primary_feature)
    return primary


def get_ai_dependent_features() -> List[FeatureDefinition]:
    """Get features that require AI extraction"""
    return [f for f in ALL_FEATURES if f.requires_ai]


def validate_feature_coverage(db_session) -> Dict[str, Dict[str, int]]:
    """
    Check which features have data coverage in the database.

    Returns:
        Dict mapping layer -> {feature_name: record_count}
    """
    from sqlalchemy import text

    coverage = {}

    for layer_name, features in FEATURES_BY_LAYER.items():
        layer_coverage = {}

        for feature in features:
            query = text(f"""
                SELECT COUNT(DISTINCT fips_code)
                FROM {feature.source_table}
                WHERE {feature.source_column} IS NOT NULL
            """)

            result = db_session.execute(query)
            count = result.scalar()
            layer_coverage[feature.name] = count

        coverage[layer_name] = layer_coverage

    return coverage


# ============================================================================
# LAYER DEFINITIONS (for scoring)
# ============================================================================

@dataclass
class LayerDefinition:
    """Definition of an analytical layer for scoring"""
    name: str
    display_name: str
    description: str
    is_penalty: bool = False  # True for risk_drag (subtractive)
    features: List[FeatureDefinition] = None


LAYER_DEFINITIONS = [
    LayerDefinition(
        name="employment_gravity",
        display_name="Employment Gravity",
        description="Income stability through sector diversification and federal spending",
        is_penalty=False,
        features=LAYER1_FEATURES
    ),
    LayerDefinition(
        name="mobility_optionality",
        display_name="Mobility Optionality",
        description="Job-change resilience via transit and highway redundancy",
        is_penalty=False,
        features=LAYER2_FEATURES
    ),
    LayerDefinition(
        name="school_trajectory",
        display_name="School System Trajectory",
        description="Family viability through enrollment trends and capital investment",
        is_penalty=False,
        features=LAYER3_FEATURES
    ),
    LayerDefinition(
        name="housing_elasticity",
        display_name="Housing Elasticity",
        description="Growth absorption capacity through supply responsiveness",
        is_penalty=False,
        features=LAYER4_FEATURES
    ),
    LayerDefinition(
        name="demographic_momentum",
        display_name="Demographic Momentum",
        description="Demand pressure from migration and household formation",
        is_penalty=False,
        features=LAYER5_FEATURES
    ),
    LayerDefinition(
        name="risk_drag",
        display_name="Risk Drag",
        description="Long-term environmental and infrastructure constraints (PENALTY)",
        is_penalty=True,
        features=LAYER6_FEATURES
    )
]


if __name__ == "__main__":
    # Validation and reporting
    print("Maryland Viability Atlas - Feature Registry")
    print("=" * 60)

    for layer_def in LAYER_DEFINITIONS:
        print(f"\n{layer_def.display_name} ({layer_def.name})")
        print(f"  Penalty layer: {layer_def.is_penalty}")
        print(f"  Features: {len(layer_def.features)}")

        for feat in layer_def.features:
            ai_flag = " [AI]" if feat.requires_ai else ""
            print(f"    - {feat.name}: {feat.description}{ai_flag}")

    print(f"\n\nTotal features: {len(ALL_FEATURES)}")
    print(f"AI-dependent features: {len(get_ai_dependent_features())}")
    print(f"Primary features for scoring: {len(get_primary_features())}")
