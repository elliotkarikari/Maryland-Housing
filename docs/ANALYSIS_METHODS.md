Maryland Growth & Family Viability Atlas — Analysis Methods

Prediction Alignment (2025)
- Goal: Align layer outputs to a “current” year (2025) without copy-forward.
- Eligibility: Minimum 3 observed years per county/tract.
- Extrapolation limit: Maximum 2 years beyond the observed range.
- Default method: Theil–Sen linear trend (robust to outliers); fallback to OLS if SciPy is unavailable.
- Constraints: Rate metrics are clipped to valid bounds (e.g., 0–1).
- Provenance: Predicted values are stored in separate *_pred fields with explicit flags and method metadata.
- No copy-forward: Past observations are never reused as-is for newer years.
- Optional effective values: If enabled, *_effective = COALESCE(original, predicted).

Low Vacancy Counties (HUD Lowvactpv)
- Source is a county-level list with occupied-unit percentages, not a vacancy-count dataset.
- Used to set low_vacancy_county_flag and to populate vacancy rates/counts only for the exact FY listed (no backfill).

Notes
- Prediction fields are conservative and optional to preserve data integrity.
- Scoring can optionally use effective values, but historical trends always use original observations.
