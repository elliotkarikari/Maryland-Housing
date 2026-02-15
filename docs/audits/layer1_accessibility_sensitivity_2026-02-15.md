# Layer 1 Accessibility Sensitivity Report

- Generated: 2026-02-15
- Data year: 2024
- LODES year: 2022
- ACS year: 2022
- Requested accessibility mode: `proxy`
- Effective base method: `haversine_proxy`
- Base thresholds: 30/45 minutes, proxy 20.0/35.0 km
- Commute-data calibration status: not performed (no observed statewide OD commute dataset configured in ingest).

## Classification Stability

| Scenario | Method | 30-min | 45-min | Proxy 30km | Proxy 45km | Unchanged counties | Stability % | Mean abs score delta | Max abs score delta |
|----------|--------|--------|--------|------------|------------|--------------------|-------------|----------------------|--------------------|
| base | `haversine_proxy` | 30 | 45 | 20.0 | 35.0 | 24 | 100.0% | 0.0000 | 0.0000 |
| km_minus_5 | `haversine_proxy` | 30 | 45 | 15.0 | 30.0 | 22 | 91.7% | 0.0312 | 0.2083 |
| km_plus_5 | `haversine_proxy` | 30 | 45 | 25.0 | 40.0 | 22 | 91.7% | 0.0243 | 0.0833 |

## Interpretation

- Higher stability % means county accessibility bands are less sensitive to threshold perturbation.
- This report evaluates Layer 1 accessibility score-band stability only; full six-layer directional classes may shift differently.
