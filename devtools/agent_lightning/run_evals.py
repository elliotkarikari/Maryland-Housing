import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from devtools.agent_lightning.evals.layer6_risk_quality import run as run_layer6
from devtools.agent_lightning.evals.timeseries_regression import run as run_timeseries


EVALS = {
    "layer6": run_layer6,
    "timeseries": run_timeseries,
}


def _load_baseline(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_baseline(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def _compare_baseline(
    current: Dict[str, Any],
    baseline: Dict[str, Any],
    coverage_tolerance: float,
    count_tolerance: int,
) -> List[str]:
    issues: List[str] = []
    if not baseline:
        return issues

    for eval_name, metrics in current.items():
        base_metrics = baseline.get(eval_name, {})
        if not base_metrics:
            continue

        if eval_name == "layer6":
            current_year = metrics.get("latest_year")
            base_year = base_metrics.get("latest_year")
            if base_year and current_year and current_year < base_year:
                issues.append(
                    f"layer6: latest_year regressed from {base_year} to {current_year}."
                )
            current_count = metrics.get("county_rows")
            base_count = base_metrics.get("county_rows")
            if base_count is not None and current_count is not None:
                if abs(current_count - base_count) > count_tolerance:
                    issues.append(
                        f"layer6: county_rows {current_count} differs from baseline {base_count}."
                    )
            for key in ["null_required_rows", "out_of_range_rows"]:
                cur = metrics.get(key)
                base = base_metrics.get(key)
                if base is not None and cur is not None and cur > base:
                    issues.append(
                        f"layer6: {key} increased from {base} to {cur}."
                    )

        if eval_name == "timeseries":
            current_year = metrics.get("latest_as_of_year")
            base_year = base_metrics.get("latest_as_of_year")
            if base_year and current_year and current_year < base_year:
                issues.append(
                    f"timeseries: latest_as_of_year regressed from {base_year} to {current_year}."
                )
            current_layers = metrics.get("coverage_ratio_by_layer", {})
            base_layers = base_metrics.get("coverage_ratio_by_layer", {})
            for layer, ratio in current_layers.items():
                base_ratio = base_layers.get(layer)
                if base_ratio is None or ratio is None:
                    continue
                if ratio < (base_ratio - coverage_tolerance):
                    issues.append(
                        f"timeseries: {layer} coverage dropped from {base_ratio:.0%} to {ratio:.0%}."
                    )
            current_counts = metrics.get("counts_by_layer", {})
            base_counts = base_metrics.get("counts_by_layer", {})
            for layer, count in current_counts.items():
                base_count = base_counts.get(layer)
                if base_count is None or count is None:
                    continue
                if abs(count - base_count) > count_tolerance:
                    issues.append(
                        f"timeseries: {layer} count {count} differs from baseline {base_count}."
                    )
            for key in ["summary_out_of_range_rows", "summary_null_rows"]:
                cur = metrics.get(key)
                base = base_metrics.get(key)
                if base is not None and cur is not None and cur > base:
                    issues.append(
                        f"timeseries: {key} increased from {base} to {cur}."
                    )

    return issues


def _print_dashboard(results: List[Any]) -> None:
    print("\n=== Eval Summary ===")
    for result in results:
        metrics = result.metrics or {}
        if result.name == "layer6_risk_quality":
            print(
                f"Layer6 {metrics.get('latest_year')}: "
                f"{metrics.get('county_rows')} counties, "
                f"nulls={metrics.get('null_required_rows')}, "
                f"range_violations={metrics.get('out_of_range_rows')}"
            )
        elif result.name == "timeseries_regression":
            print(
                f"Timeseries as_of_year {metrics.get('latest_as_of_year')}: "
                f"null_scores={metrics.get('summary_null_rows')}, "
                f"range_violations={metrics.get('summary_out_of_range_rows')}"
            )
            coverage = metrics.get("coverage_ratio_by_layer", {})
            if coverage:
                for layer, ratio in coverage.items():
                    print(f"  - {layer}: coverage {ratio:.0%}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Agent Lightning pilot evals.")
    parser.add_argument(
        "--evals",
        default="layer6,timeseries",
        help="Comma-separated evals to run: layer6,timeseries",
    )
    parser.add_argument(
        "--min-coverage-ratio",
        type=float,
        default=0.8,
        help="Minimum fraction of counties with >=3 years of coverage.",
    )
    parser.add_argument(
        "--baseline-dir",
        default="devtools/agent_lightning/baselines",
        help="Directory to read/write baseline snapshots.",
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="Write a new baseline snapshot from current metrics.",
    )
    parser.add_argument(
        "--no-compare-baseline",
        action="store_true",
        help="Skip baseline comparison.",
    )
    parser.add_argument(
        "--coverage-tolerance",
        type=float,
        default=0.05,
        help="Allowed coverage ratio drop vs baseline.",
    )
    parser.add_argument(
        "--count-tolerance",
        type=int,
        default=0,
        help="Allowed county count delta vs baseline.",
    )
    args = parser.parse_args()

    requested = [e.strip() for e in args.evals.split(",") if e.strip()]
    failures = 0
    results = []
    metrics_snapshot: Dict[str, Any] = {}

    for name in requested:
        if name not in EVALS:
            print(f"Unknown eval: {name}")
            failures += 1
            continue

        if name == "timeseries":
            result = EVALS[name](min_coverage_ratio=args.min_coverage_ratio)
        else:
            result = EVALS[name]()

        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}")
        for detail in result.details:
            print(f"  - {detail}")
        if not result.passed:
            failures += 1
        results.append(result)
        metrics_snapshot[name] = result.metrics or {}

    _print_dashboard(results)

    baseline_dir = Path(args.baseline_dir)
    baseline_path = baseline_dir / "baseline_snapshot.json"

    if args.write_baseline:
        payload = {
            "created_at": datetime.utcnow().isoformat() + "Z",
            "metrics": metrics_snapshot,
        }
        _save_baseline(baseline_path, payload)
        print(f"\nBaseline written to {baseline_path}")
    elif not args.no_compare_baseline:
        baseline_payload = _load_baseline(baseline_path)
        baseline_metrics = baseline_payload.get("metrics", {})
        issues = _compare_baseline(
            metrics_snapshot,
            baseline_metrics,
            coverage_tolerance=args.coverage_tolerance,
            count_tolerance=args.count_tolerance,
        )
        if issues:
            print("\n[FAIL] Baseline comparison")
            for issue in issues:
                print(f"  - {issue}")
            failures += 1
        else:
            print("\n[PASS] Baseline comparison")

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
