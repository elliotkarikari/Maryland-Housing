# Lessons

## 2026-02-17
- Keep architecture and methodology canonical docs explicit; stale secondary docs create requirement confusion faster than code drift.
- Run static audits before proposing deletions; objective scan output prevents removing artifacts that still carry operational value.
- Optimize CI first where change risk is low (`actions/setup-python` pip cache) before touching ingest/runtime logic.
- For doc cleanup, archive legacy files with date-stamped names and publish an old-path-to-canonical map in `docs/README.md`.
- For row-write hotspots, prioritize vectorization + `execute_batch` conversion in processing modules before broader ingest refactors.
- Store-path tests with monkeypatched `get_db` are a fast way to verify batched execution semantics without requiring a live database.
- For wide feature tables, converting to long format (`melt`) before `execute_batch` removes nested write loops with low behavioral risk.
- In ingest modules, replacing `iterrows()` with `to_dict(orient=\"records\")` plus safe coercion helpers improves throughput and avoids NaN-to-int conversion crashes.
- Shared coercion helpers (`_int_or_default`, `_float_or_none`, `_bool_or_default`) reduce repeated edge-case bugs across school/tract/county storage builders.
- Keep opportunity-index composition logic isolated in a row-builder helper so weighting behavior is explicit and testable outside DB interactions.
- Legacy ingest modules still matter for maintenance; replacing per-row `db.execute` with `execute_batch` is low-risk and immediately improves runtime.
- Large wide-table inserts benefit from a dedicated row-builder helper plus a fixed expected-column list to guarantee stable defaults across schema drift.

### Preventive Prompt Snippet
Before any broad refactor: "List canonical docs, list non-canonical docs, and prove each deletion candidate has zero runtime dependency and a documented replacement."
