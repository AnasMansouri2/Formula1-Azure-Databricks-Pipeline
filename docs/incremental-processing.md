# Incremental Processing

This document details how each layer of the pipeline handles incremental writes — what gets `MERGE`d, what gets overwritten, and why.

## The core rule

| Layer | Write pattern | Why |
|---|---|---|
| Bronze | `partitionBy("batch_id")` + `replaceWhere` | Re-ingesting a batch only touches that batch's partition — others are untouched |
| Silver | `MERGE` (via `write_to_silver()`) | Every table, no exceptions — including "static" reference data like drivers and circuits |
| Gold — dimensions & facts | `MERGE` (via `write_to_gold()`) | No aggregation involved, so incremental updates are safe |
| Gold — standings & GOAT views | Full overwrite | Aggregates across all history, so a partial/incremental write would be incorrect |

## Why MERGE even for "static" data

Drivers, circuits, and constructors don't change often — but "rarely" isn't "never" (a driver's nationality code gets corrected, a circuit gets renamed), and batches can be rerun or arrive out of order. Treating these as MERGE targets from the start means the pipeline doesn't need special-casing later if that assumption ever breaks.

## Silver: `write_to_silver()`

A reusable helper (`00-common/03.silver-helpers.ipynb`) wrapping `DeltaTable.merge()`. Every Silver table carries a `batch_id` column, and the merge condition includes anti-replay protection:

```python
.whenMatchedUpdate(condition="s.batch_id >= t.batch_id", set={...})
```

This guarantees an older or replayed batch can never overwrite data written by a newer one. `created_timestamp` is deliberately excluded from the `set` dict on match — it's set once on insert and never touched again, preserving a true "first seen" audit trail.

## Gold: `write_to_gold()`

A simplified helper (`00-common/04.gold-helpers.ipynb`) for dimension and fact tables (`dim_races`, `dim_constructors`, `dim_drivers`, `fact_session_results`). Unlike `write_to_silver()`, it has no `batch_id` handling — that guarantee is already provided upstream by Silver's anti-replay check, so Gold doesn't need to repeat it. Gold tables don't carry a `batch_id` column at all, since aggregated views built on top of them would make a single batch identifier meaningless.

Aggregated Gold objects (driver/constructor standings, GOAT views in `05-analytics/`) are excluded from this pattern — they're recomputed via full overwrite from the complete Gold layer on every run, since incremental aggregation isn't safe without much more complex windowing logic.

## End-to-end guarantee

Because Silver enforces `batch_id` ordering and Gold's MERGE targets inherit clean, deduplicated data from Silver, the pipeline can be safely rerun on the same batch, or run batches out of order, without manual cleanup or duplicate rows — verified across the `2025-01` → `2025-02` test run described in [`orchestration.md`](orchestration.md).
