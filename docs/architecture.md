# Architecture

This document describes the technical architecture of the pipeline as it stands on `main` — the incremental version, which is the core objective of this project. A full-load variant exists on a separate branch; see [`branching-strategy.md`](branching-strategy.md) for why, and how it differs.

## Environment

| Component | Value |
|---|---|
| ADLS Gen2 container | `Formula1-incr` |
| Unity Catalog catalog | `formula1_incr` |
| Schemas | `landing`, `bronze`, `silver`, `gold`, plus a `control` schema for orchestration state |

Catalog and schema names are never hardcoded in the pipeline notebooks — they're read from `00-common/01.environment-config.ipynb`, so the same code could point at a different environment (e.g. a `formula1_dev` catalog) by changing config in one place.

## Medallion layers

```
Landing (ADLS Gen2 volume, raw CSV/JSON)
   │
   ▼
Bronze  — raw ingestion, partitioned by batch_id, one notebook per source file
   │
   ▼
Silver  — cleaned, typed, deduplicated, MERGE-based (see incremental-processing.md)
   │
   ▼
Gold    — star schema: dim_races, dim_constructors, dim_drivers, fact_session_results
   │
   ▼
Analytics — Unity Catalog views over Gold, feeding the AI/BI dashboard
```

Each arrow is driven by a batch: a new batch arrives in `landing`, flows through Bronze → Silver → Gold in a single child job run, triggered by the orchestration layer described in [`orchestration.md`](orchestration.md).

## Data sources

Six source files per batch, from the [jolpica-f1](https://github.com/jolpica/jolpica-f1) project (Ergast-format F1 historical data), in mixed formats:

| Format | Files |
|---|---|
| CSV | `circuits`, `races` |
| JSON | `constructors`, `drivers`, `results`, `sprints` |

A batch corresponds to a year-month period (e.g. `2025-01`) and contains a full snapshot — not a delta — of each of these six files. Each Bronze ingestion notebook (`02-bronze/`) reads its source in the appropriate format before writing out as Delta, so the pipeline handles both structured tabular and semi-structured nested sources from the same landing volume.

## Why Gold has no `batch_id`

Silver tables carry `batch_id` and enforce ordering via the anti-replay `MERGE` condition (`s.batch_id >= t.batch_id`). Gold tables inherit that correctness guarantee from Silver and don't repeat it — and since Gold aggregates across batches (a fact table row can be touched by results from multiple periods), a single `batch_id` column on a Gold row wouldn't be a meaningful concept anyway. Traceability back to a specific batch is delegated entirely to Silver.

## Dashboard layer

The AI/BI dashboard (`formula_one_analytics.lvdash.json`) is built on Unity Catalog views defined in `05-analytics/` — not on SQL queries embedded directly in the dashboard. This is the "dashboard as code" decision: changing a metric definition means editing and re-running a versioned notebook, not clicking through the dashboard UI.

Four dashboard pages, each backed by its own view:
- Driver Championship Standings
- Constructor Championship Standings
- Dominant Drivers of All Time (custom `greatness_score` metric)
- Dominant Constructors of All Time (same metric, aggregated by constructor)

## Orchestration

Covered in full in [`orchestration.md`](orchestration.md): a stateful `batch_control` table and a master Lakeflow Job that identifies, processes, and marks batches complete with zero manually-set parameters after initial setup.
