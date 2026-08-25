# Orchestration

This document details how the pipeline decides, on its own, which batch to process next — with no manually-set parameters after the initial run.

## The `batch_control` table

Created once by `06-orchestration/00.Create Control Tables.py`:

```python
spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {catalog_name}.{control_schema}.batch_control
            (
                batch_id STRING,
                status STRING,
                created_timestamp TIMESTAMP,
                updated_timestamp TIMESTAMP
            )
          """)
```

| Column | Type | Purpose |
|---|---|---|
| `batch_id` | `STRING` | Identifies the batch (e.g. `2025-01`) |
| `status` | `STRING` | `in_progress` or `completed` |
| `created_timestamp` | `TIMESTAMP` | Set once on insert, never updated on subsequent `MERGE`s |
| `updated_timestamp` | `TIMESTAMP` | Refreshed every time the row's status changes |

`catalog_name` and `control_schema` are pulled from `00-common/01.environment-config.ipynb` — never hardcoded in the orchestration notebooks themselves.

## The master job

A Lakeflow Job (`job_formula1_incremental_batch_orchestration`) runs the following tasks in sequence on every trigger:

**1. `01.Identify Next Batch`**
Lists the batch folders present in the landing volume and compares them against the batch IDs already recorded in `batch_control`. The first one with no matching row is the next batch to process. The result is exposed to downstream tasks with:

```python
dbutils.jobs.taskValues.set(key="p_batch_id", value=next_batch_id)
dbutils.jobs.taskValues.set(key="has_batch", value=True)  # or False if nothing new
```

**2. Native if/else condition**
The job branches on `has_batch` (read via `{{tasks.Identify_Next_Batch.values.has_batch}}`). If `False`, the job ends here — nothing new to process. If `True`, it continues to step 3.

**3. `02.Create New Batch`**
Inserts a new row into `batch_control`:
```
batch_id = p_batch_id, status = 'in_progress',
created_timestamp = now(), updated_timestamp = now()
```

**4. Run Job task — the incremental refresh**
Triggers the child job that runs Bronze → Silver → Gold for that batch. `p_batch_id` is passed automatically as a job-level parameter (`{{tasks.Identify_Next_Batch.values.p_batch_id}}`) — the child notebooks never need it typed in manually.

**5. `03.Complete Batch`**
`MERGE`s the `batch_control` row for `p_batch_id`: sets `status = 'completed'` and refreshes `updated_timestamp`. `created_timestamp` is excluded from the `UPDATE SET` clause, so it always reflects the original insert time.

## Verified behavior

Tested across two successive triggers:
- **Run 1**: landing contains only `2025-01`. Job finds no row for it, creates one (`in_progress`), runs the refresh, marks it `completed`.
- **Run 2**: `2025-02` is dropped into landing. Job finds `2025-01` already `completed`, correctly identifies `2025-02` as the next batch, and repeats the cycle — without touching `2025-01` again.

No manual `batch_id` was set at any point in either run.

## Known limitations

These gaps were identified during the build and are intentionally left as next steps rather than hidden:

- **No `failed` status.** If a downstream task (Bronze/Silver/Gold refresh) fails, the batch row stays `in_progress` indefinitely — there's no third status to distinguish "still running" from "crashed."
- **No automatic retry.** A failed task requires manual re-trigger of the child job.
- **No crash recovery.** A batch stuck `in_progress` after a crash blocks nothing technically (the next run would still look for the *next* unprocessed batch_id, not retry the stuck one) but leaves an inconsistent row that has to be manually reset or investigated.

**Natural extension:** add a status-check step at the start of the orchestration job that detects `in_progress` rows older than a timeout threshold, flags them `failed`, and either alerts or triggers a retry before looking for the next new batch.
