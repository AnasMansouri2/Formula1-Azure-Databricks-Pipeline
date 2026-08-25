# Branching Strategy

## History

The project started as a **full-load** pipeline: every run reprocessed all source data from scratch, with a full overwrite at each layer. This was the natural starting point for learning the Medallion Architecture end to end.

Once the full-load version was working, the goal shifted to the real objective of this project: an **incremental** pipeline that processes only new batches, tracks its own state, and requires no manual `batch_id` after setup.

Rather than building incremental on top of full-load in place (and losing a clean, working reference version), the `full-load` branch was created **before** starting the incremental work — capturing that full-load state as a clean snapshot. Development of the incremental pipeline then continued on `main`.

## Why a branch, not a folder

The course this project is based on suggests handling this by duplicating folders (e.g. `full-load/` and `incremental/` side by side in the same branch). That was deliberately not used here, for a few reasons:

- **Signal, not clutter.** `main` represents the actual deliverable — a recruiter or reviewer landing on the repo sees the incremental pipeline immediately, not a full-load/incremental fork they have to figure out first.
- **No duplicated maintenance surface.** Two folders means two copies of overlapping logic (helpers, config) that can silently drift apart. A branch keeps them cleanly separated at the Git level instead.
- **Matches how the two versions are actually used.** They're not two features of the same running system — they're two different snapshots of the same project at different points in its evolution. A branch models that relationship directly; a folder split implies they coexist and interact, which they don't.

## What's on each branch

| Branch | Contents |
|---|---|
| `main` | The incremental pipeline: MERGE-based Silver/Gold writes, `batch_control` state tracking, parameter-free orchestration. This is the version documented throughout the rest of this repo. |
| `full-load` | The original full-refresh pipeline, preserved as-is from before incremental work began — a clean reference point showing where the project started. |
