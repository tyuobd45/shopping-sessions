# Shopping Sessions — DuckDB pipeline

Submission for the Databricks Shopping Sessions track of the Builders Buildathon.

## Run

The repository root contains the Dockerfile. Build for the scorer platform:

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```

The container entry point takes no arguments. Input is mounted at `/data`; output
Parquet tables are written under `/output/user_daily/` and
`/output/campaign_daily/`. Temporary data is written under `/scratch`.
The pipeline uses the scorer-provided DATA_DIR, OUTPUT_DIR, SCRATCH_DIR,
NCPU and MEM_GB environment variables. It requires no network at run time.

For a native Python run, install `duckdb==1.5.6` and run:

```sh
DATA_DIR=/path/to/input OUTPUT_DIR=/path/to/output python3 pipeline/pipeline.py
```

## Method

- Strict CSV validation with exact whitespace, quote and LF rules. Files with
  carriage returns use a text reader that preserves them.
- Union-find with path compression and union by size maps linked IDs to each
  component's smallest ID, including IDs absent from events and clicks.
- Earliest-ingestion event deduplication precedes semantic filtering.
- Session boundaries use millisecond timestamps and gaps strictly over 30 minutes.
  All events are assigned to the UTC date of the session start.
- Latest-click attribution uses a 7-day inclusive window with click-ID tie breaking.
- Overlapping campaign eligibility intervals are merged before counting assists,
  so repeated clicks do not count a campaign more than once per session.

## Validation completed

- All 36 official edge-case fixtures pass, including the local macOS run.
- Both output tables exactly match the published answer for the seed-1
  1,000,000-event / 250,000-click public development dataset.
- A separate generated seed-17 dataset with 20,000,000 events and 5,000,000 clicks
  runs to completion and passes key uniqueness, session/revenue conservation,
  and assist-count consistency checks. No exact reference is available for this run.
- The implementation uses no hard-coded dataset dates, IDs, seed-specific answers,
  or fixture lookups. All dependencies are pinned in requirements.txt.

Container construction and official hidden-dataset scoring have not yet been
verified. Local native timings are not official leaderboard measurements.

See rules.md for the published task specification. Large datasets, generated
outputs and Python environments are intentionally absent from this repository.
