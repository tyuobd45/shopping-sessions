# Shopping Sessions

An exact shopping-log pipeline for the Databricks Builders Buildathon track.
A parallel Rust processor performs parsing, identity linking, event deduplication,
sessionization, attribution and assists. Python and DuckDB encode the final tables
as Parquet. The previous DuckDB implementation remains available as a native
Python fallback when the compiled executable is absent.

## Build and run

The root Dockerfile builds the Rust executable with Rust 1.85 and bundles it into
a Python 3.12 image with `duckdb==1.5.6`. There are no Rust crate dependencies.
The native executable targets x86-64-v3 for the scorer's amd64 Xeon machines.

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```

The container command takes no arguments. The scorer supplies DATA_DIR, OUTPUT_DIR,
SCRATCH_DIR, NCPU and MEM_GB. The input is mounted read-only at /data. Outputs are
/output/user_daily/part-0.parquet and /output/campaign_daily/part-0.parquet; each
`day` column is a Parquet DATE. Intermediate CSVs exist only in a private temporary
folder under /scratch and are removed after conversion. No runtime network is
required.

For the Python fallback, install `duckdb==1.5.6` and run:

```sh
DATA_DIR=/path/to/input OUTPUT_DIR=/path/to/output python3 pipeline/pipeline.py
```

On x86-64 Linux, a locally compiled executable can be selected explicitly:

```sh
rustc --edition=2021 -O -C target-cpu=x86-64-v3 -C codegen-units=1 -C lto=thin pipeline/fast.rs -o /tmp/shopping-fast
SHOPPING_FAST_ENGINE=/tmp/shopping-fast DATA_DIR=/path/to/input OUTPUT_DIR=/path/to/output python3 pipeline/pipeline.py
```

## Processing

- Read-only memory-mapped input and parallel parsing preserve the exact LF-only
  CSV dialect, quoted fields, spaces, carriage returns and ASCII digit rules.
- Union-find with path compression and union by size maps linked IDs to the
  byte-wise smallest ID, including linked IDs absent from events and clicks.
- Event IDs are hash-partitioned and deduplicated by earliest ingestion before
  semantic filtering. Fingerprint collisions always compare full IDs and remain
  distinct. An invalid earliest copy is never rescued by a later valid one.
- Canonical users are partitioned among workers. Sorted events use gaps strictly
  over 30 minutes; equal timestamps share a session, and all events count on the
  UTC date of their session's start.
- A sliding click window maintains exact 7-day inclusive eligibility. Latest-click
  attribution uses the smallest click ID on a timestamp tie. Active campaign
  counts record each assisting campaign once per session, including assist-only
  campaign/day output rows.
- Per-worker daily campaign arrays are merged; all arithmetic for totals uses
  64-bit integers. Final CSV fields are explicitly typed during Parquet encoding.

## Validation

- All 36 official edge-case fixtures pass with the native processor.
- Both tables exactly match the published answer for the seed-1 1,000,000-event /
  250,000-click public development dataset.
- Both tables for a seed-17 20,000,000-event / 5,000,000-click generated dataset
  exactly match the prior DuckDB implementation.
- A Rust unit test deliberately exercises colliding fingerprints, distinct IDs,
  and earliest-invalid-copy deduplication. Run it using `rustc --edition=2021
  --test pipeline/fast.rs -o /tmp/shopping-tests` followed by `/tmp/shopping-tests`.
- The optimized native run on that local 20M-event dataset took approximately
  8.24 seconds end to end, compared with approximately 33.38 seconds for the
  earlier DuckDB run on the same dataset. These are local development timings;
  the official scorer uses different hardware and hidden data.

The previous submission at commit 3cb6c1de220d94d1a7b654efb44930055792ae3c
passed official practice scoring in 38.30 seconds. The native version still
requires official container/hidden-data evaluation. No dates, input IDs, dataset
seeds, fixture outputs or reference-answer lookups are hard-coded. See rules.md
for the published task specification. Large datasets and outputs are omitted.
