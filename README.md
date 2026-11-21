# Shopping Sessions

An exact native pipeline for the Databricks Builders Buildathon track. Rust handles
CSV parsing, identity linking, deduplication, sessions, advertising attribution and
assists, and writes Parquet directly. The scoring image starts the executable
without a Python interpreter or intermediate CSV conversion.

## Build and run

The Dockerfile builds the locked Cargo dependencies with Rust 1.85 and targets
x86-64-v3, supported by the scorer's amd64 Xeon machines:

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```

The container takes no arguments. It reads DATA_DIR (default /data), writes under
OUTPUT_DIR (default /output), and uses up to NCPU worker threads (maximum 8).
Inputs are read-only; output consists of Parquet files directly in user_daily/
and campaign_daily/. `day` is stored as the Parquet DATE logical type. No network,
Python installation or temporary CSV files are required at run time.

Build natively on x86-64 Linux:

```sh
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --locked
DATA_DIR=/path/to/input OUTPUT_DIR=/path/to/output NCPU=8 target/release/shopping-fast
```

Parquet, bytes and JSON dependencies are pinned by Cargo.toml and Cargo.lock.
The older Python/DuckDB pipeline remains in pipeline/pipeline.py for local
comparison and macOS development; it is not the container entry point.

## Optimizations and correctness

- AVX2 identifies field separators and quote positions together for short CSV
  rows. Long rows use the scalar parser. Both preserve the LF-only dialect,
  whitespace, carriage returns, quoted empty fields and malformed-row rejection.
- Eight-digit numeric blocks use SWAR arithmetic with strict ASCII validation.
  Longer and overflow-prone numbers use checked parsing.
- Short alphanumeric IDs are encoded losslessly into integers. Other short IDs
  preserve all bytes in 128-bit keys; long IDs retain the complete original value.
  Case, spaces, leading zeros and embedded zero bytes remain distinct.
- ID text lives in a contiguous arena rather than one allocation per name.
  The arena is shared safely with the Parquet writer without copying each name.
- Union-find maps every linked component to its byte-wise smallest ID, including
  IDs absent from events and clicks. Unlinked IDs are normalized across workers.
- Event IDs are hash-partitioned. Earliest ingestion wins before validation;
  complete IDs resolve every fingerprint collision. An invalid earliest copy
  cannot be rescued by a later valid copy.
- The documented 14-day generated window fits exact 32-bit millisecond offsets.
  The manifest range is checked before compact 64-bit user/time sort keys are
  used. No timestamp precision is reduced or rounded.
- Sorted canonical users use gaps strictly over 30 minutes. Equal timestamps
  share sessions; cross-midnight sessions count on their UTC start date.
- Sliding click windows maintain inclusive 7-day eligibility, latest-click
  attribution with byte-wise click-ID ties, and one assist per active campaign
  per session. Assist-only campaign/day rows are retained.
- Per-worker outputs are written in parallel as Snappy Parquet. Numeric totals
  remain 64-bit integers, and nullable top categories use definition levels.

## Validation

- All 36 official edge-case fixtures pass using the native executable directly.
- Both output tables exactly match the published 1M-event / 250K-click seed-1
  development answer.
- Both output tables on a generated seed-17 20M-event / 5M-click dataset exactly
  match the previous DuckDB implementation.
- Both tables also match the DuckDB implementation on a separately generated
  seed-73 200K-event / 50K-click dataset.
- Four Rust tests cover strict ASCII numeric parsing in every byte lane, SIMD
  boundary and malformed-quote behavior against the scalar parser, lossless ID
  key boundaries, and full-ID resolution of deliberate fingerprint collisions.

```sh
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo test --release --locked
```

A paired local benchmark on the same 20M-event dataset measured the previous
native/Python-conversion version at 7.820 and 8.293 seconds, and this direct
Parquet version at 5.441 and 4.171 seconds. These are development timings, not
leaderboard scores; the official hardware and hidden dataset differ.

The previous native submission at commit b2c792fa605e3ff25408c8cc536863205ac1723d
passed official practice evaluation in 6.58 seconds. This revised image still
requires official container and hidden-data evaluation. The prior submission
remains available in repository history. No dataset dates, user IDs, fixture
answers, seeds or reference-answer lookups are hard-coded. See rules.md for the
published task specification.
