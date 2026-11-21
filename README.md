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
- LF boundaries are scanned with memchr while preserving the exact LF-only
  dialect, including empty final fields and carriage returns.
- Eight-digit numeric blocks use SWAR arithmetic with strict ASCII validation.
  Longer and overflow-prone numbers use checked parsing.
- Short alphanumeric IDs are encoded losslessly into integers. Other short IDs
  preserve all bytes in 128-bit keys; long IDs retain the complete original value.
  Case, spaces, leading zeros and embedded zero bytes remain distinct.
- ID text lives in a contiguous arena rather than one allocation per name.
  The arena is shared safely with the Parquet writer without copying each name.
- Union-find maps every linked component to its byte-wise smallest ID, including
  IDs absent from events and clicks. Unlinked IDs are normalized across workers.
- Event IDs are distributed over smaller cache-friendly deduplication shards.
  Worker-local unknown user IDs are resolved only for surviving events, avoiding
  a separate rewrite of every raw event. Earliest ingestion wins before validation;
  complete IDs resolve every fingerprint collision. An invalid earliest copy
  cannot be rescued by a later valid copy.
- The documented 14-day generated window fits exact 32-bit millisecond offsets.
  The manifest range is checked before compact 64-bit user/time sort keys are
  used. No timestamp precision is reduced or rounded.
- Large event partitions use an exact integer radix sort of the full user/time
  key; small inputs use the standard comparison sort. Both preserve every row.
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
- Seven Rust tests cover strict ASCII numeric parsing in every byte lane, SIMD
  boundary and malformed-quote behavior against the scalar parser, lossless ID
  keys, deliberate fingerprint collisions, LF iterator equivalence, full-width
  radix ordering and payload preservation, and deferred user mapping across all
  eight workers.

```sh
RUSTFLAGS='-C target-cpu=x86-64-v3' cargo test --release --locked
```

The previous direct-Parquet submission at commit
b4c5b4e130cb627bf63a1b98d21e3a3e7d7639ab passed official practice evaluation
in **4.48 seconds**. An alternating local comparison on the same seed-17
20M-event / 5M-click dataset produced:

| Version | Three local runs (seconds) | Median |
| --- | --- | --- |
| Previous scored version | 5.092, 5.334, 5.656 | 5.334 |
| This candidate | 3.841, 4.696, 3.934 | 3.934 |

That is about 26% less wall time in this local comparison. These are development
timings, not leaderboard scores; hardware, load and hidden data differ. The new
container still requires official evaluation. Previous scored commits remain
available in repository history.

No dataset dates, user IDs, fixture answers, seeds or reference-answer lookups
are hard-coded. The compact per-worker index is checked at construction; the
published generated dataset sizes fit its 28-bit range. See rules.md for the
published task specification.
