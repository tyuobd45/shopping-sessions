# Shopping Sessions: parallel Rust processing with typed DuckDB Parquet output.
# The scorer builds this image for linux/amd64 (Intel Xeon).
FROM rust:1.85-slim-bookworm AS native-build
COPY pipeline/fast.rs /build/fast.rs
RUN rustc --edition=2021 -O -C target-cpu=x86-64-v3 -C codegen-units=1 -C lto=thin /build/fast.rs -o /build/fast-engine

FROM python:3.12-slim-bookworm

# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY pipeline/ /app/pipeline/
COPY --from=native-build /build/fast-engine /app/fast-engine
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and /output and /scratch are the only writable
# places. run.sh points every temp and cache directory at /scratch.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
CMD ["/app/run.sh"]
