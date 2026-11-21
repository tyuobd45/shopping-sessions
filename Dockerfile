# Native scoring image: SIMD parsing, compact ID keys and direct Parquet output.
FROM rust:1.85-slim-bookworm AS native-build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY pipeline/fast.rs pipeline/fast.rs
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=native-build /build/target/release/shopping-fast /app/shopping-fast
ENV HOME=/scratch
WORKDIR /app
CMD ["/app/shopping-fast"]
