from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

from analysis.data_loader import get_cache_dir, get_run_dir

EVENT_TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S%.fZ"
RESOURCE_TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S%.f"


def _load_event_log(run_dir: Path) -> pl.DataFrame:
    parquet_files = list(run_dir.glob("*.parquet"))
    if not parquet_files:
        return pl.DataFrame()
    return (
        pl.scan_parquet(str(run_dir / "*.parquet"))
        .select(["timestamp", "event_type", "message_id", "serialized_size"])
        .filter(pl.col("event_type").is_in(["Publication", "Reception"]))
        .with_columns(
            pl.coalesce(
                [
                    pl.col("timestamp").cast(pl.Datetime, strict=False),
                    pl.col("timestamp")
                    .cast(pl.Utf8)
                    .str.strptime(pl.Datetime, format=EVENT_TIMESTAMP_FMT, strict=False),
                ]
            ).alias("timestamp"),
            pl.col("serialized_size").cast(pl.Int64),
        )
        .collect()
    )


def _load_resource_metrics(run_dir: Path) -> pl.DataFrame:
    csv_files = list(run_dir.glob("*.csv"))
    if not csv_files:
        return pl.DataFrame()
    frames: list[pl.DataFrame] = []
    for csv_file in csv_files:
        # indicate different containers
        source = csv_file.stem.rsplit("-", 1)[-1] if "-" in csv_file.stem else "BROKER"
        frames.append(
            pl.read_csv(csv_file).with_columns(
                pl.lit(source).alias("source"),  
                pl.coalesce(
                    [
                        pl.col("timestamp").cast(pl.Datetime, strict=False),
                        pl.col("timestamp")
                        .cast(pl.Utf8)
                        .str.strptime(pl.Datetime, format=RESOURCE_TIMESTAMP_FMT, strict=False),
                    ]
                ).alias("timestamp"),
            )
        )
    return pl.concat(frames, how="vertical")


def throughput_for_run(
    scenario: str,
    tech: str,
    run: str,
    window_s: int,
    logs_root: str | Path = "logs",
) -> pl.DataFrame:
    cache_dir = get_cache_dir(scenario, logs_root)
    cache_path = cache_dir / f"throughput_{tech}_{run}_{window_s}s.parquet"
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    run_dir = get_run_dir(scenario, tech, run, logs_root)
    events = _load_event_log(run_dir)
    if events.is_empty():
        return pl.DataFrame()
    min_ts = events.select(pl.col("timestamp").min()).item()
    throughput = (
        events.sort("timestamp")
        # [t0, t0+window), [t0+1s, t0+1s+window), ...]
        .group_by_dynamic("timestamp", every="1s", period=f"{window_s}s", closed="left")
        .agg(pl.col("serialized_size").sum().alias("bytes"))
        .with_columns(
            (
                (
                    pl.col("timestamp").dt.epoch("ms")
                    - pl.lit(min_ts).cast(pl.Datetime).dt.epoch("ms")
                )
                / 1000
            )
            .floor()
            .cast(pl.Int64)
            .alias("time_s"),
            (pl.col("bytes") / (window_s * 1024 * 1024)).alias("throughput_mb_s"),
        )
        .select(["time_s", "throughput_mb_s"])
    )
    throughput.write_parquet(cache_path)
    return throughput


def latency_stats_for_run(
    scenario: str,
    tech: str,
    run: str,
    logs_root: str | Path = "logs",
) -> pl.DataFrame:
    cache_dir = get_cache_dir(scenario, logs_root)
    cache_path = cache_dir / f"latency_{tech}_{run}.parquet"
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    run_dir = get_run_dir(scenario, tech, run, logs_root)
    events = _load_event_log(run_dir)
    if events.is_empty():
        return pl.DataFrame()
    publications = (
        events.filter(pl.col("event_type") == "Publication")
        .select(
            pl.col("message_id"),
            pl.col("timestamp").alias("pub_ts"),
        )
    )
    consumptions = (
        events.filter(pl.col("event_type") == "Reception")
        .select(
            pl.col("message_id"),
            pl.col("timestamp").alias("cons_ts"),
        )
    )
    latency = (
        publications.join(consumptions, on="message_id", how="inner")
        .with_columns(
            (pl.col("cons_ts").dt.epoch("ms") - pl.col("pub_ts").dt.epoch("ms"))
            .cast(pl.Int64)
            .alias("latency_ms")
        )
        .select("latency_ms")
    )
    if latency.is_empty():
        return pl.DataFrame()
    stats = latency.select(
        pl.col("latency_ms").quantile(0.99).alias("p99_ms"),
        pl.col("latency_ms").quantile(0.90).alias("p90_ms"),
        pl.col("latency_ms").quantile(0.50).alias("p50_ms"),
        pl.col("latency_ms").max().alias("max_ms"),
    )
    stats.write_parquet(cache_path)
    return stats


def resource_usage_for_run(
    scenario: str,
    tech: str,
    run: str,
    logs_root: str | Path = "logs",
) -> pl.DataFrame:
    cache_dir = get_cache_dir(scenario, logs_root)
    cache_path = cache_dir / f"resources_{tech}_{run}.parquet"
    if cache_path.exists():
        return pl.read_parquet(cache_path)
    run_dir = get_run_dir(scenario, tech, run, logs_root)
    metrics = _load_resource_metrics(run_dir)
    if metrics.is_empty():
        return pl.DataFrame()
    metrics = metrics.sort(["source", "timestamp"]).with_columns(
        pl.col("disk_read").diff().over("source").alias("disk_read_delta"),
        pl.col("disk_write").diff().over("source").alias("disk_write_delta"),
        (
            pl.col("timestamp").dt.epoch("ms").diff().over("source") / 1000
        ).alias("time_delta_s"),
    )
    disk_throughput_mb_s = (
        pl.when(pl.col("time_delta_s") > 0)
        .then((pl.col("disk_read_delta") + pl.col("disk_write_delta")) / pl.col("time_delta_s") / 1_000_000)
        .otherwise(0)
        .fill_null(0)
    )
    metrics = metrics.with_columns(
        pl.when(disk_throughput_mb_s < 0)
        .then(0)
        .otherwise(disk_throughput_mb_s)
        .alias("disk_throughput_mb_s")
    )
    metrics = metrics.sort("timestamp")
    aggregated = (
        metrics.group_by_dynamic("timestamp", every="1s")
        .agg(
            pl.col("cpu_usage_perc").sum().alias("cpu_usage_perc"),
            pl.col("memory_usage").sum().alias("memory_usage"),
            pl.col("disk_throughput_mb_s").sum().alias("disk_throughput_mb_s"),
        )
        .sort("timestamp")
    )
    min_ts = aggregated.select(pl.col("timestamp").min()).item()
    aggregated = aggregated.with_columns(
        (
            (
                pl.col("timestamp").dt.epoch("ms")
                - pl.lit(min_ts).cast(pl.Datetime).dt.epoch("ms")
            )
            / 1000
        )
        .floor()
        .cast(pl.Int64)
        .alias("time_s"),
        (pl.col("memory_usage") / 1_000_000).alias("memory_mb"),
    ).select(["time_s", "cpu_usage_perc", "memory_mb", "disk_throughput_mb_s"])
    aggregated.write_parquet(cache_path)
    return aggregated


def average_curve(
    curves: Iterable[pl.DataFrame],
    value_column: str,
    time_column: str = "time_s",
) -> pl.DataFrame:
    frames = [curve for curve in curves if not curve.is_empty()]
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames, how="vertical")
    return (
        combined.group_by(time_column)
        .agg(pl.col(value_column).mean().alias(value_column))
        .sort(time_column)
    )


def average_latency(stats_frames: Iterable[pl.DataFrame]) -> pl.DataFrame:
    frames = [frame for frame in stats_frames if not frame.is_empty()]
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames, how="vertical")
    return combined.select(
        pl.col("p99_ms").mean().alias("p99_ms"),
        pl.col("p90_ms").mean().alias("p90_ms"),
        pl.col("p50_ms").mean().alias("p50_ms"),
        pl.col("max_ms").mean().alias("max_ms"),
    )
