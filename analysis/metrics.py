from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl
from analysis.data_loader import get_cache_dir, get_run_dir

EVENT_TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S%.f"
RESOURCE_TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S%.f"


def _load_event_log(run_dir: Path) -> pl.DataFrame:
    parquet_files = list(run_dir.glob("*.parquet"))
    if not parquet_files:
        return pl.DataFrame()
    lf = pl.scan_parquet(str(run_dir / "*.parquet"))
    schema = lf.collect_schema()
    select_cols = [
        "timestamp",
        "event_type",
        "message_id",
        "topic",
        "container_name",
        "logical_size",
        "serialized_size"
    ]

    topic_expr = (
        pl.col("topic").cast(pl.Utf8, strict=False)
        if "topic" in schema
        else pl.lit(None).cast(pl.Utf8).alias("topic")
    )
    container_expr = (
        pl.col("container_name").cast(pl.Utf8, strict=False)
        if "container_name" in schema
        else pl.lit(None).cast(pl.Utf8).alias("container_name")
    )
    logical_size_expr = (
        pl.col("logical_size").cast(pl.Int64, strict=False)
        if "logical_size" in schema
        else pl.lit(None).cast(pl.Int64).alias("logical_size")
    )
    serialized_size_expr = (
        pl.col("serialized_size").cast(pl.Int64, strict=False)
        if "serialized_size" in schema
        else pl.lit(None).cast(pl.Int64).alias("serialized_size")
    )
    return (
        lf.select(select_cols)
        .with_columns(
            pl.coalesce(
                [
                    pl.col("timestamp").cast(pl.Datetime, strict=False),
                    pl.col("timestamp")
                    .cast(pl.Utf8)
                    .str.strptime(
                        pl.Datetime, format=EVENT_TIMESTAMP_FMT, strict=False
                    ),
                ]
            ).alias("timestamp"),
            pl.col("message_id").cast(pl.Utf8, strict=False).alias("message_id"),
            topic_expr,
            container_expr,
            logical_size_expr,
            serialized_size_expr,
        )
        .filter(
            pl.col("event_type").is_in(
                ["Serializing", "Publication", "Reception", "Deserialized"]
            )
        )
        .filter(pl.col("message_id").is_not_null())
        .collect()
    )


def _load_resource_metrics(run_dir: Path) -> pl.DataFrame:
    csv_files = list(run_dir.glob("*.csv"))
    if not csv_files:
        return pl.DataFrame()
    frames: list[pl.DataFrame] = []
    for csv_file in csv_files:
        # indicate different containers
        source = csv_file.stem.rsplit(
            "-", 1)[-1] if "broker" not in csv_file.stem else "Broker"
        frames.append(
            pl.read_csv(csv_file).with_columns(
                pl.lit(source).alias("source"),
                pl.coalesce(
                    [
                        pl.col("timestamp").cast(pl.Datetime, strict=False),
                        pl.col("timestamp")
                        .cast(pl.Utf8)
                        .str.strptime(
                            pl.Datetime, format=RESOURCE_TIMESTAMP_FMT, strict=False
                        ),
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
    event_type: str = "Publication",
    logs_root: str | Path = "logs",
) -> pl.DataFrame:
    cache_dir = get_cache_dir(scenario, logs_root)
    event_label = event_type.lower()
    cache_path = (
        cache_dir
        / f"throughput_mb_{tech}_{run}_{event_label}_{window_s}s.parquet"
    )
    if cache_path.exists():
        cached = pl.read_parquet(cache_path)
        if (
            not cached.is_empty()
            and "throughput_mb_s" in cached.columns
            and "time_s" in cached.columns
            and cached.select(pl.col("time_s").min()).item() >= 0
            and cached.select(pl.col("time_s").n_unique()).item() == cached.height
        ):
            return cached
    run_dir = get_run_dir(scenario, tech, run, logs_root)
    events = _load_event_log(run_dir)
    if events.is_empty():
        return pl.DataFrame()

    # Fill missing serialized_size values based on stable (message_id, topic) mapping.
    # Rule: if Serializing lacks size, use Publication's size; if Reception lacks size,
    # use Deserialized's size.
    key_cols = ["message_id", "topic"]
    pub_sizes = (
        events.filter(
            (pl.col("event_type") == "Publication")
            & pl.col("serialized_size").is_not_null()
        )
        .group_by(key_cols)
        .agg(pl.col("serialized_size").first().alias("_size_from_publication"))
    )
    des_sizes = (
        events.filter(
            (pl.col("event_type") == "Deserialized")
            & pl.col("serialized_size").is_not_null()
        )
        .group_by(key_cols)
        .agg(pl.col("serialized_size").first().alias("_size_from_deserialized"))
    )
    events = (
        events.join(pub_sizes, on=key_cols, how="left")
        .join(des_sizes, on=key_cols, how="left")
        .with_columns(
            pl.when(pl.col("event_type") == "Serializing")
            .then(
                pl.coalesce(
                    [pl.col("serialized_size"), pl.col("_size_from_publication")]
                )
            )
            .when(pl.col("event_type") == "Reception")
            .then(
                pl.coalesce(
                    [pl.col("serialized_size"), pl.col("_size_from_deserialized")]
                )
            )
            .otherwise(
                pl.coalesce(
                    [
                        pl.col("serialized_size"),
                        pl.col("_size_from_publication"),
                        pl.col("_size_from_deserialized"),
                    ]
                )
            )
            .cast(pl.Int64, strict=False)
            .alias("serialized_size")
        )
        .drop(["_size_from_publication", "_size_from_deserialized"])
    )

    # Align all throughput curves to the same run start timestamp
    run_start_ts = events.select(pl.col("timestamp").min()).item()

    events = events.filter(pl.col("event_type") == event_type)
    if events.is_empty():
        return pl.DataFrame()

    events = events.filter(pl.col("serialized_size").is_not_null())
    if events.is_empty():
        return pl.DataFrame()

    throughput = (
        events.sort("timestamp")
        # [t0, t0+window), [t0+1s, t0+1s+window), ...]
        .group_by_dynamic("timestamp", every="1s", period=f"{window_s}s", closed="left")
        .agg(pl.col("serialized_size").sum().alias("bytes"))
        .with_columns(
            (
                (
                    pl.col("timestamp").dt.epoch("ms")
                    - pl.lit(run_start_ts).cast(pl.Datetime).dt.epoch("ms")
                )
                / 1000
            )
            .floor()
            .cast(pl.Int64)
            .alias("time_s"),
            (pl.col("bytes") / window_s / 1_000_000).alias("throughput_mb_s"),
        )
        .filter(pl.col("time_s") >= 0)
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
    cache_path = cache_dir / f"latency_segments_{tech}_{run}.parquet"
    if cache_path.exists():
        cached = pl.read_parquet(cache_path)
        expected_segments = {
            "serializing_to_publication",
            "publication_to_reception",
            "reception_to_deserialized",
            "end_to_end",
        }
        if (
            not cached.is_empty()
            and "segment" in cached.columns
            and expected_segments.issubset(set(cached.get_column("segment").to_list()))
        ):
            return cached
    run_dir = get_run_dir(scenario, tech, run, logs_root)
    events = _load_event_log(run_dir)
    if events.is_empty():
        return pl.DataFrame()

    base_keys = ["message_id", "topic"]
    publisher_keys = [*base_keys, "container_name"]
    consumer_keys = [*base_keys, "container_name"]

    def _segment(
        start_type: str,
        end_type: str,
        join_keys: list[str],
        segment: str,
    ) -> pl.DataFrame:
        start_df = (
            events.filter(pl.col("event_type") == start_type)
            .select(*[pl.col(k) for k in join_keys], pl.col("timestamp").alias("t0"))
        )
        end_df = (
            events.filter(pl.col("event_type") == end_type)
            .select(*[pl.col(k) for k in join_keys], pl.col("timestamp").alias("t1"))
        )
        if start_df.is_empty() or end_df.is_empty():
            return pl.DataFrame()
        latency = (
            start_df.join(end_df, on=join_keys, how="inner")
            .with_columns(
                (pl.col("t1").dt.epoch("ms") - pl.col("t0").dt.epoch("ms"))
                .cast(pl.Int64)
                .clip(lower_bound=0)
                .alias("latency_ms")
            )
            .select("latency_ms")
        )
        if latency.is_empty():
            return pl.DataFrame()
        return latency.select(
            pl.lit(segment).alias("segment"),
            pl.col("latency_ms").quantile(0.50).alias("p50_ms"),
            pl.col("latency_ms").quantile(0.90).alias("p90_ms"),
            pl.col("latency_ms").quantile(0.99).alias("p99_ms"),
            pl.col("latency_ms").max().alias("max_ms"),
        )

    # NOTE: Publication->Reception and Serializing->Deserialized can be one-to-many.
    segments = [
        _segment(
            "Serializing",
            "Publication",
            publisher_keys,
            "serializing_to_publication",
        ),
        _segment(
            "Publication",
            "Reception",
            base_keys,
            "publication_to_reception",
        ),
        _segment(
            "Reception",
            "Deserialized",
            consumer_keys,
            "reception_to_deserialized",
        ),
        _segment(
            "Serializing",
            "Deserialized",
            base_keys,
            "end_to_end",
        ),
    ]

    stats = pl.concat([s for s in segments if not s.is_empty()], how="vertical")
    if stats.is_empty():
        return pl.DataFrame()

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
    metrics = metrics.filter(
        (pl.col("source") == "Broker") | pl.col("source").str.starts_with("P")
    )
    metrics = metrics.sort(["source", "timestamp"]).with_columns(
        pl.col("disk_read").diff().over("source").alias("disk_read_delta"),
        pl.col("disk_write").diff().over("source").alias("disk_write_delta"),
        (pl.col("timestamp").dt.epoch("ms").diff().over("source") / 1000).alias(
            "time_delta_s"
        ),
    )
    disk_throughput_mb_s = (
        pl.when(pl.col("time_delta_s") > 0)
        .then(
            (pl.col("disk_read_delta") + pl.col("disk_write_delta"))
            / pl.col("time_delta_s")
            / 1_000_000
        )
        .otherwise(0)
        .fill_null(0)
    )
    metrics = metrics.with_columns(
        pl.when(disk_throughput_mb_s < 0)
        .then(0)
        .otherwise(disk_throughput_mb_s)
        .alias("disk_throughput_mb_s")
    )
    metrics = metrics.with_columns(
        pl.cum_count("source").over("source").alias("row_index")
    )
    producer_counts = (
        metrics.filter(pl.col("source").str.starts_with("P"))
        .group_by("source")
        .agg(pl.len().alias("count"))
    )
    if producer_counts.is_empty():
        return pl.DataFrame()
    min_producer_count = producer_counts.select(pl.col("count").min()).item()
    metrics = metrics.filter(pl.col("row_index") <= min_producer_count)
    aggregated = (
        metrics.group_by("row_index")
        .agg(
            pl.from_epoch(
                pl.col("timestamp").dt.epoch("ms").mean(),
                time_unit="ms",
            ).alias("timestamp"),
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
        (pl.col("memory_usage") / 1024 / 1024).alias("memory_mb"),
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
    if "segment" not in combined.columns:
        return pl.DataFrame()
    return (
        combined.group_by("segment")
        .agg(
            pl.col("p50_ms").mean().alias("p50_ms"),
            pl.col("p90_ms").mean().alias("p90_ms"),
            pl.col("p99_ms").mean().alias("p99_ms"),
            pl.col("max_ms").mean().alias("max_ms"),
        )
        .sort("segment")
    )
