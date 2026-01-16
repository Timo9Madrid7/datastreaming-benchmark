from __future__ import annotations

import polars as pl
import streamlit as st
from analysis.data_loader import discover_scenarios, infer_duration_seconds_from_logs
from analysis.metrics import (
    average_curve,
    average_latency,
    latency_stats_for_run,
    latency_samples_for_run,
    resource_usage_for_run,
    throughput_for_run,
)
from analysis.visuals import latency_table, line_chart

st.set_page_config(page_title="Benchmark Analysis", layout="wide")


@st.cache_data(show_spinner=False)
def load_scenarios() -> dict[str, dict[str, list[str]]]:
    return discover_scenarios()


@st.cache_data(show_spinner=False)
def load_throughput(
    scenario: str,
    tech: str,
    run: str,
    window_s: int,
    event_type: str,
) -> pl.DataFrame:
    return throughput_for_run(scenario, tech, run, window_s, event_type)


@st.cache_data(show_spinner=False)
def load_latency_stats(
    scenario: str,
    tech: str,
    run: str,
) -> pl.DataFrame:
    return latency_stats_for_run(scenario, tech, run)


@st.cache_data(show_spinner=False)
def load_latency_samples(scenario: str, tech: str, run: str) -> pl.DataFrame:
    return latency_samples_for_run(scenario, tech, run)


@st.cache_data(show_spinner=False)
def load_resources(scenario: str, tech: str, run: str) -> pl.DataFrame:
    return resource_usage_for_run(scenario, tech, run)


def main() -> None:
    st.title("Streaming Benchmark Analysis")

    # scenarios structure: {scenario: {tech: [run1, run2, ...], ...}, ...}
    scenarios = load_scenarios()
    if not scenarios:
        st.info("No scenarios found under logs/.")
        return
    scenario = st.sidebar.selectbox("Scenario", sorted(scenarios.keys()))

    duration_s = infer_duration_seconds_from_logs(scenario)
    if duration_s is None:
        st.sidebar.caption("Duration inferred from logs: unknown")
    else:
        st.sidebar.caption(
            f"Duration inferred from logs (nominal): {duration_s}s"
        )

    st.sidebar.subheader("Trim unstable edges")
    # IMPORTANT: do not treat inferred duration as real runtime.
    # Consumers can lag behind, making actual event timelines longer.
    max_offset = 3600
    start_offset_s = int(
        st.sidebar.number_input(
            "Start offset (s)",
            min_value=0,
            max_value=max_offset,
            value=5,
            step=1,
            help="Ignore the first N seconds for throughput and latency.",
        )
    )
    end_offset_s = int(
        st.sidebar.number_input(
            "End offset (s)",
            min_value=0,
            max_value=max_offset,
            value=5,
            step=1,
            help="Ignore the last N seconds for throughput and latency.",
        )
    )
    st.sidebar.caption(
        "Effective window (per run): "
        f"{start_offset_s}s .. (max time - {end_offset_s}s)"
    )

    def _trim_throughput_window(frame: pl.DataFrame) -> pl.DataFrame:
        if frame.is_empty():
            return frame
        max_time_s = frame.select(pl.col("time_s").max()).item()
        if max_time_s is None:
            return frame.head(0)
        end_s = int(max_time_s) - end_offset_s
        if end_s < start_offset_s:
            return frame.head(0)
        return frame.filter(
            (pl.col("time_s") >= start_offset_s) & (pl.col("time_s") <= end_s)
        )

    def _latency_stats_with_offset(scenario: str, tech: str, run: str) -> pl.DataFrame:
        if start_offset_s == 0 and end_offset_s == 0:
            return load_latency_stats(scenario, tech, run)

        samples = load_latency_samples(scenario, tech, run)
        if samples.is_empty():
            return pl.DataFrame()

        max_time_s = samples.select(pl.col("time_s").max()).item()
        if max_time_s is None:
            return pl.DataFrame()
        end_s = int(max_time_s) - end_offset_s
        if end_s < start_offset_s:
            return pl.DataFrame()
        samples = samples.filter(
            (pl.col("time_s") >= start_offset_s) & (pl.col("time_s") <= end_s)
        )
        if samples.is_empty():
            return pl.DataFrame()

        return (
            samples.group_by("segment")
            .agg(
                pl.col("latency_ms").quantile(0.50).alias("p50_ms"),
                pl.col("latency_ms").quantile(0.90).alias("p90_ms"),
                pl.col("latency_ms").quantile(0.99).alias("p99_ms"),
                pl.col("latency_ms").max().alias("max_ms"),
            )
            .sort("segment")
        )

    techs_available = sorted(scenarios[scenario].keys())
    selected_techs = st.sidebar.multiselect(
        "Technologies",
        techs_available,
        default=techs_available,
    )
    if not selected_techs:
        st.warning("Select at least one technology.")
        return

    runs_by_tech = {tech: scenarios[scenario].get(tech, []) for tech in selected_techs}

    use_all_runs = st.sidebar.checkbox("Average across all runs", value=True)
    if use_all_runs:
        selected_runs = sorted({run for runs in runs_by_tech.values() for run in runs})
    else:
        selected_runs = st.sidebar.multiselect(
            "Runs",
            sorted({run for runs in runs_by_tech.values() for run in runs}),
        )
    if not selected_runs:
        st.warning("Select at least one run.")
        return

    window_s = st.sidebar.slider(
        "Throughput window size (seconds)",
        min_value=1,
        max_value=10,
        value=10,
        step=1,
    )
    show_individual = st.sidebar.checkbox("Show individual runs", value=False)

    throughput_event_types = ["Publication", "Reception", "Deserialized"]
    throughput_frames_by_event: dict[str, list[pl.DataFrame]] = {
        event_type: [] for event_type in throughput_event_types
    }
    latency_frames: list[pl.DataFrame] = []
    cpu_frames: list[pl.DataFrame] = []
    memory_frames: list[pl.DataFrame] = []
    page_cache_frames: list[pl.DataFrame] = []
    disk_frames: list[pl.DataFrame] = []
    network_rx_frames: list[pl.DataFrame] = []
    network_tx_frames: list[pl.DataFrame] = []

    for tech in selected_techs:
        run_frames_by_event: dict[str, list[pl.DataFrame]] = {
            event_type: [] for event_type in throughput_event_types
        }
        latency_stats_frames = []
        cpu_runs = []
        mem_runs = []
        page_cache_runs = []
        disk_runs = []
        net_rx_runs = []
        net_tx_runs = []
        for run in runs_by_tech.get(tech, []):
            if run not in selected_runs:
                continue
            for event_type in throughput_event_types:
                throughput = load_throughput(
                    scenario,
                    tech,
                    run,
                    window_s,
                    event_type,
                )
                throughput = _trim_throughput_window(throughput)
                if not throughput.is_empty():
                    run_frames_by_event[event_type].append(
                        throughput.with_columns(
                            pl.lit(tech).alias("tech"), pl.lit(run).alias("run")
                        )
                    )
            latency_stats = _latency_stats_with_offset(scenario, tech, run)
            if not latency_stats.is_empty():
                latency_stats_frames.append(latency_stats)
            resources = load_resources(scenario, tech, run)
            if not resources.is_empty():
                cpu_runs.append(
                    resources.select(["time_s", "cpu_usage_perc"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )
                mem_runs.append(
                    resources.select(["time_s", "memory_mb"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )
                page_cache_runs.append(
                    resources.select(["time_s", "page_cache_mb"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )
                disk_runs.append(
                    resources.select(["time_s", "disk_throughput_mb_s"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )
                net_rx_runs.append(
                    resources.select(["time_s", "network_rx_mb_s"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )
                net_tx_runs.append(
                    resources.select(["time_s", "network_tx_mb_s"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )

        for event_type in throughput_event_types:
            avg_throughput = average_curve(
                [
                    frame.select(["time_s", "throughput_mb_s"])
                    for frame in run_frames_by_event[event_type]
                ],
                "throughput_mb_s",
            )
            if not avg_throughput.is_empty():
                throughput_frames_by_event[event_type].append(
                    avg_throughput.with_columns(
                        pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                    )
                )
        avg_latency = average_latency(latency_stats_frames)
        if not avg_latency.is_empty():
            latency_frames.append(avg_latency.with_columns(pl.lit(tech).alias("tech")))
        avg_cpu = average_curve(
            [frame.select(["time_s", "cpu_usage_perc"]) for frame in cpu_runs],
            "cpu_usage_perc",
        )
        if not avg_cpu.is_empty():
            cpu_frames.append(
                avg_cpu.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )
        avg_memory = average_curve(
            [frame.select(["time_s", "memory_mb"]) for frame in mem_runs], "memory_mb"
        )
        if not avg_memory.is_empty():
            memory_frames.append(
                avg_memory.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )
        avg_page_cache = average_curve(
            [frame.select(["time_s", "page_cache_mb"]) for frame in page_cache_runs],
            "page_cache_mb",
        )
        if not avg_page_cache.is_empty():
            page_cache_frames.append(
                avg_page_cache.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )
        avg_disk = average_curve(
            [frame.select(["time_s", "disk_throughput_mb_s"]) for frame in disk_runs],
            "disk_throughput_mb_s",
        )
        if not avg_disk.is_empty():
            disk_frames.append(
                avg_disk.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )
        avg_net_rx = average_curve(
            [frame.select(["time_s", "network_rx_mb_s"]) for frame in net_rx_runs],
            "network_rx_mb_s",
        )
        if not avg_net_rx.is_empty():
            network_rx_frames.append(
                avg_net_rx.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )
        avg_net_tx = average_curve(
            [frame.select(["time_s", "network_tx_mb_s"]) for frame in net_tx_runs],
            "network_tx_mb_s",
        )
        if not avg_net_tx.is_empty():
            network_tx_frames.append(
                avg_net_tx.with_columns(
                    pl.lit(tech).alias("tech"), pl.lit("avg").alias("run")
                )
            )

    st.header("Throughput (MB/s)")
    for event_type in throughput_event_types:
        st.subheader(event_type)
        throughput_display = (
            pl.concat(throughput_frames_by_event[event_type], how="vertical")
            if throughput_frames_by_event[event_type]
            else pl.DataFrame()
        )
        if show_individual:
            individual_frames = [
                frame
                for tech in selected_techs
                for frame in [
                    _trim_throughput_window(
                        load_throughput(
                            scenario,
                            tech,
                            run,
                            window_s,
                            event_type,
                        )
                    ).with_columns(
                        pl.lit(tech).alias("tech"), pl.lit(run).alias("run")
                    )
                    for run in runs_by_tech.get(tech, [])
                    if run in selected_runs
                ]
                if not frame.is_empty()
            ]
            if individual_frames:
                throughput_display = pl.concat(
                    [throughput_display, *individual_frames], how="vertical"
                )
        if throughput_display.is_empty():
            st.info(
                f"No {event_type} throughput data available for the selected filters."
            )
        else:
            st.altair_chart(
                line_chart(
                    throughput_display.with_columns(
                        (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
                    ),
                    x="time_s",
                    y="throughput_mb_s",
                    color="series",
                    tooltip=["time_s", "throughput_mb_s", "tech", "run"],
                    y_title=f"{event_type} throughput (MB/s)",
                ),
                width="stretch",
            )

    st.header("Latency (ms)")
    latency_table_frame = (
        pl.concat(latency_frames, how="vertical") if latency_frames else pl.DataFrame()
    )
    if latency_table_frame.is_empty():
        st.info("No latency data available for the selected filters.")
    else:
        latency_table_frame = (
            latency_table_frame.select(
                pl.col("tech").alias("tech_name"),
                pl.col("segment"),
                pl.col("p50_ms").round(2).alias("P50"),
                pl.col("p90_ms").round(2).alias("P90"),
                pl.col("p99_ms").round(2).alias("P99"),
                pl.col("max_ms").round(2).alias("Max"),
            )
            .sort(["segment", "P99"], descending=[False, True])
        )
        st.dataframe(latency_table(latency_table_frame), width="stretch")

    st.header("Resource Usage")
    cpu_display = (
        pl.concat(cpu_frames, how="vertical") if cpu_frames else pl.DataFrame()
    )
    memory_display = (
        pl.concat(memory_frames, how="vertical") if memory_frames else pl.DataFrame()
    )
    disk_display = (
        pl.concat(disk_frames, how="vertical") if disk_frames else pl.DataFrame()
    )
    page_cache_display = (
        pl.concat(page_cache_frames, how="vertical")
        if page_cache_frames
        else pl.DataFrame()
    )
    network_rx_display = (
        pl.concat(network_rx_frames, how="vertical")
        if network_rx_frames
        else pl.DataFrame()
    )
    network_tx_display = (
        pl.concat(network_tx_frames, how="vertical")
        if network_tx_frames
        else pl.DataFrame()
    )
    if show_individual:
        for tech in selected_techs:
            for run in runs_by_tech.get(tech, []):
                if run not in selected_runs:
                    continue
                resources = load_resources(scenario, tech, run)
                if resources.is_empty():
                    continue
                cpu_display = pl.concat(
                    [
                        cpu_display,
                        resources.select(["time_s", "cpu_usage_perc"]).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )
                memory_display = pl.concat(
                    [
                        memory_display,
                        resources.select(["time_s", "memory_mb"]).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )
                disk_display = pl.concat(
                    [
                        disk_display,
                        resources.select(
                            ["time_s", "disk_throughput_mb_s"]
                        ).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )
                page_cache_display = pl.concat(
                    [
                        page_cache_display,
                        resources.select(["time_s", "page_cache_mb"]).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )
                network_rx_display = pl.concat(
                    [
                        network_rx_display,
                        resources.select(["time_s", "network_rx_mb_s"]).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )
                network_tx_display = pl.concat(
                    [
                        network_tx_display,
                        resources.select(["time_s", "network_tx_mb_s"]).with_columns(
                            pl.lit(tech).alias("tech"),
                            pl.lit(run).alias("run"),
                        ),
                    ],
                    how="vertical",
                )

    if cpu_display.is_empty():
        st.info("No resource data available for the selected filters.")
        return

    cpu_chart = line_chart(
        cpu_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="cpu_usage_perc",
        color="series",
        tooltip=["time_s", "cpu_usage_perc", "tech", "run"],
        y_title="CPU Usage (%)",
    )
    memory_chart = line_chart(
        memory_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="memory_mb",
        color="series",
        tooltip=["time_s", "memory_mb", "tech", "run"],
        y_title="Memory (MB)",
    )
    disk_chart = line_chart(
        disk_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="disk_throughput_mb_s",
        color="series",
        tooltip=["time_s", "disk_throughput_mb_s", "tech", "run"],
        y_title="Disk Throughput (MB/s)",
    )
    page_cache_chart = line_chart(
        page_cache_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="page_cache_mb",
        color="series",
        tooltip=["time_s", "page_cache_mb", "tech", "run"],
        y_title="Page Cache (MB)",
    )
    network_rx_chart = line_chart(
        network_rx_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="network_rx_mb_s",
        color="series",
        tooltip=["time_s", "network_rx_mb_s", "tech", "run"],
        y_title="Network In (MB/s)",
    )
    network_tx_chart = line_chart(
        network_tx_display.with_columns(
            (pl.col("tech") + " (" + pl.col("run") + ")").alias("series")
        ),
        x="time_s",
        y="network_tx_mb_s",
        color="series",
        tooltip=["time_s", "network_tx_mb_s", "tech", "run"],
        y_title="Network Out (MB/s)",
    )

    st.subheader("CPU Usage")
    st.altair_chart(cpu_chart, width="stretch")
    st.subheader("Memory Usage")
    st.altair_chart(memory_chart, width="stretch")
    st.subheader("Disk Throughput")
    st.altair_chart(disk_chart, width="stretch")
    st.subheader("Page Cache")
    st.altair_chart(page_cache_chart, width="stretch")
    st.subheader("Network In")
    st.altair_chart(network_rx_chart, width="stretch")
    st.subheader("Network Out")
    st.altair_chart(network_tx_chart, width="stretch")


if __name__ == "__main__":
    main()
