from __future__ import annotations

import polars as pl
import streamlit as st

from analysis.data_loader import discover_scenarios, infer_duration_seconds_from_logs
from analysis.metrics import (
    average_curve,
    average_latency,
    latency_stats_for_run,
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
def load_latency_stats(scenario: str, tech: str, run: str) -> pl.DataFrame:
    return latency_stats_for_run(scenario, tech, run)


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
        st.error("Could not infer scenario duration from logs.")
        return
    st.sidebar.caption(f"Duration inferred from logs: {duration_s}s")

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
        min_value=10,
        max_value=max(10, duration_s),
        value=min(10, duration_s),
        step=1,
    )
    throughput_event_type = st.sidebar.selectbox(
        "Throughput event type",
        ["Publication", "Reception"],
        index=0,
    )
    show_individual = st.sidebar.checkbox("Show individual runs", value=False)

    st.header("Performance Summary")
    kpi_columns = st.columns(len(selected_techs))
    kpi_data = {}

    throughput_frames: list[pl.DataFrame] = []
    latency_frames: list[pl.DataFrame] = []
    cpu_frames: list[pl.DataFrame] = []
    memory_frames: list[pl.DataFrame] = []
    disk_frames: list[pl.DataFrame] = []

    for tech in selected_techs:
        run_frames = []
        latency_stats_frames = []
        cpu_runs = []
        mem_runs = []
        disk_runs = []
        for run in runs_by_tech.get(tech, []):
            if run not in selected_runs:
                continue
            throughput = load_throughput(
                scenario, tech, run, window_s, throughput_event_type
            )
            if not throughput.is_empty():
                run_frames.append(
                    throughput.with_columns(
                        pl.lit(tech).alias("tech"), pl.lit(run).alias("run")
                    )
                )
            latency_stats = load_latency_stats(scenario, tech, run)
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
                disk_runs.append(
                    resources.select(["time_s", "disk_throughput_mb_s"]).with_columns(
                        pl.lit(tech).alias("tech"),
                        pl.lit(run).alias("run"),
                    )
                )

        avg_throughput = average_curve(
            [frame.select(["time_s", "throughput_mb_s"]) for frame in run_frames],
            "throughput_mb_s",
        )
        if not avg_throughput.is_empty():
            throughput_frames.append(
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

        avg_throughput_value = (
            avg_throughput.select(pl.col("throughput_mb_s").mean()).item()
            if not avg_throughput.is_empty()
            else 0
        )
        avg_latency_value = (
            avg_latency.select(pl.col("p99_ms")).item()
            if not avg_latency.is_empty()
            else 0
        )
        kpi_data[tech] = (avg_throughput_value, avg_latency_value)

    for column, tech in zip(kpi_columns, selected_techs):
        throughput_value, latency_value = kpi_data.get(tech, (0, 0))
        column.metric(
            label=f"{tech} Avg {throughput_event_type} Throughput (MB/s)",
            value=f"{throughput_value:,.2f}",
        )
        column.metric(label=f"{tech} P99 Latency (ms)", value=f"{latency_value:,.2f}")

    st.header(f"{throughput_event_type} Throughput (MB/s)")
    throughput_display = (
        pl.concat(throughput_frames, how="vertical")
        if throughput_frames
        else pl.DataFrame()
    )
    if show_individual:
        individual_frames = [
            frame
            for tech in selected_techs
            for frame in [
                load_throughput(
                    scenario, tech, run, window_s, throughput_event_type
                ).with_columns(pl.lit(tech).alias("tech"), pl.lit(run).alias("run"))
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
        st.info("No throughput data available for the selected filters.")
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
                y_title="Throughput (MB/s)",
            ),
            width='stretch',
        )

    st.header("Latency (ms)")
    latency_table_frame = (
        pl.concat(latency_frames, how="vertical") if latency_frames else pl.DataFrame()
    )
    if latency_table_frame.is_empty():
        st.info("No latency data available for the selected filters.")
    else:
        latency_table_frame = latency_table_frame.select(
            pl.col("tech").alias("tech_name"),
            pl.col("p99_ms").round(2).alias("P99_avg"),
            pl.col("p90_ms").round(2).alias("P90_avg"),
            pl.col("p50_ms").round(2).alias("P50_avg"),
            pl.col("max_ms").round(2).alias("max_avg"),
        ).sort("P99_avg", descending=True)
        st.dataframe(latency_table(latency_table_frame), width='stretch')

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

    st.subheader("CPU Usage")
    st.altair_chart(cpu_chart, width='stretch')
    st.subheader("Memory Usage")
    st.altair_chart(memory_chart, width='stretch')
    st.subheader("Disk Throughput")
    st.altair_chart(disk_chart, width='stretch')


if __name__ == "__main__":
    main()
