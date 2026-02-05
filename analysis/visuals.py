from __future__ import annotations

import altair as alt
import pandas as pd
import polars as pl


def to_pandas(frame: pl.DataFrame) -> pd.DataFrame:
    if frame.is_empty():
        return pd.DataFrame()
    return frame.to_pandas()


def line_chart(
    frame: pl.DataFrame,
    x: str,
    y: str,
    color: str,
    tooltip: list[str],
    y_title: str,
) -> alt.Chart:
    data = to_pandas(frame)
    if data.empty:
        return alt.Chart(pd.DataFrame({x: [], y: []})).mark_line()
    return (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X(x, title="Time (s)"),
            y=alt.Y(y, title=y_title),
            color=alt.Color(color, title="Series"),
            tooltip=tooltip,
        )
        .interactive()
    )


def latency_table(frame: pl.DataFrame) -> pd.DataFrame:
    return to_pandas(frame)
