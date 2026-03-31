import os
from functools import lru_cache

import pandas as pd
import plotly.express as px
import streamlit as st


def _env(name: str, default=None):
    return os.getenv(name, default)


def _try_load_from_bigquery():
    project_id = _env("GCP_PROJECT_ID")
    dataset = _env("BQ_DATASET")
    keyfile = _env("GOOGLE_APPLICATION_CREDENTIALS")

    if not (project_id and dataset and keyfile and os.path.exists(keyfile)):
        return None

    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    table_fct = f"{project_id}.{dataset}.fct_tea_coffee_preferences"

    q_category = f"""
        SELECT
            drink_category,
            COUNT(1) AS cnt
        FROM `{table_fct}`
        GROUP BY drink_category
        ORDER BY cnt DESC
    """

    q_timeseries = f"""
        SELECT
            year,
            COUNT(1) AS cnt,
            AVG(monthly_spend) AS avg_monthly_spend
        FROM `{table_fct}`
        GROUP BY year
        ORDER BY year
    """

    df_category = client.query(q_category).to_dataframe()
    df_timeseries = client.query(q_timeseries).to_dataframe()
    return df_category, df_timeseries


@lru_cache(maxsize=1)
def _try_load_from_csv_fallback():
    path = _env("DATASET_LOCAL_PATH")
    if not path or not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    return compute_dashboard_data_from_df(df)


def compute_dashboard_data_from_df(df: pd.DataFrame):
    """Pure helper for dashboard charts (easy to unit test)."""
    df = df.copy()

    # Ensure expected dtypes for charts
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
    if "monthly_spend" in df.columns:
        df["monthly_spend"] = pd.to_numeric(df["monthly_spend"], errors="coerce")

    df_category = (
        df.groupby("drink_category")
        .size()
        .reset_index(name="cnt")
        .sort_values("cnt", ascending=False)
    )

    df_timeseries = (
        df.groupby("year")
        .size()
        .reset_index(name="cnt")
        .sort_values("year")
    )
    if "monthly_spend" in df.columns:
        df_avg = df.groupby("year")["monthly_spend"].mean().reset_index(name="avg_monthly_spend")
        df_timeseries = df_timeseries.merge(df_avg, on="year", how="left")

    return df_category, df_timeseries


def main() -> None:
    st.set_page_config(page_title="Tea vs Coffee Dashboard", layout="wide")
    st.title("Tea vs Coffee Analytics Dashboard")
    st.caption("Data pipeline: Data Lake (GCS) -> Data Warehouse (BigQuery) -> Dashboard (Streamlit)")

    loaded = _try_load_from_bigquery()
    if loaded is None:
        loaded = _try_load_from_csv_fallback()

    if loaded is None:
        st.error(
            "Cannot load data. Set `GCP_PROJECT_ID`, `BQ_DATASET`, and `GOOGLE_APPLICATION_CREDENTIALS` "
            "(or set `DATASET_LOCAL_PATH` for local CSV fallback)."
        )
        st.stop()

    df_category, df_timeseries = loaded

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Tile 1: Distribution by Drink Category")
        # Keep the chart readable by limiting to top N categories
        top_n = int(_env("TOP_N_CATEGORIES", "15"))
        df_plot = df_category.head(top_n)
        fig1 = px.bar(
            df_plot,
            x="drink_category",
            y="cnt",
            title="Count by Drink Category",
            labels={"drink_category": "Drink Category", "cnt": "Number of Records"},
        )
        fig1.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        st.subheader("Tile 2: Trend Over Time (Records per Year)")
        fig2 = px.line(
            df_timeseries,
            x="year",
            y="cnt",
            markers=True,
            title="Number of Records by Year",
            labels={"year": "Year", "cnt": "Number of Records"},
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader("Optional Insight: Avg Monthly Spend by Year")
    if "avg_monthly_spend" in df_timeseries.columns:
        fig3 = px.line(
            df_timeseries,
            x="year",
            y="avg_monthly_spend",
            markers=True,
            title="Average Monthly Spend by Year",
            labels={"year": "Year", "avg_monthly_spend": "Avg Monthly Spend"},
        )
        st.plotly_chart(fig3, use_container_width=True)


if __name__ == "__main__":
    main()

