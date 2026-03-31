import pandas as pd

from dashboard.app import compute_dashboard_data_from_df


def test_compute_dashboard_data_from_df_basic():
    df = pd.DataFrame(
        {
            "drink_category": ["Modern", "Modern", "Traditional", "Traditional", "Traditional"],
            "year": [2015, 2015, 2015, 2016, 2016],
            "monthly_spend": [10.0, 20.0, 5.0, 7.0, 3.0],
        }
    )

    df_category, df_timeseries = compute_dashboard_data_from_df(df)

    # Category distribution should be sorted by cnt desc
    assert list(df_category["drink_category"]) == ["Traditional", "Modern"]
    assert list(df_category["cnt"]) == [3, 2]

    # Timeseries should be sorted by year asc
    assert list(df_timeseries["year"]) == [2015, 2016]
    assert list(df_timeseries["cnt"]) == [3, 2]

    # avg_monthly_spend should exist and be correct
    # 2015 avg = mean([10, 20, 5]) = 11.666...
    # 2016 avg = mean([7, 3]) = 5.0
    avg_2015 = df_timeseries.loc[df_timeseries["year"] == 2015, "avg_monthly_spend"].iloc[0]
    avg_2016 = df_timeseries.loc[df_timeseries["year"] == 2016, "avg_monthly_spend"].iloc[0]
    assert round(avg_2015, 3) == round((10.0 + 20.0 + 5.0) / 3.0, 3)
    assert round(avg_2016, 3) == 5.0

