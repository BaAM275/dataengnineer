{{ config(
    materialized='table',
    partition_by={'field': 'year_date', 'data_type': 'date'},
    cluster_by=['country', 'drink_category']
) }}

with clean as (
    select *
    from {{ ref('stg_tea_coffee_global_final') }}
)

select
    -- Core dimensions for slicing dashboards
    country,
    continent,
    year,
    year_date,
    gender,
    income_level,
    drink_preference,
    favorite_drink,
    drink_category,
    drink_temperature,

    -- Measures used for aggregation and richer insights
    cups_per_day,
    sugar_level,
    milk_usage,
    caffeine_preference,
    taste_score,
    bitterness,
    acidity,
    aroma_score,
    monthly_spend,
    price_per_cup,
    home_vs_cafe_ratio,
    work_type,
    sleep_hours,
    stress_level,
    exercise_frequency,
    bmi,
    hydration_level,
    heart_rate,
    drink_reason,
    social_setting,
    time_of_day,
    loyalty_brand,
    experiment_new_drinks,
    satisfaction_level
from clean

