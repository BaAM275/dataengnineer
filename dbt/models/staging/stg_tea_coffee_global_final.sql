{{ config(materialized='view') }}

with raw as (
    select
        *
    from {{ source('tea_vs_coffee', 'tea_vs_coffee_global_final_raw') }}
)

select
    -- Dimensions
    country,
    continent,
    SAFE_CAST(year AS INT64) as year,
    SAFE_CAST(age AS INT64) as age,
    gender,
    income_level,
    drink_preference,
    favorite_drink,
    drink_category,
    drink_temperature,

    -- Numeric attributes (cast from CSV strings)
    SAFE_CAST(cups_per_day AS FLOAT64) as cups_per_day,
    sugar_level,
    milk_usage,
    caffeine_preference,
    SAFE_CAST(taste_score AS FLOAT64) as taste_score,
    SAFE_CAST(bitterness AS FLOAT64) as bitterness,
    SAFE_CAST(acidity AS FLOAT64) as acidity,
    SAFE_CAST(aroma_score AS FLOAT64) as aroma_score,
    SAFE_CAST(monthly_spend AS FLOAT64) as monthly_spend,
    SAFE_CAST(price_per_cup AS FLOAT64) as price_per_cup,
    SAFE_CAST(home_vs_cafe_ratio AS FLOAT64) as home_vs_cafe_ratio,
    work_type,
    SAFE_CAST(sleep_hours AS FLOAT64) as sleep_hours,
    SAFE_CAST(stress_level AS INT64) as stress_level,
    exercise_frequency,
    SAFE_CAST(bmi AS FLOAT64) as bmi,
    hydration_level,
    SAFE_CAST(heart_rate AS INT64) as heart_rate,

    drink_reason,
    social_setting,
    time_of_day,
    loyalty_brand,
    experiment_new_drinks,
    SAFE_CAST(satisfaction_level AS INT64) as satisfaction_level,

    -- Date helper for time-based charts
    SAFE_CAST(DATE_FROM_PARTS(SAFE_CAST(year AS INT64), 1, 1) AS DATE) as year_date

from raw

