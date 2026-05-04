{{ config(
    materialized='table',
    schema='tennis_streaming',
    partition_by={
        'field': 'tourney_year',
        'data_type': 'int64',
        'granularity': 'year'
    },
    cluster_by=['tour', 'surface']
) }}

with source as (
    select * from {{ ref('stg_realtime_matches') }}
),

age_stats as (
    select
        tour,
        tourney_year,
        surface,

        -- count metrics
        count(*) as total_matches,
        count(distinct winner_name) as unique_winners,
        count(distinct loser_name) as unique_losers,

        -- winner age stats
        round(avg(winner_age), 2) as avg_winner_age,
        round(min(winner_age), 1) as min_winner_age,
        round(max(winner_age), 1) as max_winner_age,
        round(stddev(winner_age), 2) as stddev_winner_age,

        -- loser age stats
        round(avg(loser_age), 2) as avg_loser_age,
        round(min(loser_age), 1) as min_loser_age,
        round(max(loser_age), 1) as max_loser_age,
        round(stddev(loser_age), 2) as stddev_loser_age,

        -- age gap analysis
        round(avg(age_gap), 2) as avg_age_gap,
        round(max(age_gap), 1) as max_age_gap_favoring_winner,

        -- younger winner analysis
        sum(case when winner_age_category = 'younger_winner' then 1 else 0 end) as younger_winner_count,
        round(
            sum(case when winner_age_category = 'younger_winner' then 1 else 0 end) * 100.0 / count(*),
            2
        ) as younger_winner_pct,

        -- rank upset analysis
        round(avg(rank_upset_margin), 2) as avg_rank_upset_margin,
        sum(case when rank_upset_margin > 0 then 1 else 0 end) as rank_upsets

    from source
    where winner_age is not null and loser_age is not null
    group by tour, tourney_year, surface
),

aggregated as (
    select
        tour,
        tourney_year,

        sum(total_matches) as total_matches,
        sum(unique_winners) as unique_winners,
        sum(unique_losers) as unique_losers,
        round(avg(avg_winner_age), 2) as avg_winner_age,
        round(min(min_winner_age), 1) as min_winner_age,
        round(max(max_winner_age), 1) as max_winner_age,
        round(avg(stddev_winner_age), 2) as avg_winner_age_stddev,
        round(avg(avg_loser_age), 2) as avg_loser_age,
        round(min(min_loser_age), 1) as min_loser_age,
        round(max(max_loser_age), 1) as max_loser_age,
        round(avg(stddev_loser_age), 2) as avg_loser_age_stddev,
        round(avg(avg_age_gap), 2) as avg_age_gap,
        round(max(max_age_gap_favoring_winner), 1) as max_age_gap_favoring_winner,
        sum(younger_winner_count) as younger_winner_count,
        round(
            sum(younger_winner_count) * 100.0 / sum(total_matches),
            2
        ) as younger_winner_pct,
        sum(rank_upsets) as rank_upsets

    from age_stats
    group by tour, tourney_year
)

select * from aggregated
