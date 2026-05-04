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

serve_stats as (
    select
        tour,
        tourney_year,
        surface,

        -- serve metrics
        round(avg(w_ace), 2) as avg_winner_aces,
        round(avg(l_ace), 2) as avg_loser_aces,
        round(avg(w_ace + l_ace), 2) as avg_total_aces,
        round(avg(w_df + l_df), 2) as avg_total_double_faults,

        -- first serve percentage (approximated)
        round(avg(case when w_sv_gms > 0 then w_1st_in * 100.0 / w_sv_gms end), 2) as avg_winner_1st_in_pct,
        round(avg(case when l_sv_gms > 0 then l_1st_in * 100.0 / l_sv_gms end), 2) as avg_loser_1st_in_pct,

        -- first serve win percentage
        round(avg(case when w_1st_in > 0 then w_1st_won * 100.0 / w_1st_in end), 2) as avg_winner_1st_win_pct,
        round(avg(case when l_1st_in > 0 then l_1st_won * 100.0 / l_1st_in end), 2) as avg_loser_1st_win_pct,

        -- second serve win percentage
        round(avg(case when (w_sv_gms - w_1st_in) > 0
            then w_2nd_won * 100.0 / (w_sv_gms - w_1st_in) end), 2) as avg_winner_2nd_win_pct,
        round(avg(case when (l_sv_gms - l_1st_in) > 0
            then l_2nd_won * 100.0 / (l_sv_gms - l_1st_in) end), 2) as avg_loser_2nd_win_pct,

        -- break point analysis
        round(avg(w_bp_convert), 2) as avg_winner_bp_converted,
        round(avg(l_bp_convert), 2) as avg_loser_bp_converted,
        round(avg(w_bp_save), 2) as avg_winner_bp_saved,
        round(avg(l_bp_save), 2) as avg_loser_bp_saved,

        -- duration
        round(avg(match_duration_minutes), 0) as avg_match_duration_mins,
        round(min(match_duration_minutes), 0) as min_match_duration_mins,
        round(max(match_duration_minutes), 0) as max_match_duration_mins,

        -- match counts
        count(*) as total_matches,
        count(distinct winner_name) as unique_winners,
        count(distinct loser_name) as unique_losers

    from source
    where w_ace is not null and l_ace is not null
    group by tour, tourney_year, surface
),

aggregated as (
    select
        tour,
        tourney_year,

        sum(total_matches) as total_matches,
        sum(unique_winners) as unique_winners,
        sum(unique_losers) as unique_losers,
        round(avg(avg_total_aces), 2) as avg_total_aces,
        round(avg(avg_total_double_faults), 2) as avg_total_double_faults,
        round(avg(avg_winner_1st_in_pct), 2) as avg_winner_1st_in_pct,
        round(avg(avg_loser_1st_in_pct), 2) as avg_loser_1st_in_pct,
        round(avg(avg_winner_1st_win_pct), 2) as avg_winner_1st_win_pct,
        round(avg(avg_loser_1st_win_pct), 2) as avg_loser_1st_win_pct,
        round(avg(avg_winner_2nd_win_pct), 2) as avg_winner_2nd_win_pct,
        round(avg(avg_loser_2nd_win_pct), 2) as avg_loser_2nd_win_pct,
        round(avg(avg_winner_bp_converted), 2) as avg_winner_bp_converted,
        round(avg(avg_loser_bp_converted), 2) as avg_loser_bp_converted,
        round(avg(avg_winner_bp_saved), 2) as avg_winner_bp_saved,
        round(avg(avg_loser_bp_saved), 2) as avg_loser_bp_saved,
        round(avg(avg_match_duration_mins), 0) as avg_match_duration_mins,
        round(min(min_match_duration_mins), 0) as min_match_duration_mins,
        round(max(max_match_duration_mins), 0) as max_match_duration_mins

    from serve_stats
    group by tour, tourney_year
)

select * from aggregated
