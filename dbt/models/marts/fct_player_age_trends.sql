{{
    config(
        materialized='table',
        partition_by={
            'field': 'tourney_date',
            'data_type': 'date',
            'granularity': 'year'
        },
        cluster_by=['tour']
    )
}}

with base as (
    select * from {{ ref('int_matches_enriched') }}
    where is_completed = true
      and winner_age is not null
      and loser_age is not null
)

select
    -- identifiers
    tourney_id,
    match_num,
    tour,

    -- dimensions
    tourney_date,
    tourney_name,
    tourney_level,
    surface,
    round,
    best_of,

    -- age metrics
    winner_age,
    loser_age,
    winner_age - loser_age as age_gap,

    -- player info
    winner_id,
    winner_name,
    winner_rank,
    loser_id,
    loser_name,
    loser_rank

from base