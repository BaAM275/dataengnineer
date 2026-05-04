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

    -- match outcome
    score,
    is_completed,
    sets_played,
    went_to_3rd_set,
    went_to_4th_set,
    went_to_5th_set,
    minutes

from base

where sets_played <= best_of or sets_played is null