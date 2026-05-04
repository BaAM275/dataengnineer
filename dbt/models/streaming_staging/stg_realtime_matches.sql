{{ config(
    materialized='view',
    schema='tennis_streaming'
) }}

with source as (
    select * from {{ source('tennis_streaming', 'realtime_matches') }}
),

renamed as (
    select
        -- identifiers
        id as match_id,
        tour,
        tourney_name,
        tourney_date,
        tourney_year,

        -- surface and level
        surface,
        tourney_level,
        round,
        best_of,

        -- winner
        winner_name,
        winner_rank,
        winner_age,

        -- loser
        loser_name,
        loser_rank,
        loser_age,

        -- score
        score,
        match_duration_minutes,

        -- winner stats
        w_ace, w_df, w_sv_gms,
        w_1st_in, w_1st_won, w_2nd_won,
        w_bp_save, w_bp_convert,

        -- loser stats
        l_ace, l_df, l_sv_gms,
        l_1st_in, l_1st_won, l_2nd_won,
        l_bp_save, l_bp_convert,

        -- metadata
        ingested_at,
        data_source

    from source
),

enriched as (
    select
        *,

        -- age calculations
        coalesce(winner_age, 0) - coalesce(loser_age, 0) as age_gap,
        case
            when winner_age > loser_age then 'older_winner'
            when winner_age < loser_age then 'younger_winner'
            else 'same_age'
        end as winner_age_category,

        -- serve percentages
        case
            when w_sv_gms > 0 then round(w_ace * 100.0 / w_sv_gms, 2)
            else null
        end as winner_ace_rate,

        case
            when l_sv_gms > 0 then round(l_ace * 100.0 / l_sv_gms, 2)
            else null
        end as loser_ace_rate,

        -- break point calculations
        case
            when (w_bp_save + w_bp_convert) > 0
            then round(w_bp_convert * 100.0 / (w_bp_save + w_bp_convert), 2)
            else null
        end as winner_break_conversion,

        case
            when (l_bp_save + l_bp_convert) > 0
            then round(l_bp_convert * 100.0 / (l_bp_save + l_bp_convert), 2)
            else null
        end as loser_break_conversion,

        -- rank differential
        coalesce(loser_rank, 9999) - coalesce(winner_rank, 9999) as rank_upset_margin

    from renamed
)

select * from enriched
