with source as (
    select * from {{ source('tennis_raw', 'wta_matches') }}
),

renamed as (
    select
        -- tour identifier
        'wta' as tour,

        -- identifiers
        {{ dbt_utils.generate_surrogate_key(["'wta'", 'tourney_id', 'match_num', 'winner_id', 'loser_id']) }} as match_id,
        tourney_id,
        tourney_name,
        tourney_level,
        surface,
        draw_size,
        round,
        match_num,
        best_of,

        -- dates
        parse_date('%Y%m%d', tourney_date)  as tourney_date,

        -- winner
        winner_id,
        winner_name,
        winner_hand,
        winner_ht,
        winner_ioc,
        winner_age,
        winner_rank,
        winner_rank_points,
        winner_seed,
        winner_entry,

        -- loser
        loser_id,
        loser_name,
        loser_hand,
        loser_ht,
        loser_ioc,
        loser_age,
        loser_rank,
        loser_rank_points,
        loser_seed,
        loser_entry,

        -- match stats
        score,
        minutes,
        w_ace, w_df, w_svpt, w_1stIn, w_1stWon, w_2ndWon, w_SvGms, w_bpSaved, w_bpFaced,
        l_ace, l_df, l_svpt, l_1stIn, l_1stWon, l_2ndWon, l_SvGms, l_bpSaved, l_bpFaced

    from source
)

select * from renamed