with base as (
    select * from {{ ref('int_matches_unionized') }}
),

enriched as (
    select
        *,

-- Match completion flag
case
    when score is null then null
    when upper(score) like '%RET%'       then false
    when upper(score) like '%W/O%'       then false
    when upper(score) like '%DEF%'       then false
    when upper(score) like '%ABD%'       then false
    when upper(score) like '%UNK%'       then false
    when upper(score) like '%WALKOVER%'  then false
    when upper(score) like '%PLAYED AND%' then false
    else true
end as is_completed,

-- Count sets played
case
    when score is null then null
    when upper(score) like '%RET%'        then null
    when upper(score) like '%W/O%'        then null
    when upper(score) like '%DEF%'        then null
    when upper(score) like '%ABD%'        then null
    when upper(score) like '%UNK%'        then null
    when upper(score) like '%WALKOVER%'   then null
    when upper(score) like '%PLAYED AND%' then null
    else array_length(
        split(trim(score), ' ')
    )
end as sets_played

    from base
),

final as (
    select
        *,
        sets_played >= 3 as went_to_3rd_set,
        sets_played >= 4 as went_to_4th_set,
        sets_played = 5  as went_to_5th_set
    from enriched
)

select * from final