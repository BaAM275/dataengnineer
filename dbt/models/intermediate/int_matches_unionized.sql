with atp as (
    select * from {{ ref('stg_atp_matches') }}
),

wta as (
    select * from {{ ref('stg_wta_matches') }}
)

select * from atp
union all
select * from wta