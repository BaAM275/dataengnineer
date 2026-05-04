select
    tourney_id,
    tourney_date
from {{ ref('int_matches_enriched') }}
where abs(
    cast(left(tourney_id, 4) as int64) - extract(year from tourney_date)
) > 1