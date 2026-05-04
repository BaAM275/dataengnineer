-- Winner and loser cannot be the same player
select *
from {{ ref('int_matches_enriched') }}
where winner_id = loser_id