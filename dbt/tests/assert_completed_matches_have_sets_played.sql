-- Completed matches should always have sets_played populated.
-- Any rows returned by this query indicate a data quality issue.

select *
from {{ ref('int_matches_enriched') }}
where is_completed = true
  and sets_played is null