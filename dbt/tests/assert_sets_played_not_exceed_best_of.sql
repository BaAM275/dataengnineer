-- Sets played cannot exceed the best_of value
-- some edge cases exists, where there were experiments with best of 5 in the finals
{{ config(severity='warn') }}

select *
from {{ ref('int_matches_enriched') }}
where sets_played > best_of