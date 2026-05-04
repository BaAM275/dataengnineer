-- Third set likelihood by tour and surface
-- For completed best-of-3 matches only
select
    tour,
    surface,
    count(*)                                                    as total_matches,
    countif(went_to_3rd_set)                                    as went_to_3rd,
    round(countif(went_to_3rd_set) / count(*) * 100, 1)        as pct_3rd_set,
    round(avg(minutes), 0)                                      as avg_minutes
from {{ ref('fct_match_outcomes') }}
where is_completed = true -- should this be filtered out?
  and best_of = 3
group by 1, 2
order by 1, 3 desc