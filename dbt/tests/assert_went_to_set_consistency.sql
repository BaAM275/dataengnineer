-- If a match went to the 5th set, it must have gone to the 4th
-- If a match went to the 4th set, it must have gone to the 3rd
select *
from {{ ref('int_matches_enriched') }}
where (went_to_5th_set = true and went_to_4th_set = false)
   or (went_to_4th_set = true and went_to_3rd_set = false)