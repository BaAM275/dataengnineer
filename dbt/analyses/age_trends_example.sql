-- Average winner age by year and tour
-- Shows how the age of successful players has changed over time
select
    extract(year from tourney_date)     as year,
    tour,
    round(avg(winner_age), 1)           as avg_winner_age,
    round(avg(loser_age), 1)            as avg_loser_age,
    round(avg(age_gap), 1)              as avg_age_gap,
    count(*)                            as matches
from {{ ref('fct_player_age_trends') }}
group by 1, 2
order by 1, 2