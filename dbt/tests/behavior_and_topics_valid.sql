select 'behavior' as check_name, app_id from {{ ref('player_behavior') }}
where reviews <= 0 or known_playtime < 0 or known_playtime > reviews
   or median_playtime_hours < 0 or p90_playtime_hours < median_playtime_hours
union all
select 'topics', app_id from {{ ref('review_topics') }}
where mentions <= 0 or positive_mentions < 0 or negative_mentions < 0
   or mentions <> positive_mentions + negative_mentions
union all
select 'behavior_totals', coalesce(b.app_id, a.app_id)
from (select app_id, sum(reviews) as reviews from {{ ref('player_behavior') }} group by app_id) b
full join (select app_id, sum(reviews) as reviews from {{ ref('recommendation_trend') }} group by app_id) a using (app_id)
where b.reviews is distinct from a.reviews
