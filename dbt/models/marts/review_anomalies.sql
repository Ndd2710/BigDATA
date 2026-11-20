with monthly as (
    select review_month, app_id, reviews, recommendations,
        1 - recommendation_rate as negative_rate
    from {{ ref('recommendation_trend') }}
), baseline as (
    select *,
        avg(reviews) over w as previous_avg_reviews,
        (sum(reviews - recommendations) over w)::numeric / nullif(sum(reviews) over w, 0) as previous_negative_rate,
        count(*) over w as previous_months
    from monthly
    window w as (partition by app_id order by review_month rows between 3 preceding and 1 preceding)
)
select *, negative_rate - previous_negative_rate as negative_rate_change
from baseline
where previous_months = 3 and reviews >= 100
  and reviews >= 2 * previous_avg_reviews
  and negative_rate >= previous_negative_rate + 0.20
