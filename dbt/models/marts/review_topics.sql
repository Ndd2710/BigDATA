select app_id, topic,
    sum(mentions)::bigint as mentions,
    sum(case when recommended then mentions else 0 end)::bigint as positive_mentions,
    sum(case when not recommended then mentions else 0 end)::bigint as negative_mentions
from {{ source('warehouse', 'topic_mentions') }}
group by app_id, topic
