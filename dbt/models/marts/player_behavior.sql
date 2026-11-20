select app_id, recommended, reviews, known_playtime,
    avg_playtime_minutes / 60 as avg_playtime_hours,
    median_playtime_minutes / 60 as median_playtime_hours,
    p90_playtime_minutes / 60 as p90_playtime_hours
from {{ source('warehouse', 'behavior_stats') }}
