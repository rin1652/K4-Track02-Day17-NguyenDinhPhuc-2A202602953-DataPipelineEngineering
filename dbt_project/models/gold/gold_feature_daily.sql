-- Routing-agent features, 1 row = 1 user x 1 day of EVENT time.
-- microbatch: one batch per day; every run also recomputes the previous
-- `lookback` days, so events that arrive late still land in their own day.
-- lookback = 3 = ceil(P99 lateness) measured from Bronze (python main.py --lateness).
{{ config(
    incremental_strategy='delete+insert',
    unique_key=['user_id', 'event_date'],
    event_time='event_date',
    batch_size='day',
    lookback=3,
    begin='2026-08-10'
) }}

select
    user_id,
    date_trunc('day', event_time)                                 as event_date,
    count(*)                                                      as n_events,
    count(*) filter (where type = 'click')                        as n_clicks,
    count(*) filter (where type = 'feedback' and rating = 'up')   as n_feedback_up,
    count(*) filter (where type = 'feedback' and rating = 'down') as n_feedback_down
from {{ ref('silver_events') }}      -- dbt filters this to the batch's day via event_time
group by all
