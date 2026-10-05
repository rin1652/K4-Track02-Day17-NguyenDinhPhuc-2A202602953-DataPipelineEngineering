-- Silver entity table: MERGE on the key, newer LSN wins, deletes become tombstones.
-- The dbt twin of pipeline/silver.py::upsert_silver_tickets.
{{ config(
    unique_key='ticket_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with changes as (
    select * from {{ ref('stg_ticket_changes') }}
    {% if is_incremental() %}
    -- high-water mark: only changes we have not applied yet
    where _lsn > (select coalesce(max(_lsn), 0) from {{ this }})
    {% endif %}
)
select
    ticket_id,
    user_id,
    {{ mask_pii('subject') }}  as subject,
    {{ mask_pii('body') }}     as body,
    priority,
    status,
    category,
    created_at,
    updated_at,
    (_op = 'd')                as is_deleted,
    _lsn,
    _batch_id
from changes
qualify row_number() over (partition by ticket_id order by _lsn desc) = 1
