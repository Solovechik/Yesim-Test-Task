{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(usage_date)',
        partition_by='toYYYYMM(dt)',
        order_by='(dt, id)',
        tags=['silver']
    )
}}

select
    id,
    activation_id,
    usage_date,
    usage_units,
    cost_eur,
    dt,
    _loaded_at as loaded_at
from
    {{ source('bronze_s3', 'usage_daily') }}
{% if is_incremental() %}
    where
        usage_date > (select max(usage_date) from {{ this }})
{% endif %}
