{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(created_at)',
        partition_by='toYYYYMM(dt)',
        order_by='(dt, id)',
        tags=['silver']
    )
}}

select
    id,
    payment_uid,
    amount_eur,
    created_at,
    dt,
    _loaded_at as loaded_at
from
    {{ source('bronze_s3', 'refunds') }}
{% if is_incremental() %}
    where
        created_at > (select max(created_at) from {{ this }})
{% endif %}
