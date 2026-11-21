{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(activated_at)',
        partition_by='toYYYYMM(dt)',
        order_by='(dt, id)',
        tags=['silver']
    )
}}

select
    id,
    order_id,
    user_id,
    activated_at,
    dt,
    _loaded_at as loaded_at
from
    {{ source('bronze_s3', 'activations') }}
{% if is_incremental() %}
    where
        activated_at > (select max(activated_at) from {{ this }})
{% endif %}
