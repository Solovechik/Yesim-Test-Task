{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        engine               = 'MergeTree()',
        partition_by         = 'toYYYYMM(payment_created_at)',
        order_by             = '(payment_created_at, user_id, payment_id)',
        tags                 = ['silver']
    )
}}

{% if is_incremental() %}
    {% set wm = get_cdc_watermarks(
        this,
        version_col='_version',
        batch_dt_col='payment_created_at'
    ) %}
{% endif %}

with affected_partitions as (
    select
        distinct toYYYYMM(payment_created_at)                     as part
    from
        {{ ref('stg_payments') }}

{% if is_incremental() %}
    where
        _version > {{ wm.max_ver }}
        or (
            _version = 0
            and payment_created_at > toDateTime('{{ wm.max_batch_dt }}')
        )
{% endif %}
)

select
    *
from
    {{ ref('stg_payments') }} final
where
    toYYYYMM(payment_created_at) in (select part from affected_partitions)
    and _is_deleted = 0
