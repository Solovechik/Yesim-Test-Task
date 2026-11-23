{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        engine               = 'MergeTree()',
        partition_by         = 'toYYYYMM(refund_created_at)',
        order_by             = '(refund_created_at, payment_uuid, refund_id)',
        tags                 = ['silver']
    )
}}

{% if is_incremental() %}
    {% set wm = get_cdc_watermarks(
        this,
        version_col='_version',
        batch_dt_col='refund_created_at'
    ) %}
{% endif %}

with affected_partitions as (
    select
        distinct toYYYYMM(refund_created_at)                     as part
    from
        {{ ref('stg_refunds') }}

{% if is_incremental() %}
    where
        _version > {{ wm.max_ver }}
        or (
            _version = 0
            and refund_created_at > toDateTime('{{ wm.max_batch_dt }}')
        )
{% endif %}
)

select
    *
from
    {{ ref('stg_refunds') }} final
where
    toYYYYMM(refund_created_at) in (select part from affected_partitions)
    and _is_deleted = 0
