{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        engine               = 'MergeTree()',
        partition_by         = 'toYYYYMM(usage_date)',
        order_by             = '(usage_date, activation_id, usage_id)',
        tags                 = ['silver']
    )
}}

{% if is_incremental() %}
    {% set wm = get_cdc_watermarks(
        this,
        version_col='_version',
        batch_dt_col='usage_date'
    ) %}
{% endif %}

with affected_partitions as (
    select
        distinct toYYYYMM(usage_date)                     as part
    from
        {{ ref('stg_usage_daily') }}

{% if is_incremental() %}
    where
        _version > {{ wm.max_ver }}
        or (
            _version = 0
            and usage_date > toDate('{{ wm.max_batch_dt }}')
        )
{% endif %}
)

select
    *
from
    {{ ref('stg_usage_daily') }} final
where
    toYYYYMM(usage_date) in (select part from affected_partitions)
    and _is_deleted = 0
