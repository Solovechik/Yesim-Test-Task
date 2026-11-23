{{
    config(
        materialized         = 'incremental',
        incremental_strategy = 'insert_overwrite',
        engine               = 'MergeTree()',
        partition_by         = 'toYYYYMM(activated_at)',
        order_by             = '(activated_at, order_id)',
        tags                 = ['silver']
    )
}}

{% if is_incremental() %}
    {% set wm = get_cdc_watermarks(
        this,
        version_col='_version',
        batch_dt_col='activated_at'
    ) %}
{% endif %}

with affected_partitions as (
    select
        distinct toYYYYMM(activated_at)                     as part
    from
        {{ ref('stg_activations') }}

{% if is_incremental() %}
    where
        _version > {{ wm.max_ver }}
        or (
            _version = 0
            and activated_at > toDateTime('{{ wm.max_batch_dt }}')
        )
{% endif %}
)

select
    *
from
    {{ ref('stg_activations') }} final
where
    toYYYYMM(activated_at) in (select part from affected_partitions)
    and _is_deleted = 0
