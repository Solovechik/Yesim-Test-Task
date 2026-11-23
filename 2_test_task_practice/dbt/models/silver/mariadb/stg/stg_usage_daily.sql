{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'append',
        engine                = 'ReplacingMergeTree(_version, _is_deleted)',
        partition_by          = 'toYYYYMM(usage_date)',
        order_by              = '(usage_date, activation_id, usage_id)',
        tags                  = ['silver']
    )
}}

select
    toUInt64(
        assumeNotNull(id)
    )                                                           as usage_id,

    toUInt64(
        assumeNotNull(activation_id)
    )                                                           as activation_id,

    toDate(
        assumeNotNull(usage_date)
    )                                                           as usage_date,

    toUInt32(
        assumeNotNull(usage_units)
    )                                                           as usage_units,

    toDecimal64(
        assumeNotNull(cost_eur), 2
    )                                                           as usage_cost_eur,

    toDate(
        assumeNotNull(dt)
    )                                                           as dt,

    toUInt64(0)                                                 as _version,
    toUInt8(0)                                                  as _is_deleted
from
    {{ ref('src_usage_daily') }}

{% if is_incremental() %}
    where
        dt >= (select max(dt) from {{ this }} where _version = 0)
        and toDateTime(usage_date) > (select max(usage_date) from {{ this }} where _version = 0)
{% endif %}

union all

select
    id                                                          as usage_id,
    activation_id,
    usage_date,
    usage_units,
    cost_eur                                                    as usage_cost_eur,
    toDate(usage_date)                                          as dt,
    _version,
    _is_deleted
from
    {{ source('cdc', 'usage_daily') }}

{% if is_incremental() %}
    where
        _version > (select max(_version) from {{ this }})
{% endif %}
