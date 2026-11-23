{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'append',
        engine                = 'ReplacingMergeTree(_version, _is_deleted)',
        partition_by          = 'toYYYYMM(activated_at)',
        order_by              = '(activated_at, activation_id)',
        tags                  = ['silver']
    )
}}

select
    toUInt64(
        assumeNotNull(id)
    )                                                           as activation_id,

    toUInt64(
        assumeNotNull(order_id)
    )                                                           as order_id,

    toUInt32(
        assumeNotNull(user_id)
    )                                                           as user_id,

    toDateTime(
        assumeNotNull(activated_at)
    )                                                           as activated_at,

    toDate(
        assumeNotNull(dt)
    )                                                           as dt,

    toUInt64(0)                                                 as _version,
    toUInt8(0)                                                  as _is_deleted
from
    {{ ref('src_activations') }}

{% if is_incremental() %}
    where
        dt >= (select max(dt) from {{ this }} where _version = 0)
        and toDateTime(activated_at) > (select max(activated_at) from {{ this }} where _version = 0)
{% endif %}

union all

select
    id                                                          as activation_id,
    order_id,
    user_id,
    activated_at,
    toDate(activated_at)                                        as dt,
    _version,
    _is_deleted
from
    {{ source('cdc', 'activations') }}

{% if is_incremental() %}
    where
        _version > (select max(_version) from {{ this }})
{% endif %}
