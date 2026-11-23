{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'append',
        engine                = 'ReplacingMergeTree(_version, _is_deleted)',
        partition_by          = 'toYYYYMM(payment_created_at)',
        order_by              = '(payment_created_at, user_id, payment_id)',
        tags                  = ['silver']
    )
}}

select
    toUInt64(
        assumeNotNull(id)
    )                                                           as payment_id,

    toUUID(
        assumeNotNull(payment_uid)
    )                                                           as payment_uuid,

    toUInt32(
        assumeNotNull(user_id)
    )                                                           as user_id,

    toUInt32(
        assumeNotNull(payment_system_id)
    )                                                           as payment_system_id,

    assumeNotNull(status)                                       as payment_status,

    toDecimal64(
        assumeNotNull(amount_eur), 2
    )                                                           as payment_amount_eur,

    toDateTime(
        assumeNotNull(created_at)
    )                                                           as payment_created_at,

    toDate(
        assumeNotNull(dt)
    )                                                           as dt,

    toUInt64(0)                                                 as _version,
    toUInt8(0)                                                  as _is_deleted
from
    {{ ref('src_payments') }}

{% if is_incremental() %}
    where
        dt >= (select max(dt) from {{ this }} where _version = 0)
        and toDateTime(created_at) > (select max(payment_created_at) from {{ this }} where _version = 0)
{% endif %}

union all

select
    id                                                          as payment_id,
    toUUID(payment_uid)                                         as payment_uuid,
    user_id,
    payment_system_id,
    status                                                      as payment_status,
    amount_eur                                                  as payment_amount_eur,
    created_at                                                  as payment_created_at,
    toDate(created_at)                                          as dt,
    _version,
    _is_deleted
from
    {{ source('cdc', 'payments') }}

{% if is_incremental() %}
    where
        _version > (select max(_version) from {{ this }})
{% endif %}
