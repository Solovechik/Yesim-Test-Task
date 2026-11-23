{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'append',
        engine                = 'ReplacingMergeTree(_version, _is_deleted)',
        partition_by          = 'toYYYYMM(refund_created_at)',
        order_by              = '(refund_created_at, payment_uuid, refund_id)',
        tags                  = ['silver']
    )
}}

select
    toUInt64(
        assumeNotNull(id)
    )                                                           as refund_id,

    toUUID(
        assumeNotNull(payment_uid)
    )                                                           as payment_uuid,

    toDecimal64(
        assumeNotNull(amount_eur), 2
    )                                                           as refund_amount_eur,

    toDateTime(
        assumeNotNull(created_at)
    )                                                           as refund_created_at,

    toDate(
        assumeNotNull(dt)
    )                                                           as dt,

    toUInt64(0)                                                 as _version,
    toUInt8(0)                                                  as _is_deleted
from
    {{ ref('src_refunds') }}

{% if is_incremental() %}
    where
        dt >= (select max(dt) from {{ this }} where _version = 0)
        and toDateTime(created_at) > (select max(refund_created_at) from {{ this }} where _version = 0)
{% endif %}

union all

select
    id                                                          as refund_id,
    toUUID(payment_uid)                                         as payment_uuid,
    amount_eur                                                  as refund_amount_eur,
    created_at                                                  as refund_created_at,
    toDate(refund_created_at)                                   as dt,
    _version,
    _is_deleted
from
    {{ source('cdc', 'refunds') }}

{% if is_incremental() %}
    where
        _version > (select max(_version) from {{ this }})
{% endif %}
