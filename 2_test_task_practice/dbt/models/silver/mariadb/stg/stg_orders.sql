{{
    config(
        materialized          = 'incremental',
        incremental_strategy  = 'append',
        engine                = 'ReplacingMergeTree(_version, _is_deleted)',
        partition_by          = 'toYYYYMM(order_created_at)',
        order_by              = '(order_created_at, user_id, order_id)',
        tags                  = ['silver']
    )
}}

select
    toUInt64(
        assumeNotNull(id)
    )                                                           as order_id,

    toUInt32(
        assumeNotNull(user_id)
    )                                                           as user_id,

    toUUID(payment_uid)                                         as payment_uuid,

    toUInt32(
        assumeNotNull(plan_id)
    )                                                           as plan_id,

    toUInt32(
        assumeNotNull(points_used)
    )                                                           as order_points_used,

    assumeNotNull(platform)                                     as order_platform,

    toDateTime(
        assumeNotNull(created_at)
    )                                                           as order_created_at,

    toDate(
        assumeNotNull(dt)
    )                                                           as dt,

    toUInt64(0)                                                 as _version,
    toUInt8(0)                                                  as _is_deleted
from
    {{ ref('src_orders') }}

{% if is_incremental() %}
    where
        dt >= (select max(dt) from {{ this }} where _version = 0)
        and toDateTime(created_at) > (select max(order_created_at) from {{ this }} where _version = 0)
{% endif %}

union all

select
    id                                                          as order_id,
    user_id,
    toUUID(payment_uid)                                         as payment_uuid,
    plan_id,
    points_used                                                 as order_points_used,
    platform                                                    as order_platform,
    created_at                                                  as order_created_at,
    toDate(created_at)                                          as dt,
    _version,
    _is_deleted
from
    {{ source('cdc', 'orders') }}

{% if is_incremental() %}
    where
        _version > (select max(_version) from {{ this }})
{% endif %}
