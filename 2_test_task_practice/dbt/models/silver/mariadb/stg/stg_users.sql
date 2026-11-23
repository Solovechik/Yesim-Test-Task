{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = '(user_created_at, user_id)',
        tags         = ['silver']
    )
}}

select
    toUInt32(
        assumeNotNull(id)
    )                                                           as user_id,

    assumeNotNull(country)                                      as user_country,

    toUInt8(
        assumeNotNull(is_tester)
    )                                                           as is_user_tester,

    toUInt8(
        assumeNotNull(is_staff)
    )                                                           as is_user_staff,

    toDateTime(
        assumeNotNull(created_at)
    )                                                           as user_created_at
from
    {{ ref('src_users') }}
