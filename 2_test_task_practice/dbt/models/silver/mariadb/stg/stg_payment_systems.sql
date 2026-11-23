{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = 'payment_system_id',
        tags         = ['silver']
    )
}}

select
    toUInt32(
        assumeNotNull(id)
    )                                                           as payment_system_id,

    assumeNotNull(name)                                         as payment_system_name
from
    {{ ref('src_payment_systems') }}
