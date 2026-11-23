{{
    config(
        materialized = 'table',
        engine       = 'MergeTree()',
        order_by     = 'plan_id',
        tags         = ['silver']
    )
}}

select
    toUInt32(
        assumeNotNull(id)
    )                                                           as plan_id,

    assumeNotNull(name)                                         as plan_name,

    toDecimal32(size_gb, 1)                                     as plan_size_gb,

    toUInt32(
        assumeNotNull(days)
    )                                                           as plan_days,

    toUInt8(
        assumeNotNull(is_unlimited)
    )                                                           as is_plan_unlimited,

    toDecimal32(
        assumeNotNull(price_eur), 2
    )                                                           as price_eur
from
    {{ ref('src_plans') }}
