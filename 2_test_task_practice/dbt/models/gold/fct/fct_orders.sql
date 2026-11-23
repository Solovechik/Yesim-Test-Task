{{
    config(
        materialized    = 'view',
        tags            = ['gold']
    )
}}

select
    * except (_version, _is_deleted)
from
    {{ ref('int_orders') }}
