{{
    config(
        materialized    = 'view',
        tags            = ['gold']
    )
}}

select
    *
from
    {{ ref('stg_payment_systems') }}
