{{
    config(
        materialized    = 'view',
        tags            = ['gold']
    )
}}

select
    *
from
    {{ ref('stg_users') }}
