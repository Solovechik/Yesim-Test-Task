{{
    config(
        materialized='table',
        order_by='id',
        tags=['silver']
    )
}}

select
    id,
    country,
    is_tester,
    is_staff,
    created_at
from
    {{ source('bronze_s3', 'users') }}
