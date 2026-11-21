{{
    config(
        materialized='table',
        order_by='id',
        tags=['silver']
    )
}}

select
    id,
    name
from
    {{ source('bronze_s3', 'payment_systems') }}
