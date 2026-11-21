{{
    config(
        materialized='table',
        order_by='id',
        tags=['silver']
    )
}}

select
    id,
    name,
    size_gb,
    days,
    is_unlimited,
    price_eur
from
    {{ source('bronze_s3', 'plans') }}
