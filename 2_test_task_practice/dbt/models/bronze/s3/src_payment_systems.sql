{{
    config(
        materialized    = 'view',
        tags            = ['src', 'bronze', 's3']
    )
}}

select
    *
from
    s3(minio_bronze, filename = 'payment_systems/*.parquet')
