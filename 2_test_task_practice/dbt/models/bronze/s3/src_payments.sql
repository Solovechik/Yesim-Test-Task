{{
    config(
        materialized    = 'view',
        tags            = ['src', 'bronze', 's3']
    )
}}

select
    *
from
    s3(minio_bronze, filename = 'payments/dt=*/*.parquet')
