-- Used only for testing. In prod models are created via dbt
-- 1. create databases
create database if not exists dev;
create database if not exists bronze;
create database if not exists silver;
create database if not exists gold;


-- 2.1 create non-transactional views
create view if not exists bronze.src_users (
    id UInt32,
    country LowCardinality(String),
    is_tester UInt8,
    is_staff UInt8,
    created_at DateTime
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'users/*.parquet',
        structure = '
            id UInt32,
            country LowCardinality(String),
            is_tester UInt8,
            is_staff UInt8,
            created_at DateTime
            '
    );

create view if not exists bronze.src_plans (
    id UInt32,
    name String,
    size_gb Nullable(Decimal64(1)),
    days UInt32,
    is_unlimited UInt8,
    price_eur Decimal64(2)
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'plans/*.parquet',
        structure = '
            id UInt32,
            name String,
            size_gb Nullable(Decimal64(1)),
            days UInt32,
            is_unlimited UInt8,
            price_eur Decimal64(2)
            '
    );

create view if not exists bronze.src_payment_systems (
    id UInt32,
    name String
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'payment_systems/*.parquet',
        structure = '
            id UInt32,
            name String
            '
    );


-- 2.2 create transactional views
create view if not exists bronze.src_payments (
    id UInt64,
    payment_uid UUID,
    user_id UInt32,
    payment_system_id UInt32,
    status Enum8('pending' = 1, 'success' = 2, 'declined' = 3),
    amount_eur Decimal64(2),
    created_at DateTime,
    dt Date
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'payments/dt=*/*.parquet',
        structure = '
            id UInt64,
            payment_uid UUID,
            user_id UInt32,
            payment_system_id UInt32,
            status Enum8(\'pending\' = 1, \'success\' = 2, \'declined\' = 3),
            amount_eur Decimal64(2),
            created_at DateTime,
            dt Date
            '
    );

create view if not exists bronze.src_orders (
    id UInt64,
    user_id UInt32,
    payment_uid Nullable(UUID),
    plan_id UInt32,
    points_used Int32,
    platform Enum8('web' = 1, 'ios' = 2, 'android' = 3),
    created_at DateTime,
    dt Date
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'orders/dt=*/*.parquet',
        structure = '
            id UInt64,
            user_id UInt32,
            payment_uid Nullable(UUID),
            plan_id UInt32,
            points_used Int32,
            platform Enum8(\'web\' = 1, \'ios\' = 2, \'android\' = 3),
            created_at DateTime,
            dt Date
            '
    );

create view if not exists bronze.src_activations (
    id UInt64,
    order_id UInt64,
    user_id UInt32,
    activated_at DateTime,
    dt Date
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'activations/dt=*/*.parquet',
        structure = '
            id UInt64,
            order_id UInt64,
            user_id UInt32,
            activated_at DateTime,
            dt Date
            '
    );

create view if not exists bronze.src_usage_daily (
    id UInt64,
    activation_id UInt64,
    usage_date Date,
    usage_units Int32,
    cost_eur Decimal64(2),
    dt Date
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'usage_daily/dt=*/*.parquet',
        structure = '
            id UInt64,
            activation_id UInt64,
            usage_date Date,
            usage_units Int32,
            cost_eur Decimal64(2),
            dt Date
            '
    );

create view if not exists bronze.src_refunds (
    id UInt64,
    payment_uid UUID,
    amount_eur Decimal64(2),
    created_at DateTime,
    dt Date
)
as
select
    *
from
    s3(
        minio_bronze,
        filename = 'refunds/dt=*/*.parquet',
        structure = '
            id UInt64,
            payment_uid UUID,
            amount_eur Decimal64(2),
            created_at DateTime,
            dt Date
            '
    );
