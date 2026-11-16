-- 1. create databases
create database if not exists bronze;
create database if not exists silver;
create database if not exists gold;


-- 2.1 create non-transactional tables
create table if not exists bronze.users (
    id UInt32,
    country LowCardinality(String),
    is_tester UInt8,
    is_staff UInt8,
    created_at DateTime
)
engine = S3(minio_bronze, filename = 'users/*.parquet');;

create table if not exists bronze.plans (
    id UInt32,
    name String,
    size_gb Nullable(Decimal64(1)),
    days UInt32,
    is_unlimited UInt8,
    price_eur Decimal64(2)
)
engine = S3(minio_bronze, filename = 'plans/*.parquet');

create table if not exists bronze.payment_systems (
    id UInt32,
    name String
)
engine = S3(minio_bronze, filename = 'payment_systems/*.parquet');


-- 2.2 create transactional tables
create table if not exists bronze.payments (
    id UInt64,
    payment_uid UUID,
    user_id UInt32,
    payment_system_id UInt32,
    status Enum8('pending' = 1, 'success' = 2, 'declined' = 3),
    amount_eur Decimal64(2),
    created_at DateTime,
    _loaded_at DateTime default now(),
    dt Date
)
engine = S3(minio_bronze, filename = 'payments/', partition_strategy = 'hive')
partition by dt;

create table if not exists bronze.orders (
    id UInt64,
    user_id UInt32,
    payment_uid Nullable(UUID),
    plan_id UInt32,
    points_used Int32,
    platform Enum8('web' = 1, 'ios' = 2, 'android' = 3),
    created_at DateTime,
    _loaded_at DateTime DEFAULT now(),
    dt Date
)
engine = S3(minio_bronze, filename = 'orders/', partition_strategy = 'hive')
partition by dt;

create table if not exists bronze.activations (
    id UInt64,
    order_id UInt64,
    user_id UInt32,
    activated_at DateTime,
    _loaded_at DateTime DEFAULT now(),
    dt Date
)
engine = S3(minio_bronze, filename = 'activations/', partition_strategy = 'hive')
partition by dt;

create table if not exists bronze.usage_daily (
    id UInt64,
    activation_id UInt64,
    usage_date Date,
    usage_units Int32,
    cost_eur Decimal64(2),
    _loaded_at DateTime DEFAULT now(),
    dt Date
)
engine = S3(minio_bronze, filename = 'usage_daily/', partition_strategy = 'hive')
partition by dt;

create table if not exists bronze.refunds (
    id UInt64,
    payment_uid UUID,
    amount_eur Decimal64(2),
    created_at DateTime,
    _loaded_at DateTime DEFAULT now(),
    dt Date
)
engine = S3(minio_bronze, filename = 'refunds/', partition_strategy = 'hive')
partition by dt;
