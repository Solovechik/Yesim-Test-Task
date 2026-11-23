create database if not exists cdc;


-- users
-- Clickhouse cdc logs
create table if not exists cdc.users
(
    id          UInt32,
    country     LowCardinality(String),
    is_tester   UInt8,
    is_staff    UInt8,
    created_at  DateTime('UTC'),
    _version    UInt64,
    _is_deleted UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by id;

-- Kafka stream consumer for Clickhouse
create table if not exists cdc.users_kafka
(
    id             UInt32,
    country        String,
    is_tester      UInt8,
    is_staff       UInt8,
    created_at     Int64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.users',
    kafka_group_name = 'ch_app_users',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

-- materialized view to Clickhouse
create materialized view if not exists cdc.users_mv to cdc.users as
select
    id,
    country,
    is_tester,
    is_staff,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.users_kafka
where
    _error = '';

-- Kafka stream consumer for S3
create table if not exists cdc.users_kafka_s3 as cdc.users_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.users',
    kafka_group_name = 'ch_app_users_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

-- S3 cdc logs
create table if not exists cdc.users_s3_sink
(
    id UInt32,
    country String,
    is_tester UInt8,
    is_staff UInt8,
    created_at DateTime('UTC'),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/users/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(created_at);

-- materialized view to S3
create materialized view if not exists cdc.users_s3_mv to cdc.users_s3_sink as
select
    id,
    country,
    is_tester,
    is_staff,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.users_kafka_s3
where
    _error = '';

-- plans
create table if not exists cdc.plans
(
    id           UInt32,
    name         String,
    size_gb      Nullable(Decimal(6, 1)),
    days         UInt32,
    is_unlimited UInt8,
    price_eur    Decimal(10, 2),
    _version     UInt64,
    _is_deleted  UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.plans_kafka
(
    id             UInt32,
    name           String,
    size_gb        Nullable(Float64),
    days           UInt32,
    is_unlimited   UInt8,
    price_eur      Float64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.plans',
    kafka_group_name = 'ch_app_plans',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.plans_mv to cdc.plans as
select
    id,
    name,
    toDecimal64OrNull(toString(size_gb), 1)                     as size_gb,
    days,
    is_unlimited,
    toDecimal64(price_eur, 2)                                   as price_eur,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.plans_kafka
where
    _error = '';

create table if not exists cdc.plans_kafka_s3 as cdc.plans_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.plans',
    kafka_group_name = 'ch_app_plans_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.plans_s3_sink
(
    id UInt32,
    name String,
    size_gb Nullable(Decimal(6, 1)),
    days UInt32,
    is_unlimited UInt8,
    price_eur Decimal(10, 2),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/plans/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    id % 10;

create materialized view if not exists cdc.plans_s3_mv to cdc.plans_s3_sink as
select
    id,
    name,
    toDecimal64OrNull(toString(size_gb), 1)                     as size_gb,
    days,
    is_unlimited,
    toDecimal64(price_eur, 2)                                   as price_eur,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.plans_kafka_s3
where
    _error = '';


-- payment_systems
create table if not exists cdc.payment_systems
(
    id          UInt32,
    name        String,
    _version    UInt64,
    _is_deleted UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.payment_systems_kafka
(
    id             UInt32,
    name           String,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.payment_systems',
    kafka_group_name = 'ch_app_payment_systems',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.payment_systems_mv to cdc.payment_systems as
select
    id, name,
    (_offset + 1) as _version,
    if(__deleted = 'true', 1, 0) as _is_deleted
from
    cdc.payment_systems_kafka
where
    _error = '';

create table if not exists cdc.payment_systems_kafka_s3 as cdc.payment_systems_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.payment_systems',
    kafka_group_name = 'ch_app_payment_systems_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.payment_systems_s3_sink
(
    id UInt32,
    name String,
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/payment_systems/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    id % 10;

create materialized view if not exists cdc.payment_systems_s3_mv to cdc.payment_systems_s3_sink as
select
    id,
    name,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.payment_systems_kafka_s3
where
    _error = '';

-- payments
create table if not exists cdc.payments
(
    id                UInt64,
    payment_uid       String,
    user_id           UInt32,
    payment_system_id UInt32,
    status            LowCardinality(String),
    amount_eur        Decimal(10, 2),
    created_at        DateTime('UTC'),
    _version          UInt64,
    _is_deleted       UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.payments_kafka
(
    id                UInt64,
    payment_uid       String,
    user_id           UInt32,
    payment_system_id UInt32,
    status            String,
    amount_eur        Float64,
    created_at        Int64,
    __deleted         String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.payments',
    kafka_group_name = 'ch_app_payments',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.payments_mv to cdc.payments as
select
    id,
    payment_uid,
    user_id,
    payment_system_id,
    status,
    toDecimal64(amount_eur, 2)                                  as amount_eur,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.payments_kafka
where
    _error = '';

create table if not exists cdc.payments_kafka_s3 as cdc.payments_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.payments',
    kafka_group_name = 'ch_app_payments_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.payments_s3_sink
(
    id UInt64,
    payment_uid String,
    user_id UInt32,
    payment_system_id UInt32,
    status String,
    amount_eur Decimal(10, 2),
    created_at DateTime('UTC'),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/payments/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(created_at);

create materialized view if not exists cdc.payments_s3_mv to cdc.payments_s3_sink as
select
    id,
    payment_uid,
    user_id,
    payment_system_id,
    status,
    toDecimal64(amount_eur, 2)                                  as amount_eur,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.payments_kafka_s3
where
    _error = '';

-- orders
create table if not exists cdc.orders
(
    id          UInt64,
    user_id     UInt32,
    payment_uid Nullable(String),
    plan_id     UInt32,
    points_used UInt32,
    platform    LowCardinality(String),
    created_at  DateTime('UTC'),
    _version    UInt64,
    _is_deleted UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.orders_kafka
(
    id             UInt64,
    user_id        UInt32,
    payment_uid    Nullable(String),
    plan_id        UInt32,
    points_used    UInt32,
    platform       String,
    created_at     Int64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.orders',
    kafka_group_name = 'ch_app_orders',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.orders_mv to cdc.orders as
select
    id,
    user_id,
    payment_uid,
    plan_id,
    points_used,
    platform,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.orders_kafka
where
    _error = '';

create table if not exists cdc.orders_kafka_s3 as cdc.orders_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.orders',
    kafka_group_name = 'ch_app_orders_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.orders_s3_sink
(
    id UInt64,
    user_id UInt32,
    payment_uid Nullable(String),
    plan_id UInt32,
    points_used UInt32,
    platform String,
    created_at DateTime('UTC'),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/orders/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(created_at);

create materialized view if not exists cdc.orders_s3_mv to cdc.orders_s3_sink as
select
    id,
    user_id,
    payment_uid,
    plan_id,
    points_used,
    platform,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.orders_kafka_s3
where
    _error = '';

-- 6. activations
create table if not exists cdc.activations
(
    id           UInt64,
    order_id     UInt64,
    user_id      UInt32,
    activated_at DateTime('UTC'),
    _version     UInt64,
    _is_deleted  UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.activations_kafka
(
    id             UInt64,
    order_id       UInt64,
    user_id        UInt32,
    activated_at   Int64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.activations',
    kafka_group_name = 'ch_app_activations',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.activations_mv to cdc.activations as
select
    id,
    order_id,
    user_id,
    fromUnixTimestamp64Milli(activated_at, 'UTC')               as activated_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.activations_kafka
where
    _error = '';

create table if not exists cdc.activations_kafka_s3 as cdc.activations_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.activations',
    kafka_group_name = 'ch_app_activations_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.activations_s3_sink
(
    id UInt64,
    order_id UInt64,
    user_id UInt32,
    activated_at DateTime('UTC'),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/activations/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(activated_at);

create materialized view if not exists cdc.activations_s3_mv to cdc.activations_s3_sink as
select
    id,
    order_id,
    user_id,
    fromUnixTimestamp64Milli(activated_at, 'UTC')               as activated_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.activations_kafka_s3
where
    _error = '';

-- 7. usage_daily
create table if not exists cdc.usage_daily
(
    id            UInt64,
    activation_id UInt64,
    usage_date    Date,
    usage_units   UInt32,
    cost_eur      Decimal(10, 2),
    _version      UInt64,
    _is_deleted   UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.usage_daily_kafka
(
    id             UInt64,
    activation_id  UInt64,
    usage_date     UInt32,
    usage_units    UInt32,
    cost_eur       Float64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.usage_daily',
    kafka_group_name = 'ch_app_usage_daily',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.usage_daily_mv to cdc.usage_daily as
select
    id,
    activation_id,
    toDate(usage_date)                                          as usage_date,
    usage_units,
    toDecimal64(cost_eur, 2)                                    as cost_eur,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.usage_daily_kafka
where
    _error = '';

create table if not exists cdc.usage_daily_kafka_s3 as cdc.usage_daily_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.usage_daily',
    kafka_group_name = 'ch_app_usage_daily_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.usage_daily_s3_sink
(
    id UInt64,
    activation_id UInt64,
    usage_date Date,
    usage_units UInt32,
    cost_eur Decimal(10, 2),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/usage_daily/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(usage_date);

create materialized view if not exists cdc.usage_daily_s3_mv to cdc.usage_daily_s3_sink as
select
    id,
    activation_id,
    toDate(usage_date)                                          as usage_date,
    usage_units,
    toDecimal64(cost_eur, 2)                                    as cost_eur,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.usage_daily_kafka_s3
where
    _error = '';

-- 8. refunds
create table if not exists cdc.refunds
(
    id          UInt64,
    payment_uid String,
    amount_eur  Decimal(10, 2),
    created_at  DateTime('UTC'),
    _version    UInt64,
    _is_deleted UInt8
)
engine = ReplacingMergeTree(_version, _is_deleted)
order by
    id;

create table if not exists cdc.refunds_kafka
(
    id             UInt64,
    payment_uid    String,
    amount_eur     Float64,
    created_at     Int64,
    __deleted      String
)
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.refunds',
    kafka_group_name = 'ch_app_refunds',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create materialized view if not exists cdc.refunds_mv to cdc.refunds as
select
    id,
    payment_uid,
    toDecimal64(amount_eur, 2)                                  as amount_eur,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.refunds_kafka
where
    _error = '';

create table if not exists cdc.refunds_kafka_s3 as cdc.refunds_kafka
engine = Kafka
settings
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'app_cdc.app.refunds',
    kafka_group_name = 'ch_app_refunds_s3',
    kafka_format = 'JSONEachRow',
    kafka_handle_error_mode = 'stream';

create table if not exists cdc.refunds_s3_sink
(
    id UInt64,
    payment_uid String,
    amount_eur Decimal(10, 2),
    created_at DateTime('UTC'),
    _version UInt64,
    _is_deleted UInt8
)
engine = S3('http://minio:9000/lakehouse/cdc/refunds/{_partition_id}.parquet', 'minioadmin', 'minioadmin', 'Parquet')
partition by
    toYYYYMMDD(created_at);

create materialized view if not exists cdc.refunds_s3_mv to cdc.refunds_s3_sink as
select
    id,
    payment_uid,
    toDecimal64(amount_eur, 2)                                  as amount_eur,
    fromUnixTimestamp64Milli(created_at, 'UTC')                 as created_at,
    (_offset + 1)                                               as _version,
    if(__deleted = 'true', 1, 0)                                as _is_deleted
from
    cdc.refunds_kafka_s3
where
    _error = '';
