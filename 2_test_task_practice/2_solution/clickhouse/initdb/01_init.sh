#!/bin/bash
set -e

clickhouse client -n <<-EOSQL
    -- 0. named collections
    CREATE NAMED COLLECTION IF NOT EXISTS minio_bronze AS
        url = '${S3_URL}',
        access_key_id = '${S3_ACCESS_KEY}',
        secret_access_key = '${S3_SECRET_KEY}',
        format = 'Parquet';


    -- 1. profiles
    -- analyst
    CREATE SETTINGS PROFILE IF NOT EXISTS analyst_profile SETTINGS
        readonly = 1,
        max_memory_usage = 10000000000,
        max_execution_time = 60,
        max_rows_to_read = 100000000,
        max_concurrent_queries_for_user = 3;
    -- etl
    CREATE SETTINGS PROFILE IF NOT EXISTS etl_profile SETTINGS
        readonly = 0,
        max_execution_time = 1800,
        max_memory_usage = 16000000000;


    -- 2. roles
    -- analyst
    CREATE ROLE IF NOT EXISTS analyst_role SETTINGS PROFILE 'analyst_profile';
    -- etl
    CREATE ROLE IF NOT EXISTS etl_role SETTINGS PROFILE 'etl_profile';


    -- 3. users (abstract for test task purposes)
    -- analyst
    CREATE USER IF NOT EXISTS analyst_user
    IDENTIFIED WITH sha256_password BY '${CH_ANALYST_PASSWORD}'
    DEFAULT ROLE analyst_role;
    -- etl
    CREATE USER IF NOT EXISTS etl_user
    IDENTIFIED WITH sha256_password BY '${CH_ETL_PASSWORD}'
    DEFAULT ROLE etl_role;


    -- 4.grants
    -- analyst
    GRANT SELECT ON gold.* TO analyst_role;

    -- etl
    GRANT S3 ON *.* TO etl_role;
    GRANT NAMED COLLECTION ON minio_bronze TO etl_role;
    GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE ON bronze.* TO etl_role;
    GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE ON silver.* TO etl_role;
    GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE ON gold.* TO etl_role;
    GRANT CREATE VIEW, DROP VIEW ON gold.* TO etl_role;
EOSQL
