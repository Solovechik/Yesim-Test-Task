#!/bin/bash
set -e

pip install --no-cache-dir clickhouse-connect dbt-clickhouse s3fs

airflow db migrate
airflow dag-processor &
airflow scheduler &

exec airflow api-server
