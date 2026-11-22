#!/bin/bash
set -e

airflow db migrate
airflow dag-processor &
airflow scheduler &

exec airflow api-server
