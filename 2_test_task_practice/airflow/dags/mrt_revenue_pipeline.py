from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'bi_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mrt_revenue_pipeline',
    default_args=default_args,
    description='DBT mrt_revenue pipeline',
    schedule='45 */4 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'gold', '4h', 'clickhouse'],
) as dag:
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt && dbt run -s tag:silver -t silver && dbt run -s tag:gold -t gold',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/dbt && dbt test -s tag:silver -t silver && dbt test -s tag:gold -t gold',
    )

    dbt_run >> dbt_test
