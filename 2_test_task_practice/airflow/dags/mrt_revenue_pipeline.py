from datetime import datetime, timedelta

from airflow.sdk import dag, task
from common.assets import MARIA_DIMENSIONS_ASSET, MARIA_EVENTS_ASSET

DBT_DIR = '/opt/airflow/dbt'


@dag(
    dag_id='mrt_revenue_pipeline',
    description='DBT mrt_revenue pipeline',
    schedule=MARIA_EVENTS_ASSET & MARIA_DIMENSIONS_ASSET,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'bi_team',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['dbt', 'gold', '4h', 'clickhouse'],
)
def mrt_revenue_pipeline():
    @task.bash(cwd=DBT_DIR)
    def dbt_build_silver() -> str:
        return 'dbt build -s tag:silver --fail-fast -t silver'

    @task.bash(cwd=DBT_DIR)
    def dbt_build_gold() -> str:
        return 'dbt build -s tag:gold --fail-fast -t gold'

    dbt_build_silver() >> dbt_build_gold()


dag_instance = mrt_revenue_pipeline()
