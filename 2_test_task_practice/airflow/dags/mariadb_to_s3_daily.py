import logging
import os
from datetime import datetime, timedelta

import mysql.connector as mysql
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id='sync_mariadb_to_s3_daily',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    default_args={
        'owner': 'bi_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['mariadb', 's3', 'bronze', '24h'],
)
def sync_mariadb_to_s3_daily():
    tables_to_extract = [
        'users',
        'plans',
        'payment_systems',
    ]

    @task
    def extract_and_load_to_s3(table_name: str, **context):
        execution_date = context['ds']

        mysql_conn = mysql.connect(
            host='mariadb',
            port=3306,
            user='root',
            password=os.getenv('MARIADB_ROOT_PASSWORD'),
            database='app',
        )
        s3 = s3fs.S3FileSystem(
            key=os.getenv('S3_ACCESS_KEY'),
            secret=os.getenv('S3_SECRET_KEY'),
            client_kwargs={'endpoint_url': 'http://minio:9000'},
        )

        s3_path = f's3://lakehouse/bronze/{table_name}/data.parquet'

        df = pd.read_sql(
            f'select * from `{table_name}`', con=mysql_conn, dtype_backend='pyarrow'
        )

        if df.empty:
            logger.info(
                f'No data in {table_name!r} for {execution_date!r}. Skipping upload.'
            )
            return

        table = pa.Table.from_pandas(df, preserve_index=False)

        with s3.open(s3_path, 'wb') as _file:
            pq.write_table(table, _file, compression='ZSTD')

        logger.info(f'Successfully loaded {table_name} to {s3_path} | Rows: {len(df)}')

    extract_and_load_to_s3.expand(table_name=tables_to_extract)


dag_instance = sync_mariadb_to_s3_daily()
