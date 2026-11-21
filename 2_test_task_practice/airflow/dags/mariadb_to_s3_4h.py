import logging
import os
from datetime import datetime, timedelta

import clickhouse_connect
import mysql.connector as mysql
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id='sync_mariadb_to_s3_4h',
    schedule='0 */4 * * *',
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
    default_args={
        'owner': 'bi_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['mariadb', 's3', 'bronze', '4h'],
)
def sync_mariadb_to_s3_4h():
    tables_to_extract = {
        'payments': 'created_at',
        'orders': 'created_at',
        'activations': 'activated_at',
        'usage_daily': 'usage_date',
        'refunds': 'created_at',
    }

    @task
    def extract_and_load_to_s3(table_name: str, **context):
        time_col = tables_to_extract[table_name]
        lookback_days = 3

        ch_client = clickhouse_connect.get_client(
            host='clickhouse', port=8123, username='default', password=os.getenv('CLICKHOUSE_PASSWORD'),
        )

        max_ts = None
        try:
            result = ch_client.query(f'select max({time_col}) from bronze.{table_name}')
            if result.result_rows and result.result_rows[0][0]:
                max_ts = result.result_rows[0][0]
        except Exception as e:
            error_msg = str(e)
            if 'UNKNOWN_TABLE' in error_msg or 'Code: 60' in error_msg:
                max_ts = None
                logger.warning(
                    f'Table bronze.{table_name} does not exist yet. Performing initial load.'
                )
            else:
                logger.error(
                    f'Failed to fetch max timestamp from ClickHouse due to infrastructure error: {e}'
                )
                raise

        if max_ts:
            start_ts = pd.to_datetime(max_ts) - timedelta(days=lookback_days)
        else:
            start_ts = datetime(2025, 1, 1, 0, 0, 0)

        logger.info(f'Extraction window for {table_name}: starting from {start_ts}')

        # 3. Вычитываем свежий срез из MariaDB
        with mysql.connect(
            host='mariadb',
            port=3306,
            user='root',
            password=os.getenv('MARIADB_ROOT_PASSWORD'),
            database='app',
        ) as mysql_conn:
            query = f"""
                SELECT *
                FROM `{table_name}`
                WHERE `{time_col}` >= '{start_ts}'
            """

            df = pd.read_sql(
                query,
                con=mysql_conn,
                dtype_backend='pyarrow',
            )

        if df.empty:
            logger.info(
                f'No data found in table {table_name!r} since {start_ts}. Skipping.'
            )
            return

        df[time_col] = pd.to_datetime(df[time_col])
        df['__partition_date'] = df[time_col].dt.strftime('%Y-%m-%d')

        s3 = s3fs.S3FileSystem(
            key=os.getenv('S3_ACCESS_KEY'),
            secret=os.getenv('S3_SECRET_KEY'),
            client_kwargs={'endpoint_url': 'http://minio:9000'},
        )

        run_id = context['data_interval_start'].strftime('%Y%m%d_%H%M%S')

        for part_date, group_df in df.groupby('__partition_date'):
            clean_df = group_df.drop(columns=['__partition_date'])

            new_file_path = f's3://lakehouse/bronze/{table_name}/dt={part_date}/part_{run_id}.parquet'

            table = pa.Table.from_pandas(clean_df, preserve_index=False)
            with s3.open(new_file_path, 'wb') as _file:
                pq.write_table(table, _file, compression='ZSTD')

            logger.info(
                f'Successfully uploaded new part: {new_file_path} (Rows: {len(clean_df)})'
            )

            partition_prefix = f'lakehouse/bronze/{table_name}/dt={part_date}/'
            existing_files = s3.glob(f's3://{partition_prefix}part_*.parquet')

            files_to_delete = [
                f for f in existing_files if f'part_{run_id}.parquet' not in f
            ]

            if files_to_delete:
                s3.rm(files_to_delete)
                logger.info(
                    f'Cleaned up old partition files for dt={part_date}: {files_to_delete}'
                )

    extract_and_load_to_s3.expand(table_name=list(tables_to_extract.keys()))


dag_instance = sync_mariadb_to_s3_4h()
