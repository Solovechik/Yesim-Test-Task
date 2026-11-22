import logging
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import dag, task
from common.assets import MARIA_DIMENSIONS_ASSET
from common.connections import mariadb_config, s3_config
from s3fs.core import S3FileSystem
from sqlalchemy import text

logger = logging.getLogger(__name__)


@dag(
    dag_id='sync_mariadb_to_s3_dimensions',
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
def sync_mariadb_to_s3_dimensions():
    tables_to_extract = [
        'users',
        'plans',
        'payment_systems',
    ]

    @task
    def extract_and_load_to_s3(table_name: str, **context):
        execution_date = context['ds']
        maria_cfg = mariadb_config()
        s3_cfg = s3_config()
        s3_fs: S3FileSystem = s3_cfg.filesystem()
        s3_path = s3_cfg.bronze_path(table_name)

        with maria_cfg.connect() as conn:
            df = pd.read_sql(
                text(f'select * from `{table_name}`'),
                con=conn,
                dtype_backend='pyarrow',
            )

        if df.empty:
            logger.info(
                f'No data in {table_name!r} for {execution_date!r}. Skipping upload.'
            )
            return

        table = pa.Table.from_pandas(df, preserve_index=False)

        with s3_fs.open(s3_path, 'wb') as _file:
            pq.write_table(table, _file, compression='ZSTD')

        logger.info(f'Successfully loaded {table_name} to {s3_path} | Rows: {len(df)}')

    @task(outlets=[MARIA_DIMENSIONS_ASSET])
    def mark_maria_dimensions_ready():
        pass

    (
        extract_and_load_to_s3.expand(table_name=tables_to_extract)
        >> mark_maria_dimensions_ready()
    )


dag_instance = sync_mariadb_to_s3_dimensions()
