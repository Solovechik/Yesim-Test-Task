import logging
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import dag, task
from common.assets import MARIA_EVENTS_ASSET
from common.connections import clickhouse_config, mariadb_config, s3_config
from s3fs import S3FileSystem
from sqlalchemy import text

logger = logging.getLogger(__name__)

INITIAL_LOAD_START = date(2025, 1, 1)
LOOKBACK_DAYS = 0


@dag(
    dag_id='sync_mariadb_to_s3_events',
    schedule='0 */4 * * *',
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    max_active_tasks=3,  # varies depending on memory usage
    catchup=False,
    default_args={
        'owner': 'bi_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    tags=['mariadb', 's3', 'bronze', '4h'],
)
def sync_mariadb_to_s3_events():
    tables_to_extract = {
        'payments': {'ch_name': 'src_payments', 'ts_column': 'created_at'},
        'orders': {'ch_name': 'src_orders', 'ts_column': 'created_at'},
        'activations': {'ch_name': 'src_activations', 'ts_column': 'activated_at'},
        'usage_daily': {'ch_name': 'src_usage_daily', 'ts_column': 'usage_date'},
        'refunds': {'ch_name': 'src_refunds', 'ts_column': 'created_at'},
    }

    @task
    def extract_and_load_to_s3(table_name: str, **context):
        ch_cfg = clickhouse_config()
        maria_cfg = mariadb_config()
        s3_cfg = s3_config()
        s3_fs: S3FileSystem = s3_cfg.filesystem()

        interval_start = context['data_interval_start']
        run_id = interval_start.strftime('%Y%m%d_%H%M%S')

        interval_end = context['data_interval_end']
        end_date = interval_end.date()

        time_col = tables_to_extract[table_name]['ts_column']
        ch_table_name = tables_to_extract[table_name]['ch_name']
        max_ts = None

        with ch_cfg.connect() as conn:
            table_exists = conn.query(
                'exists table {db:Identifier}.{table:Identifier}',
                parameters={'db': ch_cfg.database, 'table': ch_table_name},
            ).result_rows[0][0]

            if not table_exists:
                logger.warning(
                    f'Table {ch_cfg.database}.{ch_table_name} does not exist yet. Performing initial load.'
                )
            else:
                result = conn.query(
                    'select maxOrNull({column:Identifier}) from {db:Identifier}.{table:Identifier}',
                    parameters={
                        'db': ch_cfg.database,
                        'table': ch_table_name,
                        'column': time_col,
                    },
                )
                if result.result_rows and result.result_rows[0][0]:
                    max_ts = result.result_rows[0][0]

        if max_ts:
            start_date = pd.to_datetime(max_ts).date() - timedelta(days=LOOKBACK_DAYS)
        else:
            start_date = INITIAL_LOAD_START

        batch_count = max((end_date - start_date).days + 1, 0)

        logger.info(
            f'Extraction window for {table_name}: {start_date} - {end_date} ({batch_count} day batches)'
        )

        with maria_cfg.connect() as conn:
            for batch in range(batch_count):
                range_start = start_date + timedelta(days=batch)
                range_end = range_start + timedelta(days=1)

                df = pd.read_sql(
                    text(
                        f"""
                            select
                                *
                            from
                                `{table_name}`
                            where
                                `{time_col}` >= :range_start
                                and `{time_col}` < :range_end
                        """
                    ),
                    con=conn,
                    params={'range_start': range_start, 'range_end': range_end},
                    dtype_backend='pyarrow',
                )

                partition_prefix = (
                    f'lakehouse/bronze/{table_name}/dt={range_start:%Y-%m-%d}/'
                )
                file_path = f'{partition_prefix}part_{run_id}.parquet'
                s3_file_path = f's3://{file_path}'
                tmp_file_path = f'{s3_file_path}.tmp'

                if df.empty:
                    logger.info(
                        f'No data found in table {table_name!r} for time period '
                        f'{range_start!r} - {range_end!r}. Skipping.'
                    )
                else:
                    table = pa.Table.from_pandas(df, preserve_index=False)

                    with s3_fs.open(tmp_file_path, 'wb') as _file:
                        pq.write_table(table, _file, compression='ZSTD')
                    s3_fs.mv(tmp_file_path, s3_file_path)

                    logger.info(
                        f'Successfully uploaded new part: {s3_file_path} '
                        f'(Rows: {len(df)})'
                    )

                stale_files = [
                    path
                    for path in s3_fs.glob(f's3://{partition_prefix}part_*.parquet')
                    if s3_fs._strip_protocol(path) != file_path
                ]

                if stale_files:
                    s3_fs.rm(stale_files)
                    logger.info(
                        f'Cleaned up old partition files for '
                        f'dt={range_start:%Y-%m-%d}: {stale_files}'
                    )

    @task(outlets=[MARIA_EVENTS_ASSET])
    def mark_maria_events_ready():
        pass

    (
        extract_and_load_to_s3.expand(table_name=list(tables_to_extract.keys()))
        >> mark_maria_events_ready()
    )


dag_instance = sync_mariadb_to_s3_events()
