from airflow.sdk import Asset

MARIA_EVENTS_ASSET = Asset('s3://lakehouse/bronze/events')
MARIA_DIMENSIONS_ASSET = Asset('s3://lakehouse/bronze/dimension')
