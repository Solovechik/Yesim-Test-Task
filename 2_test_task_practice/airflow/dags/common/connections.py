import os
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from s3fs import S3FileSystem
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection, Engine


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f'Environment variable {name!r} is not set')
    return value


@dataclass(frozen=True)
class ClickhouseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @contextmanager
    def connect(self) -> Generator[Client, None, None]:
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            database=self.database,
        )

        try:
            yield client
        finally:
            client.close()


@dataclass(frozen=True)
class MariadbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    def connect_kwargs(self) -> dict[str, object]:
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
        }

    @contextmanager
    def connect(self) -> Generator[Connection, None, None]:
        url = URL.create(
            drivername='mysql+mysqlconnector',
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        engine: Engine = create_engine(url)

        try:
            with engine.connect() as conn:
                yield conn
        finally:
            engine.dispose()


@dataclass(frozen=True)
class S3Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    bronze_prefix: str

    def filesystem(self) -> S3FileSystem:
        return S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs={'endpoint_url': self.endpoint_url},
        )

    def bronze_path(self, table_name: str) -> str:
        return f's3://{self.bucket}/{self.bronze_prefix}/{table_name}/data.parquet'


def clickhouse_config() -> ClickhouseConfig:
    return ClickhouseConfig(
        host=_env('CLICKHOUSE_HOST', 'clickhouse'),
        port=int(_env('CLICKHOUSE_PORT', '8123')),
        user=_env('CLICKHOUSE_USER', 'default'),
        password=_env('CLICKHOUSE_PASSWORD'),
        database=_env('CLICKHOUSE_DATABASE', 'bronze'),
    )


def mariadb_config() -> MariadbConfig:
    return MariadbConfig(
        host=_env('MARIADB_HOST', 'mariadb'),
        port=int(_env('MARIADB_PORT', '3306')),
        user=_env('MARIADB_USER', 'root'),
        password=_env('MARIADB_ROOT_PASSWORD'),
        database=_env('MARIADB_DATABASE', 'app'),
    )


def s3_config() -> S3Config:
    return S3Config(
        endpoint_url=_env('S3_ENDPOINT_URL', 'http://minio:9000'),
        access_key=_env('S3_ACCESS_KEY'),
        secret_key=_env('S3_SECRET_KEY'),
        bucket=_env('S3_BUCKET', 'lakehouse'),
        bronze_prefix=_env('S3_BRONZE_PREFIX', 'bronze'),
    )
