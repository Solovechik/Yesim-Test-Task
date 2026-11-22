FROM apache/airflow:slim-3.4.0-python3.12

USER root
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AIRFLOW_HOME=/opt/airflow \
    UV_PYTHON=3.12

WORKDIR /opt/airflow

RUN mkdir -p /opt/airflow/data && chown -R airflow:root /opt/airflow/data

USER airflow

COPY --chown=airflow:root pyproject.toml ./
RUN --mount=type=cache,target=/home/airflow/.cache/uv,uid=50000,gid=0 \
    uv pip install --prefix /home/airflow/.local -r pyproject.toml \
    && python -c "import clickhouse_connect, mysql.connector, pandas, pyarrow, s3fs"

COPY --chown=airflow:root --chmod=755 airflow/entrypoint.sh ./entrypoint.sh
COPY --chown=airflow:root airflow/dags ./dags
COPY --chown=airflow:root dbt ./dbt

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/opt/airflow/entrypoint.sh"]
