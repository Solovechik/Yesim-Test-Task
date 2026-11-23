# ELT-пайплайн построения витрины доходов (`gold.mrt_revenue`)

Репозиторий содержит код гибридного ELT-пайплайна (MariaDB → S3/Bronze + Debezium/Redpanda CDC → ClickHouse Silver/Gold) с оркестрацией в Apache Airflow и трансформациями на dbt.

---

## 1. Запуск

Стенд воспроизводим из коробки: инициализация баз, пользователей, прав, таблиц, бакетов и DAG-ов выполняется идемпотентно.

#### Docker
```bash
make up           # чистая инфраструктура (Docker + DDL баз, ролей, таблиц + креды из .env)
make next         # следующий пакет изменений в MariaDB (всего 6 пакетов по 4 часа)
make status       # текущий номер пакета
make trigger-dags # ручной прогон пайплайна (без ожидания расписания)
make dbt-build    # сборка всех dbt-моделей вручную (silver + gold)
make dbt-docs     # генерация и раздача dbt-документации
make reset        # полный сброс стенда
```

После `make up` / `make reset` начальный прогон (`state0`) стартует в Airflow автоматически; далее для имитации суток: `make next` → `make trigger-dags` (повторить 6 раз).


#### Web Interface
##### Airflow
http://localhost:8080 — вход без пароля: включён; все пользователи считаются админами.

DAG-и связаны через Airflow Assets, а не через сдвиг расписания:
* `mariadb_to_s3_dimensions` (`0 */4 * * *`) → outlet `MARIA_DIMENSIONS_ASSET` (сигнал готовности);
* `mariadb_to_s3_events` (`0 */4 * * *`) → outlet `MARIA_EVENTS_ASSET` (сигнал готовности);
* `mrt_revenue_pipeline` — asset-based расписание `MARIA_EVENTS_ASSET & MARIA_DIMENSIONS_ASSET`, стартует сразу после готовности обоих источников.

##### Clickhouse
http://localhost:8124/play — `login: default, password: admin_pass`
##### Minio
http://localhost:9101 — `login: minioadmin, password: minioadmin`, бакет `lakehouse` с выгруженными из MariaDB батчами.
##### dbt Docs
http://localhost:8081 (если запускаете через make dbt-docs)

---

## 2. Слои, движки таблиц, пользователи и права

### Слои данных (Medallion Architecture)
* **`bronze` — Raw Layer.** Два источника:
  * `bronze.src_*` — `S3()`-таблицы поверх parquet-батчей из MinIO (историческая/регулярная выгрузка);
  * `bronze.cdc.*` — Kafka Engine + Materialized View, принимающие поток Debezium в формате JSON (upsert + soft-delete).
  Технические поля: `_version`, `_is_deleted`, `dt` (дата партиции S3-батча или дата события для CDC). Отдельной колонки `batch_dt` нет — водораздел батча в макросе `get_cdc_watermarks` считается по событийной дате конкретной таблицы (`payment_created_at`, `order_created_at`, `activated_at`, `refund_created_at`, `usage_date`).

* **`silver.stg_*` — Staging.** `ReplacingMergeTree(_version, _is_deleted)` (либо эквивалент с партиционированием): в одну таблицу сливаются **оба** потока. Приоритет задан версией: у S3-батча `_version = 0`, у CDC-события `_version = _offset + 1`. Мягкое удаление из Debezium (`__deleted`) превращается в `_is_deleted = 1`. Здесь дешёвая дедупликация на уровне движка — без чтения через `FINAL`.
* **`silver.int_*` — Dedupe / Enrichment.** `MergeTree()`, материализуется dbt-моделями (`int_payments`, `int_orders`, `int_activations`, `int_refunds`, `int_usage_daily`) стратегией `insert_overwrite` **только по затронутым месячным партициям**. Реальная дедупликация и фильтрация удалённых строк выполняются здесь один раз, а не в золотом слое.
* **`gold` — Mart Layer.** `dim_*`, `fct_*`, `mrt_revenue` читают `silver.int_*` напрямую: **`FINAL` не используется**, gold не тратит ресурсы на дедупликацию при чтении.

### Инкрементальность silver/gold
Ключевой макрос — `get_cdc_watermarks` (Jinja): он отслеживает **оба** водораздела — `max(_version)` (CDC) и `max(<event_dt>)` при `_version = 0` (S3-батчи, где `<event_dt>` — событийная дата конкретной таблицы: `payment_created_at`, `order_created_at`, `activated_at`, `refund_created_at`, `usage_date`). По ним вычисляются партиции, которые реально могли измениться, и перезаписываются через `insert_overwrite`. Поэтому повторный запуск идемпотентен, а полного перечтения больших таблиц нет.

### Движки и партиционирование
* **Bronze:** `S3()` для пруннинга прямо в S3; `Kafka()` + `Materialized View` для CDC.
* **Silver:** `ReplacingMergeTree(_version, _is_deleted)` в `stg_*`; `MergeTree()` в `int_*` с сортировкой по ключам источника.
* **Gold:** `MergeTree()`, у `mrt_revenue` — `PARTITION BY toYYYYMM(pdate)`, `ORDER BY (pdate, user_id, order_id)`: быстрый pruning партиций по датам и эффективное сжатие.

### Пользователи и права (RBAC)
Минимальные привилегии, DDL в `clickhouse/initdb/01_init.sh`:
* `etl_user`: `CREATE`, `INSERT`, `SELECT`, `DROP`, `ALTER TABLE ... DROP/REPLACE PARTITION` на `bronze`, `silver`, `gold`, а также `SELECT ON system.parts`, `system.tables`, `system.columns` — без чтения системных таблиц dbt-clickhouse не может строить список партиций для `insert_overwrite`.
* `analyst_user`: строго `SELECT` только на схему `gold`.

---

## 3. Стратегии загрузки таблиц

Выгрузка разделена на две группы.

1. **Полная перезапись (dimensions, `mariadb_to_s3_dimensions`):** `users`, `plans`, `payment_systems`.
   * Небольшие справочники (40 тыс., 48 и 5 строк), в которых правки приходят задним числом (флаги `is_tester`/`is_staff`, параметры тарифа) и нет `updated_at`. Полный снапшот в S3 с полной перезаписью таблиц `silver.stg_*` (`materialized = 'table'`) на каждом запуске гарантирует актуальное состояние без лишнего усложнения через CDC.
2. **Гибридная загрузка для событий (`mariadb_to_s3_events`):** `payments`, `orders`, `activations`, `usage_daily`, `refunds`.
   * **Батчи из S3** — инкрементально по high-watermark: `created_at` / `activated_at` / `usage_date` (таблица-специфичное поле задаётся в конфиге DAG-а вместе с `ch_name`), с перекрытием окна для опоздавших данных.
   * **CDC в реальном времени** — Debezium Server → Redpanda → ClickHouse Kafka Engine + Materialized View (`cdc.*`). Ловит смену статуса платежа (`pending → success/declined`, отзыв `success → declined`), поздние досылки, апдейты и мягкие удаления (`__deleted → _is_deleted`) **за любую дату в прошлом**.
   * Оба потока сливаются в `silver.stg_*` через `ReplacingMergeTree(_version, _is_deleted)`, поэтому порядок прихода не важен: более новое событие всегда перекрывает батч.

---

## 4. Как устроена сборка витрины

Цепочка dbt: `bronze.src_* / bronze.cdc.*` → `stg_*` → `int_*` → `dim_*`/`fct_*` → `mrt_revenue`.

* **Дедупликация — в `silver.int_payments`.** Из `stg_payments` (уже `ReplacingMergeTree` по `_version`) остаётся по одной актуальной строке на `payment_uuid` независимо от статуса; удалённые (`_is_deleted = 1`) отбрасываются. Фильтр `payment_status = 'success'` применяется выше — в CTE `success_payments` внутри `mrt_revenue`, чтобы витрина сама решала, какой платёж считать оплатой.
* **Разделение `payment` и `points`:** заказы без `payment_uuid` — `points` с `revenue_eur = 0.00` и `month_since_first = 0`. Денежные берут сумму и `paid_at` из `success_payments`.
* **Когортный расчёт:** `min() over (partition by user_id)` и `row_number()` применяются только к строкам `source = 'payment'`, поэтому балловые заказы не сдвигают `first_paid_at` и `purchase_number`.
* **Категория пакета** считается джойном с `dim_plans` по правилам `is_unlimited`/`days`/`size_gb`.
* **Пересборка:** `int_*` обновляются инкрементально по затронутым партициям (`insert_overwrite`), а сама витрина `mrt_revenue` объявлена как `materialized = 'table'` и пересобирается **целиком** при каждом запуске. Это осознанный выбор: `first_paid_at`, `first_cohort`, `month_since_first` и `purchase_number` — оконные метрики по всей истории пользователя, и отзыв старого платежа меняет их у давно закрытых строк. Инкрементальная пересборка по `pdate` дала бы некорректные окна без пересчёта всей истории пользователя. Дедупликация при этом уже сделана в `int_*`, поэтому полная пересборка `mrt_revenue` читает чистые таблицы без `FINAL` и остаётся дешёвой; в проде эти окна выносятся в отдельную модель (см. раздел 7).
* Запуск: `dbt build` в двух шагах (silver, затем gold) внутри `mrt_revenue_pipeline`.

---

## 5. Как проверяли правильность расчета и что нашли в данных

### Проверка корректности
1. **dbt-тесты схем (`dbt test`):** `unique` / `not_null` на `order_id` и `payment_uuid`, `relationships` на ссылочную целостность, `accepted_values` для `source` (`payment`/`points`) и `plan_category`.
2. **Юнит-тесты dbt** (`dbt/models/gold/mrt/unit_tests.yml`): расчёт `mrt_revenue` проверяется на моках входных таблиц — кейсы «баллы → деньги», отзыв платежа, отсутствие `usage_daily`, несколько возвратов на один `payment_uuid`.
3. **Кастомные бизнес-тесты** (`dbt/tests/`):
   * `assert_gross_profit_math` — `gross_profit_eur = revenue_eur − cogs_eur` во всех строках;
   * `assert_no_testers_or_staff` — в витрине нет пользователей с `is_tester = 1` или `is_staff = 1`;
   * `assert_revenue_payment_rules` — у `source = 'payment'` есть успешный платёж, `revenue_eur > 0`, заполнены `first_paid_at` и `purchase_number`;
   * `assert_revenue_points_rules` — у `source = 'points'` `payment_uuid` пустой, `revenue_eur = 0`, `month_since_first = 0`, `purchase_number` пустой.

### Найденные аномалии в данных
* **Тестовые и служебные аккаунты:** заказы пользователей с `is_tester = 1` / `is_staff = 1` — отфильтрованы по текущему состоянию `users` (флаг может быть проставлен задним числом, поэтому фильтр применяется на момент сборки витрины, а не при загрузке).
* **Заказы без активаций/расходов:** часть оплаченных заказов ещё не имеет `usage_daily` — обработано `LEFT JOIN` + `coalesce(..., 0)` для `cogs_eur` и `gross_profit_eur`.
* **Зависающие и отозванные платежи:** встречаются переходы `success → declined` и несколько записей одного `payment_uuid` в пределах 72 часов — обработано CDC + дедупликацией в `int_payments`; отзыв платежа убирает строку из витрины через `success_payments`.
* **Опоздавшие расходы:** `usage_daily` приходит на 1–2 дня позже — покрывается перекрытием окна батча и CDC-потоком.

---

## 6. Как это поддерживать: сбои загрузки и расхождение данных

* **Если упала выгрузка (Airflow / MariaDB / S3):** пайплайн идемпотентен — достаточно Clear Task упавшей таски; повторный запуск ничего не задублирует (`ReplacingMergeTree` по `_version`).
* **Если упал CDC:** Debezium Server хранит offset и schema history в файловом сторе (`/debezium/data`), после рестарта продолжает с последней позиции; пропущенное добирается регулярными S3-батчами.
* **Если данные разошлись с источником:**
  1. Backfill в Airflow за нужный период (или расширение окна выгрузки);
  2. `dbt build --select +mrt_revenue --full-refresh` + `dbt test` для полной пересборки зависимых моделей.

---

## 7. Что сделали бы иначе в проде и что не успели

* **Оркестрация:** asset-based связи между DAG-ами уже реализованы (`common/assets.py`); в проде добавил бы сенсоры на появление объектов в S3 и алерты в Slack/Telegram при падении `dbt test`.
* **Отказоустойчивость инфраструктуры:** в проде одиночный брокер Redpanda заменяется на кластер Kafka/Redpanda с репликацией (RF=3) и Schema Registry, а одиночный инстанс ClickHouse — на реплицируемый кластер (ReplicatedMergeTree). Также добавляется мониторинг лага консьюмеров (consumer lag).
* **Витрина:** при росте до десятков миллионов строк — вынести `first_paid_at` / `purchase_number` в отдельную intermediate-модель агрегатов пользователей (`int_user_ltv_metrics`) и пересобирать `mrt_revenue` инкрементально по `pdate`, а не целиком, чтобы не пересчитывать окна по всей истории на каждом запуске.
* **Не успели:** CI/CD (линт SQL + `dbt build --empty` в PR), мониторинг свежести (`dbt source freshness` с алертами — на стенде с имитацией времени через батчи он неактуален, но обязателен в проде), жёсткие dbt Model Contracts (`enforced: true`) на витринах `gold` и IaC для ClickHouse.


---

## 8. Сколько времени ушло

* **Проектирование архитектуры, DDL и настройка окружения:** ~6 часов.
* **Разработка Airflow DAG, Assets и логики инкрементальной выгрузки в S3:** ~6 часа.
* **CDC-контур (Debezium → Redpanda → ClickHouse Kafka Engine + MV):** ~4 часа.
* **dbt-модели (bronze/silver/int/gold), макрос `get_cdc_watermarks`, `insert_overwrite`:** ~5 часов.
* **Тесты (схемы, юнит, бизнес-ассерты), сверка с источником, CSV, документация:** ~4 часа.
* **Итого:** ~25 часов.
