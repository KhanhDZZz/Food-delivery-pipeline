import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL


DAG_ID = "data_pipeline_for_food_delivery_orders"

DATA_DIR = os.getenv("DATA_DIR", r"D:\airflow 1\data")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD")

SCHEMA = "public"
TABLE_PREFIX = "bronze_"
IF_EXISTS = "replace"
CHUNK_SIZE = 10_000
SUPPORTED_EXT = {".csv", ".xlsx", ".xls"}


BRONZE_TABLE = os.getenv(
    "BRONZE_TABLE", "bronze_global_food_delivery_enriched_market_2026"
)
SILVER_TABLE = "silver_fact_orders"
FACT_TABLE = "fact_delivery_orders"
UNKNOWN = "Unknown"


TEXT_COLS = [
    "order_id", "metro_city", "cuisine_category", "order_channel",
    "restaurant_price_tier", "weather_condition", "customer_spending_tier",
    "customer_satisfaction_tier", "primary_satisfaction_driver",
    "cot_market_dynamics_rationale",
]
MONEY_COLS = [
    "order_value_usd", "discount_applied_usd", "delivery_fee_usd",
    "net_effective_order_price", "estimated_restaurant_net_margin_usd",
]
DECIMAL_COLS = [
    "delivery_distance_km", "actual_delivery_time_min", "customer_rating",
    "discount_percentage", "delivery_speed_km_per_hr",
    "total_fulfillment_time_min", "delivery_fee_ratio",
    "logistics_friction_index", "confidence_score",
]
INT_COLS = ["estimated_prep_time_min"]
BOOL_COLS = ["is_peak_hour"]

TIMESTAMP_COLS = ["order_timestamp", "order_datetime", "order_date", "created_at"]

NON_NEGATIVE_COLS = {
    "order_value_usd", "discount_applied_usd", "delivery_fee_usd",
    "net_effective_order_price", "delivery_distance_km",
    "actual_delivery_time_min", "delivery_speed_km_per_hr",
    "total_fulfillment_time_min", "delivery_fee_ratio",
    "confidence_score", "discount_percentage", "estimated_prep_time_min",
}
RATING_RANGE = None


DIMENSIONS = {
    "dim_city": ("city_key", ["metro_city"]),
    "dim_cuisine": ("cuisine_key", ["cuisine_category"]),
    "dim_order_channel": ("order_channel_key", ["order_channel"]),
    "dim_restaurant_tier": ("restaurant_tier_key", ["restaurant_price_tier"]),
    "dim_weather": ("weather_key", ["weather_condition"]),
    "dim_customer_tier": ("customer_tier_key", ["customer_spending_tier"]),
    "dim_satisfaction": (
        "satisfaction_key",
        ["customer_satisfaction_tier", "primary_satisfaction_driver"],
    ),
}
FACT_MEASURES = [
    "order_value_usd", "discount_applied_usd", "delivery_distance_km",
    "estimated_prep_time_min", "actual_delivery_time_min", "delivery_fee_usd",
    "customer_rating", "net_effective_order_price", "discount_percentage",
    "delivery_speed_km_per_hr", "total_fulfillment_time_min",
    "delivery_fee_ratio", "logistics_friction_index",
    "estimated_restaurant_net_margin_usd", "confidence_score",
]


NUM_REGEX = "^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?$"


def T(name: str) -> str:
    return f'"{SCHEMA}"."{name}"'


def get_engine():
    if PG_PASSWORD is None:
        raise ValueError(
            "Chưa đặt biến môi trường PG_PASSWORD (khai báo trong docker-compose.yaml / file .env)."
        )

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DB,
    )
    return create_engine(url)


def read_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        for enc in ("utf-8-sig", "utf-8", "cp1258", "cp1252"):
            try:
                return pd.read_csv(path, encoding=enc)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Không đọc được encoding của file: {path}")
    return pd.read_excel(path, sheet_name=0)


def run_statements(statements: list) -> None:
    engine = get_engine()
    try:
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
    finally:
        engine.dispose()


def _text_expr(col: str) -> str:
    return f"NULLIF(BTRIM(CAST({col} AS TEXT)), '')"


def _num_expr(col: str) -> str:

    t = f"BTRIM(CAST({col} AS TEXT))"
    return f"CASE WHEN {t} ~ '{NUM_REGEX}' THEN CAST({t} AS NUMERIC) END"


def _bool_expr(col: str) -> str:
    t = f"LOWER(BTRIM(CAST({col} AS TEXT)))"
    return (
        f"CASE WHEN {t} IN ('true','t','1','1.0','yes','y') THEN TRUE "
        f"WHEN {t} IN ('false','f','0','0.0','no','n') THEN FALSE END"
    )


def _sql_type(col: str) -> str:
    if col in MONEY_COLS:
        return "NUMERIC(18,2)"
    if col in DECIMAL_COLS:
        return "NUMERIC(18,4)"
    return "INTEGER"


def silver_statements(ts_cols: list) -> list:
    func = f'"{SCHEMA}".try_cast_timestamp'
    all_cols = TEXT_COLS + MONEY_COLS + DECIMAL_COLS + INT_COLS + BOOL_COLS + ts_cols

    cleaned = []
    cleaned += [f"{_text_expr(c)} AS {c}" for c in TEXT_COLS]
    cleaned += [f"CAST(ROUND({_num_expr(c)}, 2) AS NUMERIC(18,2)) AS {c}" for c in MONEY_COLS]
    cleaned += [f"CAST(ROUND({_num_expr(c)}, 4) AS NUMERIC(18,4)) AS {c}" for c in DECIMAL_COLS]
    cleaned += [f"CAST(ROUND({_num_expr(c)}) AS INTEGER) AS {c}" for c in INT_COLS]
    cleaned += [f"{_bool_expr(c)} AS {c}" for c in BOOL_COLS]
    cleaned += [f"{func}(CAST({c} AS TEXT)) AS {c}" for c in ts_cols]

    validated = []
    for c in all_cols:
        if c in NON_NEGATIVE_COLS:

            validated.append(
                f"CAST(CASE WHEN {c} < 0 THEN NULL ELSE {c} END AS {_sql_type(c)}) AS {c}"
            )
        elif c == "customer_rating" and RATING_RANGE:
            lo, hi = RATING_RANGE
            validated.append(
                f"CAST(CASE WHEN {c} < {lo} OR {c} > {hi} THEN NULL ELSE {c} END "
                f"AS {_sql_type(c)}) AS {c}"
            )
        else:
            validated.append(c)

    sep = ",\n    "
    create_sql = f"""
CREATE TABLE {T(SILVER_TABLE)} AS
WITH cleaned AS (
  SELECT
    {sep.join(cleaned)}
  FROM {T(BRONZE_TABLE)}
),
validated AS (
  SELECT
    {sep.join(validated)}
  FROM cleaned
),
ranked AS (
  SELECT
    validated.*,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY confidence_score DESC NULLS LAST, customer_rating DESC NULLS LAST
    ) AS rn
  FROM validated
  WHERE order_id IS NOT NULL
)
SELECT
  {sep.join(all_cols)},
  LOCALTIMESTAMP AS silver_loaded_at
FROM ranked
WHERE rn = 1
"""
    stmts = []
    if ts_cols:
        stmts.append(f"""
CREATE OR REPLACE FUNCTION {func}(v TEXT) RETURNS TIMESTAMP AS $$
BEGIN
  RETURN CAST(v AS TIMESTAMP);
EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE
""")
    stmts += [
        f"DROP TABLE IF EXISTS {T(SILVER_TABLE)} CASCADE",
        create_sql,
        f"ALTER TABLE {T(SILVER_TABLE)} ADD PRIMARY KEY (order_id)",
    ]
    return stmts


COMMON_METRICS = """
    COUNT(*) AS total_orders,
    ROUND(SUM(f.order_value_usd), 2) AS total_order_value_usd,
    ROUND(AVG(f.order_value_usd), 2) AS avg_order_value_usd,
    ROUND(SUM(f.discount_applied_usd), 2) AS total_discount_usd,
    ROUND(AVG(f.discount_percentage), 2) AS avg_discount_percentage,
    ROUND(AVG(f.delivery_fee_usd), 2) AS avg_delivery_fee_usd,
    ROUND(AVG(f.actual_delivery_time_min), 2) AS avg_delivery_time_min,
    ROUND(AVG(f.total_fulfillment_time_min), 2) AS avg_fulfillment_time_min,
    ROUND(AVG(f.customer_rating), 2) AS avg_customer_rating,
    ROUND(SUM(f.estimated_restaurant_net_margin_usd), 2) AS total_restaurant_net_margin_usd
"""


def mart_queries() -> dict:
    fact = T(FACT_TABLE)
    return {
        "mart_city_performance": f"""
SELECT c.metro_city, {COMMON_METRICS}
FROM {fact} f JOIN {T('dim_city')} c ON c.city_key = f.city_key
GROUP BY c.metro_city""",
        "mart_cuisine_performance": f"""
SELECT cu.cuisine_category, {COMMON_METRICS}
FROM {fact} f JOIN {T('dim_cuisine')} cu ON cu.cuisine_key = f.cuisine_key
GROUP BY cu.cuisine_category""",
        "mart_channel_performance": f"""
SELECT ch.order_channel, {COMMON_METRICS}
FROM {fact} f JOIN {T('dim_order_channel')} ch ON ch.order_channel_key = f.order_channel_key
GROUP BY ch.order_channel""",
        "mart_city_cuisine": f"""
SELECT c.metro_city, cu.cuisine_category, {COMMON_METRICS}
FROM {fact} f
JOIN {T('dim_city')} c ON c.city_key = f.city_key
JOIN {T('dim_cuisine')} cu ON cu.cuisine_key = f.cuisine_key
GROUP BY c.metro_city, cu.cuisine_category""",
        "mart_operations_peak_weather": f"""
SELECT
    f.is_peak_hour,
    w.weather_condition,
    COUNT(*) AS total_orders,
    ROUND(AVG(f.estimated_prep_time_min), 2) AS avg_prep_time_min,
    ROUND(AVG(f.actual_delivery_time_min), 2) AS avg_delivery_time_min,
    ROUND(AVG(f.total_fulfillment_time_min), 2) AS avg_fulfillment_time_min,
    ROUND(AVG(f.delivery_speed_km_per_hr), 2) AS avg_delivery_speed_km_per_hr,
    ROUND(AVG(f.logistics_friction_index), 4) AS avg_logistics_friction_index,
    ROUND(AVG(f.customer_rating), 2) AS avg_customer_rating
FROM {fact} f JOIN {T('dim_weather')} w ON w.weather_key = f.weather_key
GROUP BY f.is_peak_hour, w.weather_condition""",
        "mart_customer_satisfaction": f"""
SELECT
    ct.customer_spending_tier,
    sa.customer_satisfaction_tier,
    sa.primary_satisfaction_driver,
    COUNT(*) AS total_orders,
    ROUND(AVG(f.customer_rating), 2) AS avg_customer_rating,
    ROUND(AVG(f.order_value_usd), 2) AS avg_order_value_usd,
    ROUND(AVG(f.total_fulfillment_time_min), 2) AS avg_fulfillment_time_min,
    ROUND(AVG(f.logistics_friction_index), 4) AS avg_logistics_friction_index
FROM {fact} f
JOIN {T('dim_customer_tier')} ct ON ct.customer_tier_key = f.customer_tier_key
JOIN {T('dim_satisfaction')} sa ON sa.satisfaction_key = f.satisfaction_key
GROUP BY ct.customer_spending_tier, sa.customer_satisfaction_tier, sa.primary_satisfaction_driver""",
    }


MART_NAMES = list(mart_queries())


def dimension_statements(dim: str) -> list:
    key, cols = DIMENSIONS[dim]
    col_ddl = ", ".join(f"{c} TEXT NOT NULL" for c in cols)
    col_list = ", ".join(cols)
    select_list = ", ".join(f"COALESCE({c}, '{UNKNOWN}')" for c in cols)
    order_by = ", ".join(str(i + 1) for i in range(len(cols)))
    return [
        f"DROP TABLE IF EXISTS {T(dim)} CASCADE",
        f"CREATE TABLE {T(dim)} ({key} INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        f"{col_ddl}, UNIQUE ({col_list}))",
        f"INSERT INTO {T(dim)} ({col_list}) "
        f"SELECT DISTINCT {select_list} FROM {T(SILVER_TABLE)} ORDER BY {order_by}",
    ]


def fact_statements() -> list:
    key_cols, joins = [], []
    for i, (dim, (key, cols)) in enumerate(DIMENSIONS.items()):
        a = f"d{i}"
        key_cols.append(f"{a}.{key}")
        cond = " AND ".join(f"{a}.{c} = COALESCE(s.{c}, '{UNKNOWN}')" for c in cols)
        joins.append(f"JOIN {T(dim)} {a} ON {cond}")

    select_cols = (
        ["s.order_id"] + key_cols + ["s.is_peak_hour"]
        + [f"s.{m}" for m in FACT_MEASURES] + ["LOCALTIMESTAMP AS gold_loaded_at"]
    )
    nl = "\n  "
    stmts = [
        f"DROP TABLE IF EXISTS {T(FACT_TABLE)} CASCADE",
        f"CREATE TABLE {T(FACT_TABLE)} AS\nSELECT\n  {(',' + nl).join(select_cols)}\n"
        f"FROM {T(SILVER_TABLE)} s\n" + "\n".join(joins),
        f"ALTER TABLE {T(FACT_TABLE)} ADD PRIMARY KEY (order_id)",
    ]
    for dim, (key, _cols) in DIMENSIONS.items():
        stmts.append(
            f"ALTER TABLE {T(FACT_TABLE)} ADD FOREIGN KEY ({key}) "
            f"REFERENCES {T(dim)} ({key})"
        )
        stmts.append(f"CREATE INDEX ON {T(FACT_TABLE)} ({key})")
    return stmts


@task(task_id="scan_source_files")
def scan_source_files() -> list[str]:
    folder = Path(DATA_DIR)
    if not folder.is_dir():
        raise FileNotFoundError(f"Không tìm thấy thư mục: {DATA_DIR}")
    files = sorted(
        str(p)
        for p in folder.iterdir()
        if p.is_file()
        and p.suffix.lower() in SUPPORTED_EXT
        and not p.name.startswith(("~$", "."))
    )
    if not files:
        raise FileNotFoundError(f"Không có file csv/xlsx nào trong: {DATA_DIR}")
    print(f"Tìm thấy {len(files)} file: {files}")
    return files


@task(task_id="load_file_to_bronze", max_active_tis_per_dag=4)
def load_file_to_bronze(file_path: str) -> dict:
    path = Path(file_path)
    table_name = f"{TABLE_PREFIX}{path.stem}"

    df = read_file(path)
    df["_source_file"] = path.name
    df["_ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    engine = get_engine()
    try:

        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        df.to_sql(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists=IF_EXISTS,
            index=False,
            chunksize=CHUNK_SIZE,
            method="multi",
        )
    finally:
        engine.dispose()

    result = {"file": path.name, "table": table_name, "rows": len(df)}
    print(f"Đã nhập {result}")
    return result


@task(task_id="validate_bronze", retries=0)
def validate_bronze(load_results) -> dict:
    results = list(load_results)
    tables = {r["table"]: r["rows"] for r in results}
    if BRONZE_TABLE not in tables:
        raise ValueError(
            f"Không có file nào tạo ra bảng {BRONZE_TABLE}. Các bảng vừa nhập: {list(tables)}. "
            "Kiểm tra lại TABLE_PREFIX / BRONZE_TABLE."
        )
    engine = get_engine()
    try:
        with engine.connect() as conn:
            for table, expected in tables.items():
                actual = conn.execute(text(f"SELECT COUNT(*) FROM {T(table)}")).scalar()
                if actual == 0 or actual < expected:
                    raise ValueError(f"{table}: đã nhập {expected} dòng nhưng bảng có {actual}")
        cols = {c["name"] for c in inspect(engine).get_columns(BRONZE_TABLE, schema=SCHEMA)}
    finally:
        engine.dispose()
    required = TEXT_COLS + MONEY_COLS + DECIMAL_COLS + INT_COLS + BOOL_COLS
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(f"Bảng Bronze thiếu các cột: {missing}")
    print(f"Bronze OK: {tables}")
    return tables


@task(task_id="transform_silver_fact_orders")
def transform_silver() -> dict:
    engine = get_engine()
    try:
        cols = {c["name"] for c in inspect(engine).get_columns(BRONZE_TABLE, schema=SCHEMA)}
        ts_cols = [c for c in TIMESTAMP_COLS if c in cols]
        with engine.begin() as conn:
            for stmt in silver_statements(ts_cols):
                conn.execute(text(stmt))
            bronze_rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(BRONZE_TABLE)}")).scalar()
            silver_rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(SILVER_TABLE)}")).scalar()
    finally:
        engine.dispose()
    stats = {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "removed_duplicates_or_null_id": bronze_rows - silver_rows,
        "timestamp_cols_cast": ts_cols,
    }
    print(f"Silver xong: {stats}")
    return stats


@task(task_id="validate_silver", retries=0)
def validate_silver() -> dict:
    neg_cols = sorted(NON_NEGATIVE_COLS)
    null_cols = MONEY_COLS + DECIMAL_COLS + INT_COLS + BOOL_COLS
    select = ["COUNT(*) AS total_rows", "COUNT(DISTINCT order_id) AS distinct_ids",
              "COUNT(*) FILTER (WHERE order_id IS NULL) AS null_ids"]
    select += [f"COUNT(*) FILTER (WHERE {c} < 0) AS neg_{c}" for c in neg_cols]
    select += [
        f"ROUND(100.0 * COUNT(*) FILTER (WHERE {c} IS NULL) / NULLIF(COUNT(*), 0), 2) AS nullpct_{c}"
        for c in null_cols
    ]
    engine = get_engine()
    try:
        with engine.connect() as conn:
            row = dict(conn.execute(text(f"SELECT {', '.join(select)} FROM {T(SILVER_TABLE)}")).mappings().one())
            bronze_rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(BRONZE_TABLE)}")).scalar()
    finally:
        engine.dispose()

    errors = []
    if row["total_rows"] == 0:
        errors.append("Silver không có dòng nào")
    if row["total_rows"] > bronze_rows:
        errors.append(f"Silver ({row['total_rows']}) nhiều hơn Bronze ({bronze_rows})")
    if row["distinct_ids"] != row["total_rows"] or row["null_ids"] > 0:
        errors.append("order_id bị trùng hoặc NULL")
    errors += [f"{c} còn {row['neg_' + c]} giá trị âm" for c in neg_cols if row["neg_" + c] > 0]
    if errors:
        raise ValueError("Silver không đạt kiểm tra: " + "; ".join(errors))

    warnings = {c: float(row["nullpct_" + c]) for c in null_cols if row["nullpct_" + c]}
    print(f"Silver OK: {row['total_rows']} dòng. Tỉ lệ NULL (%) của các cột có NULL: {warnings}")
    return {"rows": row["total_rows"], "null_pct": warnings}


@task(task_id="reset_gold_layer")
def reset_gold_layer() -> None:
    run_statements(
        [f"DROP TABLE IF EXISTS {T(FACT_TABLE)} CASCADE"]
        + [f"DROP TABLE IF EXISTS {T(m)} CASCADE" for m in MART_NAMES]
    )


@task(task_id="build_dimension")
def build_dimension(dim: str) -> dict:
    run_statements(dimension_statements(dim))
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(dim)}")).scalar()
    finally:
        engine.dispose()
    print(f"{dim}: {rows} dòng")
    return {"table": dim, "rows": rows}


@task(task_id="build_fact_delivery_orders")
def build_fact() -> dict:
    run_statements(fact_statements())
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(FACT_TABLE)}")).scalar()
    finally:
        engine.dispose()
    print(f"{FACT_TABLE}: {rows} dòng")
    return {"table": FACT_TABLE, "rows": rows}


@task(task_id="build_mart")
def build_mart(mart: str) -> dict:
    run_statements([
        f"DROP TABLE IF EXISTS {T(mart)} CASCADE",
        f"CREATE TABLE {T(mart)} AS {mart_queries()[mart]}",
    ])
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT COUNT(*) FROM {T(mart)}")).scalar()
    finally:
        engine.dispose()
    print(f"{mart}: {rows} dòng")
    return {"table": mart, "rows": rows}


@task(task_id="validate_gold", retries=0)
def validate_gold() -> dict:
    fact, silver = T(FACT_TABLE), T(SILVER_TABLE)
    checks = [
        ("số dòng fact = số dòng silver",
         f"SELECT (SELECT COUNT(*) FROM {fact}), (SELECT COUNT(*) FROM {silver})"),
        ("tổng order_value_usd: fact = silver",
         f"SELECT (SELECT COALESCE(SUM(order_value_usd), 0) FROM {fact}), "
         f"(SELECT COALESCE(SUM(order_value_usd), 0) FROM {silver})"),
        ("fact không có khóa dim bị NULL",
         f"SELECT COUNT(*), 0 FROM {fact} WHERE "
         + " OR ".join(f"{k} IS NULL" for k, _ in DIMENSIONS.values())),
    ]
    for mart in ("mart_city_performance", "mart_cuisine_performance",
                 "mart_channel_performance", "mart_city_cuisine"):
        checks.append((
            f"{mart}: SUM(total_orders) = số dòng fact",
            f"SELECT (SELECT COALESCE(SUM(total_orders), 0) FROM {T(mart)}), "
            f"(SELECT COUNT(*) FROM {fact})",
        ))

    failed, passed = [], []
    engine = get_engine()
    try:
        with engine.connect() as conn:
            for name, sql in checks:
                actual, expected = conn.execute(text(sql)).one()
                if abs(Decimal(actual) - Decimal(expected)) > Decimal("0.005"):
                    failed.append(f"{name} (thực tế {actual}, kỳ vọng {expected})")
                else:
                    passed.append(name)
    finally:
        engine.dispose()
    if failed:
        raise ValueError("Gold không đạt đối soát: " + "; ".join(failed))
    print(f"Gold OK, {len(passed)} kiểm tra đạt: {passed}")
    return {"passed": passed}


@task(task_id="pipeline_summary", retries=0)
def pipeline_summary() -> dict:
    tables = [BRONZE_TABLE, SILVER_TABLE, FACT_TABLE] + list(DIMENSIONS) + MART_NAMES
    counts = {}
    engine = get_engine()
    try:
        with engine.connect() as conn:
            for t in tables:
                counts[t] = conn.execute(text(f"SELECT COUNT(*) FROM {T(t)}")).scalar()
    finally:
        engine.dispose()
    width = max(len(t) for t in counts)
    print("=" * (width + 16))
    for t, n in counts.items():
        print(f"{t.ljust(width)} | {n:>10,}")
    print("=" * (width + 16))
    return counts


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id=DAG_ID,
    description="Medallion: Bronze (csv/xlsx) -> Silver (làm sạch) -> Gold (star schema + mart)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["food-delivery", "medallion", "bronze", "silver", "gold", "postgres"],
)
def data_pipeline_for_food_delivery_orders():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup("bronze", tooltip="Nhập file thô vào Postgres", ui_color="#ecd5b8") as bronze:
        files = scan_source_files()
        loaded = load_file_to_bronze.expand(file_path=files)
        validate_bronze(loaded)

    with TaskGroup("silver", tooltip="Làm sạch, ép kiểu, khử trùng lặp", ui_color="#dde1e6") as silver:
        transform_silver() >> validate_silver()

    with TaskGroup("gold", tooltip="Star schema và bảng tổng hợp", ui_color="#faeab0") as gold:
        reset = reset_gold_layer()
        with TaskGroup("dimensions", tooltip="Các bảng dim_* (chạy song song)") as dims:
            for dim in DIMENSIONS:
                build_dimension.override(task_id=f"build_{dim}")(dim)
        fact = build_fact()
        with TaskGroup("marts", tooltip="Các bảng mart_* (chạy song song)") as marts:
            for mart in MART_NAMES:
                build_mart.override(task_id=f"build_{mart}")(mart)
        chain(reset, dims, fact, marts, validate_gold())

    chain(start, bronze, silver, gold, pipeline_summary(), end)


dag_obj = data_pipeline_for_food_delivery_orders()
