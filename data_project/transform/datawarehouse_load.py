import os
import duckdb as db
from dagster_duckdb import DuckDBResource
from pandas import DataFrame
from src.data_project.defs.resources import database_resource
from filelock import FileLock

LOCK_DIR = "c:/users/asus/data-project/duckdbfiles/locks"
os.makedirs(LOCK_DIR, exist_ok=True)


def load_dim(df: DataFrame, table_name: str):
    globals()["_temp_df"] = df

    # path file lock riêng cho table này
    lock_path = os.path.join(LOCK_DIR, f"{table_name}.lock")

    # bọc toàn bộ truy cập DuckDB trong file lock
    with FileLock(lock_path):
        with database_resource.get_connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS dim")
            conn.execute(f"CREATE OR REPLACE TABLE dim.{table_name} AS SELECT * FROM _temp_df")

def load_df(name: str):
    lock_path = os.path.join(LOCK_DIR, f"{name}.lock")

    with FileLock(lock_path):
        con = db.connect(r"c:\users\asus\data-project\duckdbfiles\orders.duckdb")
        df = con.execute(f"SELECT * FROM {name}").fetchdf()
        con.close()
    return df

def print_df(name:str):
    with database_resource.get_connection() as conn:
        df_check = conn.execute(f"SELECT * FROM dim.{name} ").fetchdf()
        print(df_check.info())

def get_df(name:str):
    with database_resource.get_connection() as conn:
        df_check = conn.execute(f"SELECT * FROM dim.{name} ").fetchdf()
        return df_check


def load_dim_from_table(
    df: DataFrame,
    target_table: str,
    duckdb_resources: DuckDBResource,
):
    """Tạo bảng dim.{target_table} từ bảng staging/raw {source_table}"""
    with duckdb_resources.get_connection() as conn:
        # đảm bảo schema dim tồn tại
        conn.execute("CREATE SCHEMA IF NOT EXISTS dim")

        # full replace load
        conn.execute(f"""
            CREATE OR REPLACE TABLE dim.{target_table} AS
            SELECT * FROM {source_table}
        """)