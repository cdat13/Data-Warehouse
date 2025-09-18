import dagster as dg
import pandas as pd
import duckdb
from dagster import AssetKey, AssetIn
import filelock
from dagster_duckdb import DuckDBResource
import src.data_project.transform.datawarehouse_load as func
import src.data_project.transform.postgre as postgre

def loadfile_duckdb_query(duckdb_path: str, sql: str):
    """Tai file vao duckdb bang query sql va su dung them filelock"""
    lock_path = f"{duckdb_path}.lock"
    with filelock.FileLock(lock_path):
        conn = duckdb.connect(duckdb_path)
        try:
            return conn.execute(sql)
        finally:
            conn.close()

def import_path_to_duckdb(path: str, duckdb_resources: DuckDBResource, table_name: str):
    """load file bang path vao duckdb bang cau lenh duckdb"""
    with duckdb_resources.get_connection() as conn:
        row_count = conn.execute(f"""
        create or replace table {table_name} as (
            select * from read_csv_auto('{path}')
        )
    """).fetchone()
        assert row_count is not None
        row_count = row_count[0]

@dg.asset(kinds={"duckdb"}, key=["target", "main", "orders"], group_name="raw")
def orders(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_orders_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="orders_dataset",
    )

@dg.asset( kinds={"duckdb"}, key=["target", "main", "customers"], group_name="raw")
def customers(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_customers_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="customers_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "geolocation"], group_name="raw")
def geolocation(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_geolocation_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="geolocation_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "order_items"], group_name="raw")
def order_items(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_order_items_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="order_items_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "order_payments"], group_name="raw")
def payment(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_order_payments_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="order_payments_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "order_reviews"], group_name="raw")
def order_reviews(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_order_reviews_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="order_reviews_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "products"], group_name="raw")
def products(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_products_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="products_dataset",
    )

@dg.asset(kinds={"duckdb"}, key=["target", "main", "sellers"], group_name="raw")
def sellers(duckdb_resources: DuckDBResource) -> None:
    import_path_to_duckdb(
        path=r"C:\Users\ASUS\data-project\csvfiles\archive\olist_sellers_dataset.csv",
        duckdb_resources = duckdb_resources,
        table_name="sellers_dataset",
    )

# Dim asset start here!

@dg.asset(
    deps=[AssetKey(["target", "main", "customers"])],
    key=["target", "main", "customers_dim"],
    group_name="dims"
)
def dim_customers() -> pd.DataFrame:
    import src.data_project.transform.customers as ag
    df = ag.transform()
    func.load_dim(df, "customers_dim")
    return df


@dg.asset(
    deps=[AssetKey(["target", "main", "order_items"])],
    key=["target", "main", "order_items_dim"],
    group_name="dims"
)
def dim_order_items() -> pd.DataFrame:
    import src.data_project.transform.order_items as ag
    df = ag.transform()
    func.load_dim(df, "order_items_dim")
    return df


@dg.asset(
    deps=[AssetKey(["target", "main", "orders"])],
    key=["target", "main", "orders_dim"],
    group_name="dims"
)
def dim_orders() -> pd.DataFrame:
    import src.data_project.transform.orders as ag
    df = ag.transform()
    func.load_dim(df, "orders_dim")
    return df


@dg.asset(
    deps=[AssetKey(["target", "main", "order_payments"])],
    key=["target", "main", "order_payments_dim"],
    group_name="dims"
)
def dim_order_payments() -> pd.DataFrame:
    import src.data_project.transform.order_payments as ag
    df = ag.transform()
    func.load_dim(df, "order_payments_dim")
    return df


@dg.asset(
    deps=[AssetKey(["target", "main", "order_reviews"])],
    key=["target", "main", "order_reviews_dim"],
    group_name="dims"
)
def dim_order_reviews() -> None:
    import src.data_project.transform.order_reviews as ag
    df = ag.transform()
    func.load_dim(df, "order_reviews_dim")


@dg.asset(
    deps=[AssetKey(["target", "main", "products"])],
    key=["target", "main", "products_dim"],
    group_name="dims"
)
def dim_products() -> None:
    import src.data_project.transform.products as ag
    df = ag.transform()
    func.load_dim(df, "products_dim")


@dg.asset(
    deps=[AssetKey(["target", "main", "sellers"])],
    key=["target", "main", "sellers_dim"],
    group_name="dims"
)
def dim_sellers() -> None:
    import src.data_project.transform.sellers as ag
    df = ag.transform()
    func.load_dim(df,"sellers_dim")

@dg.asset(
    deps=[AssetKey(["target", "main", "geolocation"])],
    key=["target", "main", "geolocation_dim"],
    group_name="dims"
)
def dim_geolocation():
    import src.data_project.transform.geolocation as geo
    df = geo.transform()
    func.load_dim(df, "geolocation_dim")

@dg.asset(key=AssetKey(["target", "main", "fact_orders"]),
        ins={
         "orders_dim": AssetIn(key=["target", "main", "orders_dim"]),
        "order_items_dim": AssetIn(key=["target", "main", "order_items_dim"]),
        "order_payments_dim": AssetIn(key=["target", "main", "order_payments_dim"]),
        "customers_dim": AssetIn(key=["target", "main", "customers_dim"]),
    },
    group_name="fact",)
def fact_orders(orders_dim: pd.DataFrame, order_items_dim: pd.DataFrame, order_payments_dim: pd.DataFrame, customers_dim: pd.DataFrame) -> pd.DataFrame:
    import src.data_project.transform.fact_orders as fact_orders_module

    df = fact_orders_module.transform(orders_dim, order_items_dim, order_payments_dim, customers_dim)

    postgre.load_fact(df, "fact_orders")

    return df
