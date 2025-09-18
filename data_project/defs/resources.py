import dagster as dg
from dagster_duckdb import DuckDBResource

database_resource = DuckDBResource(database=r"C:\Users\ASUS\data-project\duckdbfiles\orders.duckdb")
#datawarehouse_resource = DuckDBResource(database=r"C:\Users\ASUS\data-project\datawarehouse\data_warehouse.duckdb")

@dg.definitions
def resources():
    return dg.Definitions(resources={
        "duckdb_resources" : database_resource,
    })

