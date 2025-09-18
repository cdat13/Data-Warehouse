import psycopg2
from pandas import DataFrame
from sqlalchemy import create_engine, text


engine = create_engine("postgresql+psycopg2://postgres:123@localhost:5432/postgres")

def load_fact(df:DataFrame, table_name: str):
     df.to_sql(name="fact_orders", con = engine
                    , if_exists="replace", index=False)

def print_fact(table_name:str):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))
        return result