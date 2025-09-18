from datetime import datetime
import pandas as pd
import dagster as dg
from dagster import Output
from src.data_project.transform import datawarehouse_load as func

def transform():
    df = func.load_df("customers_dataset")

    # Drop duplicates dựa trên customer_unique_id
    df = df.drop_duplicates(subset=["customer_unique_id"], keep="first")

    # Chuẩn hoá datatype & format
    df['customer_id'] = df['customer_id'].astype(str)
    df['customer_unique_id'] = df['customer_unique_id'].astype(str)
    df['customer_zip_code_prefix'] = df['customer_zip_code_prefix'].astype(str).str.zfill(5)
    df['customer_city'] = df['customer_city'].astype(str).str.title()
    df['customer_state'] = df['customer_state'].astype(str).str.title()

    # Tạo surrogate key
    df['customer_key'] = df.index + 1

    # Tracking changes (SCD type 2 simple version)
    df['insert_date'] = datetime.now()
    df['is_active'] = True

    # Load vào DuckDB (side effect)
    #func.load_dim(df, "customers_dim")

    return df