from src.data_project.transform import datawarehouse_load as func

def transform():
    df_order_reviews = func.load_df("order_reviews_dataset")
    return df_order_reviews