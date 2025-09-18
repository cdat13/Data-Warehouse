from src.data_project.transform import datawarehouse_load as func

def transform():
    df_sellers = func.load_df("sellers_dataset")
    df_sellers['seller_city'] = (df_sellers['seller_city']
                                 .str.replace(r'[@#&$%+\-/*].*', '', regex=True)
                                 .str.strip()
                                 .str.upper())
    df_sellers['seller_zip_code_prefix'] = df_sellers['seller_zip_code_prefix'].str.zfill(5)
    df_sellers['seller_state'] = df_sellers['seller_state'].str.title()
    return df_sellers

