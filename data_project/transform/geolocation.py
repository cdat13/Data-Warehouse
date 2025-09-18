from src.data_project.transform import datawarehouse_load as func

def transform():
    # Load dataframe
    df_geo = func.load_df("geolocation_dataset")

    # Transform and clean
    df_geo["geolocation_zip_code_prefix"] = (
        df_geo["geolocation_zip_code_prefix"].astype(str).str.zfill(5)
    )
    df_geo["geolocation_city"] = df_geo["geolocation_city"].astype(str).str.title()
    df_geo["geolocation_state"] = df_geo["geolocation_state"].astype(str).str.upper()

    # Create surrogate key for better join
    df_geo = df_geo.reset_index(drop=True)
    df_geo["geolocation_key"] = df_geo.index + 1
    return df_geo