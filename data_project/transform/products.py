from src.data_project.defs.assets import products
from src.data_project.transform import datawarehouse_load as func
import datetime

def transform():
    # Load dataframe
    df = func.load_df("products_dataset")

        # Remove duplicates
    df = df.drop_duplicates(subset=['product_id'], keep='first')

        # Drop missing values
    df = df.dropna()

        # Drop unnecessary columns
    drop_cols = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

        # Clean category names
    df['product_category_name'] = (
            df['product_category_name']
            .str.replace(r'[@#&$%+\-/*_].*', '', regex=True)
            .str.strip()
            .str.title()
        )

        # SCD Type 2 surrogate key
    df['product_key'] = df.index + 1

        # Tracking columns
    df['valid_from'] = datetime.datetime.now()
    df['valid_to'] = '9999-12-31'
    df['is_active'] = True
    return df
