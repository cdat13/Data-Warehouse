from src.data_project.transform import datawarehouse_load as func

def transform():
    #load dataframe
    df_order_items = func.load_df("order_items_dataset")

    # transform n clean
    df_order_items["freight_value"] = df_order_items["freight_value"].apply(lambda x: x > 0)

    #load dim
    func.load_dim(df_order_items, "order_items_dim")

    # Just to check if dim table is create or not
    #func.print_df("order_items_dim")
    return df_order_items