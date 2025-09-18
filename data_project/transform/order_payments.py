from src.data_project.transform import datawarehouse_load as func


def transform():
#load dataframe
    df_order_payments = func.load_df("order_payments_dataset")

#load dim
    #func.load_dim(df_order_payments, "order_payments_dim")

## Just to check if dim table is create or not
#func.print_df("order_payments_dim")
    return df_order_payments