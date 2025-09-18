from src.data_project.transform import datawarehouse_load as func


def transform():
    df_orders = func.load_df("orders_dataset")
## Del invoiced, cancelled, shipped
    df_orders = df_orders[df_orders["order_status"] == "delivered"]
    return df_orders
## Load into Dim Table
#dwh.load_dim(df_orders, 'orders_dim' )

## Just to check if dim table is create or not
#print_df("orders_dim")

