import pandas as pd
from src.data_project.transform import datawarehouse_load as func

def transform(orders_dim, order_items_dim, order_payments_dim, customers_dim):
    df_orders = orders_dim
    df_order_items = order_items_dim
    df_order_payment = order_payments_dim
    df_customers = customers_dim

    df = pd.merge(df_orders, df_order_items, how="left", on="order_id")
    df = pd.merge(df, df_customers, how="left", on="customer_id")
    df = pd.merge(df, df_order_payment, how="left", on="order_id")

    df['order_status'] = df['order_status'].str.lower()
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])

    df['total'] = df['price'] + df['freight_value']
    df['delivery_time'] = (df['order_delivered_customer_date'] - df['order_purchase_timestamp']).dt.total_seconds()
    df['estimated_time'] = (df['order_estimated_delivery_date'] - df['order_purchase_timestamp']).dt.total_seconds()

    df['customer_key'] = df['customer_id']
    df['product_key'] = df['product_id']
    df['seller_key'] = df['seller_id']

    if 'customer_zip_code_prefix' in df.columns:
        df['geolocation_key'] = df['customer_zip_code_prefix']
    else:
        print("Cột customer_zip_code_prefix không tồn tại. Sử dụng giá trị mặc định.")
        df['geolocation_key'] = "unknown"

    df['payment_key'] = df['payment_type'].astype('category').cat.codes + 1
    df['order_date_key'] = df['order_purchase_timestamp'].dt.date

    fact_columns = ['order_id', 'customer_key', 'product_key', 'seller_key', 'geolocation_key', 'payment_key', 'order_date_key',
                    'order_status', 'price', 'freight_value', 'total', 'payment_value',
                    'delivery_time', 'estimated_time']

    df = df[fact_columns]
    return df

#df_fact_orders = transform_fact_orders()

#load_fact(df_fact_orders, "fact_orders")

#print(print_fact("fact_orders"))
