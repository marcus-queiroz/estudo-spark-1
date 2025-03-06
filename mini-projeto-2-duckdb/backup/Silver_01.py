from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
con = duckdb.connect()

storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': 'lakehousecursoch',
    'AZURE_STORAGE_ACCESS_KEY': 'xGwP03zmc38YtWgjtsr4G6nEFgJsXvLhEa0l+jYo6suRSJOogJkDbCTE++iX9thaVZOUsd00yEk+AstB3hXmqQ==',
    'AZURE_STORAGE_CLIENT_ID': 'ea72d09e-057a-4f0f-8b24-6240d5ba5ed4',
    'AZURE_STORAGE_CLIENT_SECRET': 'lxe80~gqhvwFrnKkwP6PQemA4pE4CuatZVRWGaj5',
    'AZURE_STORAGE_TENANT_ID': '2267ad65-49e0-40a1-83fb-2d3af97de11e',
}

def escreve_delta_silver(df, tableName, modoEscrita):
    uri = f'az://silver/vendas/{tableName}'

    write_deltalake(
        uri,
        df,
        mode=modoEscrita,
        storage_options=storage_options
    )

def ler_delta_bronze(tableName):
    uri = f'az://bronze/vendas/{tableName}'

    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()

def ler_delta_silver(tableName):
    uri = f'az://silver/vendas/{tableName}'

    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()

brands = ler_delta_bronze('brands')
categories = ler_delta_bronze('categories')
customers = ler_delta_bronze('customers')
order_items = ler_delta_bronze('order_items')
orders = ler_delta_bronze('orders')
products = ler_delta_bronze('products')
staffs = ler_delta_bronze('staffs')
stocks = ler_delta_bronze('stocks')
stores = ler_delta_bronze('stores')

dtl_orders_sales = ler_delta_silver('orders_sales')

orders_sales = con.sql("""
    WITH orders_sales_bronze as
    (
        SELECT
            P.product_id
            ,P.product_name
            ,B.brand_name
            ,CT.category_name
            ,C.customer_id
            ,C.first_name || C.last_name AS customer_name
            ,S.staff_id
            ,S.first_name || S.last_name AS staff_name
            ,ST.store_id
            ,ST.store_name
            ,OI.order_id
            ,OI.item_id
            ,O.order_date
            ,OI.quantity
            ,OI.list_price
            ,OI.discount
        FROM order_items OI
        LEFT JOIN orders O ON OI.order_id = O.order_id
        LEFT JOIN products P ON P.product_id = OI.product_id
        LEFT JOIN brands B ON P.brand_id = B.brand_id
        LEFT JOIN categories CT ON P.category_id = CT.category_id
        LEFT JOIN customers C ON C.customer_id = O.customer_id
        LEFT JOIN staffs S ON S.staff_id = O.staff_id
        LEFT JOIN stores ST ON ST.store_id = O.store_id
    ),
    dt_orders_sales as
    (
        SELECT MAX(order_date) AS order_date FROM dtl_orders_sales
    )
                       
    SELECT * FROM orders_sales_bronze
    WHERE order_date > (SELECT order_date FROM dt_orders_sales)
                       
    """).to_df()

if len(orders_sales) > 0:
    escreve_delta_silver(orders_sales, 'orders_sales', 'append')


stocks_snapshot = con.sql("""
    SELECT *, current_date as dt_stock FROM stocks
    """).to_df()
escreve_delta_silver(stocks_snapshot, 'stocks_snapshot', 'append')

con.close
