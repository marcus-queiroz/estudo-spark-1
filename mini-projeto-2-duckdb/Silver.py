from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    con = duckdb.connect()
    logger.info("Conexão com DuckDB estabelecida com sucesso")

def escreve_delta_silver(df, tableName, modoEscrita):
    path = f'data/silver/vendas/{tableName}'
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta_bronze(tableName):
    return DeltaTable(f'data/bronze/vendas/{tableName}').to_pandas()

def ler_delta_silver(tableName):
    return DeltaTable(f'data/silver/vendas/{tableName}').to_pandas()

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

con.close()
