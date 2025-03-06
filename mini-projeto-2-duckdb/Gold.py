from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
con = duckdb.connect()

def escreve_delta_gold(df, tableName, modoEscrita):
    path = f'data/gold/vendas/{tableName}'
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta_gold(tableName):
    return DeltaTable(f'data/gold/vendas/{tableName}').to_pandas()

def ler_delta_silver(tableName):
    return DeltaTable(f'data/silver/vendas/{tableName}').to_pandas()

orders_sales = ler_delta_silver('orders_sales')

dtl_gold_products = ler_delta_gold('dim_products')

dim_products = con.sql("""
    WITH products as
    (
        SELECT DISTINCT
            product_id
            ,product_name
            ,brand_name
            ,category_name
        FROM orders_sales
    ),
    sk_products as
    (
        SELECT
            row_number() OVER (ORDER BY product_id) as product_sk
            ,product_id
            ,product_name
            ,brand_name
            ,category_name
        FROM products
    ),
    dt_gold_products
    (
        SELECT DISTINCT product_id FROM dtl_gold_products
    )

    SELECT * FROM sk_products
    WHERE product_id NOT IN (SELECT product_id FROM dt_gold_products)
""").to_df()


escreve_delta_gold(dim_products, 'dim_products', 'append')

dtl_gold_customers = ler_delta_gold('dim_customers')


dim_customers = con.sql("""
    WITH customers as
    (
        SELECT DISTINCT
            customer_id
            ,customer_name
        FROM orders_sales
    ),
    sk_customers as
    (
        SELECT
            row_number() OVER (ORDER BY customer_id) as customer_sk
            ,*
        FROM customers
    ),
    dt_gold_customers as
    (
        SELECT DISTINCT customer_id FROM dtl_gold_customers
    )

    SELECT * FROM sk_customers
    WHERE customer_id NOT IN (SELECT customer_id FROM dt_gold_customers)
""").to_df()

#dim_customers
escreve_delta_gold(dim_customers, 'dim_customers', 'append')

dtl_gold_staffs = ler_delta_gold('dim_staffs')

dim_staffs = con.sql("""
    WITH staffs as
    (
        SELECT DISTINCT
            staff_id
            ,staff_name
        FROM orders_sales
    ),
    sk_staffs as
    (
        SELECT
            row_number() OVER (ORDER BY staff_id) as staff_sk
            ,*
        FROM staffs
    ),
    dt_gold_staffs as
    (
        SELECT DISTINCT staff_id FROM dtl_gold_staffs
    )

    SELECT * FROM sk_staffs
    WHERE staff_id NOT IN (SELECT staff_id FROM dt_gold_staffs)
""").to_df()

#dim_staffs
escreve_delta_gold(dim_staffs, 'dim_staffs', 'append')

dtl_gold_stores = ler_delta_gold('dim_stores')

dim_stores = con.sql("""
    WITH stores as
    (
        SELECT DISTINCT
            store_id
            ,store_name
        FROM orders_sales
    ),
    sk_stores as
    (
        SELECT
            row_number() OVER (ORDER BY store_id) as store_sk
            ,*
        FROM stores
    ),
    dt_gold_stores as
    (
        SELECT DISTINCT store_id FROM dtl_gold_stores
    )

    SELECT * FROM sk_stores
    WHERE store_id NOT IN (SELECT store_id FROM dt_gold_stores)
""").to_df()

escreve_delta_gold(dim_stores, 'dim_stores', 'append')

dim_date = con.sql("""
    WITH tempo as
    (
        SELECT
            cast(strftime(generate_series, '%Y%m%d') as int) as date_id
            ,generate_series as date
            ,year(generate_series) as year
            ,month(generate_series) as month
            ,day(generate_series) as day
        FROM generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL '1 DAY')
    ),
    sk_date as
    (
        SELECT
            row_number() OVER (ORDER BY date_id) as date_sk
            ,*
        FROM tempo
    )

    SELECT * FROM sk_date
""").to_df()

escreve_delta_gold(dim_date, 'dim_date', 'append')

dim_products = ler_delta_gold('dim_products')
dim_customers = ler_delta_gold('dim_customers')
dim_staffs = ler_delta_gold('dim_staffs')
dim_stores = ler_delta_gold('dim_stores')
dim_date = ler_delta_gold('dim_date')

dtl_gold_fact_sales = ler_delta_gold('fact_sales')

fact_sales = con.sql("""
    SELECT
        P.product_sk
        ,C.customer_sk
        ,ST.staff_sk
        ,S.store_sk
        ,D.date_sk
        ,OS.item_id
        ,OS.order_id
        ,OS.order_date
        ,OS.quantity
        ,OS.list_price
        ,OS.discount
    FROM orders_sales OS
    LEFT JOIN dim_products P ON OS.product_id = P.product_id
    LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
    LEFT JOIN dim_staffs S ON OS.staff_id = S.staff_id
    LEFT JOIN dim_stores S ON OS.store_id = S.store_id
    LEFT JOIN dim_date D ON cast(strftime(OS.order_date, '%Y%m%d') as int) = D.date_id
    WHERE OS.order_date >
    (SELECT MAX(order_date) FROM dtl_gold_fact_sales)
""").to_df()

escreve_delta_gold(fact_sales, 'fact_sales', 'append')

stocks_snapshot = ler_delta_silver('stocks_snapshot')

dtl_gold_fact_stocks = ler_delta_gold('fact_stocks')

fact_stocks = con.sql("""
    SELECT
        P.product_sk
        ,S.store_sk
        ,D.date_sk
        ,SS.quantity
    FROM stocks_snapshot SS
    LEFT JOIN dim_products P ON SS.product_id = P.product_id
    LEFT JOIN dim_stores S ON SS.store_id = S.store_id
    LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id
    WHERE SS.dt_stock >
    (
        SELECT
            MAX(D.date)
        FROM dtl_gold_fact_stocks FS
        LEFT JOIN dim_date D ON FS.date_SK = D.date_SK
    )
""").to_df()

escreve_delta_gold(fact_stocks, 'fact_stocks', 'append')

con.close()
logger.info("Processo concluído com sucesso")






