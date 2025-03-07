"""
Módulo de Processamento de Dados - Camada Silver

Este script é responsável por transformar e enriquecer dados da camada Bronze.
Realiza as seguintes tarefas:
- Leitura de dados da camada Bronze
- Transformação e junção de múltiplas tabelas
- Criação de visões consolidadas
- Armazenamento de dados processados em Delta Lake

Funções Principais:
- escreve_delta_silver(): Salva dataframes processados
- ler_delta_bronze(): Lê tabelas da camada Bronze
- ler_delta_silver(): Lê tabelas da camada Silver

Fluxo de Processamento:
1. Carrega todas as tabelas da camada Bronze
2. Processa orders_sales com junções e enriquecimento
3. Cria snapshot de estoques com data atual

Estratégias:
- Processamento incremental de orders_sales
- Geração de snapshot de estoques a cada execução
"""
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os

con = duckdb.connect()

# Função para salvar DataFrame como tabela Delta na camada Silver

def escreve_delta_silver(df, tableName, modoEscrita):
    path = f'mini-projeto-2-duckdb/data/silver/vendas/{tableName}'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_deltalake(path, df, mode=modoEscrita)

# Lê tabelas das camadas

def ler_delta_bronze(tableName):
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    return DeltaTable(path).to_pandas()


def ler_delta_silver(tableName):
    path = f'mini-projeto-2-duckdb/data/silver/vendas/{tableName}'
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        return None

# Carregar tabelas Bronze
brands = ler_delta_bronze('brands')
categories = ler_delta_bronze('categories')
customers = ler_delta_bronze('customers')
order_items = ler_delta_bronze('order_items')
orders = ler_delta_bronze('orders')
products = ler_delta_bronze('products')
staffs = ler_delta_bronze('staffs')
stocks = ler_delta_bronze('stocks')
stores = ler_delta_bronze('stores')

# Registrar tabelas no DuckDB
con.register('brands', brands)
con.register('categories', categories)
con.register('customers', customers)
con.register('order_items', order_items)
con.register('orders', orders)
con.register('products', products)
con.register('staffs', staffs)
con.register('stores', stores)

# Carrega dados existentes de orders_sales na Silver
dtl_orders_sales = ler_delta_silver('orders_sales')

if dtl_orders_sales is not None:
    con.register('dtl_orders_sales', dtl_orders_sales)
    # Obtém a última data processada
    ultima_data = con.execute("SELECT MAX(order_date) as last_date FROM dtl_orders_sales").fetchone()[0]

    # Incremental baseado na última data
    orders_sales_incremental = con.sql(f"""
        SELECT
            P.product_id,
            P.product_name,
            B.brand_name,
            CT.category_name,
            C.customer_id,
            C.first_name || ' ' || C.last_name AS customer_name,
            ST.staff_id,
            ST.first_name || ' ' || ST.last_name AS staff_name,
            S.store_id,
            S.store_name,
            OI.order_id,
            OI.item_id,
            O.order_date,
            OI.quantity,
            OI.list_price,
            OI.discount
        FROM order_items OI
        LEFT JOIN orders O ON OI.order_id = O.order_id
        LEFT JOIN products P ON P.product_id = OI.product_id
        LEFT JOIN brands B ON P.brand_id = B.brand_id
        LEFT JOIN categories CT ON P.category_id = CT.category_id
        LEFT JOIN customers C ON C.customer_id = O.customer_id
        LEFT JOIN staffs ST ON ST.staff_id = O.staff_id
        LEFT JOIN stores S ON S.store_id = O.store_id
        WHERE O.order_date > (SELECT MAX(order_date) FROM dtl_orders_sales)
    """).to_df()

    if not orders_sales_incremental.empty:
        escreve_delta_silver(orders_sales_incremental, 'orders_sales', 'append')
    else:
        print("Nenhum dado novo encontrado para orders_sales.")
else:
    # Primeira execução - carga completa
    orders_sales = con.sql("""
        SELECT
            P.product_id,
            P.product_name,
            B.brand_name,
            CT.category_name,
            C.customer_id,
            C.first_name || ' ' || C.last_name AS customer_name,
            ST.staff_id,
            ST.first_name || ' ' || ST.last_name AS staff_name,
            S.store_id,
            S.store_name,
            OI.order_id,
            OI.item_id,
            O.order_date,
            OI.quantity,
            OI.list_price,
            OI.discount
        FROM order_items OI
        LEFT JOIN orders O ON OI.order_id = O.order_id
        LEFT JOIN products P ON P.product_id = OI.product_id
        LEFT JOIN brands B ON P.brand_id = B.brand_id
        LEFT JOIN categories CT ON P.category_id = CT.category_id
        LEFT JOIN customers C ON C.customer_id = O.customer_id
        LEFT JOIN staffs ST ON ST.staff_id = O.staff_id
        LEFT JOIN stores S ON S.store_id = O.store_id
    """).to_df()

    escreve_delta_silver(orders_sales, 'orders_sales', 'overwrite')

# Geração diária do snapshot de estoques
stocks_snapshot = con.sql("""
    SELECT *, current_date as dt_stock FROM stocks
""").to_df()

escreve_delta_silver(stocks_snapshot, 'stocks_snapshot', 'append')

con.close()
