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

con = duckdb.connect()

def escreve_delta_silver(df, tableName, modoEscrita):
    """
    Salva um DataFrame como tabela Delta Lake na camada Silver.
    
    Args:
        df (DataFrame): Dados a serem salvos
        tableName (str): Nome da tabela
        modoEscrita (str): Modo de escrita ('overwrite' ou 'append')
    """
    path = f'data/silver/vendas/{tableName}'
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta_bronze(tableName):
    """
    Lê uma tabela da camada Bronze.
    
    Args:
        tableName (str): Nome da tabela
    
    Returns:
        DataFrame com dados da tabela Bronze
    """
    try:
        return DeltaTable(f'data/bronze/vendas/{tableName}').to_pandas()
    except Exception:
        # Retorna DataFrame vazio com schema básico se tabela não existir
        if tableName == 'brands':
            return con.sql("SELECT NULL::INTEGER AS brand_id, NULL::VARCHAR AS brand_name LIMIT 0").to_df()
        elif tableName == 'categories':
            return con.sql("SELECT NULL::INTEGER AS category_id, NULL::VARCHAR AS category_name LIMIT 0").to_df()
        elif tableName == 'customers':
            return con.sql("""
                SELECT NULL::INTEGER AS customer_id, NULL::VARCHAR AS first_name, 
                       NULL::VARCHAR AS last_name, NULL::VARCHAR AS phone, NULL::VARCHAR AS email,
                       NULL::VARCHAR AS street, NULL::VARCHAR AS city, NULL::VARCHAR AS state,
                       NULL::VARCHAR AS zip_code LIMIT 0
            """).to_df()
        elif tableName == 'order_items':
            return con.sql("""
                SELECT NULL::INTEGER AS order_id, NULL::INTEGER AS item_id, NULL::INTEGER AS product_id,
                       NULL::INTEGER AS quantity, NULL::DECIMAL(10,2) AS list_price, 
                       NULL::DECIMAL(4,2) AS discount LIMIT 0
            """).to_df()
        elif tableName == 'orders':
            return con.sql("""
                SELECT NULL::INTEGER AS order_id, NULL::INTEGER AS customer_id, NULL::INTEGER AS order_status,
                       NULL::DATE AS order_date, NULL::DATE AS required_date, NULL::DATE AS shipped_date,
                       NULL::INTEGER AS store_id, NULL::INTEGER AS staff_id LIMIT 0
            """).to_df()
        elif tableName == 'products':
            return con.sql("""
                SELECT NULL::INTEGER AS product_id, NULL::VARCHAR AS product_name, NULL::INTEGER AS brand_id,
                       NULL::INTEGER AS category_id, NULL::INTEGER AS model_year, NULL::DECIMAL(10,2) AS list_price LIMIT 0
            """).to_df()
        elif tableName == 'staffs':
            return con.sql("""
                SELECT NULL::INTEGER AS staff_id, NULL::VARCHAR AS first_name, NULL::VARCHAR AS last_name,
                       NULL::VARCHAR AS email, NULL::VARCHAR AS phone, NULL::INTEGER AS active,
                       NULL::INTEGER AS store_id, NULL::INTEGER AS manager_id LIMIT 0
            """).to_df()
        elif tableName == 'stocks':
            return con.sql("""
                SELECT NULL::INTEGER AS store_id, NULL::INTEGER AS product_id, NULL::INTEGER AS quantity LIMIT 0
            """).to_df()
        elif tableName == 'stores':
            return con.sql("""
                SELECT NULL::INTEGER AS store_id, NULL::VARCHAR AS store_name, NULL::VARCHAR AS phone,
                       NULL::VARCHAR AS email, NULL::VARCHAR AS street, NULL::VARCHAR AS city,
                       NULL::VARCHAR AS state, NULL::VARCHAR AS zip_code LIMIT 0
            """).to_df()
        else:
            return con.sql("SELECT NULL LIMIT 0").to_df()

def ler_delta_silver(tableName):
    """
    Lê uma tabela da camada Silver.
    
    Args:
        tableName (str): Nome da tabela
    
    Returns:
        DataFrame com dados da tabela Silver
    """
    try:
        return DeltaTable(f'data/silver/vendas/{tableName}').to_pandas()
    except Exception:
        # Retorna DataFrame vazio se tabela não existir
        return con.sql("SELECT NULL LIMIT 0").to_df()

# Carrega todas as tabelas da camada Bronze
brands = ler_delta_bronze('brands')
categories = ler_delta_bronze('categories')
customers = ler_delta_bronze('customers')
order_items = ler_delta_bronze('order_items')
orders = ler_delta_bronze('orders')
products = ler_delta_bronze('products')
staffs = ler_delta_bronze('staffs')
stocks = ler_delta_bronze('stocks')
stores = ler_delta_bronze('stores')

# Recupera dados existentes de orders_sales na camada Silver
dtl_orders_sales = ler_delta_silver('orders_sales')

# Processa orders_sales com junções e enriquecimento
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

# Salva orders_sales incrementalmente
if len(orders_sales) > 0:
    escreve_delta_silver(orders_sales, 'orders_sales', 'append')

# Cria snapshot de estoques com data atual
stocks_snapshot = con.sql("""
    SELECT *, current_date as dt_stock FROM stocks
    """).to_df()
escreve_delta_silver(stocks_snapshot, 'stocks_snapshot', 'append')

con.close()


