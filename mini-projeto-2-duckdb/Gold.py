"""
Módulo de Processamento de Dados - Camada Gold

Este script é responsável por criar dimensões e fatos para um data warehouse.
Realiza as seguintes tarefas:
- Leitura de dados da camada Silver
- Criação de dimensões (produtos, clientes, funcionários, lojas, datas)
- Geração de fatos de vendas e estoque
- Armazenamento de dados processados em Delta Lake

Funções Principais:
- escreve_delta_gold(): Salva dataframes processados
- ler_delta_gold(): Lê tabelas da camada Gold
- ler_delta_silver(): Lê tabelas da camada Silver

Fluxo de Processamento:
1. Carrega dados de orders_sales da camada Silver
2. Cria dimensões com surrogate keys
3. Gera fatos de vendas e estoque
4. Adiciona novos registros incrementalmente

Estratégias:
- Processamento incremental de dimensões
- Geração de surrogate keys
- Junção de dados entre dimensões e fatos
"""
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os

# Funções utilitárias

def escreve_delta_gold(df, tableName, modoEscrita):
    path = f'mini-projeto-2-duckdb/data/gold/vendas/{tableName}'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta_gold(tableName):
    path = f'mini-projeto-2-duckdb/data/gold/vendas/{tableName}'
    try:
        return DeltaTable(path).to_pandas()
    except:
        return None

def ler_delta_silver(tableName):
    path = f'mini-projeto-2-duckdb/data/silver/vendas/{tableName}'
    return DeltaTable(path).to_pandas()

# Conexão DuckDB
con = duckdb.connect()

# Carrega orders_sales da Silver
orders_sales = ler_delta_silver('orders_sales')
con.register('orders_sales', orders_sales)

# Criação explícita das dimensões com surrogate keys

# Dimensão Produtos
dim_products = con.sql("""
    WITH produtos AS (
        SELECT DISTINCT
            product_id,
            product_name,
            brand_name,
            category_name
        FROM orders_sales
    ),
    sk_produtos AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
            *
        FROM produtos
    )
    SELECT * FROM sk_produtos
""").to_df()

escreve_delta_gold(dim_products, 'dim_products', 'overwrite')

# Dimensão Clientes
dim_customers = con.sql("""
    WITH clientes AS (
        SELECT DISTINCT
            customer_id,
            customer_name
        FROM orders_sales
    ),
    sk_clientes AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_sk,
            *
        FROM clientes
    )
    SELECT * FROM sk_clientes
""").to_df()

escreve_delta_gold(dim_customers, 'dim_customers', 'overwrite')

# Dimensão Funcionários
dim_staffs = con.sql("""
    WITH staffs AS (
        SELECT DISTINCT
            staff_id,
            staff_name
        FROM orders_sales
    ),
    sk_staffs AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY staff_id) AS staff_sk,
            *
        FROM staffs
    )
    SELECT * FROM sk_staffs
""").to_df()

escreve_delta_gold(dim_staffs, 'dim_staffs', 'overwrite')

# Dimensão Lojas
dim_stores = con.sql("""
    WITH stores AS (
        SELECT DISTINCT
            store_id,
            store_name
        FROM orders_sales
    ),
    sk_stores AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY store_id) AS store_sk,
            *
        FROM stores
    )
    SELECT * FROM sk_stores
""").to_df()

escreve_delta_gold(dim_stores, 'dim_stores', 'overwrite')

# Processa Dimensão Data
dim_date = con.sql("""
    WITH tempo AS (
        SELECT
            CAST(strftime(generate_series, '%Y%m%d') AS INTEGER) AS date_id,
            generate_series AS date,
            year(generate_series) AS year,
            month(generate_series) AS month,
            day(generate_series) AS day
        FROM generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL '1 DAY')
    )
    SELECT ROW_NUMBER() OVER (ORDER BY date_id) AS date_sk, * FROM tempo
""").to_df()

escreve_delta_gold(dim_date, 'dim_date', 'overwrite')

# Registrar dimensões
con.register('dim_products', dim_products)
con.register('dim_customers', dim_customers)
con.register('dim_staffs', dim_staffs)
con.register('dim_stores', dim_stores)
con.register('dim_date', dim_date)

# Criação do fato vendas
fact_sales = con.sql("""
    SELECT
        P.product_sk,
        C.customer_sk,
        ST.staff_sk,
        S.store_sk,
        D.date_sk,
        OS.item_id,
        OS.order_id,
        OS.order_date,
        OS.quantity,
        OS.list_price,
        OS.discount
    FROM orders_sales OS
    LEFT JOIN dim_products P ON OS.product_id = P.product_id
    LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
    LEFT JOIN dim_staffs ST ON OS.staff_id = ST.staff_id
    LEFT JOIN dim_stores S ON OS.store_id = S.store_id
    LEFT JOIN dim_date D ON CAST(strftime(OS.order_date, '%Y%m%d') AS INT) = D.date_id
""").to_df()

escreve_delta_gold(fact_sales, 'fact_sales', 'append')

# Fecha conexão
con.close()