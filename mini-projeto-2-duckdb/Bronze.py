"""
Módulo de Processamento de Dados - Camada Bronze

Este script é responsável por processar e armazenar dados brutos em formato Delta Lake.
Realiza as seguintes tarefas:
- Leitura de dados CSV da camada landing
- Criação/atualização de tabelas Delta na camada bronze
- Processamento incremental de dados

Funções Principais:
- escreve_delta(): Salva dataframes como tabelas Delta
- ler_delta(): Lê tabelas Delta existentes

Fluxo de Processamento:
1. Processa tabelas estáticas (brands, categories, etc.)
2. Processa order_items com lógica incremental
3. Processa orders com filtro de data
4. Sobrescreve dados de stocks

Estratégias:
- Primeira execução: Cria tabelas
- Execuções subsequentes: Adiciona apenas novos registros
"""

from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os

con = duckdb.connect()

def escreve_delta(df, tableName, modoEscrita):
    """
    Salva um DataFrame como tabela Delta Lake.
    
    Args:
        df (DataFrame): Dados a serem salvos
        tableName (str): Nome da tabela
        modoEscrita (str): Modo de escrita ('overwrite' ou 'append')
    """
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta(tableName):
    """
    Lê uma tabela Delta Lake existente.
    
    Args:
        tableName (str): Nome da tabela
    
    Returns:
        DeltaTable ou None se a tabela não existir
    """
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    try:
        return DeltaTable(path)
    except Exception:
        return None

# Lista de tabelas para processamento
arquivos = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores', 'order_items', 'orders', 'stocks']

# Processamento de tabelas estáticas e dimensionais
for tabela in arquivos:
    new_df = con.sql(f"""
        SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/{tabela}.csv'
    """).to_df()

    tabela_dt1 = ler_delta(tabela)
    
    if tabela_dt1 is None:
        # Se a tabela não existir, cria pela primeira vez
        escreve_delta(new_df, tabela, 'overwrite')
    else:
        # Lógica de merge existente
        coluna = 'category' if tabela == 'categories' else tabela[:-1]
        (
            tabela_dt1.merge(
                source=new_df,
                predicate=f'target.{coluna}_id = source.{coluna}_id',
                source_alias='source',
                target_alias='target'
            ).when_not_matched_insert_all()
            .execute()
        )

# Processamento de order_items com lógica incremental
order_items = ler_delta('order_items')
order_items = order_items.to_pandas() if order_items is not None else None

df = con.sql("""
            with dlt_order_items AS 
            (
                select * from order_items
            ), 
            arquivo_items AS 
            (
                select * from 'mini-projeto-2-duckdb/data/landing/bike_store/order_items.csv'
            )
            SELECT AR.* FROM arquivo_items AR
            LEFT JOIN dlt_order_items DLT
            ON hash(AR.order_id, AR.item_id, AR.product_id) = hash(DLT.order_id, DLT.item_id, DLT.product_id)
            WHERE DLT.order_id IS NULL
            """)

if len(df) > 0:
    escreve_delta(df, 'order_items', 'append')

# Processamento de orders com filtro de data
orders = ler_delta('orders')
orders = orders.to_pandas() if orders is not None else None

df = con.sql("""
            WITH arquivo_orders AS
            (
                SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/orders.csv'
            ),
            dtl_orders AS
            (
                SELECT MAX(order_date) AS order_date FROM orders
            )

            SELECT ar.* FROM arquivo_orders ar
            WHERE ar.order_date > (SELECT order_date FROM dtl_orders)
            """)

if len(df) > 0:
    escreve_delta(df, 'orders', 'append')

# Processamento dos estoques (sempre sobrescreve)
dados = con.sql("SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/stocks.csv'").to_df()
escreve_delta(dados, 'stocks', 'overwrite')

con.close()
