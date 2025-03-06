from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os

con = duckdb.connect()

def escreve_delta(df, tableName, modoEscrita):
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta(tableName):
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    try:
        return DeltaTable(path)
    except Exception:
        return None

arquivos = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores', 'order_items', 'orders', 'stocks']

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

# Processamento de order_items
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

# Processamento de orders
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
