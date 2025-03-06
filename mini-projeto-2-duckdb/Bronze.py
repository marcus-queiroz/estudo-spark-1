from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
from pathlib import Path
import os

# Create directories if they don't exist
Path("mini-projeto-2-duckdb/data/landing/bike_store").mkdir(parents=True, exist_ok=True)
Path("mini-projeto-2-duckdb/data/bronze/vendas").mkdir(parents=True, exist_ok=True)

# Check for required CSV files
required_files = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores', 
                 'order_items', 'orders', 'stocks']
for file in required_files:
    csv_path = Path(f"mini-projeto-2-duckdb/data/landing/bike_store/{file}.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo CSV necessário não encontrado: {csv_path}. "
                               "Certifique-se de que os arquivos CSV estão na pasta correta.")

con = duckdb.connect()

def escreve_delta(df, tableName, modoEscrita):
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    write_deltalake(path, df, mode=modoEscrita)

def ler_delta(tableName):
    return DeltaTable(f'mini-projeto-2-duckdb/data/landing/bike_store/{tableName}')

arquivos = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores']  # 'order_items', 'orders', 'stocks'

for tabela in arquivos:
    new_df = con.sql(f"""
        SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/{tabela}.csv'
    """).to_df()

    tabela_dt1 = ler_delta(tabela)

    coluna = ''

    if tabela[-1:] == 'categorie':
        coluna = 'category'
    else:
        coluna = tabela[-1:]
    (
        tabela_dt1.merge(
            source=new_df,
            predicate=f'target.{coluna}_id = source.{coluna}_id',
            source_alias='source',
            target_alias='target'
        ).when_not_matched_insert_all()
        .execute()
    )



order_items = ler_delta('order_items')
order_items = order_items.to_pandas()

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

# Processamento dos pedidos
orders = ler_delta('orders')
orders = orders.to_pandas()

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

# Processamento dos estoques
dados = con.sql("SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/stocks.csv'").to_df()
escreve_delta(dados, 'stocks', 'overwrite')

con.close()
