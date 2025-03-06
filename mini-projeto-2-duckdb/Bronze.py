from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
from pathlib import Path
import os
import pandas as pd

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

def ler_csv(tableName):
    """Lê arquivo CSV da landing zone"""
    path = f'mini-projeto-2-duckdb/data/landing/bike_store/{tableName}.csv'
    if not Path(path).exists():
        raise FileNotFoundError(f"Arquivo CSV não encontrado: {path}")
    return con.sql(f"SELECT * FROM '{path}'").to_df()

def ler_delta(tableName):
    """Lê tabela Delta da bronze zone"""
    path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tableName}'
    if not Path(path).exists():
        # Se não existe, cria tabela vazia
        write_deltalake(path, pd.DataFrame(), mode='append')
    return DeltaTable(path)

def processar_tabela(tabela):
    """Processa uma tabela individual"""
    # Ler dados novos do CSV
    new_df = ler_csv(tabela)
    
    # Ler tabela Delta existente
    delta_table = ler_delta(tabela)
    existing_df = delta_table.to_pandas()
    
    # Se tabela está vazia, apenas insere todos os dados
    if existing_df.empty:
        write_deltalake(delta_table.table_uri, new_df, mode='overwrite')
        return
    
    # Determina coluna chave
    coluna = 'category' if tabela == 'categories' else tabela[:-1]
    
    # Faz merge dos dados
    (
        delta_table.merge(
            source=new_df,
            predicate=f'target.{coluna}_id = source.{coluna}_id',
            source_alias='source',
            target_alias='target'
        ).when_not_matched_insert_all()
        .execute()
    )

# Processa tabelas principais
for tabela in ['brands', 'categories', 'customers', 'products', 'staffs', 'stores']:
    processar_tabela(tabela)



# Processamento especial para order_items
order_items_df = ler_csv('order_items')
existing_order_items = ler_delta('order_items').to_pandas()

if existing_order_items.empty:
    write_deltalake(
        'mini-projeto-2-duckdb/data/bronze/vendas/order_items',
        order_items_df,
        mode='overwrite'
    )
else:
    # Faz merge usando hash para identificar novos itens
    new_items = con.sql(f"""
        SELECT source.* 
        FROM order_items_df AS source
        LEFT JOIN existing_order_items AS target
        ON hash(source.order_id, source.item_id, source.product_id) = 
           hash(target.order_id, target.item_id, target.product_id)
        WHERE target.order_id IS NULL
    """).to_df()
    
    if not new_items.empty:
        write_deltalake(
            'mini-projeto-2-duckdb/data/bronze/vendas/order_items',
            new_items,
            mode='append'
        )

# Processamento especial para orders
orders_df = ler_csv('orders')
existing_orders = ler_delta('orders').to_pandas()

if existing_orders.empty:
    write_deltalake(
        'mini-projeto-2-duckdb/data/bronze/vendas/orders',
        orders_df,
        mode='overwrite'
    )
else:
    # Filtra apenas pedidos mais recentes
    max_date = existing_orders['order_date'].max()
    new_orders = orders_df[orders_df['order_date'] > max_date]
    
    if not new_orders.empty:
        write_deltalake(
            'mini-projeto-2-duckdb/data/bronze/vendas/orders',
            new_orders,
            mode='append'
        )

# Processamento dos estoques (sempre sobrescreve)
stocks_df = ler_csv('stocks')
write_deltalake(
    'mini-projeto-2-duckdb/data/bronze/vendas/stocks',
    stocks_df,
    mode='overwrite'
)

con.close()
