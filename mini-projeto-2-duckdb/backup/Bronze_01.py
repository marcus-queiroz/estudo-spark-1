from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
con = duckdb.connect()

con.execute('LOAD azure;')

con.sql("""
    CREATE SECRET azure_spn
    TYPE AZURE,
    PROVIDER SERVICE_PRINCIPAL,
    TENANT_ID '2267ad65-49e0-40a1-83fb-2d3af97de11e',
    CLIENT_ID 'ea72d09e-057a-4f0f-8b24-6240d5ba5ed4',
    CLIENT_SECRET 'lxe80~gqhvwFrnKkwP6PQemA4pE4CuatZVRWGaj5',
    ACCOUNT_NAME 'lakehousecursoch'
);
""")

storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': 'lakehousecursoch',
    'AZURE_STORAGE_ACCESS_KEY': 'xGwP03zmc38YtWgjtsr4G6nEFgJsXvLhEa0l+jYo6suRSJOogJkDbCTE++iX9thaVZOUsd00yEk+AstB3hXmqQ==',
    'AZURE_STORAGE_CLIENT_ID': 'ea72d09e-057a-4f0f-8b24-6240d5ba5ed4',
    'AZURE_STORAGE_CLIENT_SECRET': 'lxe80~gqhvwFrnKkwP6PQemA4pE4CuatZVRWGaj5',
    'AZURE_STORAGE_TENANT_ID': '2267ad65-49e0-40a1-83fb-2d3af97de11e',
}

def escreve_delta(df, tableName, modoEscrita):
    uri = f'az://bronze/vendas/{tableName}'

    write_deltalake(
        uri,
        df,
        mode=modoEscrita,
        storage_options=storage_options
    )

def ler_delta(tableName):
    uri = f'az://bronze/vendas/{tableName}'

    dt = DeltaTable(uri, storage_options=storage_options)
    return dt

arquivos = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores']  # 'order_items', 'orders', 'stocks'

for tabela in arquivos:
    new_df = con.sql(f"""
        SELECT * FROM 'abfss://landing/bike_store/{tabela}.csv'
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
    with dlt_order_items AS (
        select * from order_items
    ), arquivo_items AS (
        select * from 'abfss://landing/bike_store/order_items.csv'
    )
    SELECT AR.* FROM arquivo_order_items AR
    LEFT JOIN dlt_order_items DLT
    ON hash(AR.order_id, AR.item_id, AR.product_id) = hash(DLT.order_id, DLT.item_id, DLT.product_id)
    WHERE DLT.order_id IS NULL
""")
