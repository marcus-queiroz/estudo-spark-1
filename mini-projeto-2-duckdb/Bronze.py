from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
from pathlib import Path
import os
import pandas as pd
import logging

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Cria os diretórios necessários"""
    Path("mini-projeto-2-duckdb/data/landing/bike_store").mkdir(parents=True, exist_ok=True)
    Path("mini-projeto-2-duckdb/data/bronze/vendas").mkdir(parents=True, exist_ok=True)

def verificar_arquivos():
    """Verifica se todos os arquivos CSV necessários existem"""
    required_files = ['brands', 'categories', 'customers', 'products', 
                     'staffs', 'stores', 'order_items', 'orders', 'stocks']
    
    for file in required_files:
        csv_path = Path(f"mini-projeto-2-duckdb/data/landing/bike_store/{file}.csv")
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Arquivo CSV necessário não encontrado: {csv_path}\n"
                "Certifique-se de que os arquivos CSV estão na pasta correta."
            )
    logger.info("Todos os arquivos CSV necessários foram encontrados")

def processar_tabela_simples(tabela, con):
    """Processa tabelas com lógica simples de merge"""
    try:
        logger.info(f"Processando tabela: {tabela}")
        
        # Ler CSV
        df_csv = con.sql(f"""
            SELECT * FROM 'mini-projeto-2-duckdb/data/landing/bike_store/{tabela}.csv'
        """).to_df()
        
        # Determinar coluna chave
        coluna = 'category' if tabela == 'categories' else tabela[:-1]
        
        # Caminho da tabela Delta
        delta_path = f'mini-projeto-2-duckdb/data/bronze/vendas/{tabela}'
        
        # Se a tabela Delta não existe, cria com os dados iniciais
        if not Path(delta_path).exists():
            write_deltalake(delta_path, df_csv, mode='overwrite')
            logger.info(f"Tabela {tabela} criada com sucesso")
            return
        
        # Se já existe, faz o merge
        delta_table = DeltaTable(delta_path)
        (
            delta_table.merge(
                source=df_csv,
                predicate=f'target.{coluna}_id = source.{coluna}_id',
                source_alias='source',
                target_alias='target'
            )
            .when_not_matched_insert_all()
            .execute()
        )
        logger.info(f"Tabela {tabela} atualizada com sucesso")
        
    except Exception as e:
        logger.error(f"Erro ao processar tabela {tabela}: {str(e)}")
        raise

def main():
    """Função principal do script"""
    try:
        setup_directories()
        verificar_arquivos()
        
        con = duckdb.connect()
        
        # Processar tabelas principais
        tabelas_principais = ['brands', 'categories', 'customers', 
                             'products', 'staffs', 'stores']
        
        for tabela in tabelas_principais:
            processar_tabela_simples(tabela, con)
            
        con.close()
        logger.info("Processamento concluído com sucesso")
        
    except Exception as e:
        logger.error(f"Erro durante a execução: {str(e)}")
        raise

if __name__ == "__main__":
    main()



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
