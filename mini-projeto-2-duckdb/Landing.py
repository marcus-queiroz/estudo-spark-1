import shutil
import os
import glob

arquivos_bs = glob.glob('mini-projeto-2-duckdb/data/BikeStore/*.csv')
os.makedirs('mini-projeto-2-duckdb/data/landing/bike_store', exist_ok=True)

for arquivo in arquivos_bs:
    nome_arquivo = os.path.basename(arquivo)
    destino = os.path.join('mini-projeto-2-duckdb/data/landing/bike_store', nome_arquivo)
    print(f'Copiando: {arquivo} -> {destino}')
    shutil.copyfile(arquivo, destino)
