import shutil
import os
import glob

arquivos_bs = glob.glob('mini-projeto-2-duckdb\\data\\BikeStore\\*.csv')
os.makedirs('mini-projeto-2-duckdb/data/landing/', exist_ok=True)

for arquivo in arquivos_bs:
    nome_arquivo = os.path.basename(arquivo)
    destino = f'mini-projeto-2-duckdb/data/landing/{nome_arquivo}'
    shutil.copyfile(arquivo, destino)
