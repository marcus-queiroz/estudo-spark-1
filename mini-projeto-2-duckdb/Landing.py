import shutil
import os
import glob

arquivos_bs = glob.glob('C:\\lakehouse\\BikeStore\\*.csv')
os.makedirs('data/landing/bike_store', exist_ok=True)

for arquivo in arquivos_bs:
    nome_arquivo = os.path.basename(arquivo)
    destino = f'data/landing/bike_store/{nome_arquivo}'
    shutil.copyfile(arquivo, destino)
