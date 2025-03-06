"""
Módulo de Preparação de Dados - Camada Landing

Este script é responsável por preparar os dados brutos para processamento subsequente.
Ele realiza as seguintes tarefas:
- Localiza arquivos CSV na pasta de origem (BikeStore)
- Cria diretório de destino na camada landing
- Copia arquivos CSV para o diretório de destino

Fluxo de Processamento:
1. Identifica todos os arquivos CSV na pasta de origem
2. Cria diretório de destino se não existir
3. Copia cada arquivo para o diretório de destino
4. Imprime log de cada arquivo copiado

Variáveis:
- arquivos_bs: Lista de caminhos de arquivos CSV na fonte original
- nome_arquivo: Nome do arquivo sendo processado
- destino: Caminho completo do arquivo no diretório de destino
"""

import shutil
import os
import glob

# Localiza arquivos CSV na fonte original
arquivos_bs = glob.glob('mini-projeto-2-duckdb/data/BikeStore/*.csv')

# Cria diretório de destino, permitindo múltiplas execuções
os.makedirs('mini-projeto-2-duckdb/data/landing/bike_store', exist_ok=True)

# Processa e copia cada arquivo
for arquivo in arquivos_bs:
    nome_arquivo = os.path.basename(arquivo)
    destino = os.path.join('mini-projeto-2-duckdb/data/landing/bike_store', nome_arquivo)
    print(f'Copiando: {arquivo} -> {destino}')
    shutil.copyfile(arquivo, destino)
