from azure.storage.filedatalake import DataLakeServiceClient

service_client = DataLakeServiceClient(account_url='https://lakehousecursoch.dfs.core.windows.net', credential='xGwP03zmc38YtWgjtsr4G6nEFgJsXv')

container = service_client.get_file_system_client(file_system="landing")

diretorio_client = container.get_directory_client("bike_store")

import glob

arquivos_bs = glob.glob('C:\\lakehouse\\BikeStore\\*.csv')
for items in arquivos_bs:
    caminho_completo = items.replace('\\', '\\\\')

    lista_item = items.split('\\')
    ultimo_item = lista_item[-1]

    file_client = diretorio_client.create_file(f'{ultimo_item}')

    arquivo_local = open(f'{caminho_completo}', 'r')
    conteudoArquivo = arquivo_local.read()

    file_client.append_data(data=conteudoArquivo, offset=0, length=len(conteudoArquivo))
    file_client.flush_data(len(conteudoArquivo))
