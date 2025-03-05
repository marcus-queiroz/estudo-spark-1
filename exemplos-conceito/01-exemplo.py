import json
import os

# Definir diretório de dados
data_dir = "exemplos-conceito/01-data"
os.makedirs(data_dir, exist_ok=True)

# 1. Criar dados iniciais e salvar como JSON inicial
dados_iniciais = {
    "pessoa": {
        "nome": "Carlos",
        "idade": 30,
        "cidade": "Rio de Janeiro",
        "habilidades": ["Python", "SQL", "JavaScript"]
    }
}

# Função para salvar um dicionário em um arquivo JSON
def salvar_json(dados, arquivo):
    caminho_completo = os.path.join(data_dir, arquivo)
    with open(caminho_completo, 'w') as file:
        json.dump(dados, file, indent=4)

# Função para carregar um arquivo JSON para um dicionário
def carregar_json(arquivo):
    caminho_completo = os.path.join(data_dir, arquivo)
    with open(caminho_completo, 'r') as file:
        return json.load(file)

# Função para exibir os dados JSON formatados no console
def exibir_dados(dados):
    print(json.dumps(dados, indent=4))

# Função para acessar e exibir informações específicas do JSON
def acessar_dados(dados):
    nome = dados.get("pessoa", {}).get("nome", "N/A")
    habilidades = dados.get("pessoa", {}).get("habilidades", [])
    print(f'Nome: {nome}')
    print(f'Habilidades: {", ".join(habilidades)}')

# Função para atualizar informações dentro do JSON
def atualizar_dados(dados):
    if "pessoa" in dados:
        dados["pessoa"]["idade"] += 1  # Incrementa a idade em 1 ano
        dados["pessoa"]["cidade"] = "São Paulo"  # Atualiza a cidade

# Função para adicionar e remover elementos de listas dentro do JSON
def manipular_listas(dados):
    if "pessoa" in dados:
        dados['pessoa']['habilidades'].append('C#')  # Adiciona nova habilidade
        if 'SQL' in dados['pessoa']['habilidades']:
            dados['pessoa']['habilidades'].remove('SQL')  # Remove uma habilidade específica

# Função para validar a estrutura de um JSON recebido como string
def validar_json(json_str):
    try:
        dados = json.loads(json_str)
        print("JSON válido")
        return dados
    except json.JSONDecodeError as e:
        print(f"Erro no JSON: {e}")
        return None

# Função para combinar dois arquivos JSON em um único dicionário
def combinar_json(arquivo1, arquivo2):
    dados1 = carregar_json(arquivo1)
    dados2 = carregar_json(arquivo2)
    return {**dados1, **dados2}  # Mescla os dicionários



# Criar e salvar um JSON inicial
salvar_json(dados_iniciais, 'dados.json')

# Carregar o JSON salvo
dados = carregar_json('dados.json')

# Exibir os dados iniciais
print("Dados Iniciais:")
exibir_dados(dados)

# Acessar dados específicos do JSON
acessar_dados(dados)

# Atualizar os dados no JSON
atualizar_dados(dados)
manipular_listas(dados)

# Exibir os dados após as modificações
print("\nDados Após Modificações:")
exibir_dados(dados)

# Salvar os dados modificados em um novo arquivo
salvar_json(dados, 'dados_modificados.json')

# Validar um JSON de exemplo
json_str = '{"nome": "Rich", "idade": "trinta"}'
validar_json(json_str)

# Criar arquivos adicionais para combinar JSONs
salvar_json({"extra": "informações"}, 'dados1.json')
salvar_json({"mais_dados": [1, 2, 3]}, 'dados2.json')

# Combinar dois arquivos JSON e exibir o resultado
dados_combinados = combinar_json('dados1.json', 'dados2.json')

print("\nDados Combinados:")
exibir_dados(dados_combinados)

