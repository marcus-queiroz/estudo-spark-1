# Projeto de Processamento de Dados com DuckDB e Delta Lake

## Visão Geral

Este projeto implementa um pipeline de processamento de dados utilizando DuckDB e Delta Lake, seguindo uma arquitetura de medalhas (Bronze, Silver, Gold) para transformação e enriquecimento de dados.

## Arquitetura de Dados

### Camadas de Processamento

1. **Landing Layer (Landing.py)**
   - Responsável por coletar e preparar dados brutos
   - Localiza arquivos CSV na fonte original
   - Copia arquivos para diretório de preparação

2. **Bronze Layer (Bronze.py)**
   - Processa dados brutos e os converte para formato Delta Lake
   - Realiza processamento incremental
   - Estratégias:
     * Primeira execução: Cria tabelas
     * Execuções subsequentes: Adiciona novos registros

3. **Silver Layer (Silver.py)**
   - Transforma e enriquece dados da camada Bronze
   - Realiza junções entre múltiplas tabelas
   - Cria visões consolidadas
   - Gera snapshots de dados

4. **Gold Layer (Gold.py)**
   - Cria dimensões e fatos para data warehouse
   - Gera surrogate keys
   - Prepara dados para análise e visualização

## Tecnologias Utilizadas

- Python
- DuckDB
- Delta Lake
- Pandas

## Fluxo de Processamento

1. Executar `Landing.py` para preparar dados brutos
2. Executar `Bronze.py` para processar dados em Delta Lake
3. Executar `Silver.py` para transformar e enriquecer dados
4. Executar `Gold.py` para criar modelo dimensional

## Pré-requisitos

- Python 3.8+
- Bibliotecas:
  * deltalake
  * duckdb
  * pandas

## Instalação de Dependências

```bash
pip install deltalake duckdb pandas
```

## Execução do Pipeline

```bash
python Landing.py
python Bronze.py
python Silver.py
python Gold.py
```

## Estrutura de Diretórios

```
mini-projeto-2-duckdb/
│
├── data/
│   ├── BikeStore/         # Fonte de dados original
│   ├── landing/           # Dados brutos preparados
│   ├── bronze/            # Dados processados em Delta Lake
│   ├── silver/            # Dados transformados
│   └── gold/              # Modelo dimensional
│
├── Landing.py             # Preparação de dados
├── Bronze.py              # Processamento inicial
├── Silver.py              # Transformação de dados
└── Gold.py               # Criação de modelo dimensional
```

## Considerações

- Pipeline projetado para processamento incremental
- Suporta adição de novos registros sem reprocessar dados existentes
- Utiliza Delta Lake para versionamento e gerenciamento de dados

## Licença

[Especificar licença do projeto]
# Projeto de Processamento de Dados com DuckDB e Delta Lake

## Visão Geral

Este projeto implementa um pipeline de processamento de dados utilizando DuckDB e Delta Lake, seguindo uma arquitetura de medalhas (Bronze, Silver, Gold) para transformação e enriquecimento de dados.

## Arquitetura de Dados

### Camadas de Processamento

1. **Landing Layer (Landing.py)**
   - Responsável por coletar e preparar dados brutos
   - Localiza arquivos CSV na fonte original
   - Copia arquivos para diretório de preparação

2. **Bronze Layer (Bronze.py)**
   - Processa dados brutos e os converte para formato Delta Lake
   - Realiza processamento incremental
   - Estratégias:
     * Primeira execução: Cria tabelas
     * Execuções subsequentes: Adiciona novos registros

3. **Silver Layer (Silver.py)**
   - Transforma e enriquece dados da camada Bronze
   - Realiza junções entre múltiplas tabelas
   - Cria visões consolidadas
   - Gera snapshots de dados

4. **Gold Layer (Gold.py)**
   - Cria dimensões e fatos para data warehouse
   - Gera surrogate keys
   - Prepara dados para análise e visualização

## Tecnologias Utilizadas

- Python
- DuckDB
- Delta Lake
- Pandas

## Fluxo de Processamento

1. Executar `Landing.py` para preparar dados brutos
2. Executar `Bronze.py` para processar dados em Delta Lake
3. Executar `Silver.py` para transformar e enriquecer dados
4. Executar `Gold.py` para criar modelo dimensional

## Pré-requisitos

- Python 3.8+
- Bibliotecas:
  * deltalake
  * duckdb
  * pandas

## Instalação de Dependências

```bash
pip install deltalake duckdb pandas
```

## Execução do Pipeline

```bash
python Landing.py
python Bronze.py
python Silver.py
python Gold.py
```

## Estrutura de Diretórios

```
mini-projeto-2-duckdb/
│
├── data/
│   ├── BikeStore/         # Fonte de dados original
│   ├── landing/           # Dados brutos preparados
│   ├── bronze/            # Dados processados em Delta Lake
│   ├── silver/            # Dados transformados
│   └── gold/              # Modelo dimensional
│
├── Landing.py             # Preparação de dados
├── Bronze.py              # Processamento inicial
├── Silver.py              # Transformação de dados
└── Gold.py               # Criação de modelo dimensional
```

## Considerações

- Pipeline projetado para processamento incremental
- Suporta adição de novos registros sem reprocessar dados existentes
- Utiliza Delta Lake para versionamento e gerenciamento de dados

## Licença

[Especificar licença do projeto]
