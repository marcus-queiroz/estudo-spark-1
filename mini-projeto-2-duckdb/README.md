# Projeto de Processamento de Dados com DuckDB e Delta Lake

## Visão Geral

Este projeto implementa um pipeline completo de processamento de dados utilizando DuckDB e Delta Lake, estruturado em uma arquitetura de camadas (Landing, Bronze, Silver e Gold), ideal para processos de Data Engineering e Business Intelligence.

## Estrutura do Projeto

### 1. Camada Landing (`Landing.py`)

Responsável por:
- Identificar e coletar arquivos CSV brutos originais.
- Copiar arquivos CSV para um diretório intermediário de preparação para processamento posterior.

### 2. Camada Bronze (`Bronze.py`)

Responsável por:
- Ler os arquivos CSV da camada Landing.
- Processar e armazenar dados brutos em tabelas Delta Lake.
- Aplicar lógica incremental para evitar processamento redundante.

### 3. Camada Silver (`Silver.py`)

Responsável por:
- Realizar junções e transformações nos dados da camada Bronze.
- Criar visões consolidadas e enriquecidas para análises detalhadas.
- Gerar snapshots incrementais para acompanhamento histórico.

### 4. Camada Gold (`Gold.py`)

Responsável por:
- Criar dimensões e fatos, estruturando um modelo dimensional para Data Warehouse.
- Gerar surrogate keys para garantir integridade e performance.
- Preparar dados finais para análise, visualização e relatórios de BI.

## Tecnologias Utilizadas

- **Python**: Linguagem principal utilizada para todo o desenvolvimento.
- **DuckDB**: Banco de dados analítico eficiente para consultas rápidas.
- **Delta Lake**: Armazenamento estruturado que suporta processamento incremental e versionamento.
- **Pandas**: Biblioteca Python para manipulação eficiente dos dados.

## Como Executar o Pipeline

### Pré-requisitos
- Python 3.8 ou superior

### Instalação das Dependências
```bash
poetry add deltalake duckdb pandas
```

### Execução
Para executar todo o pipeline:
```bash
poetry run python mini-projeto-2-duckdb/Landing.py
poetry run python mini-projeto-2-duckdb/Bronze.py
poetry run python mini-projeto-2-duckdb/Silver.py
poetry run python mini-projeto-2-duckdb/Gold.py
```

## Estrutura de Diretórios

```
mini-projeto-2-duckdb/
│
├── data/
│   ├── BikeStore/         # Dados originais
│   ├── landing/           # Dados preparados
│   ├── bronze/            # Dados armazenados em formato Delta
│   ├── silver/            # Dados enriquecidos
│   └── gold/              # Modelo dimensional final
│
├── Landing.py             # Coleta e preparação de dados
├── Bronze.py              # Processamento inicial incremental
├── Silver.py              # Transformações e enriquecimento de dados
└── Gold.py                # Criação de dimensões e fatos para o DW
```

## Benefícios da Implementação

- Processamento incremental robusto, economizando recursos computacionais.
- Dados organizados para análise eficiente e ágil.
- Estrutura escalável e flexível, permitindo fácil adaptação para novos requisitos.
- Versionamento automático através do uso do Delta Lake.



