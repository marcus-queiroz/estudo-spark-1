# Projetos de exemplo

Este projeto tem o objetivo de ser uma coleção de scripts voltados a montagem de exemplos que facilitem o aprendizado de conceitos complexos.
A ideia é criar um material referência sobre **`Spark`**, **`Deltalake`** e **`DuckDB`**. Da perspectiva de técnicas de modelagem comuns em engenharia de dados.


## exemplos-conceito
Cada subprojeto é projetado para ser executado de forma independente, permitindo que os usuários explorem e modifiquem os exemplos conforme necessário para aprender e aplicar conceitos do Apache Spark.

- **01-data**: Contém exemplos de manipulação de dados básicos, incluindo leitura e escrita de arquivos JSON.
- **02-exemplo.py**: Focado em validação de dados JSON complexos, abordando técnicas de verificação de integridade e estrutura.
- **05-data**: Exemplos de filtragem e categorização de transações, com dados de exemplo em formato CSV.
- **07-data**: Demonstra o uso do Delta Lake para versionamento de dados, incluindo logs de transações.
- **08-data**: Processamento de dados em lote utilizando o Delta Lake, com exemplos de arquivos Parquet.
- **09-data**: [Em desenvolvimento] - Planejado para cobrir otimizações de desempenho e técnicas avançadas de processamento.
- **10-data**: Exemplos de criação de snapshots de dados para auditoria e recuperação de versões anteriores.
- **11-data**: Aplicação de Slowly Changing Dimensions (SCD) tipo 2, com exemplos de integração de dados históricos.
- **12-data**: Integração de dados de múltiplas fontes, demonstrando técnicas de fusão e agregação.
- **13-data**: Análise de dados avançada com Spark SQL, incluindo consultas complexas e otimizações.


## mini-projeto-1-spark

O diretório `mini-projeto-1-spark` contém um projeto prático que aplica conceitos avançados do Apache Spark em um cenário realista. Este projeto inclui:

- **simulation_cdc.py**: Implementa a lógica de Change Data Capture (CDC) para gerenciar atualizações incrementais em tabelas Delta.
- **simulation_cdc_scd.py**: Focado na aplicação de Slowly Changing Dimensions (SCD) tipo 2, permitindo o rastreamento de mudanças históricas nos dados.
- **simulation_scd.py**: Aplica técnicas de SCD tipo 2 em um contexto de dados simulado, demonstrando como gerenciar dados históricos.
- **src_bronze.py**: Contém funções para leitura e escrita segura de dados na camada Bronze, garantindo a integridade dos dados.
- **src_gold.py**: [Em desenvolvimento] - Planejado para manipulação e análise de dados na camada Gold.
- **src_silver.py**: Inclui funções para integração e transformação de dados na camada Silver, preparando-os para análise avançada.
- **test_data_pipeline.py**: Scripts de teste para garantir a funcionalidade correta do pipeline de dados.

Este mini projeto é projetado para ser um exemplo completo de como construir e gerenciar um pipeline de dados usando Apache Spark, desde a ingestão até a análise.

## mini-projeto-2-duckdb

O diretório `mini-projeto-2-duckdb` implementa um pipeline de processamento de dados robusto utilizando DuckDB e Delta Lake, seguindo uma arquitetura de medalhas (Bronze, Silver, Gold):

- **Camadas de Processamento**:
  1. **Landing Layer (Landing.py)**: Coleta e prepara dados brutos
  2. **Bronze Layer (Bronze.py)**: Converte dados para Delta Lake, processamento incremental
  3. **Silver Layer (Silver.py)**: Transforma e enriquece dados, realiza junções
  4. **Gold Layer (Gold.py)**: Cria dimensões e fatos para data warehouse

- **Características Principais**:
  - Processamento incremental de dados
  - Suporte a versionamento com Delta Lake
  - Transformações de dados em múltiplas etapas
  - Preparação de dados para análise e visualização

- **Tecnologias Utilizadas**:
  - Python
  - DuckDB
  - Delta Lake
  - Pandas

Este mini projeto demonstra técnicas avançadas de processamento de dados, com foco em flexibilidade, escalabilidade e gerenciamento de dados históricos.

## material-apoio

O diretório `material-apoio` contém recursos adicionais que complementam os exemplos práticos. Esses materiais são projetados para fornecer um contexto adicional e suporte ao aprendizado dos conceitos apresentados nos subprojetos.