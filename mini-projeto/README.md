


# **Pipeline de Dados - Mini Projeto**

Este projeto é um pipeline de dados que processa informações de vendas, clientes e estoque, organizado em três camadas principais: **Bronze, Silver e Gold**. Ele utiliza **Apache Spark e Delta Lake** para garantir processamento eficiente, versionamento de dados e ingestão incremental.

## Introdução

O Apache Spark é uma plataforma de processamento de dados em larga escala que permite a execução de tarefas de análise de dados de forma distribuída. O Delta Lake é uma camada de armazenamento que traz confiabilidade e desempenho ao processamento de dados com Spark, permitindo versionamento e transações ACID.

## Pré-requisitos

- Apache Spark instalado e configurado.
- Python 3.x instalado.
- Delta Lake configurado no ambiente Spark.
- Dependências adicionais listadas em `requirements.txt`.

## **Objetivos do Projeto**

- **Ingestão e processamento de dados de vendas e estoque**
- **Implementação de Change Data Capture (CDC) para cargas incrementais**
- **Uso de Slowly Changing Dimension (SCD) Tipo 2 para manter histórico de clientes**
- **Otimização de consultas com particionamento e Z-Ordering**

## **Arquitetura do Pipeline**

### **Camada Bronze (Ingestão de Dados Brutos)**

#### **Objetivo**

Captura dos dados brutos das fontes (CSV, JSON) sem transformações significativas, garantindo rastreabilidade.

#### **Processo**

- Leitura de arquivos de pedidos, clientes e movimentações de estoque.
- Armazenamento no **Delta Lake** em formato bruto.
- Implementação de **CDC** para ingestão incremental.

#### **Script: Ingestão de Dados**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Camada Bronze - Ingestão de Dados") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

orders_df = spark.read.option("header", True).csv("data/bronze/orders.csv")
orders_df.write.format("delta").mode("overwrite").save("/delta/bronze/orders")
```

### **Camada Silver (Transformação e Integração)**

#### **Objetivo**

Limpeza e padronização dos dados, remoção de informações inválidas e aplicação de CDC para manter dados atualizados.

#### **Processo**

- Remoção de pedidos cancelados.
- Normalização das movimentações de estoque.
- Implementação de **SCD Tipo 2** para clientes.
- Uso de Delta Lake para mesclagem incremental dos dados.

#### **Script: Transformação e Integração**

```python
from delta.tables import DeltaTable

def merge_incremental_data(delta_table_path, new_data_df, key_column):
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.alias("target").merge(
            new_data_df.alias("source"),
            f"target.{key_column} = source.{key_column}"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        new_data_df.write.format("delta").mode("overwrite").save(delta_table_path)
```

### **Camada Gold (Relatórios e Otimização)**

#### **Objetivo**

Geração de relatórios otimizados e melhoria do desempenho das consultas.

#### **Processo**

- Agregação de dados de vendas e estoque.
- Otimização com **particionamento** e **Z-Ordering**.
- Disponibilização dos dados para consumo por ferramentas de BI.

#### **Script: Agregação e Otimização**

```python
from delta.tables import DeltaTable

gold_sales_report_path = "/delta/gold/sales_report"
if DeltaTable.isDeltaTable(spark, gold_sales_report_path):
    final_report_df.write.format("delta").mode("overwrite").save(gold_sales_report_path)
else:
    final_report_df.write.format("delta").mode("overwrite").save(gold_sales_report_path)

delta_table_gold = DeltaTable.forPath(spark, gold_sales_report_path)
delta_table_gold.optimize().executeZOrderBy("order_date")
```

## **Fluxo de Execução**

1. **Bronze**:
    - Execute `poetry run python mini-projeto/src_bronze.py` para ingestão de dados brutos.
2. **Silver**:
    - Execute `poetry run python mini-projeto/src_silver.py` para transformação e limpeza.
    - Execute `poetry run python mini-projeto/simulation_cdc.py` para carga incremental.
3. **Gold**:
    - Execute `poetry run python mini-projeto/src_gold.py` para gerar relatórios agregados.
    - Execute `poetry run python mini-projeto/simulation_scd.py` para aplicar SCD Tipo 2.

## **Boas Práticas e Melhorias**

1. **Monitoramento e Logging**
    
    - Implementar logs detalhados para capturar falhas e tempos de execução.
2. **Tratamento de Erros**
    
    - Utilizar `try-except` para evitar falhas inesperadas.
3. **Testes Automatizados**
    
    - Usar `pytest` para validar a integridade dos dados.
4. **Otimização do CDC**
    
    - Melhorar o merge para evitar sobrescrita desnecessária de registros.

## **Execução de Testes**

### **Configuração do ambiente Spark**

Utiliza `pytest` para validar as transformações.

### **Testes por dataset**

1. Verificação de carga correta dos dados.
2. Checagem da presença de colunas obrigatórias.
3. Garantia de integridade dos IDs e status dos pedidos.

#### **Comando para executar os testes**

```bash
pip install pytest
pytest test_data_pipeline.py
```

## Contribuição e Suporte

Contribuições são bem-vindas! Para contribuir, faça um fork do repositório e envie um pull request. Para suporte, abra uma issue no GitHub.

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo LICENSE para mais detalhes.

Este documento foi atualizado para refletir a estrutura e melhorias no projeto, garantindo clareza e boas práticas para a execução do pipeline de dados.





