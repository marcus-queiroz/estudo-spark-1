# Documentação Explicativa do Script de Carga Incremental com PySpark e Delta Lake

## Visão Geral

Este script implementa um **processo de carga incremental** de dados utilizando **Apache Spark** com suporte ao **Delta Lake**, sem a necessidade de um cluster Hadoop. Ele realiza a leitura de dados de transações e clientes, filtra registros incrementais, enriquece os dados por meio de uma junção e os armazena no **Delta Lake**.

## 1. Configuração Inicial

### 1.1 Importação de Bibliotecas

O script importa as bibliotecas necessárias:

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr
```

- `os`: Manipula caminhos e diretórios.
- `pyspark.sql.SparkSession`: Inicializa uma sessão Spark.
- `pyspark.sql.functions`: Fornece funções para transformação de dados.

### 1.2 Definição de Caminhos

O script define caminhos para armazenar os dados de entrada, intermediários e de saída:

```python
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '07-data')
delta_dir = os.path.join(data_dir, '07-delta')
dst_dir = os.path.join(data_dir, '07-dst')
```

- `data_dir`: Diretório base para os dados.
- `delta_dir`: Armazena os dados no formato **Delta Table**.
- `dst_dir`: Pode ser utilizado para outros armazenamentos de saída.

Os arquivos de origem também são definidos:

```python
transacoes_csv_path = os.path.join(data_dir, '07-src-transacoes.csv')
clientes_json_path = os.path.join(data_dir, '07-src-clientes.json')
```

### 1.3 Criação de Diretórios

Para garantir que os diretórios existem, o script os cria, caso não existam:

```python
os.makedirs(delta_dir, exist_ok=True)
os.makedirs(dst_dir, exist_ok=True)
```

## 2. Inicialização do Spark com Suporte ao Delta Lake

Uma sessão Spark é criada com suporte ao **Delta Lake**:

```python
spark = SparkSession.builder \
    .appName("Exemplo7-CargaIncremental-Sem-Hadoop") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()
```

- **`spark.jars.packages`**: Inclui o conector **Delta Lake**.
- **`spark.sql.extensions`**: Habilita extensões do **Delta Lake**.
- **`spark.sql.catalog.spark_catalog`**: Define o **Delta Lake** como o catalog padrão.
- **`master("local[*]")`**: Executa localmente utilizando todos os núcleos disponíveis.

## 3. Leitura e Processamento dos Dados

### 3.1 Leitura dos Arquivos

Os arquivos CSV e JSON são carregados em DataFrames:

```python
transacoes_df = spark.read.csv(transacoes_csv_path, header=True, inferSchema=True)
clientes_df = spark.read.json(clientes_json_path)
```

- **`inferSchema=True`**: Permite que o Spark infira os tipos de dados automaticamente.

As colunas `id` são renomeadas para evitar ambiguidades:

```python
transacoes_df = transacoes_df.withColumnRenamed("id", "transacao_id")
clientes_df = clientes_df.withColumnRenamed("id", "cliente_id")
```

### 3.2 Filtragem Incremental

O script filtra as transações com base em uma **data de corte**:

```python
ultima_data_processada = "2024-03-01"
transacoes_filtradas = transacoes_df.filter(
    to_date(col("data"), "yyyy-MM-dd") > expr(f"'{ultima_data_processada}'")
)
```

- Apenas transações **posteriores** à `ultima_data_processada` são mantidas.

### 3.3 Exibição dos Dados Incrementais

Para depuração, os dados incrementais são exibidos:

```python
print("Transações incrementais (novas desde a última execução):")
transacoes_filtradas.show(truncate=False)
```

### 3.4 Junção com os Clientes

Os dados das transações filtradas são enriquecidos com informações dos clientes:

```python
transacoes_clientes_df = transacoes_filtradas.join(clientes_df, "cliente_id", "inner")
```

- Um **INNER JOIN** é realizado entre transações e clientes.

Os dados resultantes também são exibidos:

```python
transacoes_clientes_df.select(
    col("transacao_id"),
    col("nome").alias("cliente_nome"),
    col("valor"),
    col("categoria"),
    col("cidade"),
    col("data")
).show(truncate=False)
```

## 4. Escrita dos Dados no Delta Lake

Os dados incrementais são **anexados** ao Delta Lake:

```python
transacoes_clientes_df.write.format("delta").mode("append").save(delta_dir)
```

- O modo **append** garante que apenas os novos registros sejam adicionados, sem sobrescrever os dados existentes.

## 5. Validação dos Dados Gravados

Para garantir que os dados foram armazenados corretamente, eles são lidos de volta:

```python
print("Leitura dos dados salvos no Delta Lake:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)
```

## 6. Encerramento da Spark Session

Por fim, a sessão Spark é encerrada:

```python
spark.stop()
```

---

## Conclusão

Este script implementa um **pipeline de carga incremental** utilizando **Apache Spark e Delta Lake**, garantindo que apenas novos registros sejam processados e armazenados. O uso do **Delta Lake** permite consultas eficientes e armazenamento transacional confiável para grandes volumes de dados.