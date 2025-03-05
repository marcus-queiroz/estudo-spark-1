import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when

# Definir o caminho relativo baseado no diretório de execução (para armazenar a Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '13-data')
delta_dir = os.path.join(data_dir, '13-delta')
src_dir = os.path.join(data_dir, '13-src')

clientes_csv_path = os.path.join(src_dir, '13-src-clientes.csv')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo13-SCD-Tipo2") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leitura dos novos dados de clientes (atualizações)
clientes_atualizados_df = spark.read.csv(clientes_csv_path, header=True, inferSchema=True)

# 2. Leitura dos dados existentes da tabela de clientes no Delta Lake (dimensão existente)
try:
    clientes_dim_df = spark.read.format("delta").load(delta_dir)
    # Garantir que as colunas valid_from e valid_until existam
    clientes_dim_df = clientes_dim_df.withColumn("valid_from", when(col("valid_from").isNull(), current_date()).otherwise(col("valid_from"))) \
                                     .withColumn("valid_until", when(col("valid_until").isNull(), lit(None).cast("date")).otherwise(col("valid_until"))) \
                                     .withColumn("is_current", when(col("is_current").isNull(), lit(True)).otherwise(col("is_current")))
except Exception as e:
    print(f"Nenhum dado anterior encontrado. Iniciando o processamento com todos os dados. Erro: {e}")
    # Criação de um DataFrame vazio com as colunas necessárias
    clientes_dim_df = spark.createDataFrame([], clientes_atualizados_df.schema) \
        .withColumn("valid_from", lit(None).cast("date")) \
        .withColumn("valid_until", lit(None).cast("date")) \
        .withColumn("is_current", lit(True))

# Renomear as colunas dos dados atualizados para evitar conflitos durante o join
clientes_atualizados_df = clientes_atualizados_df \
    .withColumnRenamed("nome", "nome_atualizado") \
    .withColumnRenamed("cidade", "cidade_atualizada") \
    .withColumnRenamed("data_nascimento", "data_nascimento_atualizada")

# 3. Marcar os registros antigos como "expirados" (is_current=False)
clientes_expirados_df = clientes_dim_df \
    .join(clientes_atualizados_df, "cliente_id", "left") \
    .withColumn("is_current", when((col("nome") != col("nome_atualizado")) | 
                                   (col("cidade") != col("cidade_atualizada")) | 
                                   (col("data_nascimento") != col("data_nascimento_atualizada")), lit(False)).otherwise(col("is_current"))) \
    .withColumn("valid_until", when(col("is_current") == False, current_date()).otherwise(col("valid_until"))) \
    .select("cliente_id", "nome", "cidade", "data_nascimento", "valid_from", "valid_until", "is_current") \
    .filter(col("is_current") == False)

# 4. Criar novas versões para clientes atualizados (novos registros)
clientes_novos_df = clientes_atualizados_df \
    .withColumn("is_current", lit(True)) \
    .withColumn("valid_from", current_date()) \
    .withColumn("valid_until", lit(None).cast("date")) \
    .select("cliente_id", col("nome_atualizado").alias("nome"), col("cidade_atualizada").alias("cidade"), col("data_nascimento_atualizada").alias("data_nascimento"), "valid_from", "valid_until", "is_current")

# 5. Garantir que ambos os DataFrames tenham as mesmas colunas antes da união
clientes_expirados_df = clientes_expirados_df.select("cliente_id", "nome", "cidade", "data_nascimento", "valid_from", "valid_until", "is_current")
clientes_novos_df = clientes_novos_df.select("cliente_id", "nome", "cidade", "data_nascimento", "valid_from", "valid_until", "is_current")

# 6. Combinar os registros expirados com as novas versões de clientes
clientes_atualizados_final_df = clientes_expirados_df.union(clientes_novos_df)

# Escrever a nova versão da tabela de clientes no Delta Lake, preservando o histórico
clientes_atualizados_final_df.write.format("delta").mode("append").save(delta_dir)

# 7. Leitura da tabela Delta para verificar as alterações aplicadas
print("Leitura dos dados de clientes com SCD Tipo 2:")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
