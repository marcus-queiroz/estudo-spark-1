import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, when

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo9-SCD-Tipo2-Particionado") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Diretórios para armazenar os dados Delta
current_dir = os.path.dirname(os.path.realpath(__file__))
delta_dir = os.path.join(current_dir, '09-delta')

# 1. Criar e escrever os dados de clientes iniciais
clientes_iniciais = [
    (1, "Rich", "São Paulo", "2024-10-01"),
    (2, "Carlos", "Belo Horizonte", "2024-10-01"),
    (3, "Julia", "Brasília", "2024-10-01"),
    (4, "Ana", "Rio de Janeiro", "2024-10-01")
]

schema = ["cliente_id", "nome", "cidade", "data_cadastro"]
clientes_iniciais_df = spark.createDataFrame(clientes_iniciais, schema)

# Adicionar colunas para SCD Tipo 2 (ativo, data_inicio, data_fim)
clientes_iniciais_df = clientes_iniciais_df.withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

# Escrever os dados iniciais no Delta Lake particionado pela coluna "cidade"
clientes_iniciais_df.write \
    .format("delta") \
    .partitionBy("cidade") \
    .mode("overwrite") \
    .save(delta_dir)

# 2. Simular uma nova carga de dados com atualizações e novos clientes
clientes_atualizados = [
    (1, "Rich", "Campinas", "2024-11-01"),  # Rich mudou de cidade
    (2, "Carlos", "Belo Horizonte", "2024-11-01"),  # Carlos continua igual
    (3, "Julia", "Curitiba", "2024-11-01"),  # Julia mudou de cidade
    (5, "Marcos", "São Paulo", "2024-11-01")  # Novo cliente
]

clientes_atualizados_df = spark.createDataFrame(clientes_atualizados, schema)

# 3. Leitura dos dados históricos do Delta Lake
clientes_delta_df = spark.read.format("delta").load(delta_dir)

# 4. Aplicar a lógica SCD Tipo 2 - Atualizar registros antigos e inserir novos

# a) Marcar os registros antigos como inativos (se houve mudança de cidade)
clientes_historico_atualizado_df = clientes_delta_df.join(clientes_atualizados_df, "cliente_id", "left") \
    .withColumn("ativo", when(
        (clientes_atualizados_df.cliente_id.isNotNull()) & 
        (clientes_delta_df.cidade != clientes_atualizados_df.cidade), lit(False)
    ).otherwise(col("ativo"))) \
    .withColumn("data_fim", when(
        (clientes_atualizados_df.cliente_id.isNotNull()) & 
        (clientes_delta_df.cidade != clientes_atualizados_df.cidade), current_date()
    ).otherwise(col("data_fim")))

# Garantir que todas as colunas estejam presentes após as transformações
clientes_historico_atualizado_df = clientes_historico_atualizado_df.select(
    "cliente_id", "nome", "cidade", "data_cadastro", "ativo", "data_inicio", "data_fim"
)

# b) Inserir novos registros ou registros com nova cidade como ativos
clientes_novos_df = clientes_atualizados_df.join(clientes_delta_df, "cliente_id", "leftanti") \
    .withColumn("ativo", lit(True)) \
    .withColumn("data_inicio", current_date()) \
    .withColumn("data_fim", lit(None).cast("date"))

# Garantir que os DataFrames tenham o mesmo esquema
clientes_novos_df = clientes_novos_df.select("cliente_id", "nome", "cidade", "data_cadastro", "ativo", "data_inicio", "data_fim")

# 5. Combinar os registros históricos com os novos
clientes_final_df = clientes_historico_atualizado_df.unionByName(clientes_novos_df)

# 6. Escrever os dados finalizados no Delta Lake com particionamento
clientes_final_df.write \
    .format("delta") \
    .partitionBy("cidade") \
    .mode("overwrite") \
    .save(delta_dir)

# 7. Exibir o resultado final
print("Histórico de clientes com SCD Tipo 2 e Particionamento:")
clientes_result_df = spark.read.format("delta").load(delta_dir)
clientes_result_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
