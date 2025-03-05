import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Definir o caminho relativo baseado no diretório de execução (para armazenar Delta Table)
current_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(current_dir, '11-data')
delta_dir = os.path.join(data_dir, '11-delta')

# Configuração da Spark Session com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("Exemplo11-SCD2-Delta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Definir o esquema do DataFrame explicitamente
schema = StructType([
    StructField("cliente_id", IntegerType(), False),
    StructField("nome", StringType(), False),
    StructField("cidade", StringType(), False),
    StructField("inicio_validade", StringType(), False),  # Vamos manter como String para conversão posterior
    StructField("fim_validade", StringType(), True)  # Permitindo valor nulo
])

# 2. Definir os dados iniciais dos clientes de forma simples
clientes_iniciais = [
    (1, "Carlos", "Belo Horizonte", "2023-01-01", None),
    (2, "Julia", "Brasília", "2023-01-01", None),
    (3, "Rich", "São Paulo", "2023-01-01", None)
]

# Criar o DataFrame com o esquema definido
clientes_df = spark.createDataFrame(clientes_iniciais, schema)

# Converter as colunas de data para DateType
clientes_df = clientes_df.withColumn("inicio_validade", col("inicio_validade").cast(DateType())) \
                         .withColumn("fim_validade", col("fim_validade").cast(DateType()))

# Escrever os dados de clientes no Delta Lake (SCD inicial)
clientes_df.write.format("delta").mode("overwrite").save(delta_dir)

# Função para aplicar SCD Tipo 2
def aplicar_scd2(spark_session, delta_table_path, novos_dados_df):
    # Leitura dos dados existentes no Delta Lake
    delta_df = spark_session.read.format("delta").load(delta_table_path)

    # Filtrar registros ativos (fim_validade is NULL)
    delta_df_active = delta_df.filter(col("fim_validade").isNull())

    # Identificar registros que sofreram alterações (ex: cidade mudou)
    join_condition = delta_df_active["cliente_id"] == novos_dados_df["cliente_id"]
    atualizacoes = delta_df_active.join(novos_dados_df, join_condition, "left_outer")

    # Identificar registros que precisam ser atualizados
    registros_atualizados = atualizacoes.filter(
        (delta_df_active["cidade"] != novos_dados_df["cidade"]) &
        (novos_dados_df["cliente_id"].isNotNull())  # Apenas registros com cliente_id
    ).select(delta_df_active["cliente_id"])

    # Marcar o fim de validade dos registros antigos
    delta_df_atualizado = delta_df_active.join(registros_atualizados, "cliente_id", "left_semi") \
        .withColumn("fim_validade", current_date())

    # Inserir novos registros ou registros alterados com nova cidade
    novos_registros = novos_dados_df.withColumn("inicio_validade", current_date()) \
        .withColumn("fim_validade", lit(None).cast("date"))

    # Combinar registros históricos, registros atualizados, e novos registros
    delta_df_inactive = delta_df.filter(col("fim_validade").isNotNull())  # registros históricos
    final_df = delta_df_inactive.unionByName(delta_df_atualizado).unionByName(novos_registros)

    # Salvar os dados atualizados no Delta Lake
    final_df.write.format("delta").mode("overwrite").save(delta_table_path)

# 3. Simular uma mudança nos dados de clientes (novos dados)
novos_clientes = [
    (1, "Carlos", "Rio de Janeiro"),  # Carlos se mudou
    (3, "Rich", "Campinas"),          # Rich se mudou
    (4, "Mariana", "Curitiba")        # Novo cliente
]

# Criar o DataFrame com os novos clientes
novos_clientes_schema = StructType([
    StructField("cliente_id", IntegerType(), False),
    StructField("nome", StringType(), False),
    StructField("cidade", StringType(), False)
])

novos_clientes_df = spark.createDataFrame(novos_clientes, novos_clientes_schema)

# Aplicar o SCD Tipo 2 (processar alterações de cidade)
aplicar_scd2(spark, delta_dir, novos_clientes_df)

# 4. Leitura da tabela Delta com as mudanças de clientes
print("Clientes com versão de histórico (SCD Tipo 2):")
delta_df = spark.read.format("delta").load(delta_dir)
delta_df.show(truncate=False)

# Encerrar a Spark Session
spark.stop()
