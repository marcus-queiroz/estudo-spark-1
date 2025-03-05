from pyspark.sql import SparkSession
import os

# Determinar o caminho base do projeto
base_dir = os.path.dirname(os.path.abspath(__file__))
raw_data_dir = os.path.join(base_dir, "data", "raw")
bronze_data_dir = os.path.join(base_dir, "data", "bronze")

# Criação da Spark Session com Delta Lake
spark = SparkSession.builder \
    .appName("Camada Bronze - Ingestão de Dados Brutos") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Leitura dos dados brutos (arquivos CSV e JSON)
# Verificação de existência dos arquivos antes da leitura
def read_csv_safely(spark, file_path):
    if not os.path.exists(file_path):
        print(f"Erro: Arquivo não encontrado - {file_path}")
        return None
    try:
        return spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("mode", "PERMISSIVE") \
            .load(file_path)
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")
        return None

orders_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Orders.csv"))
customers_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Customers.csv"))
inventory_movements_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Inventory_Movements.csv"))

# Verificação de dataframes válidos antes de gravar
def save_dataframe_safely(df, path):
    if df is not None:
        try:
            df.write.format("delta").mode("overwrite").save(path)
            print(f"Dados salvos com sucesso em {path}")
        except Exception as e:
            print(f"Erro ao salvar dados em {path}: {e}")
    else:
        print(f"DataFrame para {path} é None, pulando gravação.")

# Criar diretório de destino se não existir
os.makedirs(bronze_data_dir, exist_ok=True)

save_dataframe_safely(orders_df, os.path.join(bronze_data_dir, "Orders"))
save_dataframe_safely(customers_df, os.path.join(bronze_data_dir, "Customers"))
save_dataframe_safely(inventory_movements_df, os.path.join(bronze_data_dir, "Inventory_Movements"))

spark.stop()
