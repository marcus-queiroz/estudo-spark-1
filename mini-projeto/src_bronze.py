from pyspark.sql import SparkSession
import os

# Determinar o caminho base do projeto
base_dir = os.path.dirname(os.path.abspath(__file__))
raw_data_dir = os.path.join(base_dir, "data", "raw")
bronze_data_dir = os.path.join(base_dir, "data", "bronze")

# Criação da Spark Session com Delta Lake
spark = SparkSession.builder \
    .appName("Camada Bronze - Ingestão de Dados Brutos") \
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
        return spark.read.option("header", True).option("inferSchema", True).csv(file_path)
    except Exception as e:
        print(f"Erro ao ler o arquivo {file_path}: {e}")
        return None

orders_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Orders.csv"))
customers_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Customers.csv"))
inventory_movements_df = read_csv_safely(spark, os.path.join(raw_data_dir, "Inventory_Movements.csv"))

# Verificação de dataframes válidos antes de gravar
if all([orders_df, customers_df, inventory_movements_df]):
    orders_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Orders"))
    customers_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Customers"))
    inventory_movements_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Inventory_Movements"))
else:
    print("Não foi possível processar todos os arquivos de entrada.")

# Escrita dos dados na camada Bronze (em formato Delta)
orders_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Orders"))
customers_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Customers"))
inventory_movements_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Inventory_Movements"))

spark.stop()
