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
orders_df = spark.read.option("header", True).csv(os.path.join(raw_data_dir, "Orders.csv"))
customers_df = spark.read.option("header", True).csv(os.path.join(raw_data_dir, "Customers.csv"))
inventory_movements_df = spark.read.option("header", True).csv(os.path.join(raw_data_dir, "Inventory_Movements.csv"))

# Escrita dos dados na camada Bronze (em formato Delta)
orders_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Orders"))
customers_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Customers"))
inventory_movements_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "Inventory_Movements"))

spark.stop()