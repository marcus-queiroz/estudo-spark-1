from pyspark.sql import SparkSession
import os
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__))

def validate_path(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Diretório não encontrado: {path}")
    return path

# Determinar o caminho base do projeto
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_dir = validate_path(os.path.join(base_dir, "data", "raw"))
    bronze_data_dir = os.path.join(base_dir, "data", "bronze")
    
    # Criar diretório bronze se não existir
    os.makedirs(bronze_data_dir, exist_ok=True)
    logger.info(f"Diretórios configurados:\n- Raw: {raw_data_dir}\n- Bronze: {bronze_data_dir}")
except Exception as e:
    logger.error(f"Erro na configuração de diretórios: {str(e)}")
    raise

try:
    # Criação da Spark Session com Delta Lake
    spark = SparkSession.builder \
        .appName("Camada Bronze - Ingestão de Dados Brutos") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    logger.info("Spark Session criada com sucesso")
except Exception as e:
    logger.error(f"Erro ao criar Spark Session: {str(e)}")
    raise

try:
    # Leitura dos dados brutos (arquivos CSV e JSON)
    logger.info("Iniciando leitura dos dados brutos...")
    
    orders_path = os.path.join(raw_data_dir, "orders.csv")
    customers_path = os.path.join(raw_data_dir, "customers.json")
    inventory_path = os.path.join(raw_data_dir, "inventory_movements.csv")
    
    logger.info(f"Lendo arquivos:\n- {orders_path}\n- {customers_path}\n- {inventory_path}")
    
    orders_df = spark.read.option("header", True).csv(orders_path)
    customers_df = spark.read.option("multiline", True).json(customers_path)
    inventory_movements_df = spark.read.option("header", True).csv(inventory_path)
    
    logger.info(f"DataFrames carregados com sucesso:\n- Orders: {orders_df.count()} linhas\n- Customers: {customers_df.count()} linhas\n- Inventory: {inventory_movements_df.count()} linhas")
except Exception as e:
    logger.error(f"Erro na leitura dos dados: {str(e)}")
    raise

# Escrita dos dados na camada Bronze (em formato Delta)
orders_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "orders"))
customers_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "customers"))
inventory_movements_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_data_dir, "inventory_movements"))

spark.stop()
