# %% [markdown]
# # 1. Ingesta (Capa Bronce)
# Convertimos CSV crudo a formato Delta Lake.

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import *
import os

# URL del Master (definida en docker-compose)
master_url = "spark://spark-master:7077"

# Configuración: Añadimos Delta Lake
builder = SparkSession.builder \
    .appName("Lab_SECOP_Bronze") \
    .master(master_url) \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.memory", "1g") 

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# LECTURA CSV
print("Leyendo CSV crudo...")
df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .load("/app/data/SECOP_II_Contratos_Electronicos.csv")

# %%
#Normalizar
import re

def normalize_columns(df):
    for col in df.columns:
        new_col = re.sub(r"[ ,;{}()\n\t=]", "_", col.strip().lower())
        df = df.withColumnRenamed(col, new_col)
    return df

df_raw = normalize_columns(df_raw)
    
# %%
# ESCRITURA BRONCE (Delta)
print("Escribiendo en capa Bronce...")
output_path = "/app/data/lakehouse/bronze/secop"
df_raw.write.format("delta").mode("overwrite").save(output_path)

print(f"Ingesta completada. Registros procesados: {df_raw.count()}")
