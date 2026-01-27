# %% [markdown]
# # 3. Analítica de Negocio (Capa Oro)
# Agregaciones estratégicas para toma de decisiones

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_, desc
from delta import *

# %%
builder = SparkSession.builder \
    .appName("Lab_SECOP_Gold") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer capa Plata (Silver)
df_silver = spark.read.format("delta").load(
    "/app/data/lakehouse/silver/secop"
)

# %%
# Agregación de negocio (Top 10 departamentos por inversión)
df_gold = (
    df_silver
    .groupBy("departamento")
    .agg(
        sum_("precio_base").alias("total_contratado")
    )
    .orderBy(desc("total_contratado"))
    .limit(10)
)

# %%
# Persistir capa Oro
df_gold.write.format("delta") \
    .mode("overwrite") \
    .save("/app/data/lakehouse/gold/top_deptos")

# %%
# Visualizar resultados
print("🏆 Top 10 Departamentos por contratación pública:")
df_gold.show(truncate=False)
