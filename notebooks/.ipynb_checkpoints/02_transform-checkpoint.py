# %% [markdown]
# # 2. Transformación + Quality Gate (Capa Plata)
# Limpieza, validación y ruteo de datos (Silver + Quarantine)

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, concat_ws, current_timestamp
from delta import *

# %%
builder = SparkSession.builder \
    .appName("Lab_SECOP_Silver_Quality_Gate") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# %%
# Leer Bronce
df_bronze = spark.read.format("delta").load("/app/data/lakehouse/bronze/secop")

# %%
# Transformaciones base (tipado y normalización)
# Leer Bronce
df_bronze = spark.read.format("delta").load("/app/data/lakehouse/bronze/secop")

# Tipado de fecha (columna REAL)
df_base = (
    df_bronze
    .withColumn(
        "fecha_firma",
        to_date(col("fecha_de_firma"), "yyyy-MM-dd")
    )
)


# %%
# QUALITY GATE – reglas de validación
df_validated = df_base.withColumn(
    "motivo_rechazo",
    concat_ws(", ",
        when(col("precio_base") <= 0, "Precio base inválido"),
        when(col("fecha_firma").isNull(), "Fecha de firma nula")
    )
)
# Registro válido si NO tiene motivo de rechazo

df_validated = df_validated.withColumn(
    "es_valido",
    col("motivo_rechazo") == ""
)



# %%
# SPLIT: válidos vs inválidos
df_silver = (
    df_validated
    .filter(col("es_valido"))
    .select(
        "entidad",
        "departamento",
        "precio_base",
        "fecha_firma"
    )
)

df_quarantine = (
    df_validated
    .filter(~col("es_valido"))
    .withColumn("fecha_cuarentena", current_timestamp())
    .select(
        "entidad",
        "departamento",
        "precio_base",
        "fecha_firma",
        "motivo_rechazo",
        "fecha_cuarentena"
    )
)

# %%
# Escribir SILVER
df_silver.write.format("delta") \
    .mode("overwrite") \
    .save("/app/data/lakehouse/silver/secop")

# %%
# Escribir QUARANTINE
df_quarantine.write.format("delta") \
    .mode("append") \
    .save("/app/data/lakehouse/quarantine/secop_errors")


# %%
print(f"✅ Registros válidos (Silver): {df_silver.count()}")
print(f"❌ Registros en cuarentena: {df_quarantine.count()}")

df_silver.show(5, truncate=False)
df_quarantine.show(5, truncate=False)
