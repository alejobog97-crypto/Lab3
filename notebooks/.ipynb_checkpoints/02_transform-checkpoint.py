# =========================================
# 02_transformación_Silver.py
# Ingesta SECOP → Delta Bronze
# =========================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace,
    to_timestamp, concat_ws, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DecimalType,
    LongType, TimestampType
)
from delta import *

# -----------------------------------------
# Spark Session
# -----------------------------------------

builder = SparkSession.builder \
    .appName("SECOP_Silver_Quality_Gate") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -----------------------------------------
# Leer Bronce
# -----------------------------------------
df_bronze = spark.read.format("delta") \
    .load("/app/data/lakehouse/bronze/secop")

# -----------------------------------------
# Esquema
# -----------------------------------------

silver_schema = StructType([
    StructField("nombre_entidad", StringType(), True),
    StructField("departamento", StringType(), True),
    StructField("ciudad", StringType(), True),

    StructField("valor_del_contrato", DecimalType(18, 2), True),
    StructField("valor_pagado", DecimalType(18, 2), True),
    StructField("saldo_cdp", DecimalType(18, 2), True),

    StructField("fecha_firma", TimestampType(), True),

    StructField("codigo_entidad", LongType(), True),
    StructField("nit_entidad", LongType(), True)
])

# -----------------------------------------
# Transformación + tipado
# -----------------------------------------

df_silver_base = (
    df_bronze
    .withColumn(
        "valor_del_contrato",
        regexp_replace(col("valor_del_contrato"), ",", "")
        .cast(DecimalType(18, 2))
    )
    .withColumn(
        "valor_pagado",
        regexp_replace(col("valor_pagado"), ",", "")
        .cast(DecimalType(18, 2))
    )
    .withColumn(
        "saldo_cdp",
        regexp_replace(col("saldo_cdp"), ",", "")
        .cast(DecimalType(18, 2))
    )
    .withColumn(
        "fecha_firma",
        to_timestamp(col("fecha_de_firma"))
    )
    .withColumn("codigo_entidad", col("codigo_entidad").cast(LongType()))
    .withColumn("nit_entidad", col("nit_entidad").cast(LongType()))
    .select([f.name for f in silver_schema.fields])
)

# -----------------------------------------
# Quality Gate
# -----------------------------------------

df_validated = df_silver_base.withColumn(
    "motivo_rechazo",
    concat_ws(", ",
        when(col("valor_del_contrato") <= 0, "Valor del contrato inválido"),
        when(col("fecha_firma").isNull(), "Fecha de firma nula")
    )
)

df_validated = df_validated.withColumn(
    "es_valido",
    col("motivo_rechazo") == ""
)

df_silver = (
    df_validated
    .filter(col("es_valido"))
    .drop("motivo_rechazo", "es_valido")
)

df_quarantine = (
    df_validated
    .filter(~col("es_valido"))
    .withColumn("fecha_cuarentena", current_timestamp())
)

# -----------------------------------------
# Escritura
# -----------------------------------------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/app/data/lakehouse/silver/secop")

df_quarantine.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/app/data/lakehouse/quarantine/secop_errors")


# -----------------------------------------
# Resultados
# -----------------------------------------
print(f"✅ Registros Silver: {df_silver.count()}")
print(f"❌ Registros en cuarentena: {df_quarantine.count()}")

df_silver.show(5, truncate=False)
df_quarantine.show(5, truncate=False)