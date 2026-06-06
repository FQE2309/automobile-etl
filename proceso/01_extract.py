# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 01 - Extract (Capa Bronze)
# MAGIC 
# MAGIC **Objetivo:** Leer los archivos CSV desde ADLS Gen2 (capa RAW)
# MAGIC y guardarlos como tablas Delta en la capa Bronze, SIN transformaciones.
# MAGIC 
# MAGIC - Fuente 1: `automobiles.csv` → `bronze.automobiles_raw`
# MAGIC - Fuente 2: `fuel_economy.csv` → `bronze.fuel_economy_raw`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

# Leer parámetros (vienen del workflow o se definen aquí para pruebas locales)
try:
    CATALOG_NAME    = dbutils.widgets.get("catalog_name")
    SCHEMA_BRONZE   = dbutils.widgets.get("schema_bronze")
    ADLS_BASE_PATH  = dbutils.widgets.get("adls_base_path")
except Exception:
    # Valores por defecto si ejecutas el notebook manualmente
    CATALOG_NAME    = "dbw_dataengineering_sandbox_brazilsouth_01"
    SCHEMA_BRONZE   = "bronze"
    ADLS_BASE_PATH  = "abfss://unity-catalog-storage@dbstoragefeqqbg7db2pey.dfs.core.windows.net"

# Rutas de los archivos RAW en ADLS Gen2
PATH_RAW_AUTOMOBILES = f"{ADLS_BASE_PATH}/automobiles/automobiles.csv"
PATH_RAW_FUEL        = f"{ADLS_BASE_PATH}/automobiles/fuel_economy.csv"

print(f"📂 Leyendo desde: {ADLS_BASE_PATH}")
print(f"🏷️  Destino: {CATALOG_NAME}.{SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importar librerías necesarias

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Extraer: automobiles.csv → Bronze

# COMMAND ----------

# Definir el schema explícito para evitar que Spark infiera tipos incorrectos
# Nota: usamos StringType para columnas con "?" que son nulos enmascarados
schema_automobiles = StructType([
    StructField("symboling",          StringType(), True),
    StructField("normalized_losses",  StringType(), True),   # tiene "?" 
    StructField("make",               StringType(), True),
    StructField("fuel_type",          StringType(), True),
    StructField("aspiration",         StringType(), True),
    StructField("num_doors",          StringType(), True),   # tiene "?"
    StructField("body_style",         StringType(), True),
    StructField("drive_wheels",       StringType(), True),
    StructField("engine_location",    StringType(), True),
    StructField("wheel_base",         StringType(), True),
    StructField("length",             StringType(), True),
    StructField("width",              StringType(), True),
    StructField("height",             StringType(), True),
    StructField("curb_weight",        StringType(), True),
    StructField("engine_type",        StringType(), True),
    StructField("num_cylinders",      StringType(), True),
    StructField("engine_size",        StringType(), True),
    StructField("fuel_system",        StringType(), True),
    StructField("bore",               StringType(), True),
    StructField("stroke",             StringType(), True),
    StructField("compression_ratio",  StringType(), True),
    StructField("horsepower",         StringType(), True),   # tiene "?"
    StructField("peak_rpm",           StringType(), True),
    StructField("city_mpg",           StringType(), True),
    StructField("highway_mpg",        StringType(), True),
    StructField("price",              StringType(), True),   # tiene "?"
])

# Leer el CSV desde ADLS Gen2 usando Managed Identity (configurada en External Location)
df_automobiles_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("schema", schema_automobiles)    # usamos schema explícito
    .load(PATH_RAW_AUTOMOBILES)
)

# Agregar columnas de auditoría (cuándo y desde dónde se ingirió el dato)
df_automobiles_bronze = df_automobiles_raw \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit("automobiles.csv")) \
    .withColumn("_source_layer", F.lit("raw"))

print(f"✅ automobiles.csv leído correctamente")
print(f"   Filas    : {df_automobiles_bronze.count()}")
print(f"   Columnas : {len(df_automobiles_bronze.columns)}")
df_automobiles_bronze.printSchema()

# COMMAND ----------

# Guardar en Bronze como tabla Delta en Unity Catalog
(
    df_automobiles_bronze.write
    .format("delta")
    .mode("overwrite")                        # sobreescribe si ya existe
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.automobiles_raw")
)

print(f"✅ Tabla '{CATALOG_NAME}.{SCHEMA_BRONZE}.automobiles_raw' guardada en Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extraer: fuel_economy.csv → Bronze

# COMMAND ----------

schema_fuel = StructType([
    StructField("make",                StringType(), True),
    StructField("model_year",          StringType(), True),
    StructField("avg_city_mpg",        StringType(), True),
    StructField("avg_highway_mpg",     StringType(), True),
    StructField("co2_emissions_gkm",   StringType(), True),
    StructField("fuel_cost_annual_usd", StringType(), True),
    StructField("market_segment",      StringType(), True),
    StructField("popularity_score",    StringType(), True),
    StructField("safety_rating",       StringType(), True),
])

df_fuel_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(schema_fuel)
    .load(PATH_RAW_FUEL)
)

df_fuel_bronze = df_fuel_raw \
    .withColumn("_ingestion_timestamp", F.current_timestamp()) \
    .withColumn("_source_file", F.lit("fuel_economy.csv")) \
    .withColumn("_source_layer", F.lit("raw"))

print(f"✅ fuel_economy.csv leído correctamente")
print(f"   Filas    : {df_fuel_bronze.count()}")
print(f"   Columnas : {len(df_fuel_bronze.columns)}")

# COMMAND ----------

(
    df_fuel_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.fuel_economy_raw")
)

print(f"✅ Tabla '{CATALOG_NAME}.{SCHEMA_BRONZE}.fuel_economy_raw' guardada en Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Extract Completado
# MAGIC 
# MAGIC | Tabla Bronze | Origen | Filas |
# MAGIC |---|---|---|
# MAGIC | `bronze.automobiles_raw` | automobiles.csv | ~205 |
# MAGIC | `bronze.fuel_economy_raw` | fuel_economy.csv | ~37 |
# MAGIC 
# MAGIC **Siguiente paso:** Ejecutar `proceso/02_transform.py`
