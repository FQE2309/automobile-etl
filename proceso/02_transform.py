# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 02 - Transform (Capa Silver)
# MAGIC 
# MAGIC **Objetivo:** Limpiar, estandarizar y enriquecer los datos de Bronze.
# MAGIC 
# MAGIC Transformaciones aplicadas:
# MAGIC 1. Reemplazar "?" por `null` (valores faltantes enmascarados)
# MAGIC 2. Castear tipos de datos correctos (String → Double/Integer)
# MAGIC 3. Eliminar duplicados
# MAGIC 4. Normalizar texto (make en minúsculas, sin espacios)
# MAGIC 5. Calcular columnas derivadas (precio por HP, categoría precio)
# MAGIC 6. JOIN entre automobiles y fuel_economy
# MAGIC 7. Guardar como `silver.automobiles_enriched`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

try:
    CATALOG_NAME   = dbutils.widgets.get("catalog_name")
    SCHEMA_BRONZE  = dbutils.widgets.get("schema_bronze")
    SCHEMA_SILVER  = dbutils.widgets.get("schema_silver")
    ADLS_BASE_PATH = dbutils.widgets.get("adls_base_path")
except Exception:
    CATALOG_NAME   = "dbw_dataengineering_sandbox_brazilsouth_01"
    SCHEMA_BRONZE  = "bronze"
    SCHEMA_SILVER  = "silver"
    ADLS_BASE_PATH = "abfss://raw-data@stautomobilesetl.dfs.core.windows.net"

print(f"🔄 Transformando: {CATALOG_NAME}.{SCHEMA_BRONZE} → {CATALOG_NAME}.{SCHEMA_SILVER}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer desde Bronze

# COMMAND ----------

df_bronze_auto = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.automobiles_raw")
df_bronze_fuel = spark.table(f"{CATALOG_NAME}.{SCHEMA_BRONZE}.fuel_economy_raw")

print(f"📥 Bronze automobiles  : {df_bronze_auto.count()} filas")
print(f"📥 Bronze fuel_economy : {df_bronze_fuel.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpiar automobiles_raw

# COMMAND ----------

# Paso 2a: Reemplazar "?" por null en TODAS las columnas de tipo String
# Esto es una limpieza crucial para este dataset UCI

# Lista de columnas que pueden tener "?"
cols_con_interrogacion = [
    "normalized_losses", "num_doors", "bore", "stroke",
    "horsepower", "peak_rpm", "price"
]

df_clean = df_bronze_auto

# Reemplazar "?" por null en las columnas identificadas
for col in cols_con_interrogacion:
    df_clean = df_clean.withColumn(
        col,
        F.when(F.col(col) == "?", None).otherwise(F.col(col))
    )

print("✅ Paso 2a: '?' reemplazados por null")

# COMMAND ----------

# Paso 2b: Castear columnas numéricas a sus tipos correctos
df_clean = (
    df_clean
    # Enteros
    .withColumn("symboling",         F.col("symboling").cast(IntegerType()))
    .withColumn("normalized_losses", F.col("normalized_losses").cast(IntegerType()))
    .withColumn("curb_weight",       F.col("curb_weight").cast(IntegerType()))
    .withColumn("engine_size",       F.col("engine_size").cast(IntegerType()))
    .withColumn("horsepower",        F.col("horsepower").cast(IntegerType()))
    .withColumn("peak_rpm",          F.col("peak_rpm").cast(IntegerType()))
    .withColumn("city_mpg",          F.col("city_mpg").cast(IntegerType()))
    .withColumn("highway_mpg",       F.col("highway_mpg").cast(IntegerType()))
    # Decimales
    .withColumn("wheel_base",        F.col("wheel_base").cast(DoubleType()))
    .withColumn("length",            F.col("length").cast(DoubleType()))
    .withColumn("width",             F.col("width").cast(DoubleType()))
    .withColumn("height",            F.col("height").cast(DoubleType()))
    .withColumn("bore",              F.col("bore").cast(DoubleType()))
    .withColumn("stroke",            F.col("stroke").cast(DoubleType()))
    .withColumn("compression_ratio", F.col("compression_ratio").cast(DoubleType()))
    .withColumn("price",             F.col("price").cast(DoubleType()))
)

print("✅ Paso 2b: Tipos casteados correctamente")

# COMMAND ----------

# Paso 2c: Normalizar texto - make en lowercase y sin espacios extra
df_clean = (
    df_clean
    .withColumn("make", F.lower(F.trim(F.col("make"))))
    .withColumn("fuel_type", F.lower(F.trim(F.col("fuel_type"))))
    .withColumn("body_style", F.lower(F.trim(F.col("body_style"))))
    .withColumn("drive_wheels", F.lower(F.trim(F.col("drive_wheels"))))
)

print("✅ Paso 2c: Texto normalizado (lowercase + trim)")

# COMMAND ----------

# Paso 2d: Eliminar duplicados exactos
filas_antes = df_clean.count()
df_clean = df_clean.dropDuplicates()
filas_despues = df_clean.count()

print(f"✅ Paso 2d: Duplicados eliminados ({filas_antes - filas_despues} removidos)")
print(f"   Filas restantes: {filas_despues}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Columnas derivadas (Feature Engineering)

# COMMAND ----------

df_silver_auto = (
    df_clean
    # Precio por caballo de fuerza (eficiencia de valor)
    .withColumn(
        "price_per_hp",
        F.when(
            F.col("horsepower").isNotNull() & (F.col("horsepower") > 0),
            F.round(F.col("price") / F.col("horsepower"), 2)
        ).otherwise(None)
    )
    # Categoría de precio
    .withColumn(
        "price_category",
        F.when(F.col("price") < 8000,  F.lit("economy"))
         .when(F.col("price") < 15000, F.lit("mid-range"))
         .when(F.col("price") < 25000, F.lit("premium"))
         .when(F.col("price").isNotNull(), F.lit("luxury"))
         .otherwise(F.lit("unknown"))
    )
    # Eficiencia combinada de combustible
    .withColumn(
        "combined_mpg",
        F.round((F.col("city_mpg") * 0.55 + F.col("highway_mpg") * 0.45), 1)
    )
    # Flag de turbo
    .withColumn(
        "is_turbo",
        F.when(F.col("aspiration") == "turbo", True).otherwise(False)
    )
    # Columnas de auditoría Silver
    .withColumn("_silver_timestamp", F.current_timestamp())
    .withColumn("_silver_version", F.lit("1.0"))
    # Quitar columnas de auditoría Bronze (ya no las necesitamos)
    .drop("_ingestion_timestamp", "_source_file", "_source_layer")
)

print("✅ Columnas derivadas creadas:")
print("   - price_per_hp      (precio / HP)")
print("   - price_category    (economy / mid-range / premium / luxury)")
print("   - combined_mpg      (55% city + 45% highway)")
print("   - is_turbo          (boolean)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Limpiar y preparar fuel_economy para el JOIN

# COMMAND ----------

df_silver_fuel = (
    df_bronze_fuel
    .withColumn("make",                F.lower(F.trim(F.col("make"))))
    .withColumn("model_year",          F.col("model_year").cast(IntegerType()))
    .withColumn("avg_city_mpg",        F.col("avg_city_mpg").cast(IntegerType()))
    .withColumn("avg_highway_mpg",     F.col("avg_highway_mpg").cast(IntegerType()))
    .withColumn("co2_emissions_gkm",   F.col("co2_emissions_gkm").cast(IntegerType()))
    .withColumn("fuel_cost_annual_usd", F.col("fuel_cost_annual_usd").cast(IntegerType()))
    .withColumn("popularity_score",    F.col("popularity_score").cast(IntegerType()))
    .withColumn("safety_rating",       F.col("safety_rating").cast(IntegerType()))
    .drop("_ingestion_timestamp", "_source_file", "_source_layer")
)

print(f"✅ fuel_economy limpio: {df_silver_fuel.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. JOIN: automobiles + fuel_economy
# MAGIC 
# MAGIC Usamos LEFT JOIN por `make` para enriquecer cada auto con
# MAGIC métricas de mercado (CO2, costo combustible, popularidad, etc.)

# COMMAND ----------

# Renombrar columnas de fuel para evitar conflictos en el JOIN
df_fuel_renamed = (
    df_silver_fuel
    .withColumnRenamed("avg_city_mpg",        "fe_avg_city_mpg")
    .withColumnRenamed("avg_highway_mpg",      "fe_avg_highway_mpg")
    .withColumnRenamed("co2_emissions_gkm",    "fe_co2_emissions_gkm")
    .withColumnRenamed("fuel_cost_annual_usd", "fe_fuel_cost_annual_usd")
    .withColumnRenamed("market_segment",       "fe_market_segment")
    .withColumnRenamed("popularity_score",     "fe_popularity_score")
    .withColumnRenamed("safety_rating",        "fe_safety_rating")
)

# JOIN por make (el dataset de autos no tiene model_year explícito,
# usamos el promedio de fuel_economy para 1985)
df_fuel_1985 = df_fuel_renamed.filter(F.col("model_year") == 1985).drop("model_year")

df_enriched = (
    df_silver_auto
    .join(
        df_fuel_1985,
        on="make",
        how="left"     # LEFT JOIN: conservamos todos los autos aunque no tengan match
    )
)

print(f"✅ JOIN completado: {df_enriched.count()} filas enriquecidas")
print(f"   Columnas finales: {len(df_enriched.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Guardar en Silver

# COMMAND ----------

(
    df_enriched.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_SILVER}.automobiles_enriched")
)

print(f"✅ Tabla '{CATALOG_NAME}.{SCHEMA_SILVER}.automobiles_enriched' guardada en Silver")

# COMMAND ----------

# Verificación rápida
print("\n📊 Muestra de datos Silver:")
spark.table(f"{CATALOG_NAME}.{SCHEMA_SILVER}.automobiles_enriched") \
    .select("make", "body_style", "price", "price_category", "combined_mpg", 
            "is_turbo", "fe_market_segment", "fe_safety_rating") \
    .show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Transform Completado
# MAGIC 
# MAGIC | Transformación | Resultado |
# MAGIC |---|---|
# MAGIC | Nulos ("?") → null | ✅ |
# MAGIC | Tipos correctos | ✅ |
# MAGIC | Texto normalizado | ✅ |
# MAGIC | Duplicados eliminados | ✅ |
# MAGIC | Columnas derivadas | ✅ 4 nuevas |
# MAGIC | JOIN con fuel_economy | ✅ LEFT JOIN por make |
# MAGIC 
# MAGIC **Siguiente paso:** Ejecutar `proceso/03_load.py`
