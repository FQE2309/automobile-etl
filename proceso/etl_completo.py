# Databricks notebook source
# MAGIC %md
# MAGIC # 🚗 ETL Automobile Market — Pipeline Completo
# MAGIC 
# MAGIC Pipeline Medallion completo: Bronze → Silver → Gold
# MAGIC 
# MAGIC **Catalog:** `dbw_dataengineering_sandbox_brazilsouth_01`
# MAGIC 
# MAGIC **Prerequisito:** Subir los CSV via Ingesta de datos:
# MAGIC - `automobiles.csv` → `bronze.automobiles_raw`
# MAGIC - `fuel_economy.csv` → `bronze.fuel_economy_raw`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window

CATALOG = "dbw_dataengineering_sandbox_brazilsouth_01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparación de Ambiente

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

print("✅ Schemas listos:")
print(f"   {CATALOG}.bronze")
print(f"   {CATALOG}.silver")
print(f"   {CATALOG}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extract — Capa Bronze
# MAGIC 
# MAGIC Leer las tablas cargadas via Ingesta de datos y agregar metadata de auditoría.

# COMMAND ----------

df_auto_raw = spark.table(f"{CATALOG}.bronze.automobiles_raw")
df_fuel_raw = spark.table(f"{CATALOG}.bronze.fuel_economy_raw")

# Agregar columnas de auditoría Bronze
df_auto_bronze = (df_auto_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("automobiles.csv"))
    .withColumn("_source_layer", F.lit("raw"))
)

df_fuel_bronze = (df_fuel_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.lit("fuel_economy.csv"))
    .withColumn("_source_layer", F.lit("raw"))
)

# Sobrescribir con metadata de auditoría
(df_auto_bronze.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.bronze.automobiles_raw"))

(df_fuel_bronze.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.bronze.fuel_economy_raw"))

print(f"✅ Bronze automobiles_raw  : {df_auto_bronze.count()} filas")
print(f"✅ Bronze fuel_economy_raw : {df_fuel_bronze.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transform — Capa Silver
# MAGIC 
# MAGIC Limpieza, tipado, normalización, columnas derivadas y JOIN.

# COMMAND ----------

df_auto = spark.table(f"{CATALOG}.bronze.automobiles_raw")
df_fuel = spark.table(f"{CATALOG}.bronze.fuel_economy_raw")

# Reemplazar "?" por null en todas las columnas string
df_clean = df_auto
for field in df_auto.schema.fields:
    if isinstance(field.dataType, StringType):
        df_clean = df_clean.withColumn(field.name,
            F.when(F.col(field.name) == "?", None).otherwise(F.col(field.name)))

# Castear tipos correctos
df_clean = (df_clean
    .withColumn("symboling",         F.col("symboling").cast(IntegerType()))
    .withColumn("normalized_losses", F.col("normalized_losses").cast(IntegerType()))
    .withColumn("curb_weight",       F.col("curb_weight").cast(IntegerType()))
    .withColumn("engine_size",       F.col("engine_size").cast(IntegerType()))
    .withColumn("horsepower",        F.col("horsepower").cast(IntegerType()))
    .withColumn("peak_rpm",          F.col("peak_rpm").cast(IntegerType()))
    .withColumn("city_mpg",          F.col("city_mpg").cast(IntegerType()))
    .withColumn("highway_mpg",       F.col("highway_mpg").cast(IntegerType()))
    .withColumn("wheel_base",        F.col("wheel_base").cast(DoubleType()))
    .withColumn("length",            F.col("length").cast(DoubleType()))
    .withColumn("width",             F.col("width").cast(DoubleType()))
    .withColumn("height",            F.col("height").cast(DoubleType()))
    .withColumn("bore",              F.col("bore").cast(DoubleType()))
    .withColumn("stroke",            F.col("stroke").cast(DoubleType()))
    .withColumn("compression_ratio", F.col("compression_ratio").cast(DoubleType()))
    .withColumn("price",             F.col("price").cast(DoubleType()))
    .withColumn("make",       F.lower(F.trim(F.col("make"))))
    .withColumn("fuel_type",  F.lower(F.trim(F.col("fuel_type"))))
    .withColumn("body_style", F.lower(F.trim(F.col("body_style"))))
    .dropDuplicates()
)

# Columnas derivadas
df_silver = (df_clean
    .withColumn("price_per_hp",
        F.when((F.col("horsepower").isNotNull()) & (F.col("horsepower") > 0),
               F.round(F.col("price") / F.col("horsepower"), 2)).otherwise(None))
    .withColumn("price_category",
        F.when(F.col("price") < 8000,  F.lit("economy"))
         .when(F.col("price") < 15000, F.lit("mid-range"))
         .when(F.col("price") < 25000, F.lit("premium"))
         .when(F.col("price").isNotNull(), F.lit("luxury"))
         .otherwise(F.lit("unknown")))
    .withColumn("combined_mpg",
        F.round(F.col("city_mpg") * 0.55 + F.col("highway_mpg") * 0.45, 1))
    .withColumn("is_turbo",
        F.when(F.col("aspiration") == "turbo", True).otherwise(False))
    .withColumn("_silver_timestamp", F.current_timestamp())
    .drop("_ingestion_timestamp", "_source_file", "_source_layer")
)

# Preparar fuel_economy para JOIN
df_fuel_clean = (df_fuel
    .withColumn("make", F.lower(F.trim(F.col("make"))))
    .withColumn("model_year",           F.col("model_year").cast(IntegerType()))
    .withColumn("avg_city_mpg",         F.col("avg_city_mpg").cast(IntegerType()))
    .withColumn("avg_highway_mpg",      F.col("avg_highway_mpg").cast(IntegerType()))
    .withColumn("co2_emissions_gkm",    F.col("co2_emissions_gkm").cast(IntegerType()))
    .withColumn("fuel_cost_annual_usd", F.col("fuel_cost_annual_usd").cast(IntegerType()))
    .withColumn("popularity_score",     F.col("popularity_score").cast(IntegerType()))
    .withColumn("safety_rating",        F.col("safety_rating").cast(IntegerType()))
    .filter(F.col("model_year") == 1985)
    .drop("model_year", "_ingestion_timestamp", "_source_file", "_source_layer")
    .withColumnRenamed("avg_city_mpg",         "fe_avg_city_mpg")
    .withColumnRenamed("avg_highway_mpg",      "fe_avg_highway_mpg")
    .withColumnRenamed("co2_emissions_gkm",    "fe_co2_emissions_gkm")
    .withColumnRenamed("fuel_cost_annual_usd", "fe_fuel_cost_annual_usd")
    .withColumnRenamed("market_segment",       "fe_market_segment")
    .withColumnRenamed("popularity_score",     "fe_popularity_score")
    .withColumnRenamed("safety_rating",        "fe_safety_rating")
)

# LEFT JOIN por make
df_enriched = df_silver.join(df_fuel_clean, on="make", how="left")

(df_enriched.write.format("delta").mode("overwrite")
    .option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.silver.automobiles_enriched"))

print(f"✅ Silver automobiles_enriched : {df_enriched.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load — Capa Gold
# MAGIC 
# MAGIC 4 tablas analíticas agregadas para dashboards.

# COMMAND ----------

df_silver = spark.table(f"{CATALOG}.silver.automobiles_enriched")

# Gold 1: Precio promedio por marca
df_g1 = (df_silver
    .filter(F.col("price").isNotNull())
    .groupBy("make", "fe_market_segment")
    .agg(
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.min("price"), 0).alias("min_price"),
        F.round(F.max("price"), 0).alias("max_price"),
        F.count("*").alias("total_models"),
        F.round(F.avg("horsepower"), 0).alias("avg_horsepower"),
        F.round(F.avg("combined_mpg"), 1).alias("avg_combined_mpg"),
        F.round(F.avg("fe_popularity_score"), 0).alias("avg_popularity"),
        F.round(F.avg("fe_safety_rating"), 1).alias("avg_safety_rating")
    )
    .orderBy(F.col("avg_price").desc())
    .withColumn("_gold_timestamp", F.current_timestamp())
)
(df_g1.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.gold.avg_price_by_make"))
print(f"✅ gold.avg_price_by_make       : {df_g1.count()} filas")

# Gold 2: Resumen por carrocería
df_g2 = (df_silver
    .groupBy("body_style")
    .agg(
        F.count("*").alias("total_autos"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.avg("curb_weight"), 0).alias("avg_weight_kg"),
        F.round(F.avg("combined_mpg"), 1).alias("avg_mpg"),
        F.round(F.avg("horsepower"), 0).alias("avg_hp"),
        F.sum(F.col("is_turbo").cast("int")).alias("total_turbo")
    )
    .orderBy(F.col("total_autos").desc())
    .withColumn("_gold_timestamp", F.current_timestamp())
)
(df_g2.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.gold.body_style_summary"))
print(f"✅ gold.body_style_summary      : {df_g2.count()} filas")

# Gold 3: Ranking eficiencia combustible
window_rank = Window.orderBy(F.col("avg_combined_mpg").desc())
df_g3 = (df_silver
    .filter(F.col("combined_mpg").isNotNull())
    .groupBy("make", "fuel_type")
    .agg(
        F.round(F.avg("combined_mpg"), 1).alias("avg_combined_mpg"),
        F.round(F.avg("city_mpg"), 1).alias("avg_city_mpg"),
        F.round(F.avg("highway_mpg"), 1).alias("avg_highway_mpg"),
        F.round(F.avg("fe_co2_emissions_gkm"), 0).alias("avg_co2_gkm"),
        F.round(F.avg("fe_fuel_cost_annual_usd"), 0).alias("avg_fuel_cost_usd"),
        F.count("*").alias("num_models")
    )
    .withColumn("efficiency_rank", F.rank().over(window_rank))
    .orderBy("efficiency_rank")
    .withColumn("_gold_timestamp", F.current_timestamp())
)
(df_g3.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.gold.fuel_efficiency_ranking"))
print(f"✅ gold.fuel_efficiency_ranking : {df_g3.count()} filas")

# Gold 4: Análisis por segmento de precio
df_g4 = (df_silver
    .filter(F.col("price_category") != "unknown")
    .groupBy("price_category", "fe_market_segment")
    .agg(
        F.count("*").alias("total_autos"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.avg("horsepower"), 0).alias("avg_hp"),
        F.round(F.avg("combined_mpg"), 1).alias("avg_mpg"),
        F.round(F.avg("fe_safety_rating"), 1).alias("avg_safety"),
        F.round(F.avg("fe_popularity_score"), 0).alias("avg_popularity"),
        F.round(F.sum(F.col("is_turbo").cast("int")) * 100.0 / F.count("*"), 1).alias("pct_turbo")
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
)
(df_g4.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    .saveAsTable(f"{CATALOG}.gold.price_segment_analysis"))
print(f"✅ gold.price_segment_analysis  : {df_g4.count()} filas")

print("\n🎉 Pipeline ETL completo — Bronze → Silver → Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Grants

# COMMAND ----------

grants = [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `analysts`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `analysts`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `analysts`",
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.bronze TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.silver TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.gold TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.bronze TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.silver TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG}.gold TO `data_engineers`",
]

for sql in grants:
    try:
        spark.sql(sql)
        print(f"✅ {sql[:70]}")
    except Exception as e:
        print(f"⚠️  (skipped) {str(e)[:60]}")

print("\n✅ Grants aplicados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resumen Final
# MAGIC 
# MAGIC | Capa | Tabla | Descripción |
# MAGIC |---|---|---|
# MAGIC | Bronze | `automobiles_raw` | Datos crudos con auditoría |
# MAGIC | Bronze | `fuel_economy_raw` | Datos crudos con auditoría |
# MAGIC | Silver | `automobiles_enriched` | Limpio + JOIN + columnas derivadas |
# MAGIC | Gold | `avg_price_by_make` | Precio por marca |
# MAGIC | Gold | `body_style_summary` | Métricas por carrocería |
# MAGIC | Gold | `fuel_efficiency_ranking` | Ranking eficiencia |
# MAGIC | Gold | `price_segment_analysis` | Análisis por segmento |
