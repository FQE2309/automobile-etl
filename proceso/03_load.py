# Databricks notebook source
# MAGIC %md
# MAGIC # 📤 03 - Load (Capa Gold)
# MAGIC 
# MAGIC **Objetivo:** Crear tablas agregadas y analíticas en Gold
# MAGIC listas para ser consumidas por dashboards (Power BI).
# MAGIC 
# MAGIC Tablas Gold:
# MAGIC - `gold.avg_price_by_make`         → precio promedio por marca
# MAGIC - `gold.body_style_summary`        → resumen por tipo de carrocería
# MAGIC - `gold.fuel_efficiency_ranking`   → ranking de eficiencia de combustible
# MAGIC - `gold.price_segment_analysis`    → análisis por segmento de precio

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

try:
    CATALOG_NAME  = dbutils.widgets.get("catalog_name")
    SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
    SCHEMA_GOLD   = dbutils.widgets.get("schema_gold")
except Exception:
    CATALOG_NAME  = "dbw_dataengineering_sandbox_brazilsouth_01"
    SCHEMA_SILVER = "silver"
    SCHEMA_GOLD   = "gold"

print(f"📤 Cargando: {CATALOG_NAME}.{SCHEMA_SILVER} → {CATALOG_NAME}.{SCHEMA_GOLD}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Leer desde Silver
df_silver = spark.table(f"{CATALOG_NAME}.{SCHEMA_SILVER}.automobiles_enriched")
print(f"📥 Silver leído: {df_silver.count()} filas, {len(df_silver.columns)} columnas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Gold 1: Precio promedio por marca
# MAGIC 
# MAGIC Responde: ¿Cuáles son las marcas más caras y más baratas?

# COMMAND ----------

df_gold_price_by_make = (
    df_silver
    .filter(F.col("price").isNotNull())    # solo autos con precio conocido
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

(
    df_gold_price_by_make.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_GOLD}.avg_price_by_make")
)

print(f"✅ gold.avg_price_by_make guardada ({df_gold_price_by_make.count()} filas)")
df_gold_price_by_make.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Gold 2: Resumen por tipo de carrocería

# COMMAND ----------

df_gold_body_style = (
    df_silver
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

(
    df_gold_body_style.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_GOLD}.body_style_summary")
)

print(f"✅ gold.body_style_summary guardada ({df_gold_body_style.count()} filas)")
df_gold_body_style.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Gold 3: Ranking de eficiencia de combustible

# COMMAND ----------

from pyspark.sql.window import Window

# Window function para calcular el ranking por make
window_rank = Window.orderBy(F.col("avg_combined_mpg").desc())

df_gold_fuel_ranking = (
    df_silver
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

(
    df_gold_fuel_ranking.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_GOLD}.fuel_efficiency_ranking")
)

print(f"✅ gold.fuel_efficiency_ranking guardada ({df_gold_fuel_ranking.count()} filas)")
df_gold_fuel_ranking.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla Gold 4: Análisis por segmento de precio

# COMMAND ----------

df_gold_segments = (
    df_silver
    .filter(F.col("price_category") != "unknown")
    .groupBy("price_category", "fe_market_segment")
    .agg(
        F.count("*").alias("total_autos"),
        F.round(F.avg("price"), 0).alias("avg_price"),
        F.round(F.avg("horsepower"), 0).alias("avg_hp"),
        F.round(F.avg("combined_mpg"), 1).alias("avg_mpg"),
        F.round(F.avg("engine_size"), 0).alias("avg_engine_cc"),
        F.round(F.avg("fe_safety_rating"), 1).alias("avg_safety"),
        F.round(F.avg("fe_popularity_score"), 0).alias("avg_popularity"),
        # Contar cuántos son turbo en cada segmento
        F.round(
            F.sum(F.col("is_turbo").cast("int")) * 100.0 / F.count("*"), 1
        ).alias("pct_turbo")
    )
    # Ordenar por precio de menor a mayor
    .orderBy(
        F.when(F.col("price_category") == "economy",   1)
         .when(F.col("price_category") == "mid-range", 2)
         .when(F.col("price_category") == "premium",   3)
         .when(F.col("price_category") == "luxury",    4)
         .otherwise(5)
    )
    .withColumn("_gold_timestamp", F.current_timestamp())
)

(
    df_gold_segments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG_NAME}.{SCHEMA_GOLD}.price_segment_analysis")
)

print(f"✅ gold.price_segment_analysis guardada ({df_gold_segments.count()} filas)")
df_gold_segments.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Load Completado
# MAGIC 
# MAGIC | Tabla Gold | Descripción | Uso en Dashboard |
# MAGIC |---|---|---|
# MAGIC | `gold.avg_price_by_make` | Precio por marca + KPIs | Gráfico de barras comparativo |
# MAGIC | `gold.body_style_summary` | Métricas por carrocería | Tabla resumen / pie chart |
# MAGIC | `gold.fuel_efficiency_ranking` | Ranking de eficiencia | Top N barras horizontales |
# MAGIC | `gold.price_segment_analysis` | Segmentos de mercado | Treemap / scatter |
# MAGIC 
# MAGIC **Siguiente paso:** Ejecutar `proceso/04_grants.py`
