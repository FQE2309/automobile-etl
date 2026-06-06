# Databricks notebook source
# MAGIC %md
# MAGIC # 🗑️ Reversión del Proyecto
# MAGIC 
# MAGIC ⚠️ **ADVERTENCIA:** Este notebook elimina TODAS las tablas y schemas.
# MAGIC Solo ejecutar para rollback o limpieza total.

# COMMAND ----------

CATALOG_NAME = "dbw_dataengineering_sandbox_brazilsouth_01"

tablas_gold = [
    "avg_price_by_make",
    "body_style_summary",
    "fuel_efficiency_ranking",
    "price_segment_analysis"
]

tablas_silver = ["automobiles_enriched"]

tablas_bronze = ["automobiles_raw", "fuel_economy_raw"]

# COMMAND ----------

# Eliminar tablas Gold
for tabla in tablas_gold:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.gold.{tabla}")
    print(f"🗑️  Eliminada: {CATALOG_NAME}.gold.{tabla}")

# Eliminar tablas Silver
for tabla in tablas_silver:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.silver.{tabla}")
    print(f"🗑️  Eliminada: {CATALOG_NAME}.silver.{tabla}")

# Eliminar tablas Bronze
for tabla in tablas_bronze:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.bronze.{tabla}")
    print(f"🗑️  Eliminada: {CATALOG_NAME}.bronze.{tabla}")

# Eliminar schemas
for schema in ["gold", "silver", "bronze"]:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG_NAME}.{schema} CASCADE")
    print(f"🗑️  Eliminado schema: {CATALOG_NAME}.{schema}")

print("\n✅ Reversión completada")
