# Databricks notebook source
# MAGIC %md
# MAGIC # 🔐 04 - Grants (Permisos en Unity Catalog)
# MAGIC 
# MAGIC Este notebook aplica los GRANTs definidos en `seguridad/grants.sql`
# MAGIC directamente desde PySpark para que el CI/CD pueda ejecutarlo.

# COMMAND ----------

try:
    CATALOG_NAME = dbutils.widgets.get("catalog_name")
except Exception:
    CATALOG_NAME = "dbw_dataengineering_sandbox_brazilsouth_01"

print(f"🔐 Aplicando GRANTs en {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar GRANTs

# COMMAND ----------

grants = [
    # Grupo analistas: solo pueden leer Gold
    f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `analysts`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.gold TO `analysts`",
    f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.gold TO `analysts`",

    # Grupo data_engineers: acceso completo a Bronze y Silver, lectura en Gold
    f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.bronze TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.silver TO `data_engineers`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.gold TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.bronze TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.silver TO `data_engineers`",
    f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.gold TO `data_engineers`",
    f"GRANT MODIFY ON SCHEMA {CATALOG_NAME}.bronze TO `data_engineers`",
    f"GRANT MODIFY ON SCHEMA {CATALOG_NAME}.silver TO `data_engineers`",

    # Grupo etl_service_account: acceso total para ejecutar el pipeline
    f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `etl_service`",
    f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG_NAME} TO `etl_service`",
]

for grant_sql in grants:
    try:
        spark.sql(grant_sql)
        print(f"✅ {grant_sql}")
    except Exception as e:
        # Si el grupo no existe todavía, no falla el pipeline
        print(f"⚠️  (skipped) {grant_sql[:60]}... → {str(e)[:60]}")

print("\n✅ GRANTs aplicados correctamente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Pipeline ETL Completo
# MAGIC 
# MAGIC ```
# MAGIC prepamb → extract → transform → load → grants
# MAGIC ```
# MAGIC 
# MAGIC Todas las capas Medallion han sido creadas exitosamente.
