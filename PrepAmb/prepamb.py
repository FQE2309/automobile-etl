# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Preparación de Ambiente
# MAGIC 
# MAGIC Este notebook crea toda la infraestructura necesaria en Unity Catalog:
# MAGIC - Catalog
# MAGIC - Schemas (capas Bronze, Silver, Gold)
# MAGIC - External Location (apunta al ADLS Gen2)
# MAGIC - Tablas externas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables de Configuración
# MAGIC Cambia estos valores según tu entorno de Azure

# COMMAND ----------

# ============================================================
# CONFIGURACIÓN - Ajusta estos valores a tu entorno
# ============================================================

# Nombre de tu Storage Account en Azure
STORAGE_ACCOUNT = "dbstoragefeqqbg7db2pey"          # Cámbialo por el tuyo

# Nombre del contenedor en ADLS Gen2
CONTAINER_NAME  = "unity-catalog-storage"

# Nombre de tu External Location en Unity Catalog
EXTERNAL_LOCATION = "ext_loc_automobiles"

# Catalog principal del proyecto
CATALOG_NAME = "dbw_dataengineering_sandbox_brazilsouth_01"

# Schemas por capa Medallion
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

# Ruta base del ADLS Gen2
ADLS_BASE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

print("✅ Variables configuradas:")
print(f"   Storage Account : {STORAGE_ACCOUNT}")
print(f"   Container       : {CONTAINER_NAME}")
print(f"   ADLS Base Path  : {ADLS_BASE_PATH}")
print(f"   Catalog         : {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear el Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
    COMMENT 'Catalog principal del proyecto ETL de Automóviles'
""")

print(f"✅ Catalog '{CATALOG_NAME}' creado o ya existía")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear los Schemas (Bronze, Silver, Gold)

# COMMAND ----------

# Schema Bronze - datos crudos tal como vienen del origen
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_BRONZE}
    COMMENT 'Capa Bronze: datos crudos ingestados desde ADLS Gen2'
""")
print(f"✅ Schema '{CATALOG_NAME}.{SCHEMA_BRONZE}' creado")

# Schema Silver - datos limpios y transformados
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_SILVER}
    COMMENT 'Capa Silver: datos limpios y enriquecidos'
""")
print(f"✅ Schema '{CATALOG_NAME}.{SCHEMA_SILVER}' creado")

# Schema Gold - datos agregados listos para consumo
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_GOLD}
    COMMENT 'Capa Gold: datos agregados para dashboards y reportes'
""")
print(f"✅ Schema '{CATALOG_NAME}.{SCHEMA_GOLD}' creado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear External Location
# MAGIC 
# MAGIC > ⚠️ **Importante:** La External Location debe estar creada en el Unity Catalog
# MAGIC > a través del portal de Databricks (Account Console → Data → External Locations)
# MAGIC > usando **Managed Identity** como credential.
# MAGIC > 
# MAGIC > Este paso SOLO verifica que la External Location ya existe.

# COMMAND ----------

# Verificar que la External Location existe
try:
    result = spark.sql(f"SHOW EXTERNAL LOCATIONS").filter(f"name = '{EXTERNAL_LOCATION}'")
    count = result.count()
    
    if count > 0:
        print(f"✅ External Location '{EXTERNAL_LOCATION}' encontrada")
        result.show(truncate=False)
    else:
        print(f"⚠️  External Location '{EXTERNAL_LOCATION}' NO encontrada.")
        print(f"    Crea la External Location en: Account Console → Data → External Locations")
        print(f"    URL: {ADLS_BASE_PATH}/")
        print(f"    Credential: Managed Identity (no usar SAS ni Service Principal)")
        
except Exception as e:
    print(f"⚠️  No se pudo verificar la External Location: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Definir Rutas de Trabajo

# COMMAND ----------

# Rutas en ADLS Gen2 por capa
PATH_RAW_AUTOMOBILES  = f"{ADLS_BASE_PATH}/automobiles/automobiles.csv"
PATH_RAW_FUEL         = f"{ADLS_BASE_PATH}/automobiles/fuel_economy.csv"
PATH_BRONZE           = f"{ADLS_BASE_PATH}/bronze/"
PATH_SILVER           = f"{ADLS_BASE_PATH}/silver/"
PATH_GOLD             = f"{ADLS_BASE_PATH}/gold/"

print("📁 Rutas configuradas:")
print(f"   RAW Automobiles : {PATH_RAW_AUTOMOBILES}")
print(f"   RAW Fuel Economy: {PATH_RAW_FUEL}")
print(f"   Bronze          : {PATH_BRONZE}")
print(f"   Silver          : {PATH_SILVER}")
print(f"   Gold            : {PATH_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Guardar configuración como widgets para otros notebooks

# COMMAND ----------

# Crear widgets que los otros notebooks pueden leer con dbutils.widgets.get()
dbutils.widgets.text("catalog_name",   CATALOG_NAME)
dbutils.widgets.text("schema_bronze",  SCHEMA_BRONZE)
dbutils.widgets.text("schema_silver",  SCHEMA_SILVER)
dbutils.widgets.text("schema_gold",    SCHEMA_GOLD)
dbutils.widgets.text("adls_base_path", ADLS_BASE_PATH)
dbutils.widgets.text("storage_account", STORAGE_ACCOUNT)
dbutils.widgets.text("container_name", CONTAINER_NAME)

print("✅ Widgets creados para uso en otros notebooks")
print("   Usa dbutils.widgets.get('catalog_name') para acceder a los valores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Preparación de Ambiente Completada
# MAGIC 
# MAGIC | Recurso | Estado |
# MAGIC |---------|--------|
# MAGIC | Catalog `dbw_dataengineering_sandbox_brazilsouth_01` | ✅ Creado |
# MAGIC | Schema `bronze` | ✅ Creado |
# MAGIC | Schema `silver` | ✅ Creado |
# MAGIC | Schema `gold` | ✅ Creado |
# MAGIC | External Location | ✅ Verificado |
# MAGIC 
# MAGIC **Siguiente paso:** Ejecutar `proceso/01_extract.py`
