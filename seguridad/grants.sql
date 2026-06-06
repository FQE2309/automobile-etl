-- =============================================================
-- seguridad/grants.sql
-- GRANTs, usuarios y grupos del proyecto automobile_etl
-- Ejecutar en Databricks SQL o via notebook
-- =============================================================

-- ============================================================
-- 1. CREAR GRUPOS en Unity Catalog
-- ============================================================
-- Nota: Los grupos se crean desde Account Console → Groups
-- Aquí documentamos los grupos requeridos:
-- 
--   analysts         → Consumidores de dashboards (Power BI)
--   data_engineers   → Equipo de ingeniería de datos
--   etl_service      → Service account que ejecuta el pipeline

-- ============================================================
-- 2. GRANTS a nivel de CATALOG
-- ============================================================

-- Dar acceso al catalog a ambos grupos
GRANT USE CATALOG ON CATALOG dbw_dataengineering_sandbox_brazilsouth_01 TO `analysts`;
GRANT USE CATALOG ON CATALOG dbw_dataengineering_sandbox_brazilsouth_01 TO `data_engineers`;
GRANT USE CATALOG ON CATALOG dbw_dataengineering_sandbox_brazilsouth_01 TO `etl_service`;

-- ============================================================
-- 3. GRANTS a nivel de SCHEMA
-- ============================================================

-- Analistas: SOLO pueden ver Gold (datos listos para consumo)
GRANT USE SCHEMA ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.gold TO `analysts`;
GRANT SELECT    ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.gold TO `analysts`;

-- Data Engineers: Bronze + Silver + Gold (lectura)
GRANT USE SCHEMA ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.bronze TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.gold   TO `data_engineers`;

GRANT SELECT ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.bronze TO `data_engineers`;
GRANT SELECT ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.silver TO `data_engineers`;
GRANT SELECT ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.gold   TO `data_engineers`;

-- Data Engineers: pueden escribir en Bronze y Silver
GRANT MODIFY ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.bronze TO `data_engineers`;
GRANT MODIFY ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.silver TO `data_engineers`;

-- ETL Service Account: acceso total para ejecutar el pipeline
GRANT ALL PRIVILEGES ON CATALOG dbw_dataengineering_sandbox_brazilsouth_01 TO `etl_service`;

-- ============================================================
-- 4. GRANTS a nivel de TABLA (granularidad fina)
-- ============================================================

-- Tabla específica para el dashboard de Power BI
GRANT SELECT ON TABLE dbw_dataengineering_sandbox_brazilsouth_01.gold.avg_price_by_make        TO `analysts`;
GRANT SELECT ON TABLE dbw_dataengineering_sandbox_brazilsouth_01.gold.body_style_summary        TO `analysts`;
GRANT SELECT ON TABLE dbw_dataengineering_sandbox_brazilsouth_01.gold.fuel_efficiency_ranking   TO `analysts`;
GRANT SELECT ON TABLE dbw_dataengineering_sandbox_brazilsouth_01.gold.price_segment_analysis    TO `analysts`;

-- ============================================================
-- 5. VERIFICAR PERMISOS OTORGADOS
-- ============================================================

SHOW GRANTS ON CATALOG dbw_dataengineering_sandbox_brazilsouth_01;
SHOW GRANTS ON SCHEMA dbw_dataengineering_sandbox_brazilsouth_01.gold;
SHOW GRANTS ON TABLE dbw_dataengineering_sandbox_brazilsouth_01.gold.avg_price_by_make;
