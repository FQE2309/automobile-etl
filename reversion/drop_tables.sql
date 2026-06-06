-- =============================================================
-- reversion/drop_tables.sql
-- Script para eliminar TODAS las tablas del proyecto
-- ⚠️  CUIDADO: esto borra todos los datos
-- Usar solo para rollback o limpieza del ambiente
-- =============================================================

-- ============================================================
-- 1. DROP tablas GOLD
-- ============================================================
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.gold.avg_price_by_make;
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.gold.body_style_summary;
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.gold.fuel_efficiency_ranking;
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.gold.price_segment_analysis;

-- ============================================================
-- 2. DROP tablas SILVER
-- ============================================================
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.silver.automobiles_enriched;

-- ============================================================
-- 3. DROP tablas BRONZE
-- ============================================================
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.bronze.automobiles_raw;
DROP TABLE IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.bronze.fuel_economy_raw;

-- ============================================================
-- 4. DROP schemas
-- ============================================================
DROP SCHEMA IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.gold   CASCADE;
DROP SCHEMA IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.silver CASCADE;
DROP SCHEMA IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01.bronze CASCADE;

-- ============================================================
-- 5. DROP catalog
-- ============================================================
-- ⚠️  Descomenta esta línea solo si quieres eliminar TODO
-- DROP CATALOG IF EXISTS dbw_dataengineering_sandbox_brazilsouth_01 CASCADE;
