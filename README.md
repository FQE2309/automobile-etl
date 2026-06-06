# 🚗 ETL Automobile Market — Arquitectura Medallion en Azure Databricks

Pipeline automatizado de datos para análisis del mercado automotriz usando la arquitectura Medallion (Bronze → Silver → Gold) en Azure Databricks con CI/CD completo mediante GitHub Actions.

## 🎯 Descripción

Pipeline ETL que transforma datos crudos de características técnicas y eficiencia de combustible de automóviles, implementando la Arquitectura Medallion en Azure Databricks con Unity Catalog, Delta Lake y despliegue continuo.

## ✨ Características principales

- 🔄 **ETL automatizado** — pipeline completo con despliegue automático via GitHub Actions
- 🏗️ **Arquitectura Medallion** — separación clara Bronze → Silver → Gold
- 🔐 **Managed Identity** — conexión al storage sin credenciales expuestas
- 🗂️ **Unity Catalog** — gobernanza centralizada de datos y permisos
- ⚡ **Delta Lake** — transacciones ACID y time travel
- 📊 **Power BI** — dashboards conectados a la capa Gold
- 🚀 **CI/CD integrado** — deploy automático en cada push a main

## 🏛️ Arquitectura

[Ver arquitectura interactiva](dashboard/arquitectura_blueprint.html)

```
ADLS Gen2 (raw-data)
     │
     ▼  Managed Identity
┌─────────────────────────────────────────┐
│           Unity Catalog                 │
│  ┌────────┐  ┌────────┐  ┌──────────┐  │
│  │ BRONZE │─►│ SILVER │─►│   GOLD   │──┼──► Power BI
│  │  raw   │  │ clean  │  │aggregated│  │
│  └────────┘  └────────┘  └──────────┘  │
└─────────────────────────────────────────┘
```

## 📦 Fuentes de datos

| Dataset | Descripción | Registros |
|---|---|---|
| `automobiles.csv` | Características técnicas de autos (UCI ML Repository, 1985) | 205 |
| `fuel_economy.csv` | Eficiencia de combustible, CO2 y métricas de mercado por marca | 37 |

## 📁 Estructura del repositorio

```
automobile-etl/
├── datasets/               ← Fuentes de datos CSV
├── PrepAmb/
│   └── prepamb.py          ← Crea catalog, schemas y verifica External Location
├── proceso/
│   ├── 01_extract.py       ← Bronze: ingesta desde ADLS Gen2
│   ├── 02_transform.py     ← Silver: limpieza, tipos, JOIN entre datasets
│   ├── 03_load.py          ← Gold: 4 tablas analíticas agregadas
│   └── 04_grants.py        ← Permisos por grupos en Unity Catalog
├── seguridad/
│   └── grants.sql          ← GRANTs documentados
├── reversion/
│   ├── drop_tables.sql     ← DROP de todas las tablas
│   └── drop_tables.py      ← DROP via PySpark
├── .github/workflows/
│   └── etl_pipeline.yml    ← CI/CD: PrepAmb → Extract → Transform → Load → Grants
├── dashboard/              ← Screenshots y configuración del dashboard
├── certificaciones/        ← Certificaciones del equipo
└── evidencias/             ← Capturas de ejecuciones exitosas
```

## 🔄 Pipeline CI/CD

El pipeline se activa automáticamente con cada `push` a `main`:

```
Preparación    →    Extract    →    Transform    →    Load    →    Grants
  ambiente         (Bronze)        (Silver)          (Gold)
```

### Secrets requeridos en GitHub

| Secret | Descripción |
|---|---|
| `DATABRICKS_HOST` | URL del workspace: `https://adb-xxxxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal Access Token generado en Databricks |
| `DATABRICKS_CLUSTER_ID` | ID del cluster donde corren los notebooks |

## 📊 Capas Medallion

### 🥉 Bronze
Datos ingestados tal como llegan, con metadata de auditoría.

| Tabla | Origen |
|---|---|
| `bronze.automobiles_raw` | automobiles.csv |
| `bronze.fuel_economy_raw` | fuel_economy.csv |

### 🥈 Silver
Datos limpios, tipados y enriquecidos con JOIN entre los dos datasets.

| Tabla | Transformaciones |
|---|---|
| `silver.automobiles_enriched` | Nulos, tipos correctos, texto normalizado, columnas derivadas, JOIN |

**Columnas derivadas:** `price_per_hp`, `price_category`, `combined_mpg`, `is_turbo`

### 🥇 Gold
Tablas agregadas para consumo directo en Power BI.

| Tabla | Descripción |
|---|---|
| `gold.avg_price_by_make` | Precio promedio, HP y MPG por marca |
| `gold.body_style_summary` | Métricas por tipo de carrocería |
| `gold.fuel_efficiency_ranking` | Ranking de eficiencia de combustible |
| `gold.price_segment_analysis` | Análisis por segmento (economy / mid-range / premium / luxury) |

## 🔐 Seguridad

| Grupo | Acceso |
|---|---|
| `analysts` | SELECT en Gold |
| `data_engineers` | SELECT en Bronze/Silver/Gold + MODIFY en Bronze/Silver |
| `etl_service` | ALL PRIVILEGES |

## 🚀 Configuración y uso

### 1. Clonar el repositorio
```bash
git clone https://github.com/tu-usuario/automobile-etl
cd automobile-etl
```

### 2. Configurar secrets en GitHub
Settings → Secrets and variables → Actions → New repository secret

### 3. Despliegue automático
```bash
git add .
git commit -m "feat: pipeline ETL automobile market"
git push origin main
```
GitHub Actions ejecutará el pipeline completo automáticamente.

### 4. Ejecución manual en Databricks
Ejecutar en orden desde el workspace:
```
PrepAmb/prepamb.py
proceso/01_extract.py
proceso/02_transform.py
proceso/03_load.py
proceso/04_grants.py
```

## 📈 Dashboard

Conectado a `automobile_catalog.gold` via DirectQuery desde Power BI.

Ver capturas en: [dashboard/](dashboard/)

## 👤 Autor

**Fabian Quintero E.**
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/FQE2309)

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

---

**Tecnología:** Azure Databricks + Unity Catalog + Delta Lake + GitHub Actions

