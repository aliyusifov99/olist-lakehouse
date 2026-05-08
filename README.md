# olist-lakehouse

> Production-grade Databricks lakehouse on **GCP** — Olist Brazilian e-commerce dataset, **Medallion architecture**, imperative + declarative pipelines side-by-side.

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD4?logo=delta&logoColor=white)](https://delta.io/)
[![GCP](https://img.shields.io/badge/GCP-4285F4?logo=googlecloud&logoColor=white)](https://cloud.google.com/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621)](https://www.databricks.com/product/unity-catalog)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

![Architecture overview](images/architecture-overview.svg)

---

## Overview

End-to-end lakehouse on Databricks (GCP) that ingests the Olist e-commerce dataset, transforms it through Bronze/Silver/Gold layers, and serves analytics through Unity Catalog–governed dashboards and Delta Sharing.

The project intentionally implements the same medallion **twice** — once imperatively (notebooks + Auto Loader) and once declaratively (Lakeflow Spark Declarative Pipelines, formerly DLT) — to compare the two paradigms on the same data.

**Headline findings** surfaced during the analytics phase:

- Customers reward **delivery speed**, not punctuality (month-grain correlation = **−0.91**).
- Brazilian e-commerce is **hub-and-spoke** — SP exports 1.73× its imports; 8 states are pure consumers.
- **97% of customers buy exactly once** — marketplace structure breaks textbook RFM.
- Two operational crisis months: **Dec 2017** and **March 2018** (20.5% late rate, avg score 3.78).

---

## Architecture

```
Kaggle CSVs → GCS landing → Bronze (Auto Loader) → Silver (ETL) → Gold (analytics)
                                                                       │
                                                          ┌────────────┼────────────┐
                                                          ▼            ▼            ▼
                                                     Dashboards    Alerts    Delta Sharing
```

| Layer | Tech | Purpose |
|---|---|---|
| **Bronze** | Auto Loader, Delta | Raw, source-faithful (typos & dupes preserved) |
| **Silver** | PySpark + SQL UDFs | Cleaned, typed, enriched |
| **Gold** | Spark SQL | Business-ready aggregates |
| **Serving** | SQL Warehouse, AI/BI Dashboards, Delta Sharing | BI & external recipients |
| **Governance** | Unity Catalog | RBAC, tags, comments, audit |
| **IaC** | Databricks Asset Bundles | Schemas + SDP pipeline as code |

![Unity Catalog schemas](images/unity-catalog.png)

---

## Project Phases

| # | Phase | Path |
|---|---|---|
| 0 | Environment setup, catalog & schemas | [setup/phase_0_setup/](setup/phase_0_setup/) |
| 1 | Bronze ingestion (Auto Loader) | [notebooks/phase_1_bronze_ingestion/](notebooks/phase_1_bronze_ingestion/) |
| 2 | Silver transforms (10 notebooks, 33 quality checks) | [notebooks/phase_2_silver_transforms/](notebooks/phase_2_silver_transforms/) |
| 3 | Gold analytics (RFM, delivery, revenue, geo) | [notebooks/phase_3_gold_analytics/](notebooks/phase_3_gold_analytics/) |
| 4 | Lakeflow Spark Declarative Pipelines (DLT) | [pipelines/phase_4_dlt_pipelines/](pipelines/phase_4_dlt_pipelines/) |
| 5 | SQL Warehouse, dashboards & alerts | [notebooks/phase_5_sql_warehouse_dashboards_and_alerts/](notebooks/phase_5_sql_warehouse_dashboards_and_alerts/) |
| 6 | Unity Catalog governance | [notebooks/phase_6_governance/](notebooks/phase_6_governance/) |
| 7 | Delta Sharing | [notebooks/phase_7_delta_sharing/](notebooks/phase_7_delta_sharing/) |
| 8 | Security audit & secret scopes | [notebooks/phase_8_security_audit/](notebooks/phase_8_security_audit/) |
| 9 | Databricks Asset Bundles (IaC) | [bundle/](bundle/) · [databricks.yml](databricks.yml) |

---

## Pipelines: Imperative vs. Declarative

![SDP pipeline DAG](images/sdp-pipeline-dag.png)

The same medallion is built two ways:

- **Imperative** ([notebooks/](notebooks/)) — explicit `read → transform → write`, ordering controlled by the operator.
- **Declarative** ([pipelines/phase_4_dlt_pipelines/](pipelines/phase_4_dlt_pipelines/)) — `@dp.materialized_view` definitions, dependency graph and ordering inferred by SDP.

A side-by-side write-up lives in [04_comparison_notebook.py](pipelines/phase_4_dlt_pipelines/04_comparison_notebook.py).

---

## Dashboards

Three AI/BI Dashboard tabs served from a Serverless SQL Warehouse over Gold tables.

| Tab | Focus | PDF |
|---|---|---|
| 1 | Revenue & Categories | [dashboard/revenue_and_categories.pdf](dashboard/revenue_and_categories.pdf) |
| 2 | Delivery Performance | [dashboard/delivery_performance.pdf](dashboard/delivery_performance.pdf) |
| 3 | Customers, Sellers & Geography | [dashboard/customers_sellers_and_geography.pdf](dashboard/customers_sellers_and_geography.pdf) |

![Dashboard — Revenue & Categories](images/dashboard-revenue.png)
![Dashboard — Delivery Performance](images/dashboard-delivery.png)
![Dashboard — Customers & Geography](images/dashboard-geography.png)

---

## Tech Stack

- **Cloud:** Google Cloud Platform (GCS landing zone)
- **Platform:** Databricks on GCP
- **Storage:** Delta Lake on Unity Catalog
- **Compute:** Serverless SQL Warehouse, all-purpose clusters, SDP pipeline
- **Languages:** PySpark, Spark SQL, Python
- **IaC:** Databricks Asset Bundles (YAML)
- **Governance:** Unity Catalog (tags, comments, RBAC, audit logs)
- **Sharing:** Delta Sharing (open protocol)

---

## Quickstart

```bash
# 1. Download Olist dataset from Kaggle and upload CSVs to GCS
gsutil -m cp *.csv gs://<your-bucket>/landing/

# 2. Validate the bundle
databricks bundle validate --target dev --var databricks_host=https://<workspace-host>

# 3. Deploy schemas + SDP pipeline
databricks bundle deploy --target dev

# 4. Run the SDP medallion pipeline
databricks bundle run olist_medallion_pipeline --target dev
```

Detailed bootstrap & data instructions: [data/README.md](data/README.md) · [bundle/README.md](bundle/README.md).

---

## Repository Layout

```
.
├── bundle/                       # Databricks Asset Bundle docs
├── dashboard/                    # Exported dashboard PDFs
├── data/                         # Dataset bootstrap instructions (CSVs gitignored)
├── databricks.yml                # Bundle entry point
├── docs/                         # Audit checklist & examples
├── images/                       # README screenshots
├── notebooks/                    # Phase 1, 2, 3, 5, 6, 7, 8 notebooks
├── pipelines/phase_4_dlt_pipelines/  # SDP (Lakeflow) declarative pipelines
├── resources/                    # Bundle resource definitions (schemas, pipeline)
├── setup/phase_0_setup/          # Catalog & schema bootstrap
└── precommit_audit.sh            # Credential / secret leak check
```

---

## Security

Pre-commit audit script ([precommit_audit.sh](precommit_audit.sh)) scans for tokens, hardcoded hosts, and PAT leaks before each commit. See [docs/audit_checklist.md](docs/audit_checklist.md).

---

## Dataset & License

Brazilian E-Commerce Public Dataset by Olist — [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (CC BY-NC-SA 4.0). Project code: [MIT](LICENSE).
