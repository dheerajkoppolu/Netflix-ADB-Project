# Azure Databricks Delta Live Tables Project

This project demonstrates a modern Lakehouse architecture using Azure Databricks.

Pipeline Architecture:

Raw Data → Bronze → Silver → Gold

Technologies Used:
- Azure Databricks
- Delta Live Tables
- PySpark
- ADLS Gen2
- Azure Data Factory
- Power BI

Pipeline Flow:
1. Raw data ingestion using Autoloader
2. Bronze layer for raw storage
3. Silver layer for transformations
4. Gold layer using Delta Live Tables
5. Star schema for analytics

Architecture:
                ┌────────────────────────────┐
                │        Source Systems       │
                │  Files / APIs / Databases   │
                └──────────────┬─────────────┘
                               │
                               │
                 ┌─────────────▼─────────────┐
                 │      Azure Data Factory    │
                 │      (Orchestration)       │
                 └─────────────┬─────────────┘
                               │
                               ▼
                    ┌───────────────────────┐
                    │     Raw Data Store     │
                    │     ADLS Gen2          │
                    │     Bronze Layer       │
                    │  Incremental Ingestion │
                    └──────────┬────────────┘
                               │
                               ▼
                   ┌─────────────────────────┐
                   │      Delta Live Tables   │
                   │                         │
                   │   ┌─────────────────┐   │
                   │   │   Silver Layer   │   │
                   │   │ Data Cleansing   │   │
                   │   │ Transformations  │   │
                   │   └────────┬────────┘   │
                   │            │            │
                   │            ▼            │
                   │   ┌─────────────────┐   │
                   │   │    Gold Layer    │   │
                   │   │    Star Schema   │   │
                   │   │ Business Models  │   │
                   │   └────────┬────────┘   │
                   └────────────┼────────────┘
                                │
                                ▼
                 ┌──────────────────────────┐
                 │     Data Warehouse        │
                 │      Azure Synapse        │
                 └─────────────┬────────────┘
                               │
                               ▼
                     ┌──────────────────┐
                     │      Reporting     │
                     │      Power BI      │
                     └──────────────────┘
