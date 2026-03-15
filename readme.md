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
netflix project architecture.png
