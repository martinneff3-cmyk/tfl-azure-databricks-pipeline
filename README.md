# tfl-azure-databricks-pipeline

**An end-to-end data engineering project built with Azure Databricks, PySpark, Delta Lake, and Lakeflow Declarative Pipelines to ingest, transform, and analyze public transport service data from the Transport for London (TfL) Unified API.**

## Project Overview

This project implements an event-driven data engineering pipeline that processes public transport data from the Transport for London (TfL) Unified API using Azure Databricks, PySpark, and Lakeflow Declarative Pipelines.

A separate, scheduled API ingestion job simulates an external data provider by periodically delivering raw JSON files to cloud object storage.

The Lakeflow pipeline operates independently of the API job. New files are detected through a **File Arrival Trigger**, which initiates processing through the Medallion Architecture (Bronze, Silver, Gold).

The pipeline processes transport line information, primary and secondary service statuses, and disruption data across multiple TfL transport modes.

The resulting Gold Materialized Views provide curated datasets for a Databricks AI/BI Dashboard, supporting current service monitoring, historical status analysis, and monthly line comparisons.

The project was originally developed in Azure Databricks using Azure Storage and subsequently migrated to Databricks Free Edition with Amazon S3 for continued historical data collection.

## Technology Stack

| Technology | Purpose |
|---|---|
| Azure Databricks | Data engineering and analytics platform |
| Apache Spark / PySpark | Distributed data transformations |
| Lakeflow Declarative Pipelines | Declarative Bronze, Silver, and Gold processing |
| Delta Lake | Storage format for processed tables |
| Azure Storage | Cloud object storage for simulated external file delivery |
| Databricks Jobs | Scheduled API data delivery and independently triggered pipeline execution |
| Unity Catalog | Data organization and governance |
| Databricks SQL / AI/BI Dashboards | SQL-based analytics and visualization |
| Git / GitHub | Version control and project documentation |

## Data Source

**Transport for London Unified API**

The pipeline retrieves data from TfL's public API, including:

- **Transport Lines:** Information about TfL lines and their associated transport modes.
- **Line Statuses:** Primary and secondary service statuses, including severity codes and descriptive information.
- **Disruptions:** Information about reported service disruptions.

The ingestion process performs approximately 720 API requests per execution, collecting the responses and uploading the raw data to Azure Storage.

Source: [TfL Unified API](https://api.tfl.gov.uk/)

## Data Architecture

## Data Architecture

```text
           TfL Unified API
                  |
                  v
       Scheduled API Ingestion Job
       (External Data Simulation)
                  |
                  v
         Cloud Object Storage
       (Azure Storage / Amazon S3)
                  |
                  | New JSON files arrive
                  v
          File Arrival Trigger
                  |
                  v
       Independent Lakeflow Pipeline
                  |
                  v
          +------------------+
          | Bronze Layer     |
          | Raw Ingestion    |
          +------------------+
                  |
                  v
          +------------------+
          | Silver Layer     |
          | Streaming Tables |
          | Data Validation  |
          | Normalization    |
          +------------------+
                  |
                  v
          +------------------+
          | Gold Layer       |
          | Materialized     |
          | Views            |
          | Analytics        |
          +------------------+
                  |
                  v
       Databricks AI/BI Dashboard
```

### Event-Driven Processing

The project separates data delivery from data processing to simulate a common enterprise data engineering scenario.

A scheduled notebook job periodically retrieves TfL API data and writes the resulting JSON files to cloud object storage. This job acts as a simulated external data provider.

The Lakeflow pipeline is managed independently and uses a File Arrival Trigger to detect incoming data. It does not depend on the execution status of the API ingestion job.

A one-minute wait after the last detected file change provides an additional buffer before pipeline execution.

This design demonstrates decoupled ingestion, event-driven processing, and the ability to process newly delivered files without directly orchestrating the upstream data provider.

### Bronze Layer – Raw Data Ingestion

The Bronze layer ingests the raw API datasets from Azure Storage.

It preserves the source information and adds ingestion metadata to support downstream processing and the identification of related data snapshots.

### Silver Layer – Data Transformation

The Silver layer processes the raw data using **Lakeflow Streaming Tables**.

The transformations include:

- Parsing and flattening nested JSON structures.
- Extracting and normalizing relevant attributes.
- Separating primary and secondary service status information.
- Preserving multiple secondary status records associated with the same transport line.
- Preparing structured datasets for downstream joins and aggregations.
- Applying data quality expectations.

One important challenge was handling multiple secondary statuses for a single line. The initial assumption of one secondary status per line required revisiting the source data and adjusting the Silver transformation logic to prevent information loss.

### Gold Layer – Analytical Data Models

The Gold layer uses **Lakeflow Materialized Views** to create curated datasets for reporting and analysis.

Three analytical tables are implemented:

| Gold Table | Description |
|---|---|
| `line_status_snapshot` | Provides the latest observed line statuses, including primary and aggregated secondary status information. |
| `line_status_daily` | Aggregates historical status observations by line, date, and status category. |
| `line_reliability` | Calculates monthly status observation counts and percentages for comparing transport lines. |

#### Snapshot Processing

The snapshot table combines related Silver datasets using line identifiers and a shared ingestion timestamp.

Secondary status information is aggregated to preserve the intended granularity of one row per line in the latest snapshot, without losing additional status information.

#### Historical Status Analysis

The daily and monthly datasets support the analysis of recurring service conditions and observed delay patterns.

**Important:** The calculated metrics represent observations collected from the TfL API. They are not official TfL reliability statistics and do not directly measure the actual duration or number of individual service disruptions.

For example, multiple consecutive observations of `Minor Delays` may refer to the same ongoing disruption.

## Databricks AI/BI Dashboard

The project includes an interactive dashboard built using the curated Gold datasets.

The dashboard provides:

- **Current Service Overview:** Latest observed service conditions across TfL lines.
- **Status Distribution:** Breakdown of observed service statuses, including additional visibility for rare statuses.
- **Daily Delay Trend:** Percentage of observations reporting Minor or Severe Delays over time.
- **Monthly Line Comparison:** Comparison of Underground lines based on their observed delay percentages.

The SQL queries and analytical definitions were developed as part of the project. Databricks AI assistance was also used to customize dashboard visualizations and layouts.

## Dashboard Screenshots

### Initial Deployment

![TfL Dashboard - Initial Deployment](Screenshots/tfl_dashboard_screenshot_01.png)


> This screenshot shows the dashboard shortly after its initial deployment. Historical data collection had only recently started, so time-series visualizations contain a limited number of observations.

Further screenshots will be added as additional historical data becomes available.

## Project Structure

The repository separates ingestion, table definitions, transformation logic, and setup scripts.

The Databricks pipeline source files are organized by Medallion layer, with additional files for schemas and Gold transformations.

A setup notebook provides the SQL DDL required to initialize the relevant database objects.

## Key Engineering Challenges

### Handling Nested API Data

The TfL API returns nested JSON responses that require parsing, schema handling, and normalization before they can be used for analytics.

### Multiple Secondary Statuses

Some transport lines can contain multiple secondary status records within a single snapshot.

The transformation logic was adjusted to preserve all relevant records while avoiding incorrect counts in the Gold layer.

### Consistent Snapshot Identification

Related datasets originate from separate ingestion and transformation paths.

A shared ingestion timestamp is used to associate corresponding observations and prevent historical status information from being incorrectly included in the latest snapshot.

### Designing Meaningful Metrics

Status observations are not equivalent to individual disruption events.

The Gold layer therefore distinguishes between observed service statuses and actual disruption frequency, avoiding misleading interpretations of the aggregated data.

## Deployment and Configuration

The project was originally developed and tested in **Azure Databricks**, using Azure Storage as the raw data landing zone.

Following the initial GitHub release, the project was successfully redeployed to a separate **Databricks Free Edition workspace**, using Amazon S3 for raw file storage.

The same Git-versioned Bronze, Silver, and Gold transformation code is used across both environments, while storage configuration, secrets, and workspace-specific resources are configured separately.

The deployment includes:

- Unity Catalog setup through a DDL notebook.
- Cloud object storage access and Secret Scope configuration.
- A scheduled API ingestion job that simulates external data delivery.
- An independently configured Lakeflow pipeline with a File Arrival Trigger.
- Databricks AI/BI Dashboard datasets and visualizations.

Environment-specific credentials and secrets are not included in this repository.

## Future Improvements – Version 2

Planned enhancements include:

- API ingestion logging with request timestamps, response status codes, and error information.
- Improved traceability between API requests, raw files, and processed datasets.
- CDC processing using Lakeflow capabilities where appropriate.
- Data quarantine for invalid records instead of failing the entire pipeline.
- Additional data quality checks and operational monitoring.

These improvements will extend the initial implementation toward a more robust and production-oriented data pipeline.

## Project Purpose

This project was developed as a hands-on data engineering portfolio project to apply Apache Spark, Azure Databricks, and Lakeflow concepts to a real external data source.

It demonstrates the implementation of an end-to-end data pipeline, from API ingestion and structured data transformation to analytical modeling and dashboard reporting.

The project also provides practical experience with data quality, historical snapshot processing, pipeline orchestration, version control, and cloud infrastructure.
