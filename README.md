# AWS Serverless Data Lake Pipeline — Nurse Staffing Analytics

## Overview

This project implements a fully serverless, end-to-end data pipeline on AWS that ingests real-world healthcare staffing data from Google Drive, processes it through a structured multi-layer data lake, enforces data quality rules, and exposes analytics-ready datasets for querying and visualization.

Unlike simple ETL projects, this pipeline is designed to reflect real production systems, with a strong focus on reliability, traceability, and data integrity.

Key capabilities include:

- Automated ingestion of large external datasets (~214 MB) using AWS Lambda
- Structured data lake architecture (raw → refined → validation → curated)
- Explicit schema enforcement and data standardization
- Data quality validation with quarantine handling
- Partitioned Parquet datasets for efficient querying
- End-to-end orchestration using AWS Step Functions
- Monitoring, alerting, and failure recovery using CloudWatch, SNS, and SQS

---

## Why This Project Matters

In real-world data engineering, pipelines must do more than just move data — they must ensure that data is:

- **Reliable** → no silent failures or partial loads  
- **Traceable** → every dataset can be tied back to its origin  
- **Reproducible** → reruns do not corrupt or duplicate data  
- **Observable** → failures are detectable and debuggable  

This project was designed with those principles in mind, incorporating production-grade patterns that are often missing from portfolio projects.

---

## Manifest-Driven Ingestion (Key Design Feature)

A critical component of this pipeline is the use of **manifest files** during ingestion.

After each ingestion run, the pipeline generates a manifest file that records:

- File name
- Source location
- Ingestion timestamp
- S3 destination path
- Run identifier

### Why Manifests Matter

Manifests enable several important capabilities:

#### 1. Idempotency
The pipeline can safely rerun without duplicating or reprocessing the same files.

#### 2. Traceability
Every dataset in the data lake can be traced back to its exact ingestion event.

#### 3. Debugging & Recovery
If a downstream failure occurs, the system can identify exactly which files were processed and replay only those datasets.

#### 4. Auditability
Provides a clear audit trail of what data was ingested, when, and from where — a requirement in many real-world data systems.

#### 5. Decoupled Processing
Downstream jobs (Glue, Athena, etc.) can rely on manifests instead of scanning entire S3 directories, improving performance and consistency.

---

## Architecture Diagram

![Architecture Diagram](architecture/architecture_diagram.png)

---

## High-Level Data Flow

1. AWS Lambda retrieves data from Google Drive  
2. Raw data is stored in Amazon S3  
3. AWS Step Functions orchestrates the pipeline  
4. AWS Glue processes data through multiple layers:  
   - Refined  
   - Validation  
   - Curated  
5. Amazon Athena provides querying capabilities  
6. Amazon QuickSight delivers dashboards  
7. CloudWatch + SNS handle monitoring and alerts  
8. SQS captures failed executions (dead-letter queue)  

---

## Tech Stack

- AWS Lambda
- Amazon S3
- AWS Step Functions
- AWS Glue
- Amazon Athena
- Amazon QuickSight
- Amazon CloudWatch
- Amazon SNS
- Amazon SQS
- AWS Secrets Manager
- Python

---

## Data Sources

### Nurse Staffing Data (Fact Table)

- One row per provider per day
- Includes staffing hours and patient census

### Provider Information Data (Dimension Table)

- Provider metadata
- Includes:
  - Provider ID (CCN)
  - Bed count
  - Hospital affiliation
  - Rating

---

## Data Lake Architecture

![S3 Data Lake](images/s3_data_lake.png)

### Raw Layer

- Stores original CSV files
- Immutable
- Partitioned by ingestion date

### Refined Layer

- Standardized column names
- Explicit data typing
- Preserved leading zeroes for 'provnum' field
- Added metadata fields

### Curated Layer

- Analytics-ready datasets
- Stored as Parquet
- Partitioned by:
  - work_year
  - work_month

---

## Step Functions Orchestration

![Step Functions Workflow](images/step_functions_workflow.png)

Pipeline flow:

1. Invoke ingestion Lambda
2. Run refine Glue job
3. Run validation Glue job
4. Run curated Glue job

### Features

- Retry logic
- Error handling (Catch)
- CloudWatch logging
- SQS dead-letter queue integration

---

## Lambda Ingestion

- Uses Google Drive API (OAuth)
- Credentials stored in Secrets Manager
- Streams large files (~214 MB)
- Optimized for memory and speed

---

## Glue ETL Jobs

### Refine Job

- Standardizes schema
- Casts data types
- Preserves identifiers
- Adds metadata columns

### Validation Job

- Referential integrity checks
- Null checks
- Schema validation

#### Key Finding

- 1,547 rows missing provider references

#### Solution

Instead of dropping data:

- Created inferred provider records
- Added flags:
  - provider_reference_gap_flag
  - provider_reference_source

### Curated Job

- Creates fact and dimension tables
- Derives partition columns
- Writes Parquet output

---

## Data Modeling

### Fact Table: fact_staffing_daily

- Grain: provider per day
- Contains staffing and census data

### Dimension Table: dim_provider

- Provider attributes
- Includes ratings, bed counts, hospital flags

---

## Athena Layer

![Athena Tables](images/athena_tables.png)

- Tables created via manual DDL
- Partitioned by year and month
- Optimized for query performance

---

## QuickSight Dashboard

![QuickSight Dashboard Part 1](images/quicksight_dashboard_1.png)
![QuickSight Dashboard Part 2](images/quicksight_dashboard_2.png)

### Metrics

- Hours Reported by Nurse Type
- Employee vs Contractor Hours
- Avg Overall Rating by Providers Located In/Out of Hospitals
- Count of Providers Located In/Out of Hospitals
- Total Hours Reported by Provider

---

## Monitoring & Alerting

![CloudWatch Logs - Lambda Function](images/cloudwatch_logs_lambda.png)
![CloudWatch Alarms](images/cloudwatch_alarms.png)

- CloudWatch logs for all services (only Lambda logs shown above)
- SNS alerts for failures

---

## Dead Letter Queue (SQS)

![Dead Letter Queue](images/sqs_dead_letter_queue.png)

- Captures failed pipeline executions
- Stores error context for debugging
- Enables replay and recovery

---

## Key Design Decisions

### Step Functions vs Glue Workflows

Chosen for:
- Better error handling
- Native retries
- Cross-service orchestration

### Manual DDL vs Crawlers

Chosen for:
- Strict schema control
- Predictability
- Avoiding inference errors

### Handling Missing Data

Some provider information was missing, so instead of dropping:

- Created inferred dimension records
- Flagged them for transparency

---

## Scalability

- Lambda optimized for large file ingestion
- Glue scales horizontally
- Step Functions supports high concurrency

---

## Results

This pipeline successfully:

- Ingests external data securely
- Enforces data quality
- Handles edge cases
- Produces analytics-ready datasets
- Supports real-time business insights

---

## What This Project Demonstrates

- End-to-end data pipeline design
- AWS serverless architecture expertise
- Data modeling and warehousing
- Real-world data quality handling
- Observability and reliability patterns

---

## Future Improvements

- Add CI/CD pipeline (GitHub Actions)
- Implement data cataloging (Glue Data Catalog enhancements)
- Add automated data quality dashboards
- Introduce streaming ingestion for real-time processing

---

## Author

Built by Johnathon Smith, a data engineer focused on designing production-ready, scalable data systems.
