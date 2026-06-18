This project implements an end-to-end data engineering pipeline on AWS for processing and analyzing YouTube trending data. The objective was to ingest raw CSV and JSON datasets, store them in a scalable cloud environment, transform and optimize the data for analytical workloads, and generate business insights through interactive dashboards. The architecture follows a modern data engineering approach by separating data ingestion, storage, transformation, querying, and visualization into distinct layers.

## Services Used

**Amazon S3:** It was used as the centralized storage solution for the project. Raw CSV and JSON datasets were stored in S3 buckets, providing scalable, durable, and cost-effective storage. The processed datasets were also stored in S3 after transformation, creating a clear separation between raw and curated data.

**AWS Lambda:** It was used to perform event-driven data processing. Lambda functions were triggered automatically whenever new JSON files were uploaded to Amazon S3. The functions processed and converted the JSON data into Parquet format, enabling serverless data transformation and automation without the need to manage infrastructure.

**AWS Glue:** It was used as the ETL service for processing and integrating the datasets. Glue Crawlers were used to infer schemas and update the Data Catalog, while Glue Jobs performed data cleaning, transformation, and conversion of CSV files into optimized Parquet format. The ETL jobs also joined and merged the Parquet datasets generated from both the JSON and CSV sources, creating a unified curated dataset that was subsequently queried through Amazon Athena for analysis.

**Amazon Athena:** It was used as the analytics layer of the project. Athena enabled serverless SQL querying directly on the transformed data stored in Amazon S3, allowing efficient analysis without the need for dedicated database infrastructure.

**Power BI:** It was used for data visualization and dashboard creation. Power BI was connected to Amazon Athena using the Amazon Athena ODBC Driver, enabling direct querying of the curated datasets stored in Amazon S3. Interactive dashboards were developed to analyze YouTube trending metrics, category performance, regional trends, and other key insights.


## Project Workflow

```text
YouTube Dataset
          |
          ▼
Amazon S3
(Raw CSV & JSON Data)
          │
    ┌─────┴─────┐
    ▼            ▼
AWS Lambda   AWS Glue
(JSON →      (CSV →
 Parquet)     Parquet)
    │           |
    └─────┬─────┘
          ▼
AWS Glue ETL Job
(Parquet Datasets Join & Merge)
          │
          ▼
Amazon S3
(Curated Dataset)
          │
          ▼
Amazon Athena
(SQL Analytics & Querying)
          │
          ▼
Power BI
(Dashboard & Visualization)
```
