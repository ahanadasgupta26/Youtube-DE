This project implements an end-to-end data engineering pipeline on AWS to process YouTube trending data. The pipeline ingests raw CSV and JSON data into Amazon S3, transforms the data into optimized Parquet format using AWS Lambda and AWS Glue, and enables analytical querying through Amazon Athena. The curated dataset is then visualized using Power BI by connecting directly to Athena.


## Services Used

Amazon S3 – Data storage (raw & processed layers)
AWS Lambda – JSON to Parquet transformation
AWS Glue – Data catalog & schema management
Amazon Athena – SQL querying on S3 data
Power BI – Dashboard & visualization (connected to Athena) 
