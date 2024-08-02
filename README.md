Project Setup: Data Pipeline Project

Overview
This project implements a data pipeline using Azure Data Lake Storage (ADLS) and Databricks. The pipeline ingests, processes, and stores data using the medallion architecture (Bronze, Silver, Gold layers).


**Project Setup**
Log in to the Azure portal.

Create Azure Storage account under name of `storagemav001`

created datapipeline container, create the following directories:

**landing**/: This will serve as your landing zone for raw data.
**medallion**/: This will store the transformed data following the medallion architecture.
**checkpoints**/: This will store the checkpoints for your streaming jobs.
Create Subdirectories under landing/:

Inside the landing/ directory, create the following subdirectories for your datasets:
**raw_customers**/: For raw customer data.
**raw_transactions**/: For raw transaction data.
**raw_branches**/: For raw branch data.

Directory Structure:
Your ADLS container structure should look like this:

**datapipeline/
├── landing/
│   ├── raw_customers/
│   ├── raw_transactions/
│   └── raw_branches/
├── medallion/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── checkpoints/**

**Important note**: dont forgot to add your ADLS container Access keys in connections notebookunder access key value variable, you will be able to run this pipeline without any issue. 

Final Steps
Run Your Data Pipeline:
Execute your data pipeline scripts, ensuring that data flows from the landing zone to the medallion architecture's Bronze, Silver, and Gold layers.
