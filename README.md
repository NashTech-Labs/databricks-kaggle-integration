# Databricks-Kaggle-Integration: End-to-End ETL operations
## 1. Data Extraction from Kaggle Datasets
The Kaggle API enables users to interact with Kaggle, download datasets, compete in challenges, and carry out various data-related tasks on the Kaggle platform.

Generate token in Kaggle account setting by clicking on **Create New Token**
![Screenshot from 2024-01-25 15-07-43](https://github.com/NashTech-Labs/databricks-kaggle-integration/assets/125345690/0b3fc2fe-d81b-44d9-a3e1-820a3f5882a7)

The next step is to set up Kaggle auth using the API key and connect to the platform for dataset extraction.

The default location for the auth file storage should be,

`/root/.kaggle/kaggle.json`

## 2. Data Transfer to Databricks Filestore

Moving data from the cluster driver to a consumption layer(Filestore) or bucket is vital in the data processing pipeline.

`dbutils.fs.mv('file:/databricks/driver/sales_data.csv', 'dbfs:/Filestore/', recurse=True)`

Data movement is attained by utilizing the Databricks dbutils filesystem move command.

## 3. Data Transformation and Partitioning

In Apache Spark, data transformation turns messy data into valuable insights through cleaning, aggregating, and structuring, and by separating data into manageable chunks, 
data partitioning increases processing speed and resource efficiency.

Now, You can perform complex transformations and visualizations.
