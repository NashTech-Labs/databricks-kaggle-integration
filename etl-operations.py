# Databricks notebook source
# DBTITLE 1,setup kaggle
# MAGIC %run /Users/sant.singh@knoldus.com/setup-kaggle

# COMMAND ----------

# DBTITLE 1,extract dataset from kaggle
# MAGIC %sh
# MAGIC kaggle datasets download -d vincentcornlius/sales-orders

# COMMAND ----------

# DBTITLE 1,List downloaded files
dbutils.fs.ls('file:/databricks/driver')

# COMMAND ----------

# DBTITLE 1,Unzip the files
# MAGIC %sh
# MAGIC unzip /databricks/driver/sales-orders.zip

# COMMAND ----------

# DBTITLE 1,Move to DBFS Filestore
dbutils.fs.mv('file:/databricks/driver/sales_data.csv', 'dbfs:/FileStore/tables', recurse=True)

# COMMAND ----------

# MAGIC %%markdown
# MAGIC #1 Data Exploration

# COMMAND ----------

# DBTITLE 1,1.1 Load the sales_data.csv dataset
dataframe = spark.read.csv('dbfs:/FileStore/tables/sales_data.csv', header=True)

# COMMAND ----------

# DBTITLE 1,1.2 Performing preliminary analysis
# Get a summary of the dataset
dataframe.printSchema()

# Descriptive statistics for numerical columns
dataframe.show()

# COMMAND ----------

# MAGIC %%markdown
# MAGIC #2 Data Preprocessing

# COMMAND ----------

# DBTITLE 1,2.1 Removing spaces from column name
# List of new column names
new_column_names = ['order_date', 'order_id', 'product_name', 'product_ean', 'category', 'purchase_address', 'quantity_ordered', 'price_each', 'cost_price', 'turnover', 'margin']

# Rename all columns
for i, col_name in enumerate(dataframe.columns):
    dataframe = dataframe.withColumnRenamed(col_name, new_column_names[i])

# COMMAND ----------

# DBTITLE 1,2.2 cast the column to appropriate types
from pyspark.sql.functions import *
dataframe = dataframe.withColumn('order_date', col('order_date').cast('timestamp'))
dataframe = dataframe.withColumn('quantity_ordered', dataframe['quantity_ordered'].cast('double'))
dataframe = dataframe.withColumn('price_each', dataframe['price_each'].cast('double'))
dataframe = dataframe.withColumn('cost_price', dataframe['cost_price'].cast('double'))
dataframe = dataframe.withColumn('turnover', dataframe['turnover'].cast('double'))
dataframe = dataframe.withColumn('margin', dataframe['margin'].cast('double'))

# COMMAND ----------

#display(dataframe)
dataframe.printSchema()

# COMMAND ----------

# DBTITLE 1,2.3 Aggregate sales data on a monthly basis
# Extract month and year from 'order_date'
dataframe = dataframe.withColumn('Month', month('order_date'))
dataframe = dataframe.withColumn('Year', year('order_date'))

# Aggregate sales data by month
monthly_sales_data = dataframe.groupby('Year', 'Month')\
                               .agg(sum('price_each').alias('total_price'),
                                    sum('quantity_ordered').alias('total_quantity'))\
                               .orderBy('Year', 'Month')  # Optional: order by year and month

# Show the result
monthly_sales_data.show()

# COMMAND ----------

# DBTITLE 1,2.4 Create new features
# Create 'Total Sales' feature
dataframe = dataframe.withColumn('total_sales', col('quantity_ordered') * col('price_each'))

# COMMAND ----------

display(dataframe)

# COMMAND ----------

# MAGIC %%markdown
# MAGIC #Data Analysis

# COMMAND ----------

# DBTITLE 1,3.1 Plot the aggregated sales data over time to visualize the trend
#Visualizing aggregated sales over time helps in identifying trends, seasonality, and any anomalies in the data.

monthly_sales_data = monthly_sales_data.withColumn(
    'date',
    to_date(concat(col('Year').cast('string'), lit('-'), col('Month').cast('string'), lit('-01')))
)
display(monthly_sales_data)

# COMMAND ----------

# DBTITLE 1,3.2 Monthly Sales Growth Rate
#This visualization will help in understanding the dynamics of sales growth and identifying periods of rapid growth or decline.

from pyspark.sql.window import Window
window_spec = Window.orderBy('date')

# Calculate 'Sales Growth' using the lag function
monthly_sales_data = monthly_sales_data.withColumn(
    'sales_growth',
    ((col('total_price') / lag('total_price').over(window_spec)) - 1) * 100
)

display(monthly_sales_data)

# COMMAND ----------

# DBTITLE 1,3.3 Product-wise Sales
#Understanding which products are the best-sellers can guide inventory decisions and marketing strategies.

product_sales = dataframe.groupby('product_name').agg(sum('total_sales').alias('product_sales')).orderBy('product_sales', ascending=False)
display(product_sales)

# COMMAND ----------

# DBTITLE 1,3.4 Sales by Category
#Sales distribution across different categories can provide insights into which categories are most popular and deserve more attention.

category_sales = dataframe.groupby('category').agg(sum('total_sales').alias('Category Sales'))
display(category_sales)

# COMMAND ----------

# DBTITLE 1,3.5 Geographical Sales Distribution
#Knowing which regions or cities generate the most sales can guide regional marketing efforts and distribution strategies.

dataframe = dataframe.withColumn('city', split(col('purchase_address'), ',')[1].cast('string').alias('city'))
city_sales = dataframe.groupby('city').agg(sum('total_sales').alias('city_sales')).orderBy('city_sales', ascending=False)
display(city_sales)

# COMMAND ----------

# DBTITLE 1,3.6 Day-wise Sales Distribution
#Identifying peak sales days can help in optimizing promotions and marketing efforts for specific days of the week.
dataframe = dataframe.withColumn('day_of_week', date_format('order_date', 'EEEE'))
day_sales = dataframe.groupby('day_of_week').agg(sum('total_sales').alias('day_sales')).orderBy('day_of_week')
display(day_sales)

# COMMAND ----------

display(dataframe)

# COMMAND ----------

# DBTITLE 1,Load the delta table in warehouse
dataframe.write.format("delta").mode("overwrite").saveAsTable("sales_data")
