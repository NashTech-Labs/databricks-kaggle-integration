# Databricks notebook source
import os
os.makedirs("/root/.kaggle")

# COMMAND ----------

# DBTITLE 1,Function to setup Kaggle Authentication
import json

def kaggle_auth_setup(kaggle_auth, output_file):
    try:
        data = json.loads(kaggle_auth)
        with open(output_file, 'w') as file:
            json.dump(data, file, indent=4)
        print('Setup successful')
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
        print("Setup failed")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Setup failed")

# COMMAND ----------

kaggle_auth_setup('{"username":"snsingh01","key":"c4fdc36eddbaa3b4b1b768874b55ef9f"}', '/root/.kaggle/kaggle.json')

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /root/.kaggle/kaggle.json

# COMMAND ----------

# DBTITLE 1,restrict the read permission (mandatory when working on shared cluster)
# MAGIC %sh
# MAGIC chmod 600 /root/.kaggle/kaggle.json

# COMMAND ----------

# MAGIC %sh
# MAGIC kaggle datasets list
