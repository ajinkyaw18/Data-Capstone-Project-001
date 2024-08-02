# Databricks notebook source
storage_account = "storagemav001"
container_name = "medallion"
storage_account_conf_key = f"fs.azure.account.key.{storage_account}.dfs.core.windows.net"
storage_account_conf_key_value = "qtbTAjrcrwCpkGwb+PCcTW2BQ6qYsqFcrGkUA8d/qdOExvoeacd9vW93N3M5maVCgIFSAfMizrpW+AStsmN7GA=="

spark.conf.set(storage_account_conf_key,storage_account_conf_key_value)
dir_name = "bronze/"
checkpoints_dir_name = "/checkpoints/"
container_landing_name = "landing"
containe_medallion_name = "medallion"
container_checkpoints_name = "checkpoints"

raw_customer_dir = "/raw_customers/"
raw_transactions_dir = "/raw_transactions/"
raw_branches_dir = "/raw_branches/"
landing_dir = "/landing/"
download_dir = "/download/"


# COMMAND ----------

bronze_dir_path = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/{dir_name}'

download_path = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/{download_dir}'

checkpoints_path = f'abfss://{container_checkpoints_name}@{storage_account}.dfs.core.windows.net{checkpoints_dir_name}'

raw_customer_path = f'abfss://{container_landing_name}@{storage_account}.dfs.core.windows.net{raw_customer_dir}'

raw_transactions_path = f'abfss://{container_landing_name}@{storage_account}.dfs.core.windows.net{raw_transactions_dir}'

raw_branches_path = f'abfss://{container_landing_name}@{storage_account}.dfs.core.windows.net{raw_branches_dir}'
