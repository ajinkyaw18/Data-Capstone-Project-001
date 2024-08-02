# Databricks notebook source
# MAGIC %md
# MAGIC ####Prerequsite

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Connections"

# COMMAND ----------

# MAGIC %md
# MAGIC #Schema Creation

# COMMAND ----------

def create_bronze_schema(catalog_name):
    print(f"Using {catalog_name}_catalog")
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`bronze`""")
    print(f"bronze schema Created in '{catalog_name}_catalog'")
    print("*************************")

# COMMAND ----------


def create_silver_schema(catalog_name):
    print(f'Using {catalog_name}')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`silver`""")
    print(f"silver schema Created in '{catalog_name}'")
    print("*************************")

# COMMAND ----------

def create_gold_schema(catalog_name):
    print(f'Using {catalog_name}')
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`gold`""")
    print(f"gold schema Created in '{catalog_name}'")
    print("*************************")

# COMMAND ----------

# MAGIC %md
# MAGIC _____________________________________________

# COMMAND ----------

# MAGIC %md
# MAGIC #Creating Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating raw_customer table 

# COMMAND ----------

def createTable_bronze_customer(catalog_name):
    print(f"Creating bronze_customer Table  in {catalog_name}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog_name}`.`bronze`.`bronze_customer`(
    customer_id STRING, 
    name STRING,         
    email STRING,        
    phone INT,         
    address STRING,              
    credit_score INT,          
    join_date DATE,            
    last_update TIMESTAMP      
    )""")
    print("*******************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating raw_transactions table

# COMMAND ----------

def createTable_bronze_transactions(catalog_name):
    print(f"Creating bronze_transactions Table  in {catalog_name}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog_name}`.`bronze`.`bronze_transactions`(
        transaction_id STRING,
        customer_id_trans STRING,
        branch_id_trans STRING,
        channel STRING,
        transaction_type STRING,
        amount DOUBLE,
        currency STRING,
        timestamp STRING,
        status STRING,
        Extract_Time_UTC Timestamp
    )""")
    print("*******************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creating raw_branches table

# COMMAND ----------

def createTable_bronze_branches(catalog_name):
    print(f"Creating bronze_branches Table  in {catalog_name}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS `{catalog_name}`.`bronze`.`bronze_branches`(
            branch_id STRING,
            name STRING,
            location STRING,
            timezone STRING,
            Extract_Time_UTC Timestamp
            )""")
    print("*******************************")

# COMMAND ----------

# MAGIC %md
# MAGIC #Calling All The Functions

# COMMAND ----------

create_bronze_schema("spark_catalog")
create_silver_schema("spark_catalog")
create_gold_schema("spark_catalog")

##############################################

createTable_bronze_customer("spark_catalog")
createTable_bronze_transactions('spark_catalog')
createTable_bronze_branches("spark_catalog")
