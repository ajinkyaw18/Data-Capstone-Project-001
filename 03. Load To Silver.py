# Databricks notebook source
# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Connections"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Common Functions Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC _________

# COMMAND ----------

# MAGIC %md
# MAGIC # `Customers` Table Transformation

# COMMAND ----------

df_bronzeCustomers = read_stream_table("spark_catalog", "bronze", "bronze_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling duplicate rows

# COMMAND ----------

def remove_duplicates(df):
    print('Removing Duplicates values: ', end='')
    df_dup = df.dropDuplicates()
    print('Success')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling Null values by replacing them

# COMMAND ----------

def handle_NULLs(df,columns):
    print('Replacing NULL values on String Columns with "Unknown" ', end="")
    df_string = df.fillna("Unknown",subset=columns)
    print("Sccuess")

    print('Replacing NULL values on Numeric Columns with "0" ', end="")
    df_clean = df_string.fillna(0,subset = columns)
    print("Succeess")
    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform_address_column

# COMMAND ----------


def transform_address_column(StreamingDF):
    from pyspark.sql.functions import col, to_date, to_timestamp, split, when
    print("Address column transformation: ", end="")
    df_bronzeCustomers = (StreamingDF
                        .withColumn("address_parts", split(col("address"), ","))
                        .withColumn('city', col('address_parts')[1])
                        .withColumn("state", split(col("address_parts")[2]," ")[1])
                        .withColumn('zip_code', split(col('address_parts')[2]," ")[2])
                        .drop("address_parts")
                        .withColumn("zip_code",col("zip_code").cast('integer')))
    print("Success")
    return df_bronzeCustomers
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transform_customer_columns

# COMMAND ----------


def transform_customer_columns(StreamingDF):
    from pyspark.sql.functions import col, to_date, to_timestamp, split, regexp_replace
    print("Customer columns `join_date`,`last_update`,`credit_score`, `phone` transformation: ", end="")
    df_transformed = (StreamingDF
                      .withColumn('join_date', to_date(col('join_date'), "dd-MM-yyyy"))
                      .withColumn('last_update', to_timestamp(col('last_update'), "dd-MM-yyyy HH:mm"))
                      .withColumn('credit_score', col('credit_score').cast('integer'))
                      .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")))
    
    print("Successful")
    return df_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credit Score Segmentation

# COMMAND ----------

def create_credit_Score_category(StreamingDF):
    from pyspark.sql.functions import col, when, to_date, to_timestamp, split, regexp_replace
    print("Creating Credit Score Segmentation: ", end="")
    df = StreamingDF.withColumn(
        "credit_score_category",
        when(col("credit_score") < 300, "very poor")
        .when((col("credit_score") >= 300) & (col("credit_score") < 600), "poor")
        .when((col("credit_score") >= 600) & (col("credit_score") < 700), "fair")
        .when((col("credit_score") >= 700) & (col("credit_score") < 800), "good")
        .otherwise("excellent")
    )
    print("Success")
    return df
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Time Column Function

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calling All Functions 

# COMMAND ----------

# Reading the data from bronze table

#To remove duplicate rows
df_bronzeCustomers = remove_duplicates(df_bronzeCustomers)

# To replace any NULL values
allcolumns = df_bronzeCustomers.schema.names
df_bronzeCustomers = handle_NULLs(df_bronzeCustomers, allcolumns)
#__________________________________________________________________

df_bronzeCustomers = transform_address_column (df_bronzeCustomers)

df_bronzeCustomers = transform_customer_columns(df_bronzeCustomers)
#____________________________________________________________________
# Credit Score Segmentation
df_bronzeCustomers = create_credit_Score_category(df_bronzeCustomers)

# # To convert UTC extract column time to IST time.
df_bronzeCustomers = create_TransformedIndianTime(df_bronzeCustomers, "Extract_Time_UTC")

# COMMAND ----------

allcolumns = df_bronzeCustomers.schema.names
df_bronzeCustomers = handle_NULLs(df_bronzeCustomers, allcolumns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writting the Transformed data to Silver Table

# COMMAND ----------

write_silver_table(df_bronzeCustomers, 'spark_catalog','silver_customers', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ______

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #`Transactions` Transformation Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading the data from bronze table

# COMMAND ----------

read_stream_table('spark_catalog', 'bronze', 'bronze_transactions')

# COMMAND ----------

df_bronzeTransactions = read_bronze_table('spark_catalog','bronze_transactions')

# COMMAND ----------

# Handling NULL values 
all_columns = df_bronzeTransactions.schema.names
bronze_transactions = handle_NULLs(df_bronzeTransactions, all_columns)

# COMMAND ----------

#count of df
count_of_df(df_bronzeTransactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling duplicate 

# COMMAND ----------

df_bronzeTransactions = remove_duplicates(df_bronzeTransactions)

# COMMAND ----------

#count of df
count_of_df(df_bronzeTransactions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Timestamp Column Transformation

# COMMAND ----------

# Timestamp Column Transformation
df_bronzeTransactions = transform_timestamp(df_bronzeTransactions,"timestamp" )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Extract UTC Column Transformation INTO IST

# COMMAND ----------

#Extract UTC Column Transformation INTO IST
df_bronzeTransactions = create_TransformedIndianTime(df_bronzeTransactions, 'Extract_Time_UTC')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Column channel values transforming into lower values

# COMMAND ----------

df_bronzeTransactions = transform_values_into_lower(df_bronzeTransactions, 'channel')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Writting the Transactions Transformed data to Silver Table

# COMMAND ----------

write_silver_table(df_bronzeTransactions, 'spark_catalog', 'silver_transactions', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ___________________

# COMMAND ----------

# MAGIC %md 
# MAGIC # `Branch` Table Transformation silver

# COMMAND ----------

# MAGIC %md
# MAGIC ###Reading the data from bronze table

# COMMAND ----------

df_bronzeBranches = read_table('spark_catalog', "bronze" ,'bronze_branches')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling duplicates records

# COMMAND ----------

count_of_df(df_bronzeBranches)

# COMMAND ----------

df_bronzeBranches = remove_duplicates(df_bronzeBranches)

# COMMAND ----------

count_of_df(df_bronzeBranches)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Handling Null values

# COMMAND ----------

allcolumns = df_bronzeBranches.schema.names
df_bronzeBranches = handle_NULLs(df_bronzeBranches, allcolumns)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transform Extract Time UTC Column Into IST  

# COMMAND ----------

df_bronzeBranches = create_TransformedIndianTime(df_bronzeBranches, "Extract_Time_UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writting Data to silver table

# COMMAND ----------

write_nonstream_silver_table(df_bronzeBranches, 'spark_catalog', 'silver_branches', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ___________
