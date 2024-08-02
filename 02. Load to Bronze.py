# Databricks notebook source
# MAGIC %md
# MAGIC Note: Used availableNow Trigger To Prevent Lab Cost while using it, we can change to Proccessing Time for Transactions for continuous or default for continous check.

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Connections"
# MAGIC

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Common Functions Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Customers` Data

# COMMAND ----------


def read_customers_Data():
    from pyspark.sql.functions import current_timestamp, to_date, col
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
    print("Reading the Raw Customers Data: ", end="" )
    custom_schema = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("address", StringType()),
        StructField("credit_score", IntegerType()),
        StructField("join_date", StringType()),
        StructField("last_update", StringType())
        ])

    rawCustomers_stream = (spark.readStream.format('cloudFiles')
                .option('cloudFiles.format', 'csv')
                .option('cloudFiles.schemaLocation',f'{checkpoints_path}/raw_customers/schemaInfer')
                .option('header','True')
                .schema(custom_schema)
                .load(raw_customer_path)
                .withColumn("Extract_Time_UTC", current_timestamp())
                )

    print("Reading Success")
    print("****************")

    return rawCustomers_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Transactions` Data 

# COMMAND ----------

def read_transaction_data():
    from pyspark.sql.functions import current_timestamp, to_date, col, expr
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
    print("Reading the Transactions Data: ", end="")
    custom_schema_transactions = StructType([
        StructField('transaction_id', StringType()),
        StructField('customer_id_trans', StringType()),
        StructField('branch_id_trans', StringType()),
        StructField('channel', StringType()),
        StructField('transaction_type', StringType()),
        StructField('amount', DoubleType()),
        StructField('currency', StringType()),
        StructField('timestamp', StringType()),
        StructField('status', StringType())
    ])

    readTransaction_stream = (spark.readStream.format('cloudFiles')
                .option('cloudFiles.format', 'csv')
                .option('cloudFiles.schemaLocation', f'{checkpoints_path}/raw_transactions/schemaInfer')
                .option('header', 'True')
                .schema(custom_schema_transactions)
                .load(raw_transactions_path)
                .withColumn("Extract_Time_UTC", current_timestamp())
                )
    
    print("Reading Success")
    print("*************")
    return readTransaction_stream


# COMMAND ----------

# MAGIC %md
# MAGIC ## `Branches` Data

# COMMAND ----------


def readBranches_data():    
    from pyspark.sql.functions import current_timestamp
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
    print('Reading Branches Data: ', end="")
    custom_schema_branches = StructType([
    StructField('branch_id', StringType()),
    StructField('name', StringType()),
    StructField('location', StringType()),
    StructField('timezone', StringType())
    ])
    
    read_branches_data = (spark.read.format('csv')
                        .option('header', 'True')
                        .schema(custom_schema_branches)
                        .load(raw_branches_path)
                        .withColumn('Extract_Time_UTC', current_timestamp()))
    
    print('Reading Success')
    print('***************')
    return read_branches_data

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## `Raw_Customers` Data

# COMMAND ----------

def write_raw_customer_Data(StreamingDF,catalog_name):
    write_Stream_customers = (StreamingDF.writeStream
                             .format('delta')
                             .option('checkpointLocation',f'{checkpoints_path}/rawCustomersLoad/checkpoint')
                             .outputMode('append')
                             .queryName('rawCustomersWriteStream')
                             .trigger(availableNow=True)
                             .toTable(f'`{catalog_name}`.`bronze`.`bronze_customers`'))
    
    write_Stream_customers.awaitTermination()

    print(f"Write Success into `bronze_customers` table")
    print('**************')


# COMMAND ----------

# MAGIC %md
# MAGIC ##`Raw_Transactions` Data

# COMMAND ----------

def write_raw_trasactions_data(StreamindDF,catalog_name):
    write_Stream_transactions = (StreamindDF.writeStream
                                .format('delta')
                                .option('checkpointLocation',f'{checkpoints_path}/rawTrasactions/checkpoint')
                                .outputMode('append')
                                .queryName('rawTrasactionsWriteStream')
                                .trigger(availableNow=True)
                                .toTable(f"`{catalog_name}`.`bronze`.`bronze_transactions`"))
    write_Stream_transactions.awaitTermination()
    print('Write Success into `bronze_transactions` Table')
    print('******************')

# COMMAND ----------

# MAGIC %md
# MAGIC ##`Raw_Branches` Data 

# COMMAND ----------

def write_raw_branches_data(StaticDF,Catalog_Name):
    write_stream_branches = (StaticDF.write
                            .format('delta')
                            .option('checkpointLocation', f'{checkpoints_path}/rawBranches/checkpoint')
                            .mode('append')
                            .saveAsTable(f'{Catalog_Name}.bronze.bronze_branches')
                            )
    print('Write Success into `bronze_branches` Table')
    print('******************')
    

# COMMAND ----------

# MAGIC %md
# MAGIC #Calling All Functions

# COMMAND ----------

# Read Functions 

read_customers_df = read_customers_Data()
read_transactions_df = read_transaction_data() 
read_branches_df = readBranches_data()
#_____________________________________________

# Write Functions
write_raw_customer_Data(read_customers_df, 'spark_catalog')
write_raw_trasactions_data(read_transactions_df,'spark_catalog')
write_raw_branches_data(read_branches_df,'spark_catalog')
