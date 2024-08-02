# Databricks notebook source
# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Connections"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading the data from bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table ReadOnly function

# COMMAND ----------

def read_table(Calatlog_Name, Schema_Name, Table_Name):
    print(f"Reading the Table {Table_Name}: ", end="")
    df = spark.read.table(f'`{Calatlog_Name}`.`{Schema_Name}`.`{Table_Name}`') 
    print("Success")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table readStream Function 

# COMMAND ----------

def read_stream_table(catalog_name, schema_name, table_name):
    print(f'Reading the Table Data {table_name}: ', end='')
    df = (spark.readStream
                 .table(f'`{catalog_name}`.`{schema_name}`.`{table_name}`'))
    print(f'Reading {catalog_name}.{schema_name}.{table_name} Success')
    return df

# COMMAND ----------

def read_bronze_table(catalog_name, table_name):
    print(f'Reading the Bronze Table Data for {table_name}: ', end='')
    df_bronze = (spark.readStream
                 .table(f'`{catalog_name}`.`bronze`.`{table_name}`'))
    print(f'Reading {catalog_name}.bronze.{table_name} Success')
    return df_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ##Count function

# COMMAND ----------

def count_of_df(StreamingDF):   
    count_of = StreamingDF.groupBy().count()
    return count_of

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling duplicate rows functions

# COMMAND ----------

def remove_duplicates(df):
    print('Removing Duplicates values: ', end='')
    df_dup = df.dropDuplicates()
    print('Success')
    return df_dup

# COMMAND ----------

# MAGIC %md
# MAGIC #Timestamp Column transformation 
# MAGIC into Data type Timestamp as per spark Date Standard Format 

# COMMAND ----------

def transform_timestamp(StreamingDF, column_name):
    from pyspark.sql.functions import to_timestamp, col
    print(f'Transformaing {column_name} into timestamp data type: ', end='')
    df = (StreamingDF.withColumn(column_name, to_timestamp(col(column_name), 'dd-MM-yyy HH:mm')))
    print('Success')                                  
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #Transform Extract UTC Time Column TO IST Function 

# COMMAND ----------

def create_TransformedIndianTime(StreaminDF,Column_Name):
    from pyspark.sql.functions import from_utc_timestamp
    print('Tranforming Extract_Time_UTC to Extract_Time_IST: ', end="")
    df = StreaminDF.withColumn(
        "Extract_Time_IST", from_utc_timestamp(Column_Name, "Asia/Kolkata")
    ).drop(Column_Name)
    print("Success")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling Null values by replacing them Function

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
# MAGIC #Lower Function For Column Values

# COMMAND ----------

def transform_values_into_lower(StreamingDF, column_name):
    from pyspark.sql.functions import lower, col
    print(f'Lowering all values present in {column_name}:', end="")
    df = StreamingDF.withColumn(column_name, lower(col(column_name)))
    print('Success')
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC #Write Transformed data to Table

# COMMAND ----------

def write_silver_table(StreamingDF, catalog_name, table_name, checkpoints_path):
    print(f"Writing the {table_name} Data to Silver Layer: ", end="")
    checkpoint_location = f"{checkpoints_path}/{table_name}/checkpoint"
    
    write_stream_silver = (StreamingDF.writeStream
                           .format('delta')
                           .option('checkpointLocation', checkpoint_location)
                           .outputMode('append')
                           .queryName(f"silver_{table_name}_write_stream")
                           .trigger(availableNow=True)
                           .toTable(f"{catalog_name}.silver.{table_name}"))
    
    print(f'Writing {catalog_name}.silver.{table_name} Success')
    return write_stream_silver.awaitTermination()

# COMMAND ----------

def write_to_gold_table(StreamingDF, catalog_name, table_name, checkpoints_path):
    print(f"Writing the {table_name} Data to Gold Layer: ", end="")
    checkpoint_location = f"{checkpoints_path}/{table_name}/checkpoint"
    
    write_stream_gold = (StreamingDF.writeStream
                           .format('delta')
                           .option('checkpointLocation', checkpoint_location)
                           .outputMode('append')
                           .queryName(f"gold_{table_name}_write_stream")
                           .trigger(availableNow=True)
                           .toTable(f"{catalog_name}.gold.{table_name}"))
    
    print(f'Writing {catalog_name}.gold.{table_name} Success')
    return write_stream_gold.awaitTermination()

# COMMAND ----------

def write_st_to_gold_table(StaticDF, catalog_name, table_name, checkpoints_path):
    print(f"Writing the {table_name} Data to Gold Layer: ", end="")
    checkpoint_location = f"{checkpoints_path}/{table_name}/checkpoint"
    df = (StaticDF.write 
        .format('delta') 
        .mode('append')
        .option('checkpointLocation', checkpoint_location)
        .saveAsTable(f"{catalog_name}.gold.{table_name}"))

    print('Success')
    return df

# COMMAND ----------

def write_nonstream_silver_table(StaticDF, catalog_name, table_name, checkpoints_path):
    print(f"Writing the {table_name} Data to Silver Layer: ", end="")
    checkpoint_location = f"{checkpoints_path}/{table_name}/checkpoint"
    
    write_silver = (StaticDF.write
                .format('delta')
                .option('checkpointLocation', checkpoint_location)
                .mode('append')
                .saveAsTable(f"{catalog_name}.silver.{table_name}"))

    print(f'Writing {catalog_name}.silver.{table_name} Success')
    return write_silver
