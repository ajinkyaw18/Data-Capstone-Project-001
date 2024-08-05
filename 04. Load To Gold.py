# Databricks notebook source
# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Connections"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/ajinkya_1718880890999@npmavericsystems.onmicrosoft.com/Common Functions Notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC #Reading the Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##silverCustomers

# COMMAND ----------

df_silverCustomers = read_table("spark_catalog", "silver", "silver_customers")


# COMMAND ----------

# MAGIC %md
# MAGIC ##silverBranches

# COMMAND ----------

df_silverBranches  = read_table("spark_catalog", "silver", "silver_branches")

# COMMAND ----------

# MAGIC %md
# MAGIC ##silverTransactions

# COMMAND ----------

df_silverTransactions = read_table('spark_catalog', 'silver', 'silver_transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Joining Tables 

# COMMAND ----------

def join_df_transactions_customers_branches():
    print("Performing Join: ", end='')
    df_joined = (df_silverTransactions
                .join(df_silverCustomers, df_silverTransactions.customer_id_trans == df_silverCustomers.customer_id, "inner")
                .join(df_silverBranches, df_silverTransactions.branch_id_trans == df_silverBranches.branch_id, "inner")
                .select(
                    df_silverTransactions["transaction_id"],
                    df_silverTransactions["customer_id_trans"],
                    df_silverTransactions["branch_id_trans"],
                    df_silverTransactions["channel"],
                    df_silverTransactions["transaction_type"],
                    df_silverTransactions["amount"],
                    df_silverTransactions["currency"],
                    df_silverTransactions["timestamp"],
                    df_silverTransactions["status"],
                    df_silverTransactions["Extract_Time_IST"].alias("transaction_extract_time_IST"),
                    df_silverCustomers["name"].alias("customer_name"),
                    df_silverCustomers["email"],
                    df_silverCustomers["phone"],
                    df_silverCustomers["address"],
                    df_silverCustomers["city"],
                    df_silverCustomers["state"],
                    df_silverCustomers["zip_code"],
                    df_silverCustomers["credit_score_category"],
                    df_silverCustomers["Extract_Time_IST"].alias('customer_extract_time_IST'),
                    df_silverBranches["name"].alias("branch_name"),
                    df_silverBranches["location"],
                    df_silverBranches["timezone"],
                    df_silverBranches["Extract_Time_IST"].alias("branch_extract_time_IST") 
                ))
    print("Success")
    return df_joined


# COMMAND ----------

df_joined = join_df_transactions_customers_branches()

# COMMAND ----------

# MAGIC %md
# MAGIC #Writting the Table

# COMMAND ----------

write_st_to_gold_table(df_joined, 'spark_catalog', 'gold_joined_transactions_customers_branches', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer Segmentation

# COMMAND ----------


def generate_customer_segments(df_transactions, df_customers):
    from pyspark.sql.functions import sum, lit, current_timestamp, col, add_months, max, current_date, date_add
    print('Generating customers segments table: ', end='')
    high_value_threshold = 10000
    df_high_value = (df_transactions
                     .groupBy("customer_id_trans")
                     .agg(sum("amount").alias("total_amount"))
                     .filter(col("total_amount") > high_value_threshold)
                     .select(col("customer_id_trans").alias("customer_id"),
                             lit("High_Value").alias("segment_name"),
                             lit("Customers with high transaction volume").alias("segment_description"),
                             current_timestamp().alias("last_update"))
                    )

   
    df_new_user = (df_customers
                   .filter(col("join_date") >= add_months(current_date(), -1))
                   .select(col("customer_id"),
                           lit("New_User").alias("segment_name"),
                           lit("Customers who joined in last 30 days").alias("segment_description"),
                           current_timestamp().alias("last_update"))
                  )

    
    df_inactive = (df_transactions
                   .groupBy("customer_id_trans")
                   .agg(max("timestamp").alias("last_transaction"))
                   .filter(col("last_transaction") < add_months(current_date(), -3))
                   .select(col("customer_id_trans").alias("customer_id"),
                           lit("Inactive").alias("segment_name"),
                           lit("No transactions in last 90 days").alias("segment_description"),
                           current_timestamp().alias("last_update"))
                  )

    
    credit_risk_threshold = 600
    df_credit_risk = (df_customers
                      .filter(col("credit_score") < credit_risk_threshold)
                      .select(col("customer_id"),
                              lit("Credit_Risk").alias("segment_name"),
                              lit("Customers with low credit scores").alias("segment_description"),
                              current_timestamp().alias("last_update"))
                     )

 
    loyalty_threshold_years = 5
    days_in_year = 365
    df_loyal = (df_customers
                .filter(col("join_date") < date_add(current_date(), -loyalty_threshold_years * days_in_year))
                .select(col("customer_id"),
                        lit("Loyal").alias("segment_name"),
                        lit("Consistent activity for over 5 years").alias("segment_description"),
                        current_timestamp().alias("last_update"))
               )


    df_segments = df_high_value.union(df_new_user).union(df_inactive).union(df_credit_risk).union(df_loyal)
    
    print("Success")
    return df_segments

# COMMAND ----------

df_segments2 = generate_customer_segments(df_silverTransactions, df_silverCustomers)

# COMMAND ----------

write_st_to_gold_table(df_segments2, 'spark_catalog', 'gold_customer_segments', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Fraud Flag

# COMMAND ----------

def generate_fraud_flags(df_joined):
    from pyspark.sql.functions import lit, col, current_timestamp

    # 1. Unusual Amount Flag
    unusual_amount_threshold = 10000  # Set your threshold
    df_unusual_amount = (df_joined
                         .filter(col("amount") > unusual_amount_threshold)
                         .select(
                             lit("F1001").alias("flag_id"),
                             col("transaction_id"),
                             lit("unusual_amount").alias("flag_type"),
                             lit(0.75).alias("confidence_score"),
                             current_timestamp().alias("timestamp")
                         ))

    # 2. Velocity Check Flag
    velocity_check_threshold = 5 
    df_velocity_check = (df_joined
                         .groupBy("customer_id_trans")
                         .agg({'transaction_id': 'collect_list'})
                         .withColumn("count", col("collect_list(transaction_id)").getItem(0)) 
                         .filter(col("count") > velocity_check_threshold)
                         .select(
                             lit("F1002").alias("flag_id"),
                             col("collect_list(transaction_id)").getItem(0).alias("transaction_id"),
                             lit("velocity_check").alias("flag_type"),
                             lit(0.60).alias("confidence_score"),
                             current_timestamp().alias("timestamp")
                         ))

    # 3. New Geolocation Flag (Placeholder logic)
    df_new_geolocation = (df_joined
                          .filter(col("location") != "usual_location")  # Replace with actual logic
                          .select(
                              lit("F1003").alias("flag_id"),
                              col("transaction_id"),
                              lit("new_geolocation").alias("flag_type"),
                              lit(0.80).alias("confidence_score"),
                              current_timestamp().alias("timestamp")
                          ))

    # 4. Watchlist Match Flag (Placeholder logic)
    df_watchlist_match = (df_joined
                          .filter(col("customer_name").isin(["Watchlisted Name 1", "Watchlisted Name 2"]))
                          .select(
                              lit("F1004").alias("flag_id"),
                              col("transaction_id"),
                              lit("watchlist_match").alias("flag_type"),
                              lit(0.95).alias("confidence_score"),
                              current_timestamp().alias("timestamp")
                          ))

    # 5. Pattern Anomaly Flag (Placeholder logic)
    df_pattern_anomaly = (df_joined
                          .filter(col("transaction_type") == "suspicious")
                          .select(
                              lit("F1005").alias("flag_id"),
                              col("transaction_id"),
                              lit("pattern_anomaly").alias("flag_type"),
                              lit(0.70).alias("confidence_score"),
                              current_timestamp().alias("timestamp")
                          ))

    # Combine all flags into a single DataFrame
    df_flags = (df_unusual_amount.unionByName(df_velocity_check)
                                .unionByName(df_new_geolocation)
                                .unionByName(df_watchlist_match)
                                .unionByName(df_pattern_anomaly))
    
    return df_flags


# COMMAND ----------

# Example usage:
df_flags = generate_fraud_flags(df_joined)

# COMMAND ----------

# Write the flags DataFrame to the Gold table
write_st_to_gold_table(df_flags, 'spark_catalog', 'gold_fraud_flags', checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generate_comprehensive_aggregated_table

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg, col, desc

def generate_comprehensive_aggregated_table(df_joined):
    print("Generating comprehensive aggregated table: ", end='')

    # Customer-Level Aggregation
    df_customer_metrics = (df_joined
                           .groupBy("customer_id_trans", "customer_name")
                           .agg(
                               sum("amount").alias("total_spent"),
                               avg("amount").alias("average_transaction_value"),
                               count("transaction_id").alias("transaction_count")
                           )
                           .withColumn("CLTV", col("average_transaction_value") * col("transaction_count"))
                          )

    # Branch-Level Aggregation
    df_branch_metrics = (df_joined
                         .groupBy("branch_id_trans", "branch_name")
                         .agg(
                             sum("amount").alias("total_branch_revenue"),
                             count("transaction_id").alias("total_branch_transactions")
                         )
                        )

    # Channel-Level Aggregation
    df_channel_metrics = (df_joined
                          .groupBy("channel")
                          .agg(
                              count("transaction_id").alias("total_channel_transactions"),
                              sum("amount").alias("total_channel_revenue")
                          )
                         )

    # Join all the metrics together into one comprehensive table
    df_comprehensive_aggregated = (df_customer_metrics
                                   .join(df_branch_metrics, df_joined["branch_id_trans"] == df_branch_metrics["branch_id_trans"], "left")
                                   .join(df_channel_metrics, df_joined["channel"] == df_channel_metrics["channel"], "left")
                                   .select(
                                       "customer_id_trans", "customer_name",
                                       "total_spent", "average_transaction_value", "transaction_count", "CLTV",
                                       "branch_name", "total_branch_revenue", "total_branch_transactions",
                                       "channel", "total_channel_transactions", "total_channel_revenue"
                                   )
                                  )

    print("Success")
    return df_comprehensive_aggregated



# COMMAND ----------

df_comprehensive_aggregated = generate_comprehensive_aggregated_table(df_joined)

# COMMAND ----------

write_st_to_gold_table(df_comprehensive_aggregated,'spark_catalog','gold_comprehensive_aggregated',checkpoints_path)

# COMMAND ----------

# MAGIC %md
# MAGIC __________
