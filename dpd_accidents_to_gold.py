# Databricks notebook source
# import pandas as pd
import time
import pyspark
from pyspark.sql.functions import *

# COMMAND ----------

# write the data to the silver table
spark_df = spark.sql(f"""
    SELECT *
    FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_silver
    """)

# COMMAND ----------

def run_query_dq_check():
    results = spark.sql(f"""
    SELECT 
        incident_number,
        SUM(CASE WHEN incident_number IS NULL THEN 1 ELSE 0 END) = 0 AS incident_number_should_not_be_null,
        SUM(CASE WHEN nature_of_call IS NULL THEN 1 ELSE 0 END) = 0 AS call_should_not_be_null,
        COUNT(*) = 1 AS is_data_deduped,
        COUNT(*) > 0 AS is_there_data
    FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_silver
    GROUP BY incident_number
    """)

    # Check if DataFrame is empty
    if results.count() == 0:
        raise ValueError("The query returned no results!")

    # Iterate through collected results
    for result in results.collect():  # Convert DataFrame to list of Row objects
        for column in result.asDict().values():  # Extract values from Row
            if isinstance(column, bool):  # Check if value is boolean
                assert column, f"Data quality check failed: {column} is False"

run_query_dq_check()

# COMMAND ----------

# write the data to the gold table
spark_df.write.mode("append").saveAsTable("tabular.dataexpert.josephgabbrielle62095_dpd_accidents_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- delete bronze table if there are no data errors
# MAGIC ---DROP TABLE IF EXISTS tabular.dataexpert.josephgabbrielle62095_dpd_accidents_bronze;
# MAGIC DELETE FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_silver where event_date = current_date()