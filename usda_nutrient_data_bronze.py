# Databricks notebook source
# MAGIC %md
# MAGIC # USDA NUTRITION AND BRANDED FOOD INFORMATION
# MAGIC This Python script retrieves food nutrition information from the USDA FoodData Central Database, then cleans the data by handling missing values, removing duplicates, and standardizing formats.
# MAGIC
# MAGIC **Fetch Data** → The script imports files with branded food nutrition information from USDA. The data is updated twice a year, in April and October
# MAGIC
# MAGIC **Process & Structure** → Next, the script cleans the data by fixing field types, removing unnecessary data, and restructuring it into a table for easy analysis and access.
# MAGIC
# MAGIC **Perform Unit Tests** → Before pushing the data to the gold table, unit tests check for missing values, data integrity, and consistency.
# MAGIC
# MAGIC **Store in Gold Table** → After validation, the clean data is written to the gold table, ensuring high-quality nutrition information.
# MAGIC
# MAGIC This automated pipeline ensures accurate, nutrition data, supporting stakeholder health goals. 🍎📊

# COMMAND ----------

# IMPORT NECESSARY LIBRARIES
import requests
import pandas as pd
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRANDED FOOD DATA
# MAGIC ### IMPORT FILE TO GET BRANDED FOOD DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_branded_food = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/branded_food.csv')
df_branded_food.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_branded_food = df_branded_food.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_branded_food.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_branded_food_bronze")

# LOOK AT THE TABLE
display(df_branded_food)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRANDED FOOD NUTRITION DATA
# MAGIC ### IMPORT FILE TO GET FOOD DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_food = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/food.csv')
df_food.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_food = df_food.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_food.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_bronze")

# VIEW THE DATA
display(df_food)

# COMMAND ----------

# MAGIC %md
# MAGIC ## INGREDIENT DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_food_attribute = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/food_attribute.csv')
df_food_attribute.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_food_attribute = df_food_attribute.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_food_attribute.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_attribute_bronze")

# VIEW THE DATA
display(df_food_attribute)

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNIQUE ID MAPPING DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING. THIS FILE CONTAINS THE MAPPINGS FOR THE UNIQUE ID

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_food_nutrient = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/food_nutrient.csv')
df_food_nutrient.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_food_nutrient = df_food_nutrient.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_food_nutrient.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_nutrient_bronze")

# VIEW THE DATA
display(df_food_nutrient)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FOOD UPDATE LOG DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING. THIS FILE INCLUDES A LOG OF THE LAST TIME DATA WAS UPDATED

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_food_update_log = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/food_update_log_entry.csv')
df_food_update_log.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_food_update_log = df_food_update_log.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_food_update_log.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_update_log_bronze")

# VIEW THE DATA
display(df_food_update_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MEASUREMENT UNITS DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING. THIS FILE CONTAINS THE FOOD MEASUREMENT UNITS.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_measure_unit = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/measure_unit.csv')
df_measure_unit.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_measure_unit = df_measure_unit.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_measure_unit.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_measure_unit_bronze")

# VIEW THE DATA
display(df_measure_unit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MICROBE DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING. THIS FILE INCLUDES THE GUT BACTERIA IN FOOD.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_microbe = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/microbe.csv')

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_microbe = df_microbe.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_microbe.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_microbe_bronze")

# VIEW THE DATA
display(df_microbe)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NUTRIENT DATA
# MAGIC ### IMPORT FILE TO GET DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# READ THE FILE AND INFER THE SCHEMA
df_nutrient = spark.read \
    .option('header', True) \
    .option('inferSchema', True) \
    .option('multiLine', True) \
    .option("escape", "\"")  \
    .csv('/Volumes/tabular/dataexpert/josephgabbrielle62095/usda_capstone/nutrient.csv')
df_nutrient.repartition(6)

# CREATE A TIMESTAMP OF WHEN THE DATA WAS UPLOADED
df_nutrient = df_nutrient.withColumn("uploaded_date", current_date())

# WRITE THE DATA TO THE BRONZE TABLE
df_nutrient.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_nutrient_bronze")

# VIEW THE DATA
display(df_nutrient)