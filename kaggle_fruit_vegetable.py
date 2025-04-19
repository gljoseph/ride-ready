# Databricks notebook source
# MAGIC %md
# MAGIC # FRUIT AND VEGETABLE NUTRITION INFORMATION
# MAGIC This Python script retrieves fruit and vegetable nutrition information, then cleans the data by handling missing values, removing duplicates, and standardizing formats.
# MAGIC
# MAGIC **Fetch Data** → The script imports files with fruit and vegetable nutrition information from Kaggle.
# MAGIC
# MAGIC **Process & Structure** → Next, the script cleans the data by fixing field types, removing unnecessary data, and restructuring it into a table for easy analysis and access.
# MAGIC
# MAGIC **Perform Unit Tests** → Before pushing the data to the gold table, unit tests check for missing values, data integrity, and consistency.
# MAGIC
# MAGIC **Store in Gold Table** → After validation, the clean data is written to the gold table, ensuring high-quality nutrition information.
# MAGIC
# MAGIC This automated pipeline ensures accurate, nutrition data, supporting stakeholder health goals. 🍎📊

# COMMAND ----------

# data sources: 
# https://www.kaggle.com/datasets/yoyoyloloy/fruits-and-vegetables-nutritional-values
# https://www.kaggle.com/datasets/cid007/food-and-vegetable-nutrition-dataset?resource=download

# COMMAND ----------

# IMPORT NECESSARY LIBRARIES
import numpy as np
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## FRUIT DATA
# MAGIC ### IMPORT FILE TO GET FRUIT NUTRITION DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# SET SCHEMA FOR FRUIT DATASET
schema_fruit = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("energy (kcal/kJ)", StringType(), nullable=True),
    StructField("water (g)", DoubleType(), nullable=True),
    StructField("protein (g)", DoubleType(), nullable=True),
    StructField("total fat (g)", DoubleType(), nullable=True),
    StructField("carbohydrates (g)", DoubleType(), nullable=True),
    StructField("fiber (g)", DoubleType(), nullable=True),
    StructField("sugars (g)", DoubleType(), nullable=True),
    StructField("calcium (mg)", DoubleType(), nullable=True),
    StructField("iron (mg)", DoubleType(), nullable=True),
    StructField("magnessium (mg)", DoubleType(), nullable=True),
    StructField("phosphorus (mg)", DoubleType(), nullable=True),
    StructField("potassium (mg)", DoubleType(), nullable=True),
    StructField("sodium (g)", DoubleType(), nullable=True),
    StructField("vitamin A (IU)", DoubleType(), nullable=True),
    StructField("vitamin C (mg)", DoubleType(), nullable=True),
    StructField("vitamin B1 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B2 (mg)", DoubleType(), nullable=True),
    StructField("viatmin B3 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B5 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B6 (mg)", DoubleType(), nullable=True),
    StructField("vitamin E (mg)", DoubleType(), nullable=True)
])

# READ IN THE FILE
df_kaggle_fruit = spark.read.csv("/Volumes/tabular/dataexpert/josephgabbrielle62095/nutrition_data/fruits.csv", schema=schema_fruit, header=True)

# SELECT COLUMNS AND CHANGE COLUMN NAMES
df_kaggle_fruit = df_kaggle_fruit.select(
    col("name"),
    col("energy (kcal/kJ)").alias("energy_kcal_kj"),
    col("water (g)").alias("water_g"),
    col("protein (g)").alias("protein_g"),
    col("total fat (g)").alias("total_fat_g"),
    col("carbohydrates (g)").alias("carbohydrates_g"),
    col("fiber (g)").alias("fiber_g"),
    col("sugars (g)").alias("sugars_g"),
    col("calcium (mg)").alias("calcium_mg"),
    col("iron (mg)").alias("iron_mg"),
    col("magnessium (mg)").alias("magnessium_mg"),
    col("phosphorus (mg)").alias("phosphorus_mg"),
    col("potassium (mg)").alias("potassium_mg"),
    col("sodium (g)").alias("sodium_g"),
    col("vitamin A (IU)").alias("vitamin_a_iu"),
    col("vitamin C (mg)").alias("vitamin_c_mg"),
    col("vitamin B1 (mg)").alias("vitamin_b1_mg"),
    col("vitamin B2 (mg)").alias("vitamin_b2_mg"),
    col("viatmin B3 (mg)").alias("vitamin_b3_mg"),
    col("vitamin B5 (mg)").alias("vitamin_b5_mg"),
    col("vitamin B6 (mg)").alias("vitamin_b6_mg"),
    col("vitamin E (mg)").alias("vitamin_e_mg")
)

# REMOVE / FROM COLUMN NAMES
df_kaggle_fruit = df_kaggle_fruit.withColumn("kcal", split(df_kaggle_fruit["energy_kcal_kj"], "/").getItem(0)).withColumn("kJ", split(df_kaggle_fruit["energy_kcal_kj"], "/").getItem(1))

# CAST COLUMNS TO DOUBLE
df_kaggle_fruit = df_kaggle_fruit.withColumn('kcal', col('kcal').cast('double')) \
       .withColumn('kJ', col('kJ').cast('double'))

# VIEW THE DATA
display(df_kaggle_fruit)
df_kaggle_fruit.printSchema()

# CREATE TIMESTAMP COLUMN
df_kaggle_fruit = df_kaggle_fruit.withColumn("uploaded_date", current_date())

# WRITE DATA TO DATABASE
df_kaggle_fruit.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LET'S CLEAN THE FRUIT BRONZE DATA
# MAGIC TO GET THE FRUIT DATA INTO A USEABLE FORMAT, THE DATA MUST BE IMPROVED BY CHANGING THE DATA TYPES AND THE COLUMN NAMES. THIS STEP WILL EXCLUDE ANY UNNEEDED DATA.

# COMMAND ----------

# QUERY BRONZE TABLE
fruit_bronze = spark.sql('''SELECT * FROM tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_bronze''')

# CHOOSE SELECTED COLUMNS
fruit_bronze = fruit_bronze.select(
    fruit_bronze['name'].alias("food_description"),
    fruit_bronze['water_g'].alias("Water"),
    fruit_bronze['protein_g'].alias("Protein"),
    fruit_bronze['total_fat_g'].alias("Total Fat"),
    fruit_bronze['carbohydrates_g'].alias("Carbohydrates"),
    fruit_bronze['fiber_g'].alias("Fiber"),
    fruit_bronze['sugars_g'].alias("Sugars"),
    fruit_bronze['sodium_g'].alias("Sodium"),
    fruit_bronze['kcal'].alias("Energy kcal"),
    fruit_bronze['kJ'].alias("Energy kJ")
) 

# UNPIVOT THE FOOD DESCRIPTION COLUMN
fruit_bronze = fruit_bronze.unpivot("food_description", ["Water", "Protein", "Total Fat", "Carbohydrates", "Fiber", "Sugars", "Sodium", "Energy kcal", "Energy kJ"], "nutrient", "nutrient_amount")

# SET MEASUREMENT TYPES
fruit_bronze = fruit_bronze.withColumn(
    "unit_name",
    when(fruit_bronze["nutrient"] == "Energy kcal", "kcal")
    .when(fruit_bronze["nutrient"] == "Energy kJ", "kJ")
    .otherwise("g")
)

# FILTER OUT KCAL TO ONLY HAVE ONE ENERGY SOURCE
fruit_bronze = fruit_bronze.filter(col("nutrient") != "Energy kcal")

# REMOVE KJ FROM ENERGY TITLE
fruit_bronze = fruit_bronze.withColumn('nutrient', split(col('nutrient'), 'kJ', 2).getItem(0))

# REMOVE UNNEEDED WORDS AND COMMA
fruit_bronze = fruit_bronze.withColumn('food_description', split(col('food_description'), ' ', 2).getItem(0)).withColumn('food_description', split(col('food_description'), ',', 2).getItem(0))

# CREATE UPLOADED DATA COLUMN
fruit_bronze = fruit_bronze.withColumn("uploaded_date", current_date())

# WRITE DATA TO SILVER TABLE
fruit_bronze.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_silver")

display(fruit_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VEGETABLE DATA
# MAGIC ### IMPORT FILE TO GET VEGETABLE NUTRITION DATA
# MAGIC THIS WILL CREATE A TABLE FOR THE RAW DATA WITH MINIMAL PROCESSING.

# COMMAND ----------

# SET THE SCHEMA
schema_vegetable = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("energy (kcal/kJ)", StringType(), nullable=True),
    StructField("water (g)", DoubleType(), nullable=True),
    StructField("protein (g)", DoubleType(), nullable=True),
    StructField("total fat (g)", DoubleType(), nullable=True),
    StructField("carbohydrates (g)", DoubleType(), nullable=True),
    StructField("fiber (g)", DoubleType(), nullable=True),
    StructField("sugars (g)", DoubleType(), nullable=True),
    StructField("calcium (mg)", DoubleType(), nullable=True),
    StructField("iron (mg)", DoubleType(), nullable=True),
    StructField("magnessium (mg)", DoubleType(), nullable=True),
    StructField("phosphorus (mg)", DoubleType(), nullable=True),
    StructField("potassium (mg)", DoubleType(), nullable=True),
    StructField("sodium (g)", DoubleType(), nullable=True),
    StructField("vitamin A (IU)", DoubleType(), nullable=True),
    StructField("vitamin C (mg)", DoubleType(), nullable=True),
    StructField("vitamin B1 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B2 (mg)", DoubleType(), nullable=True),
    StructField("viatmin B3 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B5 (mg)", DoubleType(), nullable=True),
    StructField("vitamin B6 (mg)", DoubleType(), nullable=True),
    StructField("vitamin E (mg)", DoubleType(), nullable=True)
])

# READ IN THE FILE
df_kaggle_vegetable = spark.read.csv("/Volumes/tabular/dataexpert/josephgabbrielle62095/nutrition_data/vegetables.csv", schema=schema_vegetable, header=True)

# SELECT COLUMNS AND RENAME THEM
df_kaggle_vegetable = df_kaggle_vegetable.select(
    col("name"),
    col("energy (kcal/kJ)").alias("energy_kcal_kj"),
    col("water (g)").alias("water_g"),
    col("protein (g)").alias("protein_g"),
    col("total fat (g)").alias("total_fat_g"),
    col("carbohydrates (g)").alias("carbohydrates_g"),
    col("fiber (g)").alias("fiber_g"),
    col("sugars (g)").alias("sugars_g"),
    col("calcium (mg)").alias("calcium_mg"),
    col("iron (mg)").alias("iron_mg"),
    col("magnessium (mg)").alias("magnessium_mg"),
    col("phosphorus (mg)").alias("phosphorus_mg"),
    col("potassium (mg)").alias("potassium_mg"),
    col("sodium (g)").alias("sodium_g"),
    col("vitamin A (IU)").alias("vitamin_a_iu"),
    col("vitamin C (mg)").alias("vitamin_c_mg"),
    col("vitamin B1 (mg)").alias("vitamin_b1_mg"),
    col("vitamin B2 (mg)").alias("vitamin_b2_mg"),
    col("viatmin B3 (mg)").alias("vitamin_b3_mg"),
    col("vitamin B5 (mg)").alias("vitamin_b5_mg"),
    col("vitamin B6 (mg)").alias("vitamin_b6_mg"),
    col("vitamin E (mg)").alias("vitamin_e_mg")
)

# REMOVE THE /
df_kaggle_vegetable =df_kaggle_vegetable.withColumn("kcal", split(df_kaggle_vegetable["energy_kcal_kj"], "/").getItem(0)).withColumn("kJ", split(df_kaggle_vegetable["energy_kcal_kj"], "/").getItem(1))

# CAST THE COLUMNS TO DOUBLE
df_kaggle_vegetable = df_kaggle_vegetable.withColumn('kcal', col('kcal').cast('double')) \
       .withColumn('kJ', col('kJ').cast('double'))

# LOOK AT TABLE AND THE SCHEMA
display(df_kaggle_vegetable)
df_kaggle_vegetable.printSchema()

# ADD UPLOADED TIMESTAMP COLUMN
df_kaggle_vegetable = df_kaggle_vegetable.withColumn("uploaded_date", current_date())

# WRITE TO BRONZE TABLE
df_kaggle_vegetable.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_bronze")

display(df_kaggle_vegetable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LET'S CLEAN THE VEGETABLE BRONZE DATA
# MAGIC TO GET THE VEGETABLE DATA INTO A USEABLE FORMAT, THE DATA MUST BE IMPROVED BY CHANGING THE DATA TYPES AND THE COLUMN NAMES. THIS STEP WILL EXCLUDE ANY UNNEEDED DATA.

# COMMAND ----------

# QUERY THE BRONZE TABLE
vegetable_bronze = spark.sql('''SELECT * FROM tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_bronze''')

# SELECT COLUMNS AND RENAME THEM
vegetable_bronze = vegetable_bronze.select(
    vegetable_bronze['name'].alias("food_description"),
    vegetable_bronze['water_g'].alias("Water"),
    vegetable_bronze['protein_g'].alias("Protein"),
    vegetable_bronze['total_fat_g'].alias("Total Fat"),
    vegetable_bronze['carbohydrates_g'].alias("Carbohydrates"),
    vegetable_bronze['fiber_g'].alias("Fiber"),
    vegetable_bronze['sugars_g'].alias("Sugars"),
    vegetable_bronze['sodium_g'].alias("Sodium"),
    vegetable_bronze['kcal'].alias("Energy kcal"),
    vegetable_bronze['kJ'].alias("Energy kJ")
) 

# UNPIVOT THE FOOD DESCRIPTION COLUMN
vegetable_bronze = vegetable_bronze.unpivot("food_description", ["Water", "Protein", "Total Fat", "Carbohydrates", "Fiber", "Sugars", "Sodium", "Energy kcal", "Energy kJ"], "nutrient", "nutrient_amount")

# SET UNIT_NAME
vegetable_bronze = vegetable_bronze.withColumn(
    "unit_name",
    when(vegetable_bronze["nutrient"] == "Energy kcal", "kcal")
    .when(vegetable_bronze["nutrient"] == "Energy kJ", "kJ")
    .otherwise("g")
)

# REMOVE UNNEEDED DATA
vegetable_bronze = vegetable_bronze.filter(col("nutrient") != "Energy kcal")

# REMOVE KJ FROM ENERGY TITLE
vegetable_bronze = vegetable_bronze.withColumn('nutrient', split(col('nutrient'), 'kJ', 2).getItem(0))

# CREATE TIMESTAMP OF UPLOADING TIME
vegetable_bronze = vegetable_bronze.withColumn("uploaded_date", current_date())

# OVERWRITE THE VEGETABLE SILVER TABLE
vegetable_bronze.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_silver")

display(vegetable_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LET'S PERFORM THE UNIT TESTS
# MAGIC BY PERFORMING UNIT TESTS, END USERS CAN BE SURE OF THE QUALITY OF THE DATA. THIS WILL AVOID PUTTING INCORRECT OR WRONG DATA INTO PRODUCTION. DATASET OWNERS FREQUENTLY CHANGE THE DATA SCHEMA OR DATA TYPE. THIS WILL FIND ANY CHANGES THAT WOULD AFFECT PRODUCTION DATA.

# COMMAND ----------

# CREATE A FUNCTION TO MAKE SURE NEEDED COLUMNS ARE NOT NULL AND THAT DATA EXISTS IN THE DATASET FOR BOTH THE FRUIT AND VEGETABLE DATA

def run_query_dq_check():
    # CHECK FRUIT DATASET
    results_fruit = spark.sql(f"""
    SELECT 
        food_description,
        SUM(CASE WHEN food_description IS NULL THEN 1 ELSE 0 END) = 0 AS description_should_not_be_null,
        SUM(CASE WHEN nutrient IS NULL THEN 1 ELSE 0 END) = 0 AS name_should_not_be_null,
        SUM(CASE WHEN unit_name IS NULL THEN 1 ELSE 0 END) = 0 AS unit_name_should_not_be_null,
        COUNT(*) > 0 AS is_there_data
    FROM tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_silver
    GROUP BY food_description
    """)

    # CHECK IF DATASET IS EMPTY
    if results_fruit.count() == 0:
        raise ValueError("The fruit query returned no results!")

    # ITERATE THROUGH 
    for result_fruit in results_fruit.collect():  # CONVERT TO ROW OBJECTS
        for column in result_fruit.asDict().values():  # EXTRACT VALUES FROM ROWS
            if isinstance(column, bool):  # CHECK IF VALUE IS BOOLEAN
                assert column, f"Fruit Data quality check failed: {column} is False"
    
    # CHECK VEGETABLE DATASET
    results_vegetable = spark.sql(f"""
    SELECT 
        food_description,
        SUM(CASE WHEN food_description IS NULL THEN 1 ELSE 0 END) = 0 AS description_should_not_be_null,
        SUM(CASE WHEN nutrient IS NULL THEN 1 ELSE 0 END) = 0 AS name_should_not_be_null,
        COUNT(*) > 0 AS is_there_data
    FROM tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_silver
    GROUP BY food_description
    """)

    # CHECK IF DATASET IS EMPTY
    if results_vegetable.count() == 0:
        raise ValueError("The vegetable query returned no results!")

    # ITERATE THROUGH
    for result_vegetable in results_vegetable.collect():  # CONVERT DATAFRAME TO LIST OF OBJECTS
        for column in result_vegetable.asDict().values():  # EXTRACT VALUES FROM ROWS
            if isinstance(column, bool):  # CHECK IF VALUE IS BOOLEAN
                assert column, f"Vegetable Data quality check failed: {column} is False"

run_query_dq_check()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## COMBINE FRUIT AND VEGETABLE TABLES FOR EASIER USE

# COMMAND ----------

# UNION FRUIT AND VEGETABLE TABLE
kaggle_nutrition = spark.sql ('''
SELECT * FROM tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_silver

UNION ALL

SELECT * FROM tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_silver
''')
display(kaggle_nutrition)

# COMMAND ----------

# WRITE DATA TO THE GOLD TABLE
kaggle_nutrition.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_nutrition_gold")

# WRITE DATA TO THE GOLD TABLE
vegetable_bronze.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_gold")

# WRITE DATA TO THE GOLD TABLE
fruit_bronze.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC --- REMOVE TODAY'S UPLOAD FROM THE SILVER TABLE
# MAGIC DELETE FROM tabular.dataexpert.josephgabbrielle62095_kaggle_fruit_silver where uploaded_date = current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC --- REMOVE TODAY'S UPLOAD FROM THE SILVER TABLE
# MAGIC DELETE FROM tabular.dataexpert.josephgabbrielle62095_kaggle_vegetable_silver where uploaded_date = current_date()