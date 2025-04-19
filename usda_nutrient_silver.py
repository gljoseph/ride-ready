# Databricks notebook source
# MAGIC %md
# MAGIC # USDA NUTRITION AND BRANDED FOOD INFORMATION
# MAGIC This Python script takes the USDA FoodData Central Database bronze tables and changes the data into a useable format.
# MAGIC
# MAGIC **Process & Structure** → The script cleans the data by fixing field types, removing unnecessary data, and restructuring it into a table for easy analysis and access.
# MAGIC
# MAGIC **Perform Unit Tests** → Before pushing the data to the gold table, unit tests check for missing values, data integrity, and consistency.
# MAGIC
# MAGIC **Store in Gold Table** → After validation, the clean data is written to the gold table, ensuring high-quality nutrition information.
# MAGIC
# MAGIC This automated pipeline ensures accurate, nutrition data, supporting stakeholder health goals. 🍎📊

# COMMAND ----------

# IMPORT NECESSARY LIBRARIES
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## LET'S CLEAN THE DATA
# MAGIC TO GET THE USDA DATA INTO A USEABLE FORMAT, THE DATA MUST BE IMPROVED BY CHANGING THE DATA TYPES AND THE COLUMN NAMES. THIS STEP WILL EXCLUDE ANY UNNEEDED DATA.

# COMMAND ----------

# READ IN DATA
food_bronze = spark.sql("SELECT * FROM tabular.dataexpert.josephgabbrielle62095_food_bronze").repartition(10, 'fdc_id')

# SELECT CERTAIN COLUMNS
food_bronze = food_bronze.select("fdc_id", "data_type", "description", "food_category_id", "market_country", "publication_date")

# VIEW THE DATA
display(food_bronze)

# COMMAND ----------

# READ IN DATA
branded_food_bronze = spark.sql("SELECT * FROM tabular.dataexpert.josephgabbrielle62095_branded_food_bronze").repartition(10, 'fdc_id')

# SELECT CERTAIN COLUMNS
branded_food_bronze = branded_food_bronze.select("fdc_id", "brand_owner", "brand_name", "subbrand_name", "ingredients", "not_a_significant_source_of", "serving_size", "serving_size_unit", "branded_food_category", "modified_date")

# VIEW DATA
display(branded_food_bronze)

# COMMAND ----------

# READ IN DATA
usda_food_bronze = spark.sql("SELECT fdcid, description, foodNutrients As nutrients, publicationDate AS publication_date FROM tabular.dataexpert.josephgabbrielle62095_usda_food_bronze").repartition(10, 'fdcId')

# SELECT CERTAIN COLUMNS AND FLATTEN DATA
usda_food_bronze = usda_food_bronze.select("fdcid", "description", "publication_date", explode("nutrients").alias("nutrients"))

# RENAME COLUMNS
usda_food_bronze = usda_food_bronze.select(
    col("fdcid").alias("fdc_id"), 
    col("description").alias("food_description"), 
    col("nutrients.amount").alias("amount"), 
    col("nutrients.derivationCode").alias("derivation_code"), 
    col("nutrients.derivationDescription").alias("derivation_description"), 
    col("nutrients.name").alias("nutrient"), 
    col("nutrients.number").alias("nutrient_amount"), 
    col("nutrients.unitName").alias("unit_name"))

# FILTER OUT UNNECESSARY DATA
usda_food_bronze = usda_food_bronze.filter(
    (col("nutrient") == "Carbohydrate, by summation") | (col("nutrient").like("Energy")) | (col("nutrient").like("%Fatty acids%")) | (col("nutrient").like("Fiber%")) | (col("nutrient") == "Fructose") | (col("nutrient") == "Glucose") | (col("nutrient") == "Protein") | (col("nutrient").like("%Sodium%")) | (col("nutrient") == "Total Sugars") | (col("nutrient") == "Water")
    ).select("fdc_id", "food_description", "derivation_description", "nutrient", "nutrient_amount", "unit_name")

# FILTER OUT ENERGY IN KCAL, KEEPS EVERY IN KJ
usda_food_bronze = usda_food_bronze.filter(col("unit_name") != "KCAL")

# VIEW DATA AND SCHEMA
usda_food_bronze.printSchema()
display(usda_food_bronze)

# COMMAND ----------

# COMBINE DATA SOURCES
food_combined = usda_food_bronze \
                .join(broadcast(branded_food_bronze), usda_food_bronze.fdc_id == branded_food_bronze.fdc_id, "left") \
                .join(broadcast(food_bronze), food_bronze.fdc_id == usda_food_bronze.fdc_id, "left") \
                .select(
                    usda_food_bronze['fdc_id'],
                    usda_food_bronze['food_description'],
                    usda_food_bronze['nutrient'],
                    usda_food_bronze['nutrient_amount'],
                    usda_food_bronze['unit_name'],
                    branded_food_bronze['brand_owner'],
                    branded_food_bronze['brand_name'],
                    branded_food_bronze['subbrand_name'],
                    branded_food_bronze['ingredients'],
                    branded_food_bronze['branded_food_category']
                ) 

# WRITE THE DATA TO THE GOLD TABLE
food_combined.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_nutrient_silver")

# VIEW THE DATA
display(food_combined)


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## LET'S PERFORM THE UNIT TESTS
# MAGIC BY PERFORMING UNIT TESTS, END USERS CAN BE SURE OF THE QUALITY OF THE DATA. THIS WILL AVOID PUTTING INCORRECT OR MISSING DATA INTO PRODUCTION. API OWNERS FREQUENTLY CHANGE THE DATA SCHEMA OR DATA TYPE. THIS WILL FIND ANY CHANGES THAT WILL AFFECT PRODUCTION DATA.

# COMMAND ----------

# QUERY THE SILVER TABLE
nutrient_silver = spark.sql("SELECT * FROM tabular.dataexpert.josephgabbrielle62095_food_nutrient_silver")
display(nutrient_silver)
nutrient_silver.printSchema()

# COMMAND ----------

# PRE-DETERMINED COLUMNS
nutrient_columns = ["fdc_id", "food_description", "nutrient", "nutrient_amount", "unit_name", "brand_owner", "brand_name", "subbrand_name", "ingredients", "branded_food_category"]

# CHECK THAT EVERY COLUMN IS THERE
for i in nutrient_columns:
    if i in nutrient_silver.columns:
        print(f"Column '{i}' exists in DataFrame")
    else:
        raise ValueError(f"Missing column: {i}")

# CHECK THE DATA ISN'T EMPTY
if nutrient_silver.count() > 1:
    print("Data found")
else:
    raise ValueError("There is no data!")

# COLUMNS THAT SHOULDN'T BE NULL
columns_to_check = [
   "fdc_id", "food_description", "nutrient", "nutrient_amount", "unit_name"
]

# LOOP THROUGH THE COLUMNS TO CHECK FOR NULL DATA
for col_name in columns_to_check:
    if nutrient_silver.filter(col(col_name).isNull()).limit(1).count() > 0:
        raise ValueError(f"There is a null in the {col_name} column!")

print("No nulls found in the dataset")

display(nutrient_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## USDA NUTRITION GOLD TABLES
# MAGIC IF THE DATA PASSES THE UNIT TESTS, THEN THE DATA CAN BE WRITTEN INTO THE GOLD TABLE.

# COMMAND ----------

# WRITE THE DATA TO THE GOLD TABLE
nutrient_silver.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_food_nutrient_gold")

# VIEW THE DATA
display(nutrient_silver)