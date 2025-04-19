# Databricks notebook source
# MAGIC %sh pip install geopy

# COMMAND ----------

import requests
import pandas as pd
import time
import pyspark
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC ---drop table tabular.dataexpert.josephgabbrielle62095_dpd_accidents_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC ---drop table tabular.dataexpert.josephgabbrielle62095_dpd_accidents_bronze
# MAGIC ---DELETE FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_bronze where event_date = current_date()

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_silver where event_date = current_date()

# COMMAND ----------

# Initialize geolocator
geolocator = Nominatim(user_agent="dallas_data_geocoder")

def fetch_data_from_api(url):
    """Fetch data from the API and return it as a Pandas DataFrame."""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            df['Address'] = df['block'] + ' ' + df['location'] + ' ' + 'Dallas, TX'
            print(f"✅ Successfully retrieved {len(df)} records!")
            return df
        else:
            print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
            return None
    except Exception as e:
        print(f"⚠️ Error fetching data: {e}")
        return None

def geocode_address(address):
    """Geocode an address and return latitude & longitude."""
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None  # No result found
    except GeocoderTimedOut:
        print(f"⏳ Timeout for address: {address}")
        return None, None

def add_geocode_data(df, address_column):
    """Add latitude and longitude columns to DataFrame by geocoding addresses."""
    latitudes, longitudes = [], []

    for address in df[address_column]:
        lat, lon = geocode_address(address)
        latitudes.append(lat)
        longitudes.append(lon)
        time.sleep(1)  # To avoid rate limiting

    df["latitude"] = latitudes
    df["longitude"] = longitudes
    return df

# API Endpoint
url = "https://www.dallasopendata.com/resource/5rz9-3h28.json"

# Fetch data
raw_dpd_df = fetch_data_from_api(url)

# Ensure the DataFrame has an address column (adjust column name as needed)
if raw_dpd_df is not None and "Address" in raw_dpd_df.columns:
    raw_dpd_df = add_geocode_data(raw_dpd_df, "Address")
    print(raw_dpd_df.head())  # Show results with latitude & longitude
else:
    print("⚠️ Address column not found in dataset. Check the API response.")

# COMMAND ----------

bronze_table = spark.createDataFrame(raw_dpd_df)
bronze_table = bronze_table.withColumn("event_date", current_date())

# COMMAND ----------

# write the data to the bronze table
bronze_table.write.mode("append").saveAsTable("tabular.dataexpert.josephgabbrielle62095_dpd_accidents_bronze")
display(bronze_table)

# COMMAND ----------

spark_df = spark.sql('''
        SELECT 
            incident_number,
            division,
            nature_of_call,
            Address,
            CASE 
                WHEN address IS NULL THEN NULL
                ELSE try_cast(latitude AS DOUBLE)
            END AS latitude,
            CASE 
                WHEN address IS NULL THEN NULL
                ELSE try_cast(longitude AS DOUBLE)
            END AS longitude,
            event_date
        FROM tabular.dataexpert.josephgabbrielle62095_dpd_accidents_bronze 
        WHERE nature_of_call like '%Accident%'
        AND event_date = current_date() ---- INTERVAL 1 DAY
        ''')
spark_df = spark_df.dropDuplicates()
display(spark_df)

# COMMAND ----------

'''
spark_df = bronze_table.filter(col("nature_of_call").like("%Accident%"))
spark_df = spark_df \
                .select(
                    spark_df['incident_number'], 
                    spark_df['division'],
                    spark_df['nature_of_call'],
                    spark_df['Address'].alias('address'),
                    spark_df['latitude'],
                    spark_df['longitude'],
                    spark_df['event_date']
                )

spark_df = spark_df.withColumn(
    "latitude",
    when(col("address").isNull() | (col("address") == ""), None).otherwise(col("latitude"))
).withColumn(
    "longitude",
    when(col("address").isNull() | (col("address") == ""), None).otherwise(col("longitude"))
).dropDuplicates()
'''

# COMMAND ----------

display(spark_df)

# COMMAND ----------

# write the data to the silver table
spark_df.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_dpd_accidents_silver")