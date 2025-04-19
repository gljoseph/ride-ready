# Databricks notebook source
# MAGIC %md
# MAGIC # NATIONAL WEATHER SERVICE
# MAGIC This Python script retrieves weather data from the National Weather Service (NWS) API to get hourly and weekly forecasts, which are updated every hour.
# MAGIC
# MAGIC **Fetch Data** → The script sends a GET request to the NWS API using a specific location’s latitude and longitude.
# MAGIC
# MAGIC **Extract Forecasts** → It retrieves hourly weather data (temperature, humidity, wind speed, etc.) and weekly weather forecasts.
# MAGIC
# MAGIC **Convert to Structured Format** → The response is processed into a PySpark DataFrame.
# MAGIC
# MAGIC **Update Regularly** → Since the NWS updates its forecasts every hour, the script is scheduled to run periodically to keep the data current.
# MAGIC
# MAGIC This approach automates weather data collection, making it useful for tracking real-time and future weather conditions. 🌤️🌡️

# COMMAND ----------

# IMPORT REQUIRED LIBRARIES
import requests
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## LET'S CREATE BRONZE TABLES
# MAGIC THIS SECTION OF CODE USES NWS API TO ACQUIRE WEATHER DATA. IT CREATES TWO RAW DATA TABLES, ONE FOR HOURLY DATA AND ONE FOR THE WEEKLY FORECAST.

# COMMAND ----------

# DEFINE SCHEMA
period_schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("isDaytime", BooleanType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("temperatureUnit", StringType(), True),
    StructField("windSpeed", StringType(), True),
    StructField("windDirection", StringType(), True),
    StructField("shortForecast", StringType(), True),
    StructField("detailedForecast", StringType(), True)
])

def fetch_weather_data(api_url):
    """Fetch weather forecast data from NWS API and return it as a PySpark DataFrame."""
    try:
        # Step 1: FETCH LOCATION METADATA
        response = requests.get(api_url)
        if response.status_code != 200:
            print(f"❌ Failed to fetch metadata. Status Code: {response.status_code}")
            return None

        metadata = response.json()

        # Step 2: EXTRACT
        forecast_url = metadata.get("properties", {}).get("forecast")
        if not forecast_url:
            print("⚠️ No forecast URL found in metadata!")
            return None
        
        # Step 3: FETCH DATA
        forecast_response = requests.get(forecast_url)
        if forecast_response.status_code != 200:
            print(f"❌ Failed to fetch forecast data. Status Code: {forecast_response.status_code}")
            return None

        forecast_data = forecast_response.json()

        # Step 4: EXTRACT THE DATA
        periods_data = forecast_data["properties"].get("periods", [])

        if not periods_data:
            print("⚠️ No forecast periods available!")
            return None

        # Step 5: CONVERT TO PYSPARK DATAFRAME
        df = spark.createDataFrame(periods_data, schema=period_schema)

        print(f"✅ Successfully retrieved {df.count()} records!")
        return df
    
    except Exception as e:
        print(f"⚠️ Error fetching data: {e}")
        return None

# API ENDPOINT FOR WHITE ROCK LAKE
nws_url =  "https://api.weather.gov/points/38.8894,-77.0352"

# FETCH WEATHER DATA
weather_df = fetch_weather_data(nws_url)

# ADD A TIMESTAMP TABLE
weather_df = weather_df.withColumn("uploaded_timestamp", current_timestamp())

# WRITE THE WEEKLY DATA TO THE BRONZE TABLE
weather_df.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_nws_wrl_weekly_bronze")
display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LET'S CREATE THE NWS HOURLY BRONZE TABLE

# COMMAND ----------

# DEFINE SCHEMA
hourly_period_schema = StructType([
    StructField("number", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("startTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("isDaytime", BooleanType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("temperatureUnit", StringType(), True),
    StructField("windSpeed", StringType(), True),
    StructField("windDirection", StringType(), True),
    StructField("shortForecast", StringType(), True),
    StructField("detailedForecast", StringType(), True)
])

def fetch_hourly_weather_data(api_url):
    """Fetch hourly weather forecast data from NWS API and return it as a PySpark DataFrame."""
    try:
        # FETCH FORECAST DATA
        response = requests.get(api_url)
        if response.status_code != 200:
            print(f"❌ Failed to fetch forecast data. Status Code: {response.status_code}")
            return None

        forecast_data = response.json()

        # EXTRACT PERIOD DATA
        periods_data = forecast_data["properties"].get("periods", [])

        if not periods_data:
            print("⚠️ No forecast periods available!")
            return None

        # CONVERT TO PYSPARK DATAFRAME
        df = spark.createDataFrame(periods_data, schema=hourly_period_schema)

        print(f"✅ Successfully retrieved {df.count()} hourly forecast records!")
        return df
    
    except Exception as e:
        print(f"⚠️ Error fetching data: {e}")
        return None

# API ENDPOINT FOR WHITE ROCK LAKE
nws_hourly_url = "https://api.weather.gov/gridpoints/LWX/97,71/forecast/hourly"

# FETCH FORECAST DATA
hourly_weather_df = fetch_hourly_weather_data(nws_hourly_url)

hourly_weather_df = hourly_weather_df.withColumn("uploaded_timestamp", current_timestamp())

# WRITE DATA TO THE HOURLY BRONZE TABLE
hourly_weather_df.write.mode("overwrite").saveAsTable("tabular.dataexpert.josephgabbrielle62095_nws_wrl_hourly_bronze")
display(hourly_weather_df)