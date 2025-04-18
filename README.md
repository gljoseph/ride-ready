# Ride Ready 🚴🏽‍♀️
![sunrise_cyclists](https://github.com/user-attachments/assets/aef2e1aa-de32-4c0a-a416-4aef2312f4d1)

RideReady is a smart cycling companion that helps riders prepare for every journey with confidence. It provides daily updates on local traffic accidents, tailored nutrition guidance, and accurate weather forecasts — all in one place. Whether you're commuting, training, or exploring, RideReady ensures you're informed, fueled, and ready to ride safely.

## Overview 🎯

The scope of this project aims to make cycling in urban environments safer, smarter, and more accessible. By leveraging traffic accident, nutrition, and weather data, the project supports cyclists with key ride planning tools. This project fetches daily updated Dallas Police Department traffic accident data. It also integrates hourly and weekly weather forecasts from the National Weather Service (NWS) API to provide comprehensive weather insights. The US Department of Agriculture provided data from labels of national and international branded foods. The data is updated daily for traffic updates, every hour for weather, and monthly for USDA nutrition data.

The insights are presented through an interactive Databricks dashboard to show traffic accident updates, nutrition updates, and how weather will affect cyclists' rides.

## Problem Statement 🔍

Cyclists in urban environments like Dallas face significant challenges when planning safe and effective rides. These include inconsistent weather forecasts, limited access to real-time safety data, lack of nutritional planning resources, and insufficient information on rest stop availability. Dallas, in particular, has seen a sharp rise in pedestrian and cyclist fatalities—up 172% since 2010—highlighting the need for more informed ride planning.

Despite the availability of data, there is no centralized, user-friendly tool that integrates weather comparisons, traffic incident mapping, nutritional guidance, and route-based rest stop planning. This lack of integration makes it difficult for cyclists to make confident decisions about when, where, and how to ride safely and effectively.

This project addresses these issues by developing a data-driven dashboard and backend pipeline to equip cyclists with critical, real-time information for smarter, safer rides.

## Objectives 📋

1. Develop a comprehensive **data pipeline** that aggregates and processes daily and historical data from multiple sources, including weather forecasts, traffic accident reports, nutrition databases, and location services.

2. Design and build an **interactive dashboard** that presents weather comparisons, accident heatmaps, and nutritional recommendations to support informed ride planning.

3. Enhance cyclist safety and performance by providing a centralized, **user-friendly tool** that helps riders make data-driven decisions about timing, route selection, fueling strategies, and rest stop planning.

## Features

✅ Traffic reporting: Fetches Dallas police traffic accident calls.

✅ Nutrient Data: Fetches nutritional data from labels of national and international branded foods.

✅ Weather forecasting: Pulls hourly and weekly weather forecasts from the NWS API.

✅ Data Cleaning & Processing: Ensures data integrity before storage.

✅ Unit Testing: Validates data accuracy before updating the Gold Table.

✅ Automated Pipeline: Runs continuously to maintain fresh and reliable data.

## Tech Stack

- Python 🐍 (Data fetching, processing, and transformation)

- Apache Spark (PySpark) ⚡ (Data handling and transformations)

- Databricks 📊 (Data processing, storage, orchestration)

## Data Sources

### 1. Dallas Police Department API 🚨
- **About:** This API offers a simple, free way of accessing Dallas police active calls related to traffic accidents. It provides a list of every police call from that day, including incident number, division, locaton, and nature of call. It updates data every day. 
- **Need:** This API provided the location of traffic accidents so cyclists can either avoid the area entirely or be cautious in that area.
- **Data includes:** Incident number, division, nature of call, priority, date time, unit number, block, location, beat, reporting area, and status.
- **Data problems:** This data was limited to when the police were called. It does not include every location that had an accident.

### 2. National Weather Service API ☀️
- **About:** The National Weather Service is a government entity that collects weather data. It provides critical forecast, alerts, and observations. This API offers public access to a wide range of essential weather data. NWS refreshes every hour.
- **Need:** NWS provides hourly and weekly data for DFW airport managers to gauge the likeliness of airport delays due to rain, snow, wind, or other extreme weather.
- **Data includes:** temperature, wind speed, wind direction, short forecast, and a detailed forecast
- **Data problems:** The API documentation did not clearly define the rate limits.

### 3. US Department of Agriculture Branded Foods API 🍎
- **About:** The USDA is a government organization providing data from labels of national and international branded foods collected by a public-private partnership, updated monthly.
- **Need:** This API provides cyclists information about the food they eat. It allows cyclists insight into how much carb, protein, fat, sugar, etc. are in their food so they can determine if their ride will be well fueled.
- **Data includes:** food description, nutrient, nutrient amount, unit name, brand owner, brand name, ingredients, and branded category.
- **Data problems:** This dataset has a large representation of the food eaten in the USA, but does not have every product sold.

## Architecture and Methodology 📝
Medallion Architecture was implemented to enhance data quality, organization, and reliability throughout the data pipeline. 

In the bronze layer, raw data was ingested in its original form, serving as the foundational data source. Before transitioning to the silver layer, rigorous data processing techniques were applied, including data cleansing, transformation, deduplication, and filtering, to ensure consistency and accuracy. Finally, before promoting data to the gold layer, comprehensive unit tests were conducted to validate data integrity, preventing bad or incomplete data from reaching production-level tables. 

This structured approach ensures that only high-quality, reliable data is used for analysis and decision-making.

### Data Pipeline Architecture 🛠️

<img width="834" alt="image" src="https://github.com/user-attachments/assets/552e9c51-b415-4b8f-a3e0-c8d96c093992" />

### Data Model Design ⚙️
<img width="550" alt="image" src="https://github.com/user-attachments/assets/51c9b504-f001-49ae-87c6-8f88709fc9b7" />

In this project, data was intentionally not normalized because the primary use case is visualization via a Databricks dashboard. By maintaining a denormalized structure, we reduce the need for complex joins and improve query performance when retrieving insights. This approach ensures that airport operations and weather data can be accessed quickly and efficiently without unnecessary overhead.

### Batch Processing
- Configured a Databricks workflow to schedule hourly updates, ensuring the dashboard uses the latest National Weather Service and USDA data.

### Data Quality 🔢
To maintain the integrity and reliability of the gold-level tables, unit tests were implemented to validate the data. These tests ensured:
- No null values were present in critical fields.
- All expected columns existed in the dataset.
- Duplicate records were identified and removed.

### Orchestration
<img width="615" alt="image" src="https://github.com/user-attachments/assets/fba961a4-e431-4bc1-bdd6-91b18b53260c" />



- Dallas Police Department workflow was scheduled for 10 minutes before midnight.
- National Weather Service workflow was scheduled for 5 minutes after the hour.
- USDA workflow was scheduled for the first day of every month.

## Key Metrics & User Value 📈
1. **Improved Rider Safety:** By visualizing accident data and identifying high-risk areas, the dashboard empowers cyclists to avoid dangerous routes, contributing to reduced injury rates and increased rider confidence.
2. **Enhanced User Experience & Engagement:** The integration of weather forecasts and fueling guidance delivers a highly personalized and practical planning tool, improving satisfaction and retention for platforms serving cyclists.
3. **Data-Driven Decision Making:** Centralizing key ride planning data enables more informed decisions, which can lead to more consistent ride performance and better health outcomes for users.

## Visualizations 📊
Here are some of the visuals:


**Dallas Car Accidents:**
<img width="1265" alt="image" src="https://github.com/user-attachments/assets/5b9ff585-47f9-40be-8baf-6bef1788c949" />

**Weather:**
<img width="975" alt="image" src="https://github.com/user-attachments/assets/2b4bfaef-6ee7-4dc4-ba8f-4b9028f1ec56" />

**Nutrition Information:**
<img width="1270" alt="image" src="https://github.com/user-attachments/assets/db5f8fdc-af79-4af7-8ce0-136fcff5bd57" />



## Future Enhancements
- Extend data coverage to include more cities so more cyclists can stay safe.
- Improve traffic accident data to include all traffic accidents whether or not the police were called.
- Incorporate UV weather data to enhance weather-related decision-making.
- Add location information for services that cyclists may need: bike shops, gas stations, toilets.
  
## Installation & Setup ⚙️
1. Clone the repository: https://github.com/gljoseph/ride-ready.git

2. Set up the repository on Databricks

3. Install Python libraries: requests, time, pyspark, json

4. Update Volumes table locations

## Contributing

Contributions are always welcome! Feel free to submit issues or pull requests.
