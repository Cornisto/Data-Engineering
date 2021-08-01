## Introduction
Data from all UFC events is available at http://ufcstats.com.
The goal of the project is to build a process that will enable to do the analytics on this data. 

Project consists of the following steps:
1) Scraping data from UFC website using Scrapy and save it in json files. Data will not be preprocessed on the scrape level intentionally to present the data wrangling in Spark.
2) Processing and cleaning raw data and saving data in parquet using Spark
3) Modelling schema for analytics purposes
4) Orchestration of the process using Airflow
5) Creating example queries on the dataset and creating dashboard

## Datasets
The data source consists of the following files:

- fighters.json - File containing basic information about every UFC fighter, like: height, weight, record, profile url etc.

- events.json - File containing information about UFC events (date, location, etc.) and fights along with each fighter's performance statistics

## Project structure
1) spark_etl.py - runs the ETL process using Spark
2) dl.cfg - contains AWS credentials such as access key id and secret access key
3) tests - directory containing tests of particular functionalities
4) delete_s3_bucket.py - empties selected Amazon S3 bucket and then deletes the bucket

## Star Schema
A star schema optimized for queries on fighters and events analysis will be created using Spark. This includes the following tables:

**Fact Table**
1) fights - records containing information about fight and each fighter's statistics in particular round  
    *fight_id, fighter_id, location_id, fight_date, ...*
    
**Dimension Tables**
1) fighters - all fighters that ever fought in UFC  
    *fighter_id, first_name, last_name, nickname, profile_url, birth_date, height, weight, reach, stance, wins, loses, draws, no_contest*
2) locations - locations where events took place  
    *location_id, country, state, city*
3) time - dates of events broken down into specific units  
    *fight_date, day, week, month, year, weekday*
