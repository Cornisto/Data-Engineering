# Purpose of the project
The purpose of the project is to allow Sparkify startup company to analyze songs data.  
The analysis will help the company understand which songs are popular and possibly draw conclusions about the trends in the music industry. 
This will also allow them to satisfy their app's users by fulfilling their needs as well as make good financial decisions whether buying the rights from musicians to stream their music will be profitable or not.


# Datasets
## Songs dataset
The songs dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. 
The files are partitioned by the first three letters of each song's track ID.

## Logs dataset
The logs dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset are partitioned by year and month.

Below there is an example of how the logs file looks like:
<img src="images/log-data.png">


# Database design
In order to achieve these goals data should be stored in an intuitive format, queried fast and allow aggregations with ease.
This is why the star schema concept with fact and dimensions tables has been used to solve this problem. For analytics purposes it's best to use the OLAP approach.

Such design of the database will allow Sparkify company to see, for example, which artist is the most often listen to or most listened albums for each artist. The sonplays table contains references to other tables so it's easy to query them.

## Staging Tables
- staging_events - table storing data from log files
- staging_songs - table storing data from songs files 

## Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong. Data will be distributed across the nodes by song_id distrubution key, because it will be often used by queries, for example to see how many times the song was played or how many listeners does it have.

## Dimension Tables
- users - users in the app
- songs - songs in music database. Data will be distributed across the nodes by song_id distrubution key.
- artists - artists in music database. Since this table is relatively small it can be distributed across the nodes with ALL distribution style.
- time - timestamps of records in songplays broken down into specific units. Since this table is relatively small it can be distributed across the nodes with ALL distribution style.


# Project files
Project consists of the following files:
1. dwh.cfg - config file with settings regarding Redshift cluster, IAM role, database credentials and data source
2. sql_queries.py - script storing all queries used in project, including droping and creating tables and other select queries
3. create_tables.py - script used to execute queries that drop tables (if they exist) and create them
4. etl.py - ETL process used to populate data in the database


# Running scripts
Python scripts can be run from terminal with the following commands:
1. python create_tables.py
2. python etl.py