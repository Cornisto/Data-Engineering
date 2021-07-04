# Purpose of the project
The purpose of this database is to allow Sparkify startup to analyze songs data.  
The analysis will help the company understand which songs are popular and possibly draw conclusions about the trends in the music industry.   
This will also allow them to satisfy their app's users by fulfilling their needs as well as make good financial decisions whether buying the rights from musicians to stream their music will be profitable or not.


# Database design
In order to achieve these goals data should be stored in an intuitive format, queried fast and allow aggregations with ease.
This is why the star schema concept with fact and dimensions tables has been used to solve this problem. For analytics purposes it's best to use the OLAP approach.

Such design of the database will allow Sparkify company to see, for example, which artist is the most often listen to or most listened albums for each artist. The sonplays table contains references to other tables via foreign keys.

## Database tables description
1. users
This table contains information about the Sparkify streaming app such as:
- user_id - unique identifier for user, also the PRIMARY KEY of the table
- first name
- last name
- gender 
- level - account type (free/paid)

2. artists
This table contains information about artists available in Sparkify streaming app such as:
- artist_id - unique identifier for artist, also the PRIMARY KEY of the table
- name - artist name
- location
- latitude
- longitude

3. songs
This table contains information about songs available in Sparkify streaming app such as:
- song_id - unique identifier for song, also the PRIMARY KEY of the table
- artist_id - an id pointing to the artist table containing information about song creator
- title - song title
- year
- duration - song length in seconds

4. time
This table contains information about timestamps of records in songplays on different granularity levels:
- start time - the actual timestamp of a song play, also the PRIMARY KEY of the table
- hour
- day
- week 
- month
- year 
- weekday

5. songplays
- songplay_id - unique identifier for song play, also the PRIMARY KEY of the table
- start_time - FOREIGN KEY to the time table
- user_id - FOREIGN KEY to the user table
- level - account type (free/paid)
- song_id - FOREIGN KEY to the songs table)
- artist_id - FOREIGN KEY to the artists table
- session_id - unique identifier for user session
- location
- user_agent - agent used by user to connect to the Sparkify app


# Project files
Project consists of the following files:
1. sql_queries.py - script storing all queries used in project, including droping and creating tables and other select queries
2. create_tables.py - script used to execute queries that drop tables (if they exist) and create them
3. etl.ipynb - Jupyter Notebook used to design the ETL process
4. etl.py - ETL process used to populate data in the database
5. test.ipynb - script used to check if data was populated correctly in the database


# Running scripts
Python scripts can be run from terminal with the following commands:
1. python create_tables.py
2. python etl.py

ETL script will print out the message after processing each file. In total, 71 song files and 30 log files will be processed.
