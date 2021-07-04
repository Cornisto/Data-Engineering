import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
[LOG_DATA, JSON_PATH, SONG_DATA] = [*config['S3'].values()]
[ARN] = [*config['IAM_ROLE'].values()]

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(100),
    user_gender VARCHAR(10),
    item_in_session INTEGER,
    user_last_name VARCHAR(150),
    song_length DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id BIGINT,
    song_title VARCHAR(255),
    status INTEGER, 
    ts VARCHAR(50),
    user_agent VARCHAR(255),
    user_id INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL sortkey, 
    user_id INTEGER NOT NULL, 
    level VARCHAR(20), 
    song_id VARCHAR(100) distkey, 
    artist_id VARCHAR(80), 
    session_id BIGINT, 
    location VARCHAR(150), 
    user_agent VARCHAR(200),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY(start_time) REFERENCES time(start_time));
""")

user_table_create = ("""
CREATE TABLE users (
    user_id       INTEGER NOT NULL PRIMARY KEY sortkey,
    first_name    VARCHAR(20) NOT NULL,
    last_name     VARCHAR(50) NOT NULL,
    gender        VARCHAR(1) NOT NULL,
    level         VARCHAR(20) NOT NULL)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id       VARCHAR(80) NOT NULL PRIMARY KEY sortkey distkey,
    title         VARCHAR(200) NOT NULL,
    artist_id     VARCHAR(100) NOT NULL,
    year          INTEGER NOT NULL,
    duration      NUMERIC(14,8) NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id      VARCHAR(100) NOT NULL PRIMARY KEY sortkey,
    name           VARCHAR(200) NOT NULL,
    location       VARCHAR(200),
    latitude       NUMERIC(14,8),
    longitude      NUMERIC(14,8))
diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time     TIMESTAMP NOT NULL PRIMARY KEY sortkey,
    hour           INTEGER NOT NULL,
    day            INTEGER NOT NULL,
    week           INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    year           INTEGER NOT NULL,
    weekday        INTEGER NOT NULL)
diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    TIMESTAMP 'epoch' + E.ts/1000 * interval '1 second' as start_time, 
    E.user_id, 
    E.user_level, 
    S.song_id,
    S.artist_id, 
    E.session_id,
    E.location, 
    E.user_agent
FROM staging_events E, staging_songs S
WHERE E.page = 'NextSong' 
AND E.song_title = S.title 
AND E.artist_name = S.artist_name 
AND E.song_length = S.duration;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    user_id, 
    user_first_name, 
    user_last_name, 
    user_gender, 
    user_level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
