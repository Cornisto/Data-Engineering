# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id SERIAL PRIMARY KEY, 
                                start_time TIMESTAMP NOT NULL, 
                                user_id INT NOT NULL, 
                                level VARCHAR(20), 
                                song_id VARCHAR(80), 
                                artist_id VARCHAR(80), 
                                session_id INT, 
                                location VARCHAR(50), 
                                user_agent VARCHAR(200),
                                CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES users(user_id),
                                CONSTRAINT fk_song FOREIGN KEY(song_id) REFERENCES songs(song_id),
                                CONSTRAINT fk_artist FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
                                CONSTRAINT fk_start_time FOREIGN KEY(start_time) REFERENCES time(start_time)
                                );""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            user_id INT PRIMARY KEY, 
                            first_name VARCHAR(20) NOT NULL, 
                            last_name VARCHAR(50) NOT NULL, 
                            gender VARCHAR(1) NOT NULL, 
                            level VARCHAR(20) NOT NULL);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id VARCHAR(80) PRIMARY KEY, 
                            artist_id VARCHAR(80), 
                            title VARCHAR(100) NOT NULL, 
                            year INT, 
                            duration numeric(14,8));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id VARCHAR(80) PRIMARY KEY, 
                            name VARCHAR(100) NOT NULL, 
                            location VARCHAR(100), 
                            latitude numeric(14,8), 
                            longitude numeric(14,8));""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp PRIMARY KEY, 
                            hour INT NOT NULL,  
                            day INT NOT NULL, 
                            week INT NOT NULL,  
                            month INT NOT NULL,  
                            year INT NOT NULL,  
                            weekday INT NOT NULL);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING;""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;""")

song_table_insert = ("""INSERT INTO songs (song_id, artist_id, title, year, duration) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING;""")

# FIND SONGS

song_select = ("""SELECT S.song_id, A.artist_id 
                    FROM songs S JOIN artists A ON S.artist_id = A.artist_id
                    WHERE S.title = %s AND A.name = %s AND S.duration = %s;""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]