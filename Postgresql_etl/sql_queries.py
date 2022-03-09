# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = "CREATE TABLE IF NOT EXISTS songplays\
                                (songplay_id serial PRIMARY KEY, start_time time NOT NULL,\
                                 user_id varchar(60) NOT NULL, level varchar(255), \
                                 song_id varchar(255), artist_id varchar(255),\
                                 session_id int , location varchar(255),\
                                 user_agent varchar(255) NOT NULL, unique (songplay_id),\
                                 CONSTRAINT fk_artists_0 FOREIGN KEY(artist_id)\
                                 REFERENCES artists(artist_id),\
                                 CONSTRAINT fk_song_id FOREIGN KEY(song_id)\
                                 REFERENCES songs(song_id),\
                                 CONSTRAINT fk_users FOREIGN KEY(user_id)\
                                 REFERENCES users(user_id))"

user_table_create = "CREATE TABLE IF NOT EXISTS users\
                            (user_id varchar(60) PRIMARY KEY, first_name varchar(255) NOT NULL,\
                            last_name varchar(255) NOT NULL, gender varchar(60), level varchar(255),\
                            unique (user_id))"

artist_table_create = "CREATE TABLE IF NOT EXISTS artists\
                            (artist_id varchar(255) PRIMARY KEY, name varchar(255) NOT NULL,\
                            location varchar(255), latitude float, longitude float,\
                            unique (artist_id))"

song_table_create = "CREATE TABLE IF NOT EXISTS songs\
                            (song_id varchar(255) PRIMARY KEY, title varchar(255) NOT NULL,\
                            artist_id varchar(255) NOT NULL, year int, duration float,\
                            unique (song_id), CONSTRAINT fk_artists FOREIGN KEY(artist_id)\
                            REFERENCES artists(artist_id))"

time_table_create = "CREATE TABLE IF NOT EXISTS time\
                            (start_time timestamp, hour int, day int, week int,\
                            month int, year int, weekday int)"


# INSERT RECORDS

songplay_table_insert =  "INSERT INTO songplays\
                        (start_time, user_id, level, song_id, artist_id,\
                        session_id, location, user_agent)\
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"

user_table_insert = "INSERT INTO users\
                    (user_id, first_name, last_name, gender, level)\
                    VALUES (%s,%s,%s,%s,%s)\
                    ON CONFLICT (user_id)\
                    DO UPDATE \
                    SET first_name  = EXCLUDED.first_name,\
                    last_name = EXCLUDED.last_name,\
                    gender = EXCLUDED.gender,\
                    level = EXCLUDED.level"

song_table_insert = "INSERT INTO songs\
                    (song_id, title, artist_id, year, duration)\
                    VALUES (%s,%s,%s,%s,%s)\
                    ON CONFLICT (song_id)\
                    DO UPDATE \
                    SET title  = EXCLUDED.title,\
                    artist_id = EXCLUDED.artist_id,\
                    year = EXCLUDED.year,\
                    duration = EXCLUDED.duration" 

artist_table_insert = "INSERT INTO artists\
                        (artist_id, name, location, latitude, longitude)\
                        VALUES (%s,%s,%s,%s,%s)\
                        ON CONFLICT (artist_id)\
                        DO UPDATE\
                        SET name  = EXCLUDED.name,\
                        location = EXCLUDED.location,\
                        latitude = EXCLUDED.latitude,\
                        longitude = EXCLUDED.longitude;"


time_table_insert = "INSERT INTO time\
                    (start_time, hour, day, week, month, year, weekday)\
                    VALUES (%s,%s,%s,%s,%s,%s,%s)"

# FIND SONGS

song_select = "SELECT song_id, s.artist_id\
                FROM songs s\
                JOIN artists a ON s.artist_id = a.artist_id\
                WHERE title=%s\
                AND name=%s\
                AND duration=%s"

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]