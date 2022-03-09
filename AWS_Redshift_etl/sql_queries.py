
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                        CREATE TABLE IF NOT EXISTS staging_events(
                            artist VARCHAR(255),
                            auth VARCHAR(100),
                            firstName VARCHAR(100),
                            gender VARCHAR(10),
                            itemInSession INT,
                            lastName VARCHAR(100),
                            lenght FLOAT,
                            level VARCHAR(100),
                            location VARCHAR(255),
                            method VARCHAR(100),
                            page VARCHAR(100),
                            registration DECIMAL(38,0),
                            sessionId INT,
                            song VARCHAR (255),
                            status INT,
                            ts BIGINT,
                            userAgent VARCHAR(255),
                            userID INT
                        );
                        """)

staging_songs_table_create = ("""
                        CREATE TABLE IF NOT EXISTS staging_songs (
                            num_songs TEXT,
                            artist_id VARCHAR(30),
                            artist_latitude FLOAT,
                            artist_longitude FLOAT,
                            artist_location VARCHAR(100),
                            artist_name VARCHAR(100),
                            song_id VARCHAR(30),
                            title VARCHAR(100),
                            duration FLOAT,
                            year INT
                        );
                        """)

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                            start_time time,
                            user_id VARCHAR(60),
                            level VARCHAR(255),
                            song_id VARCHAR(255) distkey,
                            artist_id VARCHAR(255) sortkey,
                            session_id INTEGER,
                            location VARCHAR(255),
                            user_agent VARCHAR(255)
                            );

                        """)

user_table_create = ("""
                    CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER sortkey,
                        firstName VARCHAR(100),
                        lastName VARCHAR(100),
                        gender VARCHAR(10),
                        level VARCHAR(100)
                        )
                    diststyle all;
                    """)

song_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (
                        song_id VARCHAR(100) sortkey distkey,
                        title VARCHAR(255),
                        artist_id VARCHAR(100),
                        year INT,
                        duration FLOAT
                        );
                    """)

artist_table_create = ("""
                    CREATE TABLE IF NOT EXISTS artists (
                        artist_id VARCHAR(100) sortkey,
                        name VARCHAR(100),
                        location VARCHAR(255),
                        latitude FLOAT,
                        longitude FLOAT
                    )
                    """)

time_table_create = ("""
                    CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP,
                    hour INT,
                    day INT,
                    week INT,
                    month INT,
                    year INT,
                    weekday INT
                    )
                    
                    """)

# STAGING TABLES

staging_events_copy = (""" COPY staging_events from 's3://udacity-dend/log_data'
                        credentials 'aws_iam_role={}'
                        compupdate off
                        region 'us-west-2'
                        JSON 's3://udacity-dend/log_json_path.json';
                        """).format(config.get("IAM_ROLE","ARN"))

staging_songs_copy =  (""" COPY staging_songs from 's3://udacity-dend/song_data'
                        credentials 'aws_iam_role={}'
                        compupdate off 
                        region 'us-west-2'
                        JSON 'auto' truncatecolumns;
                        """).format(config.get("IAM_ROLE","ARN"))

# FINAL TABLES

user_table_insert = ("""
                    INSERT INTO users(
                        user_id,
                        firstName,
                        lastName,
                        gender,
                        level
                    )
                    SELECT DISTINCT
                    e.userID,
                    e.firstName,
                    e.lastName,
                    e.gender,
                    e.level
                    FROM staging_events e
                    """)

song_table_insert = ("""
                    INSERT INTO songs(
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration
                    )
                    SELECT DISTINCT
                    s.song_id,
                    s.title,
                    s.artist_id,
                    s.year,
                    s.duration
                    FROM
                    staging_songs s
                    """)

artist_table_insert = ("""
                        INSERT INTO artists(
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude
                        )
                        SELECT DISTINCT
                        s.artist_id,
                        s.artist_name,
                        s.artist_location,
                        s.artist_latitude,
                        s.artist_longitude
                        FROM
                        staging_songs s
                        """)

                       
time_table_insert = ("""
                    INSERT INTO time(
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday
                    )
                    SELECT DISTINCT
                    start_time,
                    EXTRACT(HOUR FROM start_time) AS hour,
                    EXTRACT(DAY FROM start_time) AS day,
                    EXTRACT(WEEK FROM start_time) AS week,
                    EXTRACT(MONTH FROM start_time) AS month,
                    EXTRACT(YEAR FROM start_time) AS year,
                    EXTRACT(DOW FROM start_time) AS weekday
                    FROM (
                    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time
                    FROM staging_events
                    ) 
                    """)

songplay_table_insert = (""" 
                        INSERT INTO songplays (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                        )
                        SELECT
                        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second',
                        e.userID,
                        e.level,
                        s.song_id,
                        s.artist_id,
                        e.sessionId,
                        e.location,
                        e.userAgent
                        FROM
                        staging_events e,
                        staging_songs s
                        WHERE
                        e.artist = s.artist_name
                        AND e.song = s.title
                        AND e.lenght = s.duration
                        AND e.page = 'NextSong'
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
