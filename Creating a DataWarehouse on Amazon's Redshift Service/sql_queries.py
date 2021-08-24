import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table
(
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration FLOAT,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts FLOAT,
userAgent VARCHAR,
userId INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table 
(
num_songs INTEGER NOT NULL,
artist_id VARCHAR NOT NULL,
artist_latitude FLOAT, 
artist_longitude FLOAT,
artist_location VARCHAR, 
artist_name VARCHAR, 
song_id VARCHAR NOT NULL, 
title VARCHAR, 
duration FLOAT, 
year INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplays
(
songplay_id INTEGER IDENTITY(0,1), 
start_time TIMESTAMP NOT NULL, 
user_id INTEGER NOT NULL, 
level VARCHAR, 
song_id VARCHAR NOT NULL sortkey distkey, 
artist_id VARCHAR NOT NULL, 
session_id INTEGER, 
location VARCHAR, 
user_agent VARCHAR

)
""")

user_table_create = ("""
CREATE TABLE users
(
user_id INTEGER NOT NULL sortkey, 
first_name VARCHAR, 
last_name VARCHAR, 
gender VARCHAR, 
level VARCHAR

) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs
(
song_id VARCHAR NOT NULL sortkey, 
title VARCHAR, 
artist_id VARCHAR NOT NULL, 
year INTEGER, 
duration FLOAT
) diststyle auto;
""")

artist_table_create = ("""
CREATE TABLE artists
(
artist_id VARCHAR NOT NULL sortkey, 
name VARCHAR, 
location VARCHAR, 
latitude FLOAT,
longitude FLOAT
) diststyle auto;
""")

time_table_create = ("""
CREATE TABLE time
(
start_time TIMESTAMP NOT NULL sortkey, 
hour INTEGER NOT NULL, 
day INTEGER NOT NULL, 
week INTEGER NOT NULL, 
month INTEGER NOT NULL, 
year INTEGER NOT NULL, 
weekday INTEGER NOT NULL
) diststyle all;
""")

# STAGING TABLES


staging_events_copy = ("""
COPY staging_events_table
FROM {}
IAM_ROLE  {}
FORMAT AS JSON {}
REGION 'us-west-2';

""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs_table
FROM {}
IAM_ROLE  {}
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + log.ts/1000 * interval '1 second' as start_time,
    log.userId AS user_id,
    log.level AS level,
    songs.song_id AS song_id,
    songs.artist_id AS artist_id,
    log.sessionId AS session_id,
    log.location AS location,
    log.userAgent AS user_agent
FROM staging_events_table log JOIN staging_songs_table songs ON log.song = songs.title AND log.artist = songs.artist_name
WHERE log.page = 'NextSong' AND log.userId IS NOT NULL
""")

user_table_insert = (""" 
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events_table
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT song_id AS song_id,
    title,
    artist_id,
    year,
    duration

FROM staging_songs_table
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs_table
WHERE artist_id IS NOT NULL

""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day , week, month, year , weekday)
SELECT
    DISTINCT start_time,
    EXTRACT (HOUR FROM start_time),
    EXTRACT (DAY FROM start_time),
    EXTRACT (WEEK FROM start_time),
    EXTRACT (MONTH FROM start_time),
    EXTRACT (YEAR FROM start_time),
    EXTRACT (DAYOFWEEK FROM start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
