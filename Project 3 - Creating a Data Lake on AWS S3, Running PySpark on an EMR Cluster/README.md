# DATA LAKE ON AMAZON'S S3 SERVICE QUERIED THROUGH PYSPARK RUNNING ON AMAZON'S EMR SERVICE


## Purpose of this database in context of the startup, Sparkify, and their analytical goals.
Sparkify's events log data is stored in json files. From these json files it is difficult to do any direct analysis.

Therefore, we had to turn them into a star schem. 
Later we used the data stored in log_files and song_data to populate our designed star schema of 1 Fact and 4 Dimension tables.

Sparkify team can now easily analyze their song plays and gain deep insight into user behaviour on their app. It will help them design a better product for end users and eventually grow their revenues.


## Our design for database schema design and ETL pipeline.
The Songplays table is our main FACT table as it comprises of events. (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
Our dimension tables are songs (song_id, title, artist_id, year, duration), 
artists (artist_id, name, location, lattitude, longitude), 
users (user_id, first_name, last_name, gender, level) and 
time (start_time, hour, day, week, month, year, weekday).

## Implementation of Data Lake
1. Define process_song_data function which reads song_data json files from udacity's s3 bucket.
2. After all the data is read by our sparksession. We will start to extract relevant columns from the overall dataframe. 
3. Transform and Load the songs table. We remove duplicate values on songId.
4. Transform and Load the artists table. We remove duplicate values on artistId.
5. Define process_log_data which reads log_data json files from udacity's public s3 bucket.
6. Create and fill users table. We remove duplicate values on UserId.
7. Create and fill time table with all different values of year, month, day, week etc. We remove duplicate values on the 'start_time' timestamp column.
8. Finally create our songplays table which will need the log data joined with songs table. A songplay_id column has been included which includes unique integer values for all columns.

Note: All output files are saved in parquet format which ensures fast retrieval of information for quick querying. Output files are also partitioned in folders according to most suitable values.


## How to Run Project Files
1. Open Terminal run the python file etl.py. (please note that since it is reading and writing actual files to s3 storage buckets, it takes some time for the file to run.)


## Example queries and results for song play analysis.
1. Most Popular Song by Number of Users Listening. We can calculate most popular song by number of users by using our songplay and user tables.

    SELECT	
    songs.title,
    artists.name,
	count(distinct(user_id)) AS users
FROM songplays
JOIN songs on songplays.song_id = songs.song_id     
JOIN artists ON songplays.artist_id = artists.artist_id
GROUP BY songs.title, artists.name
ORDER BY users DESC
LIMIT 5

    RESULT: You're The One by Dwight Yoakam is the most popular song with 22 Unique Users having listened to it.

2. Most Popular Song by Number of Plays
SELECT  
	songs.title,
	count(songs.title) ss
FROM songplays
JOIN songs on songplays.song_id = songs.song_id                           
GROUP BY songs.title
ORDER BY ss DESC
LIMIT 5

    RESULT: You're The One by Dwight Yoakam is the most popular song by total number of plays with 37 total playcount.

3. Artists with the Highest Number of Songs included in the database
SELECT  
	artists.name, 
	COUNT(DISTINCT(songs.song_id)) as ss
FROM songs
JOIN artists ON songs.artist_id = artists.artist_id
GROUP BY artists.name       
ORDER BY ss DESC         
LIMIT 5

    RESULT: 6 artists are tied with the highest number of songs included in database at 9 songs each. These artists are "Alison Krauss / Union Station", "Polygon Window", "Badly Drawn Boy", "Aphex Twin", "Bill & Gloria Gaither" and "Alison Krauss"