
# Purpose of this database in context of the startup, Sparkify, and their analytical goals.
Sparkify's events log data is stored in json files. From these json files it is difficult to do any direct analysis.

Therefore, we had to add them to a database. We added this data into our "staging" table.
Later we used the data stored in the staging tables to populate our designed star schema of 1 Fact and 4 Dimension tables.

Sparkify team can now easily analyze their song plays and gain deep insight into user behaviour on their app. It will help them design a better product for end users and eventually grow their revenues.


# Our design for database schema design and ETL pipeline.
The Songplays table is our main FACT table as it comprises of events. This table in turn include different foreign keys referencing our dimension table. (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
Our dimension tables are songs (song_id, title, artist_id, year, duration), 
artists (artist_id, name, location, lattitude, longitude), 
users (user_id, first_name, last_name, gender, level) and 
time (start_time, hour, day, week, month, year, weekday).

Moreoever, we have used staging tables that first receive all data from json files. Subsequently, the data stored in these staging tables is used to fill our main tables in the star schema model.

By using appropriate distkeys and sortkeys we have ensured fast query times using a minimal of costly AWS resources. 


# How to Run Project Files
1. First Create Tables using create_tables.py
2. Run etl.py to populate the tables with all log and song data.
3. Go to Amazon Redshift and run required queries for data analysis.

# Example queries and results for song play analysis.
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