import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    '''
    This function creates our spark session which will be used throughout the program.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function contains the first half of our program. It will read the json files located on udacity's public s3 bucket and build a spark dataframe from the data.
    We will then wrangle and query the spark dataframe to populate our songs and artists table.
    These tables are written into our private s3 bucket in proper .parquet file formats with appropriate partition keys.
    '''


    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_data_df = spark.read.option("header",True).json(song_data)
    
    # extract columns to create songs table 
    song_data_df.createOrReplaceTempView("song_data_df_table")
    songs_df = spark.sql('''
                        SELECT DISTINCT(song_id), title, artist_id, year, duration 
                        FROM song_data_df_table
                        WHERE song_id IS NOT NULL AND song_id != ""
                        ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_df.write.option("header",True).partitionBy("year","artist_id").mode("overwrite").parquet(output_data+"songs.parquet")
    
    # extract columns to create artists table
    artists_df = spark.sql('''
                        SELECT DISTINCT(artist_id), artist_name, artist_location, artist_latitude, artist_longitude 
                        FROM song_data_df_table
                        WHERE artist_id IS NOT NULL AND artist_id != "" 
                        ''') 
    
    # write artists table to parquet files
    artists_df.write.option("header",True).mode("overwrite").parquet(output_data+"artists.parquet")
    

def process_log_data(spark, input_data, output_data):
    '''
    This function contains the second half of our etl program.
    First it loads the log data from udacity's public s3 bucket. After the data is properly loaded into a pyspark dataframe we perform some functions on 
    it to make the data ready to use.

    Later we populate the users, time and songplays table with this data.
    All these tables are written into our private s3 bucket in proper .parquet file formats with appropriate partition keys.


    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    log_data_df = spark.read.option("header",True).json(log_data) 
    
    
    # filter by actions for song plays
    log_data_df = log_data_df.filter("page == 'NextSong'").withColumn("start_time",  F.to_timestamp(F.col("ts")/1000)) 

    # extract columns for users table    
    log_data_df.createOrReplaceTempView("log_data_df_table")
    users_df = spark.sql('''
                        SELECT cast(userId as int) userId, firstName, lastName, gender, level 
                        FROM log_data_df_table 
                        WHERE userID IS NOT NULL AND userID != ''
                        ORDER BY userId ASC
    
                        ''')
    users_df = users_df.dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_df.write.option("header",True).partitionBy("gender","level").mode("overwrite").parquet(output_data + "users.parquet")
    
    # extract columns to create time table
    log_data_df.createOrReplaceTempView("log_data_df_table")
    time_df = spark.sql('''
            SELECT 
                start_time, 
                EXTRACT (hour FROM start_time) AS hour,
                EXTRACT (day FROM start_time) AS day,
                EXTRACT (week FROM start_time) AS week,
                EXTRACT(month FROM start_time) as month,
                EXTRACT(year FROM start_time) AS year, 
                EXTRACT(dayofweek FROM start_time) AS weekday
                
            FROM log_data_df_table
            ''')

    time_df = time_df.dropDuplicates(['start_time']) 
    
    # write time table to parquet files partitioned by year and month
    time_df.write.option("header",True).partitionBy("year","month").mode("overwrite").parquet(output_data+"time.parquet")

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    songs_df_for_songplays = spark.read.option("header",True).json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songs_df_for_songplays.createOrReplaceTempView("songs_df_for_songplays_table")
    log_data_df.createOrReplaceTempView("log_data_df_table")

    songplays_df = spark.sql('''
                SELECT
                    start_time,
                    userId,
                    level,
                    s.song_id,
                    s.artist_id,
                    sessionId,
                    location,
                    userAgent,
                    EXTRACT (month FROM start_time) AS month,
                    EXTRACT (year FROM start_time) AS year
                FROM log_data_df_table l
                LEFT JOIN songs_df_for_songplays_table s ON (l.song = s.title AND l.artist = s.artist_name AND l.length = s.duration)

                ''')

    songplays_df = songplays_df.withColumn("songplay_id", monotonically_increasing_id())

    songplays_df = songplays_df.select("songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "year", "month") 

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.option("header",True).partitionBy("year","month").mode("overwrite").parquet(output_data + "songplays.parquet")
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
