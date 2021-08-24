import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """ Go through a json file given by filepath and enter it's values in a pandas dataframe.
        Extract required columns from the dataframe and enter them into the database tables of "songs" and "artists".
        
        Care is taken to avoid duplicates. 
    
    
    
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates(subset=['song_id']).values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' ]].drop_duplicates(subset=['artist_id']).values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """ Open and process a log file of json format given by the filepath in the function arguments.
        From this file, extract the data and enter into a dataframe.
        Later, relevant columns of the dataframe are seperated to include them in database tables of time, users and songplays.
    """
    
    
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['dtvalues'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    
    time_data = [t.values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ('timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday')
    
    timedict = {}

    for i in range(0, len(column_labels)):
        timedict[column_labels[i]] = time_data[i]
    
    time_df = pd.DataFrame.from_dict(timedict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.dtvalues, row.userId, row.level, songid,  artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    
    """ This function allows us to go through multiple json files present in the directory at once.
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()