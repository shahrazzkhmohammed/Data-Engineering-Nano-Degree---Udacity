import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# The process_song_file function reads the json song files
# inserts the data in song and artist tables accordingly.
def process_song_file(cur, filepath):
    '''Song JSON files in data/song_data are read into a dataframe using pandas'''
    # open song file
    df = pd.read_json(filepath,lines=True)

    '''All songs information is extracted and inserted into the songs table through the postgres cursor'''
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist() 
    cur.execute(song_table_insert, song_data)

    '''All artists information is extracted and inserted into the songs table through the postgres cursor'''
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

# The process_log_file function reads the json log files and
# inserts the data in songplay, time, user tables accordingly.
def process_log_file(cur, filepath):

    '''Log JSON files in data/log_data are read into a dataframe using pandas'''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'],unit='ms')
    
    # insert time data records
    '''The series of tuples is generated which is then converted into a list in the time data frame'''
    time_data = (df['ts'].values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values)
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(data=list(zip(*time_data)), columns=column_labels)


    '''The i,row pairs will contain a column name and every row of data for that column. 
    Using for loop to loop through column names and their data for dataframe then finalluy inserting into the time table'''
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    '''The i,row pairs will contain a column name and every row of data for that column. 
    Using for loop to loop through column names and their data for dataframe then finalluy inserting into the user table'''
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    '''The i,row pairs will contain a column name and every row of data for that column. 
    Using for loop to loop through column names and their data for dataframe then finalluy inserting into the songs table'''
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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    '''The process_data function gets all files matching extension (.json) from the directory. 
    It iterates over files and processes using your function passed into it'''
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