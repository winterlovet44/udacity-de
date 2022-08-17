import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process data in song file.
    Load and transform data in song file and write to database.
    
    Parameters
    ----------
    cur: psycopg2 cursor
    filepath: str
        Path to song file
    
    Return
    ------
    None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process log file and write data to database.
    Function will process data in log file and write to
    'table', 'time' and 'playsong'
    
    Parameters
    ----------
    cur: psycopg2 cursor
    filepath: str
        Path to song file
    
    Return
    ------
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    df['ts'] = pd.to_datetime(df.ts, unit='ms')

    # filter by NextSong action
    df = df = df[df.page == 'NextSong'].reset_index(drop=True)

    # convert timestamp column to datetime
    t = df.ts
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.concat(time_data, axis=1)
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.artist))
        results = cur.fetchone()
        
        if results:
            print(results)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (str(row.ts), 
                         row.userId, 
                         row.level, 
                         songid,
                         artistid,
                         row.sessionId,
                         row.location,
                         row.userAgent
                         )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process data in specific file.
    
    Parameters
    ----------
    cur: psycopg2 cursor
    conn: Database connection
    filepath: str
        folder contains data
    func: callable
        Function to process data
    
    Return
    ------
    None
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