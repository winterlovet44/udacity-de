import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get('GEO', 'REGION')


# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users CASCADE;"
song_table_drop = "DROP table IF EXISTS songs CASCADE;"
artist_table_drop = "DROP table IF EXISTS artists CASCADE;"
time_table_drop = "DROP table IF EXISTS time CASCADE;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_songs
(
  num_songs int,
  artist_id varchar,
  artist_latitude float,
  artist_longitude float,
  artist_location varchar,
  artist_name varchar,
  song_id varchar,
  title varchar,
  duration float,
  year int
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_events
(
    artist varchar,
    auth varchar, 
    firstName varchar,
    gender varchar,   
    itemInSession int,
    lastName varchar,
    length float,
    level varchar, 
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts timestamp,
    userAgent varchar,
    userId int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int IDENTITY (0,1) primary key,
    start_time timestamp REFERENCES  time(start_time) not null,
    user_id int REFERENCES  users(user_id) not null,
    level varchar not null,
    song_id varchar REFERENCES  songs(song_id),
    artist_id varchar REFERENCES  artists(artist_id),
    session_id int,
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int primary key,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar primary key,
    title varchar not null,
    artist_id varchar,
    year int,
    duration float not null
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar primary key,
    name varchar not null,
    location varchar,
    latitude float,
    logitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp primary key,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    iam_role {role_arn}
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'], 
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    iam_role {role_arn}
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], 
            role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    select distinct se.ts, 
                    se.userId, 
                    se.level, 
                    ss.song_id, 
                    ss.artist_id, 
                    se.sessionId, 
                    se.location, 
                    se.userAgent
    from staging_events se 
    inner join staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    where se.page = 'NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
    select distinct se.userId, 
                    se.firstName, 
                    se.lastName, 
                    se.gender, 
                    se.level
    from staging_events se
    where se.userId IS NOT NULL;
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
    select distinct ss.song_id, 
                    ss.title, 
                    ss.artist_id, 
                    ss.year, 
                    ss.duration
    from staging_songs ss
    where ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, logitude)
    select distinct ss.artist_id, 
                    ss.artist_name, 
                    ss.artist_location,
                    ss.artist_latitude,
                    ss.artist_longitude
    from staging_songs ss
    where ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
    select distinct  se.ts,
                    EXTRACT(hour from se.ts),
                    EXTRACT(day from se.ts),
                    EXTRACT(week from se.ts),
                    EXTRACT(month from se.ts),
                    EXTRACT(year from se.ts),
                    EXTRACT(weekday from se.ts)
    from staging_events se
    where se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
