# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table songplays (
        songplay_id serial primary key, 
        start_time timestamp references time(start_time) not null, 
        user_id int references users(user_id) not null, 
        level text not null, 
        song_id text references songs(song_id), 
        artist_id text references artists(artist_id), 
        session_id int, 
        location text, 
        user_agent text
        )
""")

user_table_create = ("""
create table users (
        user_id int primary key, 
        first_name text, 
        last_name text, 
        gender text, 
        level text)
""")

song_table_create = ("""
create table songs (
        song_id text primary key, 
        title text not null, 
        artist_id text, 
        year int, 
        duration float not null
        )
""")

artist_table_create = ("""
create table artists (
        artist_id text primary key, 
        name text not null, 
        location text, 
        latitude float, 
        longitude float)
""")

time_table_create = ("""
create table time (
        start_time timestamp primary key, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday text)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (songplay_id)
    DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO UPDATE SET level=EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id)
    DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id \
FROM songs \
INNER JOIN artists on songs.artist_id = artists.artist_id \
WHERE songs.title=%s AND songs.duration=%s AND artists.name=%s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]