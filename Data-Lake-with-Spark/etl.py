import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# from pyspark.sql.functions import udf, col
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"].get('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"].get('AWS_SECRET_ACCESS_KEY')
access_key = config["AWS"].get('AWS_ACCESS_KEY_ID')
secret_key = config["AWS"].get('AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Create a Spark session.
    
    Parameter
    ---------
    None
    
    Return:
    Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Perform ELT progress and extract data for 
    Songs and artists file.
    
    Parameters
    ----------
    spark: Spark session
    input_data: str
        S3 bucket URI of input data
    output_data: str
        S3 bucket URI of output path
        
    Return
    ------
    None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
#     song_data = os.path.join(input_data, "song_data/A/B/A/*.json")
    
    
    # read song data file
    print("Read data...")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_cols).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = os.path.join(output_data, "songs.parquet")
    print(f"write songs data to {songs_output}")
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
                .parquet(path=songs_output)

    # extract columns to create artists table
    artists_cols = ['artist_id', 'artist_name', 'artist_location',
                    'artist_latitude', 'artist_longitude']
    artists_table = df.select(artists_cols).distinct()
    
    # write artists table to parquet files
    artist_output = os.path.join(output_data, "artists.parquet")
    print(f"write artists data to {artist_output}")
    artists_table.write.mode("overwrite").parquet(path=artist_output)


def process_log_data(spark, input_data, output_data):
    """
    Perform ELT progress and extract data for 
    users, songplays and time file.
    
    Parameters
    ----------
    spark: Spark session
    input_data: str
        S3 bucket URI of input data
    output_data: str
        S3 bucket URI of output path
        
    Return
    ------
    None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data + "log_data")
#     log_data = "s3a://udacity-dend/log_data/2018/11/*"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.where(F.col("page") == 'NextSong')

    # extract columns for users table    
    users_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = log_df.select(users_cols).distinct()
    
    # write users table to parquet files
    users_output = os.path.join(output_data, "users.parquet")
    print(f"write user data to {users_output}")
    users_table.write.mode("overwrite").parquet(path=users_output)

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn("start_time", (F.col('ts')/1000)\
                               .cast("timestamp").alias('start_time'))
    
    # create datetime column from original timestamp column
    log_df = log_df.withColumn('weekday', F.date_format(F.col('start_time'), 'E'))
    log_df = log_df.withColumn('year', F.year(F.col('start_time')))
    log_df = log_df.withColumn('month', F.month(F.col('start_time')))
    log_df = log_df.withColumn('week', F.weekofyear(F.col('start_time')))
    log_df = log_df.withColumn('day', F.dayofmonth(F.col('start_time')))
    log_df = log_df.withColumn('hour', F.hour(F.col('start_time')))
    
    # extract columns to create time table
    time_cols = ['start_time', 'weekday', 'year', 'month', 'week', 'day', 'hour']
    time_table = log_df.select(time_cols).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_output = os.path.join(output_data, "time.parquet")
    print(f"write time data to {time_output}")
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
                            .parquet(path=time_output)

    # read in song data to use for songplays table
    song_path = os.path.join(input_data, "song_data/*/*/*/*.json")
#     song_path = os.path.join(input_data, "song_data/A/B/A/*.json")
    song_df = spark.read.json(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    # Because song_df also has year column, we need to replace column year 
    # in song_df to another name before join two df
    merged_df = log_df.join(song_df.withColumnRenamed('year', 'year_song'),\
                                  (log_df.song == song_df.title)\
                                   & (log_df.artist == song_df.artist_name)\
                                   & (log_df.length == song_df.duration), "left")
    songplays_cols = ['start_time', 'userId', 'level', 'song_id',
                      'artist_id', 'sessionId','location','userAgent',
                      'year', 'month']
    songplays_table = merged_df.select(songplays_cols).distinct()
    # write songplays table to parquet files partitioned by year and month
    songplays_output = os.path.join(output_data, "songplays.parquet")
    print(f"write songplay data to {songplays_output}")
    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                                .parquet(path=songplays_output)


def main():
    spark = create_spark_session()
    print(access_key,secret_key)
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifyoutput/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
