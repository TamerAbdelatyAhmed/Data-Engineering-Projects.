import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.8.5') \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        col('song_id'), 
        col('title'),
        col('artist_id'), 
        col('year'), 
        col('duration')
    ).distinct()

    # write songs table to parquet files partitioned by year and month
    songs_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songs.parquet"), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(
        col('artist_id'), 
        col('artist_name'), 
        col('artist_location'),
        col('artist_latitude'), 
        col('artist_longitude')
    ).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), mode="overwrite")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    user_table = df.select(
        col('userId').alias('user_id'),
        col('firstName').alias('first_name'), 
        col('lastName').alias('last_name'), 
        col('gender'), 
        col('level')
    ).distinct()

    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, "users.parquet"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), T.TimestampType())

    df = df.withColumn('timestamp', get_timestamp(col('ts')))

    # create datetime column from original timestamp column
    df = df.withColumn('start_time', get_timestamp(col('ts')))

    # extract columns to create time table
    time_table = df.select(
        col('start_time'), 
        hour('timestamp').alias('hour'), 
        dayofmonth('timestamp').alias('day'), 
        weekofyear('timestamp').alias('week'),
        month('timestamp').alias('month'),
        year('timestamp').alias('year'), 
        dayofweek('timestamp').alias('weekday')
    ).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"), mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs.parquet"))



    # extract columns from joined song and log datasets 
    # to create songplays table
    songplays_table = df.join(song_df, (song_df.title == df.song) & (song_df.artist_name == df.artist), 'inner') \
                       .select(
                          col('start_time'), 
                          col('userId').alias('user_id'), 
                          col('level'), 
                          col('sessionId').alias('session_id'),
                          col('location'), 
                          col('userAgent').alias('user_agent'), 
                          col('song_id'), 
                          col('artist_id'),
                          year('start_time').alias('year'),
                          month('start_time').alias('month')).withColumn('songplay_id', monotonically_increasing_id())



    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays.parquet"), mode="overwrite")
                            


def main():

    spark = create_spark_session()
    input_data = "s3://udacity-dend/"

    
    output_data = ""

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()