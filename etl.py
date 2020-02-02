import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col, year, month
from pyspark.sql.functions import dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import dayofweek, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Returns an instance of a SparkSession
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Process song_data, extracts songs and artists details
    and creates parquet structures for each

    Arguments:
    ----------
    spark -- spark session instance\n
    input_data -- origin of data to process\n
    output_data -- destination of processed data
    '''
    # get filepath to song data file
    song_data = '{}song_data/*/*/*'.format(input_data)

    # read song data file
    song_data_df = spark.read.json(song_data)

    # create partial SQL table
    song_data_df.createOrReplaceTempView("song_table")

    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT song_id, title, artist_id, year, duration
        FROM song_table
    ''')

    # replace table for later use in join action
    songs_table.createOrReplaceTempView("song_table")

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT artist_id, artist_name, artist_location, artist_latitude,
            artist_longitude
        FROM song_table
    ''')

    # write songs table to parquet files partitioned by year and artist
    song_table.write\
        .partitionBy('year', 'artist_id')\
        .parquet('{}songs/song_table.pq'.format(output_data), mode="overwrite")

    # write artists table to parquet files
    artist_table.write\
        .parquet('{}artists/artist_table.pq'.format(output_data), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Process log_data, extract user details
    and creates parquet structures for user table,
    timestamp table and songplay table

    Arguments:
    ----------
    spark -- spark session instance\n
    input_data -- origin of data to process\n
    output_data -- destination of processed data
    '''
    # get filepath to log data file
    log_data_filepath = '{}log_data/*.json'.format(input_data)

    # read log data file
    log_data = spark.read.json(log_data_filepath)
    log_data.createOrReplaceTempView("user_log_table")

    # filter by actions for song plays
    log_data = spark.sql('''
        SELECT *
        FROM user_log_table
        WHERE page='NextSong'
    ''')

    # overwrite user_log_table after filtering
    log_data.createOrReplaceTempView("user_log_table")

    # extract columns for users table
    user_data = spark.sql('''
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM user_log_table
    ''')

    # write users table to parquet files
    user_data.write\
        .parquet('{}users/user_data.pq'.format(output_data), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x / 1000.0),
        T.TimestampType()
    )
    log_data = log_data.withColumn("timestamp", get_timestamp(log_data.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x / 1000.0),
        T.DateType()
    )
    log_data = log_data.dropna(how="any", subset=["ts"])
    log_data = log_data.withColumn("datetime", get_datetime(log_data.ts))

    # extract columns to create time table
    time_table = time_table = (
        log_data
        .withColumn('day', dayofmonth(log_data.datetime))
        .withColumn('week', weekofyear(log_data.datetime))
        .withColumn('month', month(log_data.datetime))
        .withColumn('year', year(log_data.datetime))
        .withColumn('weekday', dayofweek(log_data.datetime))
        .select(['ts', 'day', 'week', 'month', 'year', 'weekday'])
    )

    # write time table to parquet files partitioned by year and month
    time_table.write\
        .partitionBy('year', 'month')\
        .parquet('{}timetable/time_table.pq'.format(output_data), mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.sql('''
        SELECT *
        FROM song_table
    ''')

    # extract columns from joined song and log datasets to
    # create songplays table
    songplays_table = (
        log_data
        .withColumn('songplay_id', monotonically_increasing_id())
        .join(song_df, log_data.song == song_df.title)
        .select([
            'songplay_id', 'ts', 'userId', 'level', 'song_id',
            'artist_id', 'itemInSession', 'location', 'userAgent'
        ])
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
        .parquet('{}songplays/songplays_table.pq'.format(output_data), mode="overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
