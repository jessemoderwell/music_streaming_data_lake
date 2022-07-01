import configparser
from datetime import datetime
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, DecimalType, DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Creates spark sql object spark session
    
    Keyword arguments:
    None
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark,
                      # not used
#                       input_data, output_data
                     ):
        '''Performs ETL on song data in s3
    
    Keyword arguments:
    spark -- the spark session object needed to use the api
    input_data -- song data from s3
    output_data -- star schema parquet files written to s3
    '''
    # get filepath to song data file
    song_data = "s3://udacity-dend/song_data/A/A/A/"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    s3_songs_output = 's3a://emr-test4/songs_table.parquet'

    songs_table.write.partitionBy('artist_id','year') \
    .mode('overwrite').parquet(s3_songs_output)

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude')

    s3_output = 's3a://emr-test4/'

    # write artists table to parquet files
    artists_table.write.partitionBy('artist_name') \
    .mode('Overwrite').parquet(s3_output + 'artists_table.parquet')


def process_log_data(spark, 
                     #not used
#                      input_data, output_data
                    ):
    '''Performs ETL on log data in s3
    
    Keyword arguments:
    spark -- the spark session object needed to use the api
    input_data -- log data from s3
    output_data -- star schema parquet files written to s3
    '''
    
    # get filepath to log data file
    log_data = ('s3a://udacity-dend/log-data/*/*/*.json')

    # if schema needs to be specified
#     schema = StructType([ \
#     StructField("artist",StringType(),True), \
#     StructField("auth",StringType(),True), \
#     StructField("firstName",StringType(),True), \
#     StructField("gender", StringType(),True), \
#     StructField("IteminSession", IntegerType(), True), \
#     StructField("lastName", StringType(), True), \
#     StructField("length", DoubleType(), True), \
#     StructField("level",StringType(),True), \
#     StructField("location",StringType(),True), \
#     StructField("method",StringType(),True), \
#     StructField("page", StringType(), True), \
#     StructField("registration", DecimalType(), True), \
#     StructField("sessionId", StringType(), True), \
#     StructField("song", StringType(), True), \
#     StructField("status",IntegerType(),True), \
#     StructField("date", StringType(),True), \
#     StructField("userAgent",StringType(),True), \
#     StructField("userId", IntegerType(), True), \
#     ])

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')

    s3_output = 's3a://emr-test4/'

    # write users table to parquet files
    users_table.write.partitionBy('lastName') \
    .mode('Overwrite').parquet(s3_output + 'users_table.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int((int(x) / 1000))) 
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('date', get_datetime(df.timestamp))

    # extract columns to create time table
    time_table = df.select('date', hour('date').alias('hour'), \
    dayofmonth('date').alias('day'), weekofyear('date').alias('week'), \
    month('date').alias('month'), year('date').alias('year'), dayofweek('date').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('hour') \
    .mode('Overwrite').parquet(s3_output + 'time_table.parquet')

    # read in song data to use for songplays table
    song_data = "s3a://udacity-dend/song_data/A/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    
    df.createOrReplaceTempView("log_data")
    song_df.createOrReplaceTempView("song_data")
    songplays_table = spark.sql("""
    select distinct l.sessionId as songplay_id, l.ts as start_time, l.userId as user_id,
    l.level, s.song_id, s.artist_id, l.sessionId, l.location, l.userAgent as user_agent
    from 
    log_data l
    inner join song_data s on concat(l.artist, l.song) = concat(s.artist_name, s.title)
    """)
    
    #Didn't work
#     songplays_table = song_df.join(df, (df.artist + df.song) == (song_df.artist_name + song_df.title)) \
#     .select(df.sessionId.alias('songplay_id'), df.ts.alias('start_time'), df.userId.alias('user_id'), df.level, \
#     song_df.song_id, song_df.artist_id, df.sessionId, df.location, df.userAgent.alias('user_agent'))
        
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.withColumn(
        'start_time', get_timestamp(songplays_table.start_time))
    songplays_table = songplays_table.withColumn('month', month('start_time'))
    songplays_table = songplays_table.withColumn('year', year('start_time'))

    songplays_table \
    .write.partitionBy('year', 'month') \
    .mode('Overwrite') \
    .parquet(s3_output + 'songplays_table.parquet')
    
def main():
    spark = create_spark_session()
#     input_data = "s3a://udacity-dend/"
#     output_data = ""
    
    process_song_data(spark, 
                      # not used
#                       input_data, output_data
                     )    
    process_log_data(spark, 
                      # not used
#                       input_data, output_data
                    )


if __name__ == "__main__":
    main()
