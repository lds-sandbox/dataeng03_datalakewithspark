import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')
output_path = config.get('AWS','S3_OUTPUT_BUCKET')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    captures the entire set of song data json files and extracts the 
    unique songs and artists
    
    parameters:
    spark - pointer to the spark session
    input_data - top level path within the S3 bucket that contains the song information
    output_data - top level path to where the song and artist data is written
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    #song_data = input_data + "song_data/A/B/C/TRABCAS128F14A25E2.json"
    
    # read song data file
    df = spark.read.json(song_data)
    #df.printSchema()

    
    # extract columns to create songs table
    df.createOrReplaceTempView("song_data_view")  # create a view to access the data
    songs_table = spark.sql('''
                    SELECT DISTINCT 
                        song_id,
                        title,
                        artist_id,
                        artist_name,
                        year,
                        duration
                    FROM song_data_view
    ''')
    
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table.show()
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + 'songs_table/')

    
    # extract columns to create artists table
    artists_table = spark.sql('''
                    SELECT DISTINCT 
                        artist_id,
                        artist_name,
                        artist_location,
                        artist_latitude,
                        artist_longitude
                    FROM song_data_view
    ''')
    
    
    # write artists table to parquet files
    #artists_table.show()
    artists_table.write.mode("overwrite").parquet(output_data + 'artists_table/')
    

def process_log_data(spark, input_data, output_data, song_data = None):
    """
    captures the entire set of log data json files and extracts the 
    unique information by song-listen
    
    parameters:
    spark - pointer to the spark session
    input_data - top level path within the S3 bucket that contains the log information
    output_data - top level path to where the unique song-liste data is written;
                  path is also used to indicate the source of the song information
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    #log_data = input_data + 'log_data/2018/11/2018-11-01-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    #df.printSchema()
    
    # create dataframe view
    df.createOrReplaceTempView("log_data_view")
    
    # extract columns for users table    
    users_table = spark.sql('''
                    SELECT DISTINCT
                    userId AS user_id
                    , firstName AS first_name
                    , lastName AS last_name
                    , gender
                    , level
                    FROM log_data_view
    ''')
    
    # write users table to parquet files
    #users_table.show()
    users_table.write.mode("overwrite").parquet(output_data + 'users_table/')

    # extract columns to create time table
    time_table = spark.sql('''
                    SELECT DISTINCT
                    ts AS start_time
                    , hour(to_timestamp(ts / 1000.0)) AS hour
                    , dayofmonth(to_timestamp(ts / 1000.0)) AS day
                    , weekofyear(to_timestamp(ts / 1000.0)) AS week
                    , month(to_timestamp(ts / 1000.0)) AS month
                    , year(to_timestamp(ts / 1000.0)) AS year
                    , dayofweek(to_timestamp(ts / 1000.0)) AS weekday
                    FROM log_data_view
    ''')
    
    # write time table to parquet files partitioned by year and month
    #time_table.show()
    time_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'time_table/')
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs_table/')
    song_df.createOrReplaceTempView('song_data_view')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
                        SELECT 
                        l.ts AS start_time
                        , month(to_timestamp(l.ts / 1000.0)) AS month
                        , year(to_timestamp(l.ts / 1000.0)) AS year
                        , l.userId AS user_id
                        , l.level
                        , s.song_id
                        , s.artist_id
                        , l.sessionId as session_id
                        , l.location
                        , l.userAgent as user_agent
                        FROM log_data_view l
                        LEFT JOIN song_data_view s ON l.song = s.title and l.artist = s.artist_name
    ''')

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.show()
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'songplays_table/')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = output_path  # expects output bucket to be specified in configuration file (AWS -> S3_OUTPUT_BUCKET)
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
