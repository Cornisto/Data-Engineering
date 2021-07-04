import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofweek, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def monitoring_logs(df, message):
    """
        This function helps user to monitor progress of processing data.
        
        Parameters:
            df      = Spark DataFrame
            message = message to be displayed to the user
    """
    
    print('****************************')
    print(message)
    df.printSchema()
    print('****************************')
    
    
def process_song_data(spark, input_data, output_data):
    """
        This function loads song_data from S3, processes songs and artist tables and loads them back to S3

        Parameters:
            spark       = Spark Session
            input_data  = location of input song_data file
            output_data = location to store processed data
    """

    # get filepath to song data file
    song_data = "{}*/*/*/*.json".format(input_data)
    
    # read song data file
    songs_df = spark.read.json(song_data)
    songs_df.createOrReplaceTempView("songs_table")

    # extract columns to create songs table
    songs_table = songs_df.select(col("song_id"), col("title"), col("artist_id"), col("year"), col("duration")).distinct()
    
    monitoring_logs(songs_table, 'Songs table created')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet("{}songs".format(output_data))
    
    # extract columns to create artists table
    artists_table = songs_df.select(col("artist_id"), col("artist_name"), col("artist_location"), \
                                    col("artist_latitude"), col("artist_longitude")).distinct()
    
    monitoring_logs(artists_table, 'Artists table created')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("{}artists".format(output_data))

def process_log_data(spark, input_data, output_data):
    """
        This function loads log_data from S3, processes artists, time and songplays tables and loads them back to S3
        
        Parameters:
            spark       = Spark Session
            input_data  = location of input song_data file
            output_data = location to store processed data
    """
    
    # get filepath to log data file
    log_data = "{}*/*/*events.json".format(input_data)
    
    # read log data file
    logs_df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    logs_df = logs_df.filter(logs_df.page == "NextSong").cache()
    
    # extract columns for users table    
    users_table = logs_df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId")).distinct()
    
    monitoring_logs(users_table, 'Users table created')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("{}users".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    logs_df = logs_df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    logs_df = logs_df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    logs_df = logs_df.withColumn("hour", hour("timestamp"))
    logs_df = logs_df.withColumn("day", dayofmonth("timestamp"))
    logs_df = logs_df.withColumn("month", month("timestamp"))
    logs_df = logs_df.withColumn("year", year("timestamp"))
    logs_df = logs_df.withColumn("week", weekofyear("timestamp"))
    logs_df = logs_df.withColumn("weekday", dayofweek("timestamp"))
    
    time_table = logs_df.select(col("start_time"), col("hour"), col("day"), col("week"), \
                                col("month"), col("year"), col("weekday")).distinct()
    
    monitoring_logs(time_table, 'Time table created')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet("{}time".format(output_data))
    
    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM songs_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(logs_df, song_df.artist_name == logs_df.artist, "inner") \
                        .distinct() \
                        .select(col("start_time"), col("userId"), col("level"), col("sessionId"), \
                                col("location"), col("userAgent"), col("song_id"), col("artist_id")) \
                        .withColumn("songplay_id", monotonically_increasing_id())
    
    monitoring_logs(songplays_table, 'Songplays table created')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet("{}songplays".format(output_data))
    

def main():
    spark = create_spark_session()
    song_input_data = config['S3']['SONG_DATA']
    log_input_data = config['S3']['LOG_DATA']
    output_data = config['S3']['OUTPUT_DATA']
    
    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
