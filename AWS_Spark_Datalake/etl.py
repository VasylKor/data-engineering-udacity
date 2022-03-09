from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    
    print("Getting songs data path")
    
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    
    # read song data file
    
    print("Reading songs data")   
    
    df = spark.read.option("header", "true").option("inferSchema","true").json(song_data)

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), col("title"),
                            col("artist_id"), col("year"),
                            col("duration"))
                               
        
    # write songs table to parquet files partitioned by year and artist
    
    print("Writing songs table to S3 as parquet")
    
    songs_table.write \
    .partitionBy("year",'artist_id') \
    .mode("overwrite") \
    .parquet(os.path.join(output_data, 'songs'))


    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), col("artist_name"), 
                              col("artist_location"), col("artist_latitude"),
                              col("artist_longitude")) \
    .withColumnRenamed("artist_name","name") \
    .withColumnRenamed("artist_longitude","longitude") \
    .withColumnRenamed("artist_latitude","latitude") \
    .withColumnRenamed("artist_location","location") \
    .dropDuplicates(["artist_id"])
    
    
    # write artists table to parquet files
    
    print("Writing artists table to S3 as parquet")
    
    artists_table.write \
    .mode("overwrite") \
    .parquet(os.path.join(output_data, 'artists'))
    
    print("Songs data processed")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    print("Getting logs data path")
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    print("Reading logs data")
    df = spark.read.option("header", "true").option("inferSchema","true").json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select(col("userId"), col("firstName"),
                            col("lastName"), col("gender"),
                            col("level"))\
    .withColumnRenamed("userId","user_id") \
    .withColumnRenamed("firstName","first_name") \
    .withColumnRenamed("lastName","last_name") \
    .dropDuplicates(["user_id"])
    
    
    # write users table to parquet files
    
    print("Writing users table to S3 as parquet")
    
    users_table.write \
    .mode("overwrite") \
    .parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0))
    df = df.withColumn("time_stamp", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select(col("start_time")) \
    .withColumn("hour",hour(col("start_time"))) \
    .withColumn("day",dayofmonth(col("start_time"))) \
    .withColumn("week", weekofyear(col("start_time"))) \
    .withColumn("month",month(col("start_time"))) \
    .withColumn("year",year(col("start_time"))) \
    .withColumn("weekday",dayofweek(col("start_time")))
    
    
    # write time table to parquet files partitioned by year and month
    
    print("Writing time table to S3 as parquet")
    
    time_table.write \
    .partitionBy("year",'month') \
    .mode("overwrite") \
    .parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    song_df = spark.read.option("header", "true").option("inferSchema","true").json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,
                              [df.artist == song_df.artist_name,
                              df.song == song_df.title,
                              df.length == song_df.duration],
                              "inner")
                            
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()) \
                                     .withColumn("start_time", get_timestamp("ts")) \
                                     .withColumnRenamed("userAgent","user_agent") \
                                     .withColumnRenamed("sessionId","session_id") \
                                     .withColumnRenamed("userId","user_id")
                            
    songplays_table = songplays_table.select(col("songplay_id"), col("start_time"),
                                             col("user_id"), col("level"),
                                             col("song_id"), col("artist_id"),
                                             col("session_id"), col("location"),
                                             col("user_agent")) \
                      .withColumn("year", year(col("start_time"))) \
                      .withColumn("month", month(col("start_time")))
    

    # write songplays table to parquet files partitioned by year and month
    
    print("Writing songplays table to S3 as parquet")
    
    songplays_table.write \
    .partitionBy("year",'month') \
    .mode("overwrite") \
    .parquet(os.path.join(output_data, 'songplays'))

    print("Logs data processed")



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
