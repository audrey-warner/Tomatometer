# Write your code here
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, LongType, ArrayType, MapType, TimestampType
from pyspark.sql.functions import from_json, col, expr, sum, avg, min, max

BOOTSTRAP_SERVERS = "confluent-local-broker-1:60242"
TOPIC = "wikimedia_events"

def main():
    
    schema = StructType([
        StructField("timestamp", TimestampType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("user", StringType()),
        StructField("meta", StructType([
            StructField("domain", StringType())
            ])),
        StructField("length", StructType([
            StructField("old", LongType()),
            StructField("new", LongType())
            ]))
        ])
    
    spark = SparkSession.builder \
                        .appName('WikiMedia_Events') \
                        .getOrCreate()
    
    kafka_stream_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
        .option("subscribe", TOPIC)\
        .load()
    
    parsed_stream_df = kafka_stream_df.select(from_json(col("value").cast("string"), schema).alias("data"))

    selected_fields_df = parsed_stream_df.select(
        col("data.timestamp"), 
        col("data.bot"), 
        col("data.minor"), 
        col("data.user"), 
        col("data.meta.domain"), 
        col("data.length.old").alias("old_length"), 
        col("data.length.new").alias("new_length"))

    selected_fields_df = selected_fields_df.withColumn("length_diff", col("new_length") - col("old_length"))

    selected_fields_df = selected_fields_df.withColumn("length_diff_percent", expr("CASE WHEN old_length != 0 THEN (new_length - old_length) * 100.0 / old_length ELSE NULL END"))

    ## Shows in the console window
    # query = selected_fields_df.writeStream\ 
    #     .outputMode("append")\
    #     .format("console")\
    #     .start()\
    #     .awaitTermination()

    ## Writing to a csv!
    # selected_fields_df.writeStream \   
    # .outputMode("append")\
    # .option("checkpointLocation", "output")\
    # .format("csv")\
    # .option("path", "./output")\
    # .option("header", True)\
    # .trigger(processingTime="10 seconds")\
    # .start()\
    # .awaitTermination()

    ## Domain Counts
    # domain_counts = selected_fields_df.filter(col("domain").isNotNull())
    # domain_counts = domain_counts.groupBy('domain').count().orderBy(col("count").desc()).limit(5)
    # query = domain_counts.writeStream\
    #     .outputMode("complete")\
    #     .format("console")\
    #     .start()\
    #     .awaitTermination()

    # # User Counts
    # user_word_counts = selected_fields_df.filter(col("user").isNotNull() & col("length_diff").isNotNull())
    # user_word_counts = user_word_counts.withColumn("length_diff", col("length_diff").cast("int"))
    # user_length_diff = user_word_counts \
    #     .groupBy("user") \
    #     .agg(sum(col("length_diff")).alias("total_length_diff")) \
    #     .orderBy(col("total_length_diff").desc()) \
    #     .limit(5)  # Limit to top 5 users by total length_diff
    # query = user_length_diff.writeStream\
    #     .outputMode("complete")\
    #     .format("console")\
    #     .start()\
    #     .awaitTermination()

 # Metrics 
    stats = selected_fields_df.agg(
        F.count("*").alias("total_count"),
        (F.sum(F.col("bot").cast("int")) / F.count("*")).alias("percent_bot"),
        F.avg("length_diff").alias("average_length_diff"),
        F.min("length_diff").alias("min_length_diff"),
        F.max("length_diff").alias("max_length_diff")
    )
        
    query = stats.writeStream\
        .outputMode("complete")\
        .format("console")\
        .start()\
        .awaitTermination()


if __name__ == "__main__":
    main()