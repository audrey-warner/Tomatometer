# Write your code here
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from pyspark.sql.functions import expr, split

BOOTSTRAP_SERVERS = "confluent-local-broker-1:65519"
TOPIC = "server_logs"

def write_to_postgres(df, epoch_id):
    mode="overwrite" # append for kafka_stream_df, overwrite for agg_errors_df
    # the following variables are unique to each person:
    postgres_port="5432"
    database_name="server_logs"
    user_name="postgres"
    password="hellosql"
    table_name="errors_by_path"
    # then use these values:
    url = f"jdbc:postgresql://host.docker.internal:{postgres_port}/{database_name}"
    properties = {"user": user_name, "password": password, "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("ClickAnalytics") \
        .getOrCreate()

    raw = spark.readStream.format("kafka")\
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)\
        .option("subscribe", TOPIC)\
        .load()
    
    kafka_stream_df = raw.selectExpr("CAST(value AS STRING)") \
    .selectExpr("split(value, ' ') as data") \
    .select(
        expr("data[0]").alias("ip_address"),
        expr("data[1]").alias("user_name"),
        expr("data[2]").alias("user_id"),
        expr("data[3]").alias("timestamp"),
        expr("data[4]").alias("http_method"),
        expr("data[5]").alias("path"),
        expr("data[6]").alias("status_code"))

    error_agg = kafka_stream_df.filter(F.col('status_code').isin('404', '500'))

    agg_errors = error_agg.groupBy("path")\
        .agg(F.sum(F.when(F.col("status_code") == '404', 1).otherwise(0)).alias("404_errors"), \
             F.sum(F.when(F.col("status_code") == '500', 1).otherwise(0)).alias("500_errors"))\
    .withColumn("total_errors", F.col("404_errors") + F.col("500_errors"))

    # Detecting DOS attack
    window_duration = '1 minute'

    windowed_counts = kafka_stream_df \
        .groupBy(F.window("timestamp", window_duration), "ip_address") \
        .count() \
        .withColumn("dos_attack", F.col("count") > 100) \
        .filter(F.col("dos_attack") == True) \
        .select("window", "ip_address", "count", "dos_attack") \
        .orderBy(F.col("window").desc())
    
# Write to Console
    query = windowed_counts\
    .writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()\
    .awaitTermination()

# # Write to Postgres
#     query = agg_errors\
#     .writeStream\
#     .outputMode("complete")\
#     .foreachBatch(write_to_postgres)\
#     .start()\
#     .awaitTermination()

# #Window Aggregation
#     window_duration = "1 minute"
#     window_agg = kafka_stream_df.select(F.count("name").alias("total_count"), F.avg("attack")).withColumn('timestamp', F.current_timestamp())

if __name__ == "__main__":
    main()
