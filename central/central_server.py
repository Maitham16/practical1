from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import json

def update_global_model(df, epoch_id):
    # Check if the dataframe is empty
    if df.isEmpty():
        print("No data received in this batch")
        return

    # Parse the JSON data from the Kafka message
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_data").selectExpr("CAST(json_data AS MAP<String, String>) as data")

    # Logic to update global models based on received data
    # Access the data fields using parsed_df['data']['field_name']

    # Example: Update a global model using received data
    # model.update(parsed_df['data']['current_speed'], parsed_df['data']['charge'], ...)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("CentralServer") \
        .getOrCreate()

    # Set the Spark configuration
    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    
    # Define Kafka parameters
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'node1_data,node2_data,node3_data,node4_data'

    # Create DataFrame representing the stream of input lines from Kafka
    kafkaStream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("auto.offset.reset", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # Write final results to console (for debugging) or to external storage
    query = kafkaStream \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(update_global_model) \
        .start()

    query.awaitTermination()
