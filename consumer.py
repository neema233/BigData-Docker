from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Kafka topic and broker configuration
kafka_topic = "customer_Churn"
kafka_broker = "172.25.0.12:9092"

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .load()

# Define schema for the expected JSON data
schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Tenure", StringType(), True),
    StructField("Usage Frequency", StringType(), True),
    StructField("Support Calls", StringType(), True),
    StructField("Payment Delay", StringType(), True),
    StructField("Subscription Type", StringType(), True),
    StructField("Contract Length", StringType(), True),
    StructField("Total Spend", StringType(), True),
    StructField("Last Interaction", StringType(), True),
    StructField("Churn", StringType(), True)
])

# Convert binary 'value' to string
df_parsed = df.withColumn("value_string", col("value").cast("string"))

# Parse the JSON string in the 'value_string' column
df_parsed = df_parsed.withColumn("parsed_json", from_json(col("value_string"), schema))

# Select fields from the parsed JSON
df_final = df_parsed.select(
    col("parsed_json.CustomerID").alias("CustomerID"),
    col("parsed_json.Age").alias("Age"),
    col("parsed_json.Gender").alias("Gender"),
    col("parsed_json.Tenure").alias("Tenure"),
    col("parsed_json.`Usage Frequency`").alias("UsageFrequency"),
    col("parsed_json.`Support Calls`").alias("SupportCalls"),
    col("parsed_json.`Payment Delay`").alias("PaymentDelay"),
    col("parsed_json.`Subscription Type`").alias("SubscriptionType"),
    col("parsed_json.`Contract Length`").alias("ContractLength"),
    col("parsed_json.`Total Spend`").alias("TotalSpend"),
    col("parsed_json.`Last Interaction`").alias("LastInteraction"),
    col("parsed_json.Churn").alias("Churn")
)
df_final = df_final.coalesce(1)
# Write the output to a destination (e.g., parquet)
myquery = df_final.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/new1/") \
    .option("checkpointLocation", "hdfs://namenode:9000/new2/") \
    .trigger(processingTime='30 seconds') \
    .start()

myquery.awaitTermination()


# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import from_json,col

# spark = SparkSession.builder \
#     .appName("KafkaSparkStreaming") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
#     .getOrCreate()

# kafka_topic = "customer_Churn"
# kafka_broker = "172.25.0.12:9092"
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", kafka_topic) \
#     .load()

# df_string = df.withColumn("value_string", col("value").cast("string"))
# schema = StructType([
#     StructField("CustomerID", StringType(), True),
#     StructField("Age", StringType(), True),
#     StructField("Gender", StringType(), True),
#     StructField("Tenure", StringType(), True),
#     StructField("Usage Frequency", StringType(), True),
#     StructField("Support Calls", StringType(), True),
#     StructField("Payment Delay", StringType(), True),
#     StructField("Subscription Type", StringType(), True),
#     StructField("Contract Length", StringType(), True),
#     StructField("Total Spend", StringType(), True),
#     StructField("Last Interaction", StringType(), True),
#     StructField("Churn", StringType(), True)
# ])

# # Parse the JSON string in the 'value' column
# df_parsed = df_string.withColumn("parsed_json", from_json(col("value"), schema))

# # Select each field from the parsed JSON
# df_final = df_parsed.select(
#     col("parsed_json.CustomerID").alias("CustomerID"),
#     col("parsed_json.Age").alias("Age"),
#     col("parsed_json.Gender").alias("Gender"),
#     col("parsed_json.Tenure").alias("Tenure"),
#     col("parsed_json.`Usage Frequency`").alias("UsageFrequency"),
#     col("parsed_json.`Support Calls`").alias("SupportCalls"),
#     col("parsed_json.`Payment Delay`").alias("PaymentDelay"),
#     col("parsed_json.`Subscription Type`").alias("SubscriptionType"),
#     col("parsed_json.`Contract Length`").alias("ContractLength"),
#     col("parsed_json.`Total Spend`").alias("TotalSpend"),
#     col("parsed_json.`Last Interaction`").alias("LastInteraction"),
#     col("parsed_json.Churn").alias("Churn")
# )

# # Show the final DataFrame
# df_final.show(truncate=False)

# myquery = df_final.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "hdfs://namenode:9000/logs") \
#     .option("checkpointLocation", "hdfs://namenode:9000/user/") \
#     .start()

# myquery.awaitTermination()

