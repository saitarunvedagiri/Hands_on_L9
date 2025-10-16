from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Task3_WindowedAnalytics").getOrCreate()

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

raw = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9999).load()
parsed = raw.select(from_json(col("value"), schema).alias("data")).select("data.*")

parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))

windowed = parsed.withWatermark("event_time", "1 minute")         .groupBy(window(col("event_time"), "5 minutes", "1 minute"))         .agg(_sum("fare_amount").alias("sum_fare"))

query = windowed.writeStream.format("csv")         .option("path", "outputs/task3_outputs")         .option("checkpointLocation", "/tmp/checkpoints/task3")         .outputMode("append")         .start()

query.awaitTermination()
