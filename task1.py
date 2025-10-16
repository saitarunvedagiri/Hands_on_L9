from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Task1_BasicIngestion").getOrCreate()

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

raw = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9999).load()

parsed = raw.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed.writeStream.format("console").option("truncate", "false").start()

query.awaitTermination()
