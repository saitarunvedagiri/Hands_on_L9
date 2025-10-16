from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("Task2_Aggregations").getOrCreate()

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("timestamp", StringType())
])

raw = spark.readStream.format("socket").option("host", "127.0.0.1").option("port", 9999).load()
parsed = raw.select(from_json(col("value"), schema).alias("data")).select("data.*")

agg = parsed.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

query = agg.writeStream.format("csv")         .option("path", "outputs/task2_outputs")         .option("checkpointLocation", "/tmp/checkpoints/task2")         .outputMode("complete")         .start()

query.awaitTermination()
