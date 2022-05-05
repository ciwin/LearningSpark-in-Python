from pyspark.sql import SparkSession

# Create a Spark Session:
spark = (SparkSession \
         .builder \
         .appName("Chapter_5_Examples") \
         .getOrCreate())

# Specify an existing avro file in the repository:
datafile = """../DB_Spark/LearningSparkV2/databricks-datasets/\
learning-spark-v2/flights/summary-data/avro/*"""

# Read the avro file:
df_avro = (spark.read.format("avro").load(datafile))

# Show it on the terminal
df_avro.show()

# Write the DataFrame as an avro file to a new location:
(df_avro.write.format("avro")
    .mode("overwrite")
    .save("data/temp/avro"))
