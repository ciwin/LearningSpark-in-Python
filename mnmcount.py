# Spark Sample Program to count M and Ms
# From the Book "Learning Spark 2.0"
# Christoph Windheuser, 2-2-22

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
	if len(sys.argv) != 2:
		print("Usage: mnmcount <file>", file=sys.stderr)
		sys.exit(-1)

	spark = (SparkSession
		.builder
		.appName("PythonMnMCount")
		.getOrCreate())

	# Get the M&M data set filename

	mnm_file = sys.argv[1]

	# Read the data file into a Spark DataFrame
	mnm_df = (spark.read.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.load(mnm_file))

	# Use DataFram APIs to select, group and count the data
	count_mnm_df = (mnm_df
		.select("State", "Color", "Count")
		.groupBy("State", "Color")
		.agg(count("Count").alias("Total"))
		.orderBy("Total", ascending=False))

	# Show the results
	count_mnm_df.show(n=60, truncate=False)
	print("Total Rows = %d" %(count_mnm_df.count()))

	# Find the aggregate count for California
	ca_count_mnm_df = (mnm_df
		.select("State", "Color", "Count")
		.where(mnm_df.State == "CA")
		.groupBy("State", "Color")
		.agg(count("Count").alias("Total"))
		.orderBy("Total", ascending=False))

	# Show the result for California:
	ca_count_mnm_df.show(n=10, truncate=False)

	#Stop SparkSession
	spark.stop()
