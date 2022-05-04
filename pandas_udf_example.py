import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

spark = (SparkSession \
         .builder \
         .appName("Chapter_5_Examples") \
         .getOrCreate())

def cubed (a: pd.Series) -> pd.Series:
	return a * a * a

cubed_udf = pandas_udf(cubed, returnType=LongType())

x = pd.Series([0, 1, 2, 3])

# print (cubed(x))

df = spark.range(0, 4)

df.select("id", cubed_udf(col("id"))).show()
