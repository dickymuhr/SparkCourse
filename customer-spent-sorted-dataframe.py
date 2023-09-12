from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerSpent").master("local[*]").getOrCreate()

schema = StructType([ \
                     StructField("CustomerID", StringType(), True), \
                     StructField("ProductID", StringType(), True), \
                     StructField("Spent", FloatType(), True)])

customerSpent = spark.read.schema(schema).csv("customer-orders.csv")
customerSpent.printSchema()

df = customerSpent.select("CustomerID", "Spent")
df_agg = df.groupBy("CustomerID").agg(func.round(func.sum("spent"),2).alias("TotalSpent")).sort("TotalSpent")

df_agg.show(df_agg.count())

spark.stop()