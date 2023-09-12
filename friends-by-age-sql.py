from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()
people = people.select("age","friends")
# SQL can be run over DataFrames that have been registered as a table.
# Register the DataFrame as a temporary SQL table
people.createOrReplaceTempView("people")
friend_by_age = spark.sql("SELECT age, ROUND(AVG(friends),2) as total_friends FROM people GROUP BY age SORT BY age ASC")

friend_by_age.show(5)

# Using direct SQL-Like method
results = people.groupBy("age").agg(F.round(F.avg("friends"),2).alias("total_friends")).sort("age").take(5)
for result in results:
    print(result)

spark.stop