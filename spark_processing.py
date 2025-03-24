from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("PeopleDataProcessing").getOrCreate()

# Load CSV
df = spark.read.csv("people.csv", header=True, inferSchema=True)

# Show schema
df.printSchema()

# Basic transformation: count by gender
gender_count = df.groupBy("sex").count()
gender_count.show()

# Stop Spark session
spark.stop()
