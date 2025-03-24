from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# Initialize Spark session
spark = SparkSession.builder.appName("PeopleDataProcessing").getOrCreate()

# Load the dataset
df = spark.read.csv("people.csv", header=True, inferSchema=True)

# Show the first few rows of the dataset
df.show(5)

# Example of simple data processing: Filter females and people born after 1990
filtered_df = df.filter((col("sex") == "female") & (col("date of birth") > "1990-01-01"))

# Show the filtered data
filtered_df.show(5)

# Example: Add a new column with the year of birth
df_with_year = df.withColumn("year_of_birth", year(col("date of birth")))

# Show the modified dataframe with the new column
df_with_year.show(5)

# Example: Count the number of people per year of birth
people_per_year = df_with_year.groupBy("year_of_birth").count().orderBy("year_of_birth")

# Show the aggregated result
people_per_year.show()

# Save the processed data to a new CSV file (if needed)
df_with_year.write.mode("overwrite").csv("processed_people.csv", header=True)

# Stop the Spark session
spark.stop()
