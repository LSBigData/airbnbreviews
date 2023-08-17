from pyspark.sql import SparkSession
from pyspark.sql.functions import mean
from pyspark.sql.functions import avg,col

# Create a Spark session
spark = SparkSession.builder.appName("AirbnbAnalysis").getOrCreate()

# Load the Airbnb dataset
listings = "gs://dataproc-staging-us-central1-775352645648-1dhaqwl6/pyspark.nlpp/data/listings.csv"
airbnb_data = spark.read.csv(listings, header=True, inferSchema=True)

# Delete rows with null values in the "price" column
airbnb_data = airbnb_data.na.drop(subset=["price"])

# Perform the transformation: Find the average price per room type
avg_price_per_room = airbnb_data.groupBy(col("name").alias("room_type"), "city_location").agg(avg(col("price")).alias("avg_price"))

avg_price_per_room = avg_price_per_room.na.drop(subset=["avg_price"])

# Show the results
avg_price_per_room.show()

# Stop the Spark session
spark.stop()