

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, lower
import re
from collections import Counter
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# reviews comments csv file data repository
reviews_df=spark.read.csv("gs://dataproc-staging-us-central1-37934093779-s6gyozbt/project/data/reviewscomments.csv",header=True, inferSchema=True)
#reviews_df.show(5)

# listings csv file data repository
listings_df = spark.read.csv("gs://dataproc-staging-us-central1-37934093779-s6gyozbt/project/data/listings.csv", header=True, inferSchema=True)
# listings_df.show(5)

# properties csv file data repository
properties_df = spark.read.csv("gs://dataproc-staging-us-central1-37934093779-s6gyozbt/project/data/properties.csv", header=True, inferSchema=True)
# products_df.show(5)

# joined reviews, listings and properties to get the listing_id
df = reviews_df.join(listings_df, reviews_df["listing_id"] == listings_df["id"], "inner")\
                        .join(properties_df,listings_df["id"] == properties_df["listing_id"], "inner")\
                        .select(reviews_df["listing_id"], reviews_df["comments"], reviews_df["city_location"], properties_df["amenities"])						
# df.show(5)
df = df.withColumn("combined_text", concat_ws(" ", df["comments"], df["amenities"]))

# Convert the text to lowercase and split into words using regular expressions
df = df.withColumn("words", lower(df["combined_text"]))

# Explode the array of words into separate rows
df = df.selectExpr("listing_id", "city_location", "explode(split(words, ' ')) as word")

# Filter out non-word characters
df = df.filter(df.word.rlike(r'\w+'))

# Group by word and count occurrences
word_freq = df.groupBy("word").count()

word_freq = word_freq.orderBy(F.desc("count"))

# Show the resulting DataFrame
word_freq.show()

# Stop the Spark session
spark.stop()


