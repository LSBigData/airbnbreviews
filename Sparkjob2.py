from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, explode, desc
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, NGram

# Initialize Spark session
spark = SparkSession.builder.appName("TopFeedbackCombinations").getOrCreate()

# Load feedbacks data
feedbacks_data_path = "gs://dataproc-staging-us-central1-954993014604-xcdhfinw/project_1024/dataset/allreviews.csv"
feedbacks_data = spark.read.format("csv").option("header", "true").load(feedbacks_data_path)

# Drop rows with missing or null comments
feedbacks_data = feedbacks_data.na.drop(subset=["comments"])

# Convert comments to lowercase
feedbacks_data = feedbacks_data.withColumn("comments_lower", lower(col("comments")))

# Tokenize and preprocess comments
tokenizer = RegexTokenizer(inputCol="comments_lower", outputCol="words", pattern="\\W")
feedbacks_data = tokenizer.transform(feedbacks_data)

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
feedbacks_data = remover.transform(feedbacks_data)

# Create trigrams (3-grams) from tokens
ngram = NGram(n=3, inputCol="filtered_words", outputCol="trigrams")
feedbacks_data = ngram.transform(feedbacks_data)

# Explode trigrams to separate rows
feedbacks_data = feedbacks_data.withColumn("trigram", explode(col("trigrams")))

# Group data by trigram and count occurrences
trigram_counts = feedbacks_data.groupBy("trigram").count()

# Order by count in descending order and limit to top 100
top_100_trigrams = trigram_counts.orderBy(desc("count")).limit(100)

# Show the top 100 trigrams and their counts
top_100_trigrams.show(truncate=False)

# Save the DataFrame to a CSV file in Google Cloud Storage
output_path = "gs://dataproc-staging-us-central1-954993014604-xcdhfinw/project_1024/result/output.csv"
top_100_trigrams.write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
