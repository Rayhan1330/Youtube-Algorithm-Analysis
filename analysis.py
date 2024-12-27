from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr
from pyspark.sql.functions import when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType,DoubleType
from pyspark.sql.functions import avg
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler,Tokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


# Create a Spark session
spark = SparkSession.builder.appName("YouTubeAnalysis").getOrCreate()


#Schema creation
schema = StructType([
    StructField("Video Id", StringType()),
    StructField("Title",StringType()),
    StructField("Published At",TimestampType()),
    StructField("Duration",StringType()),
    StructField("Views", IntegerType()),
    StructField("Likes", IntegerType()),
    StructField("Dislikes",IntegerType()),
    StructField("Comments",IntegerType())
])

# Load data from HDFS into a Spark DataFrame
youtube_data = spark.read.csv("hdfs://127.0.0.1:9000/user/Rayhan/youtube_data_input/*.csv", header=True, inferSchema=False, schema=schema)

# Show the schema of the DataFrame
#youtube_data.printSchema()


#Display the video titles
titles = youtube_data.select(*["Title"])
titles.show()


# Example analysis: Count videos with more than 1 million views
high_views_count = youtube_data.filter(col("Views") > 1000000).count()
youtube_data.filter(col("Views") > 1000000).select(*["Title","Views","Comments"]).show()
print(f"Number of videos with more than 1 million views: {high_views_count}")

# Example analysis: Calculate average likes per view
youtube_data = youtube_data.withColumn("Likes Per View", when(col("Views") != 0, col("Likes") / col("Views")).otherwise(0))

# Select specific columns
selected_columns = ["Likes Per View"]
selected_data = youtube_data.select(*selected_columns)



# Show the selected DataFrame
selected_data.show(truncate=False)


# Calculate averages
average_views = youtube_data.select(avg("Views")).collect()[0][0]
average_likes = youtube_data.select(avg("Likes")).collect()[0][0]
average_dislikes = youtube_data.select(avg("Dislikes")).collect()[0][0]
average_comments = youtube_data.select(avg("Comments")).collect()[0][0]

print(f"Average Views: {average_views}")
print(f"Average Likes: {average_likes}")
print(f"Average Dislikes: {average_dislikes}")
print(f"Average Comments: {average_comments}")

# Calculate the average Likes-to-Views Ratio
average_ratio = youtube_data.select(avg("Likes Per View")).collect()[0][0]

# Filter videos with "Likes Per View" higher than the average
high_likes_per_view_videos = youtube_data.filter(col("Likes Per View") > average_ratio)


import matplotlib.pyplot as plt
import numpy as np

# Set the figure size
plt.figure(figsize=(12, 6))

# Extract the Likes Per View values for each video
likes_per_view_values = youtube_data.select("Likes Per View").rdd.flatMap(lambda x: x).collect()

# Plot the Likes Per View for each video as bars
plt.bar(range(len(likes_per_view_values)), likes_per_view_values, label="Likes Per View", alpha=0.7)

# Plot the average Likes Per View as a line
average_likes_per_view_line = [average_ratio] * len(likes_per_view_values)
plt.plot(average_likes_per_view_line, label="Average Likes Per View", linestyle='--', color='red')


# Set labels and title
plt.xlabel("Video Index")
plt.ylabel("Likes Per View")
plt.title("Likes Per View for All Videos")

# Add legend
plt.legend()

# Show the combined chart
plt.show()


correlation_matrix = youtube_data.stat.corr("Views", "Likes")
print("Value of correlation : ",correlation_matrix)
print("0.9> Correlation >0.7 implies there is a STRONG POSITIVE CORRELATION between Views and Likes")

youtube_data = youtube_data.withColumn(
    "DurationSeconds",
    expr(
        "CASE WHEN INSTR(Duration, 'M') > 0 AND INSTR(Duration, 'S') > 0 THEN " +
        "substring(Duration, 3, INSTR(Duration, 'M') - 3) * 60 + substring(Duration, INSTR(Duration, 'M') + 1, INSTR(Duration, 'S') - INSTR(Duration, 'M') - 1) " +
        "WHEN INSTR(Duration, 'M') > 0 THEN " +
        "substring(Duration, 3, INSTR(Duration, 'M') - 3) * 60 " +
        "WHEN INSTR(Duration, 'S') > 0 THEN " +
        "substring(Duration, 3, INSTR(Duration, 'S') - 3) " +
        "ELSE " +
        "0 " +
        "END"
    ).cast(DoubleType())
)

# Define conditions for different duration ranges
duration_less_than_1_minute = youtube_data.filter(col("DurationSeconds") < 60)
duration_less_than_5_minutes = youtube_data.filter((col("DurationSeconds") >= 60) & (col("DurationSeconds") < 300))
duration_less_than_10_minutes = youtube_data.filter((col("DurationSeconds") >= 300) & (col("DurationSeconds") < 600))
duration_greater_than_10_minutes = youtube_data.filter(col("DurationSeconds") >= 600)

# Display the filtered data for each duration range
print("Videos with Duration Less Than 1 Minute:")
duration_less_than_1_minute.select("Title", "Views", "Duration").show(truncate=False)

print("Videos with Duration Less Than 5 Minutes:")
duration_less_than_5_minutes.select("Title", "Views", "Duration").show(truncate=False)

print("Videos with Duration Less Than 10 Minutes:")
duration_less_than_10_minutes.select("Title", "Views", "Duration").show(truncate=False)

print("Videos with Duration Greater Than 10 Minutes:")
duration_greater_than_10_minutes.select("Title", "Views", "Duration").show(truncate=False)

counts = [
    duration_less_than_1_minute.count(),
    duration_less_than_5_minutes.count(),
    duration_less_than_10_minutes.count(),
    duration_greater_than_10_minutes.count()
]

# Labels for the bars
labels = ['< 1 min', '< 5 mins', '< 10 mins', '> 10 mins']

# Plot the bar graph
plt.bar(labels, counts, color=['blue', 'green', 'orange', 'red'])
plt.title('Videos vs Duration')
plt.xlabel('Video Duration Ranges')
plt.ylabel('Number of Videos')
plt.show()

duration_bins = [0, 60, 300, 600, float('inf')]  # in seconds
duration_labels = ['<1 min', '1-5 mins', '5-10 mins', '>10 mins']






'''# Prepare features and label for regression
assembler = VectorAssembler(inputCols=["Likes", "Comments"], outputCol="features")
youtube_data = assembler.transform(youtube_data)

# Example in PySpark
train_data, test_data = youtube_data.randomSplit([0.8, 0.2], seed=42)


# Fit a linear regression model
lr = LinearRegression(featuresCol="features", labelCol="Views",regParam = 0.1)
model = lr.fit(train_data)

predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="Views", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")'''

# Define duration categories
duration_bins = [0, 60, 300, 600, float('inf')]  # in seconds
duration_labels = ['<1 min', '1-5 mins', '5-10 mins', '>10 mins']

# Create a new column for duration categories
youtube_data = youtube_data.withColumn(
    "DurationCategory",
    expr("CASE WHEN DurationSeconds < 60 THEN '<1 min' " +
         "WHEN DurationSeconds <= 300 THEN '1-5 mins' " +
         "WHEN DurationSeconds <= 600 THEN '5-10 mins' " +
         "ELSE '>10 mins' END")
)

# Calculate the average views for each duration category
avg_views_by_duration = youtube_data.groupBy("DurationCategory").avg("Views")

# Convert to Pandas DataFrame for plotting
avg_views_pd = avg_views_by_duration.toPandas()

# Plot the bar graph
plt.figure(figsize=(10, 6))
plt.bar(avg_views_pd['DurationCategory'], avg_views_pd['avg(Views)'], color='blue')
plt.xlabel('Duration Category')
plt.ylabel('Average Views')
plt.title('Average Views by Duration Category')
plt.show()

tokenizer = Tokenizer(inputCol="Title", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="features")

# Create a pipeline for processing text data
text_pipeline = Pipeline(stages=[tokenizer, remover, vectorizer])
pipeline_model = text_pipeline.fit(youtube_data)
youtube_data = pipeline_model.transform(youtube_data)

# Get the vocabulary (i.e., keywords) from the CountVectorizerModel
keywords = pipeline_model.stages[-1].vocabulary

# Display the keywords
print("Keywords:", keywords)

# Stop the Spark session
spark.stop()
