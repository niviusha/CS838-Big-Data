"""
Implementing a simple spark application that emits the number of retweets (RT), 
mention (MT) and reply (RE) for an hourly window that is updated every 30 minutes 
based on the timestamps of the tweets.

The output is printed to the console.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import window

from sys import argv, exit

if __name__ == "__main__":
  # Check command line arguments
  if len(argv) != 2:
    print("Error: usage " + argv[0] + " <input_directory>")
    exit()

  # Initialize the spark context.
  spark = SparkSession\
    .builder\
    .master("spark://10.254.0.23:7077")\
    .appName("CS-838-Assignment2-PartB-1")\
    .config("spark.driver.memory", "1g")\
    .config("spark.eventLog.enabled", True)\
    .config("spark.eventLog.dir", "/home/ubuntu/logs/apps/")\
    .config("spark.executor.memory", "1g")\
    .config("spark.executor.cores", "4")\
    .config("spark.task.cpus", "1")\
    .getOrCreate()

  # Read all the csv files written atomically in a directory
  userSchema = StructType()\
          .add("userA", "integer")\
          .add("userB", "integer")\
          .add("timestamp", "timestamp")\
          .add("type", "string")

  csvDF = spark\
      .readStream\
      .option("sep", ",")\
      .schema(userSchema)\
      .csv(argv[1])

  # Group the data by window and type and compute count of each group
  windowedCounts = csvDF.groupBy(
      window(csvDF.timestamp, '60 minutes', '30 minutes'),
      csvDF.type
  ).count().orderBy('window.start')

  output = windowedCounts\
      .select("window.start", "type", "count")

  # Start running the query that prints the windowed count
  # truncate ensures that the entire row is printed, else words are limited to 20 chars
  # numRows determines the number of rows displayed. 
  query = output\
      .writeStream\
      .outputMode('complete')\
      .format('console')\
      .option('truncate', 'false')\
      .option('numRows', 100000000)\
      .start()

  query.awaitTermination()
