"""
Implementing a simple spark application that emits the twitter IDs 
of users that have been mentioned by other users every 10 seconds.

The output is written to a file in hdfs

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.streaming import ProcessingTime
from pyspark.sql.functions import window

from sys import argv, exit

if __name__ == "__main__":
  # Checking the system arguments
  if len(argv) != 2:
    print("Error: usage " + argv[0] + " <input_file_directory> ")
    exit()

  # Initialize the spark context.
  spark = SparkSession\
    .builder\
    .master("spark://10.254.0.23:7077")\
    .appName("CS-838-Assignment2-PartB-2")\
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
      .csv("dataDirectory/")

  # Select users from the table who have been mentioned
  output = csvDF\
      .filter("type='MT'")\
      .select("userB")\
      #.distinct()\
      #.orderBy("userB")
 
  # Start running the query that prints the desired content to hdfs file system
  query = output\
      .writeStream\
      .queryName('mention_freq')\
      .format('parquet')\
      .option('path', 'output')\
      .option('checkpointLocation', 'checkpointDir')\
      .option('name', 'user_mention')\
      .option('numRows', 1000000000)\
      .trigger(processingTime="10 seconds")\
      .outputMode("append")\
      .start()

  query.awaitTermination()
