"""
Implementing a simple spark application that determines the number
 of tweet actions of a user.

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.streaming import ProcessingTime
from pyspark.sql.functions import window

from sys import argv, exit

if __name__ == "__main__":
  # Checking the system arguments
  if len(argv) != 3:
    print("Error: usage " + argv[0] + " <input_file_directory> <comma_separated_user_id_list>")
    exit()

  userids = "(" + argv[2] + ")"

  # Initialize the spark context.
  spark = SparkSession\
    .builder\
    .master("spark://10.254.0.23:7077")\
    .appName("CS-838-Assignment2-PartB-3")\
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

  # Select distinct users from the table
  output = csvDF\
      .select("userA")\
      .where("userA in " + userids)\
      .groupby("userA")\
      .count()
 
  # Start running the query that prints the desired content 
  query = output\
      .writeStream\
      .format('console')\
      .option('truncate', 'false')\
      .option('numRows', 100000000)\
      .trigger(processingTime="5 seconds")\
      .outputMode("complete")\
      .start()

  query.awaitTermination()
