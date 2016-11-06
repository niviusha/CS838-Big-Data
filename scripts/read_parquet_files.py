"""
Spark application to read a parquet file
"""

from pyspark.sql import SparkSession
from sys import argv, exit

if __name__ == "__main__":
  # Checking the system arguments
  if len(argv) != 2:
    print("Error: usage " + argv[0] + " <input_file_directory>")
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

  parquetFile = spark.read.parquet(argv[1])
  parquetFile.createOrReplaceTempView("parquetFile")

  output = spark.sql("SELECT * FROM parquetFile")
  output.show()
