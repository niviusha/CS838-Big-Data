"""
Implementing PageRank algorithm without any custom paritioning or RDD persistence.

The PageRank algorithm from the spark examples was used and partition of size
20 was added. Here we used mapValues instead of map as it preserves the partitioning.
Also we are caching the intermediate values.

To get the RRD lineage, add ("spark.logLineage","true") to the config and then print
the debug string.
"""

from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .master("spark://10.254.0.23:7077")\
        .appName("CS-838-Assignment2-PartA-3")\
        .config("spark.driver.memory", "8g")\
        .config("spark.eventLog.enabled", True)\
        .config("spark.eventLog.dir", "/home/ubuntu/logs/apps/")\
        .config("spark.executor.memory", "8g")\
        .config("spark.executor.cores", "4")\
        .config("spark.task.cpus", "1")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    # Set the number of partitions to 4 to corresponding to the number of cores in the system
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().partitionBy(20).groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    
    ranks = links.mapValues(lambda url_neighbors: 1.0)
    #ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    #print(ranks.toDebugString())
    spark.stop()
