### modified from examples/src/main/python/pagerank.py
"""
Modified from examples/src/main/python/pagerank.py
"""
from __future__ import print_function

import re
import sys, time
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
    # begining of app
    t_begin = time.time()

    N_PARTITIONS = 50

    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print("Usage: app_name.py <file> <iterations> [partitions]", file=sys.stderr)
        exit(-1)

    if len(sys.argv) == 4:
        N_PARTITIONS = int(sys.argv[3])

    # default spark master, can be overiten by passing command line arg --master
    sparkMaster = "spark://10.254.0.142:7077"
    # app name
    appName = "CS-838-Assignment2-PartA-Question2" #+ '-' + str(N_PARTITIONS)

    # Initialize the spark context as required.
    spark = SparkSession\
        .builder\
        .appName(appName)\
        .config("spark.master", sparkMaster)\
        .config("spark.driver.memory", "1g")\
        .config("spark.eventLog.dir", "/home/ubuntu/logs/spark")\
        .config("spark.eventLog.enabled", "true")\
        .config("spark.executor.instances", "5")\
        .config("spark.executor.memory", "1g")\
        .config("spark.executor.cores", "4")\
        .config("spark.task.cpus", "1")\
        .getOrCreate()

        #


    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).groupByKey(numPartitions = N_PARTITIONS)
    print("links has %d partitions" % links.getNumPartitions())

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0), preservesPartitioning=True)
    print("ranks has %d partitions" % ranks.getNumPartitions())

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]), preservesPartitioning=True)

        print("contribs has %d partitions" % contribs.getNumPartitions())

        # Re-calculates URL ranks based on neighbor contributions.
        # mapValues retains the original RDD's partitioning
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

        print("ranks has %d partitions" % ranks.getNumPartitions())

    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    spark.stop()

    # ending of app
    t_end = time.time()
    print("\nApplication took %f ms" % (1000.0 * (t_end - t_begin)), file=sys.stderr)
