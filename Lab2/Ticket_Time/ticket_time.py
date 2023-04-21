from __future__ import print_function
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import when, col, min, max, avg, concat, lit, desc, substring, length
from pyspark.sql import SparkSession
import numpy as np
import sys

if __name__ = "__main__":
  spark = SparkSession.builder.appName("Ticket_Time").getOrCreate()

  spark.read.format("csv").option("header", "true").load(sys.argv[1])\
      .select('Violation Time').na.drop()\
      .filter(length(col('Violation Time')) == 5)\
      .withColumn('Violation Hour', col("Violation Time").substr(1, 2))\
      .withColumn('Violation AP', col("Violation Time").substr(5,1))\
      .withColumn('Violation Hour', concat(col('Violation Hour'),
                                            col('Violation AP')))\
      .withColumn('Count', lit(1))\
      .groupBy('Violation Hour').sum('Count')\
      .orderBy(desc('sum(Count)')).show(1)
  
  spark.stop()
