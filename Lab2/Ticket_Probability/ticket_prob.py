from __future__ import print_function
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Ticket_Probability_KMean").getOrCreate()
    
    # load in dataset
    df = spark.read.format("csv").options("header", "true").load(sys.argv[1])
    df.show(n=3)
    
    # preprocessing
    
    
    # train k-means model
    
    
    # evaluate
    
    
    # show poi cluster
    
    
    spark.stop()
