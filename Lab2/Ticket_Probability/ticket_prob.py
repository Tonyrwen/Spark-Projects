from __future__ import print_function
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import when, col, min, max, avg, concat, lit, desc
from pyspark.sql import SparkSession
import numpy as np
import sys

if __name__ = "__main__":
  spark = SparkSession.builder.appName("Ticket_Prob").getOrCreate()
  
  # load in data and preprocess 
  df = spark.read.format("csv").option("header", "true").load("/content/parking-violation-2023.csv")\
     .select('Vehicle Color','Street Code1', 'Street Code2', 'Street Code3','Street Name').na.drop()\
     .filter((col("Street Code1").isin(['34510','10030','34050']))|\
             (col("Street Code2").isin(['34510','10030','34050']))|\
             (col("Street Code3").isin(['34510','10030','34050'])))
  
  # check and return how many black cars are in the dataset
  print("There is {:.2f}% chance of a Black Car to be ticketed\nif it is parked illegally at a street containing code:\n34510 or 10030 or 34050"\
        .format(df.filter(col('Vehicle Color').isin(["BLACK",'BLK','Black', "BK"])).count()/df.count()*100))
  spark.stop()
