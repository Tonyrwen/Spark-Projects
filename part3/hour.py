import sys
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Parking").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Load from hdfs
    violations = spark.read.csv(sys.argv[1], inferSchema=True, header=True)
    #print(violations.columns)
    data = violations.select('Violation Time').na.drop().rdd
    hour = data.map(lambda x: x[0]).map(lambda x: (x[:2]+ x[-1], 1)).reduceByKey(add)
    for h in hour.sortBy(lambda x: x[1],ascending=False).take(25):
        print(h)
    spark.stop()
