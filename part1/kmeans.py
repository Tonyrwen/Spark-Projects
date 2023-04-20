import sys
from pyspark.sql.functions import when, round
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("kmeans").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Load from hdfs
    shotData = spark.read.csv(sys.argv[1], inferSchema=True, header=True).select("SHOT_DIST", "SHOT_RESULT", "CLOSE_DEF_DIST", "SHOT_CLOCK", "player_name", "player_id")        
    
    avgShotData = shotData.groupBy('player_id').avg("SHOT_DIST", "CLOSE_DEF_DIST", "SHOT_CLOCK")
    trainData = avgShotData.drop('player_id')
    
    assembler = VectorAssembler(inputCols=trainData.columns, outputCol='features')
    
    trainData = assembler.transform(trainData)
    
    #Pick James Harden, Chris Paul, Stephen Curry and Lebron James
    players = ['201939', '201935', '2544', '101108']
    testData = avgShotData.filter(avgShotData.player_id.isin(players))
    testData = assembler.transform(testData)

    #create hit rate data
    hitRate = shotData.withColumn('made', when(shotData.SHOT_RESULT == "made", 1).otherwise(0)).select('player_id', 'player_name', 'SHOT_RESULT', 'made')
    hitRate = hitRate.filter(hitRate.player_id.isin(players))
    hitRate = hitRate.groupBy('player_id', 'player_name').agg({'made':'sum', 'player_id':'count'})
    hitRate = hitRate.withColumn('hitRate', round(hitRate['sum(made)'] / hitRate['count(player_id)'],4)*100)

    # Train a k-means model.
    kmeans = KMeans(k=4).setSeed(1)
    model = kmeans.fit(trainData)

    # Make predictions
    predictions = model.transform(testData)
    predictions.show()

    hitRate = hitRate.join(predictions.select('player_id', 'prediction'), 'player_id', 'inner')
    hitRate.show()
    # Shows the result.
    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    spark.stop()
