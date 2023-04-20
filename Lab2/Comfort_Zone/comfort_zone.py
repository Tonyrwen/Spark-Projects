from __future__ import print_function
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import when, col, min, max, avg
from pyspark.sql import SparkSession
import numpy as np
import sys

spark = SparkSession.builder.appName("Comfort_Zone_KMean").getOrCreate()

df = spark.read.format("csv").option("header", "true").load(sys.argv[1])

# data preprocessing
# drop irrelevant columns, type correct, and drop NAs
df = \
df.select("player_id", "player_name", "SHOT_DIST", "CLOSE_DEF_DIST", "SHOT_CLOCK", "SHOT_RESULT")\
  .withColumn('SHOT_RESULT', when(df.SHOT_RESULT == "made", 1)\
                            .when(df.SHOT_RESULT == "missed", 0)\
                            .otherwise(None))\
  .withColumn('player_id', col("player_id").cast('int'))\
  .withColumn('SHOT_DIST', col('SHOT_DIST').cast('double'))\
  .withColumn('CLOSE_DEF_DIST', col('CLOSE_DEF_DIST').cast('double'))\
  .withColumn('SHOT_CLOCK', col('SHOT_CLOCK').cast('double'))\
  .na.drop()

# get player average stats
cz_df = \
df.groupBy("player_id").avg('SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK')\
  .withColumnRenamed('avg(SHOT_DIST)', 'MEAN_SHOT_DIST')\
  .withColumnRenamed('avg(CLOSE_DEF_DIST)', 'MEAN_CLOSE_DEF_DIST')\
  .withColumnRenamed('avg(SHOT_CLOCK)', 'MEAN_SHOT_CLOCK')
  
# create features column and standardize it
assemble = VectorAssembler(inputCols = ['MEAN_SHOT_DIST','MEAN_CLOSE_DEF_DIST','MEAN_SHOT_CLOCK'],
                           outputCol = 'features')
scaler = StandardScaler(inputCol = 'features', outputCol = 'standardized')
cz_df = assemble.transform(cz_df)
cz_df = scaler.fit(cz_df).transform(cz_df)

# kmean instantiate, fit, predict
kmeans = KMeans(featuresCol = 'standardized').setK(4).setSeed(1)
model = kmeans.fit(cz_df)
output = model.transform(cz_df)

# evaluate cluster quality
silhouette = ClusteringEvaluator().evaluate(output)

# check cluster stat range
clst_df = \
output.groupby('prediction')\
      .agg(min("MEAN_SHOT_DIST").alias("MIN_SHOT_DIST"),max("MEAN_SHOT_DIST").alias("MAX_SHOT_DIST"), avg("MEAN_SHOT_DIST").alias("MEAN_SHOT_DIST"), 
           min("MEAN_CLOSE_DEF_DIST").alias("MIN_CLOSE_DEF_DIST"),max("MEAN_CLOSE_DEF_DIST").alias("MAX_CLOSE_DEF_DIST"),avg("MEAN_CLOSE_DEF_DIST").alias("MEAN_CLOSE_DEF_DIST"),
           min("MEAN_SHOT_CLOCK").alias("MIN_SHOT_CLOCK"),max("MEAN_SHOT_CLOCK").alias("MAX_SHOT_CLOCK"),avg("MEAN_SHOT_CLOCK").alias("MEAN_SHOT_CLOCK"),)


clst_pd = clst_df.toPandas()

# get player of interest best zone based on accuracy
poi_lst = ["james harden", 'chris paul', "stephen curry", "lebron james"]
zone_lst, zone_shot_dist_lst, zone_close_def_dist_lst, zone_shot_clock_lst = [],[],[],[]
accuracy_lst = []

for player in poi_lst:
  zone_performance = []
  for cluster in clst_df.collect():
    player_df = \
    df.filter((df.player_name == player) &\
              (df.SHOT_DIST.between(cluster.MIN_SHOT_DIST, cluster.MAX_SHOT_DIST)) &\
              (df.CLOSE_DEF_DIST.between(cluster.MIN_CLOSE_DEF_DIST, cluster.MAX_CLOSE_DEF_DIST)) &\
              (df.SHOT_CLOCK.between(cluster.MIN_SHOT_CLOCK, cluster.MAX_SHOT_CLOCK)))
    
    # must have enough shot volume
    if player_df.count() < 10: continue
    
    zone_performance.append(sum(player_df.select('SHOT_RESULT').toPandas()['SHOT_RESULT'])/player_df.count())

  zone = int(np.argmax(zone_performance))
  #print(player, zone)
  zone_row = clst_df.filter(clst_df.prediction == zone).select('MEAN_SHOT_DIST', "MEAN_CLOSE_DEF_DIST", "MEAN_SHOT_CLOCK")
  
  zone_lst.append(zone)
  zone_shot_dist_lst.append(zone_row.first()['MEAN_SHOT_DIST'])
  zone_close_def_dist_lst.append(zone_row.first()['MEAN_CLOSE_DEF_DIST'])
  zone_shot_clock_lst.append(zone_row.first()['MEAN_SHOT_CLOCK'])
  acuuracy_lst.append(np.max(zone_performance)*100)

# output
print('**************************************************\n')
print("Silhouette with squared euclidean distance = " + str(silhouette)+"\n")
print('\nPlayer of Interest Best Zone and Accuracy:')
for i in range(len(poi_lst)):
  print("{}:\tZone {} - [{:.1f}, {:.1f}, {:.1f}]\tAccuracy: {:.1f}%".format(
                        poi_lst[i], zone_lst[i], 
                        zone_shot_dist_lst[i], 
                        zone_close_def_dist_lst[i],
                        zone_shot_clock_lst[i],
                        accuracy_lst[i]
                        ))
print("\nComfort Zone Cluster Profile:\n")
print(clst_pd)
print('**************************************************')
spark.stop()
