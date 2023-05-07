
from __future__ import print_function
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import when, col, min, max, avg, concat, lit, desc, substring, length
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import sys

spark = SparkSession.builder.appName("Toxic Comment Classification")\
                            .enableHiveSupport()\
                            .config('spark.executor.memory','4G')\
                            .config('spark.driver.memory','18G')\
                            .config('spark.executor.cores','7')\
                            .config('spark.python.worker.memory','4G')\
                            .config('spark.driver.maxResultSize','0')\
                            .config('spark.sql.crossJoin.enabled','true')\
                            .config('spark.serializer','org.apache.spark.serializer.KryoSerializer')\
                            .config('spark.default.parallelism','2')\
                            .getOrCreate()

# load in train and test
train_df = pd.read_csv(sys.argv[1])
train_df.fillna('', inplace = True)
test_df = pd.read_csv(sys.argv[2])
test_df.fillna('', inplace = True)

train_df = spark.createDataFrame(train_df)
test_df = spark.createDataFrame(test_df)


# tokenize sentences > count frequency > convert to tfidf for classification
tokenizer = Tokenizer(inputCol = "comment_text", outputCol = "tokens")
hashingTF = HashingTF(inputCol = "tokens", outputCol = "tf")
idf = IDF(inputCol = "tf", outputCol = "features")

train_tf = hashingTF.transform(tokenizer.transform(train_df))
trainIDFmodel = idf.fit(train_tf)
train_tfidf = trainIDFmodel.transform(train_tf)

test_tf = hashingTF.transform(tokenizer.transform(test_df))
testIDFmodel = idf.fit(test_tf)
test_tfidf = trainIDFmodel.transform(test_tf)

# Logisitic Regression
lr = LogisticRegression(featuresCol = 'features',
                        labelCol = "toxic",
                        regParam = 0.1)
lrModel = lr.fit(train_tfidf)
train_result = lrModel.transform(train_tfidf)
test_result = lrModel.transform(test_tfidf)

# Evaluate Prediction Result
evaluator = BinaryClassificationEvaluator(rawPredictionCol = "rawPrediction",
                                          labelCol = "toxic")
train_auc = evaluator.evaluate(train_result)
test_auc = evaluator.evaluate(test_result)
print("Train AUC: {}".format(train_auc))
print("Test AUC: {}".format(test_auc))
