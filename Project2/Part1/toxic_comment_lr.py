from __future__ import print_function
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import udf, when, col, min, max, avg, concat, lit, desc, substring, length
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import string, re, nltk, sys

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
def to_spark_df(csv, withLabels = True, withText = True):
  df = pd.read_csv(csv)
  df.fillna('', inplace = True)

  if withLabels:
    if withText: return spark.createDataFrame(df[['id','comment_text','toxic']])
    else: return spark.createDataFrame(df[['id', 'toxic']])
  else: return spark.createDataFrame(df[['id','comment_text']])

train_df = to_spark_df(sys.argv[1])
test_df = to_spark_df(sys.argv[2], False)
test_label = to_spark_df(sys.argv[3], True, False)
test_df = test_df.join(test_label, ['id'], 'inner')

# clean text
nltk.download('stopwords')
stopwords = list(string.punctuation) + nltk.corpus.stopwords.words('english')
cleanTextUDF = udf(lambda text: "".join(i for i in text.replace('\n', ' ').lower() if i not in stopwords),
                   StringType())
train_df = train_df.withColumn("cleaned_comment", cleanTextUDF(col('comment_text')))
test_df = test_df.withColumn("cleaned_comment", cleanTextUDF(col('comment_text')))\
                 .withColumn('toxic', col('toxic')*-1)

# tokenize sentences > count frequency > convert to tfidf for classification
pipeline = Pipeline(stages = [
    Tokenizer(inputCol = "cleaned_comment", outputCol = "words"),
    HashingTF(inputCol = "words", outputCol = "word_frequency"),
    IDF(inputCol = "word_frequency", outputCol = "features"),
    LogisticRegression(featuresCol = "features", labelCol = "toxic", regParam = 0.1)
])
fit_model = pipeline.fit(train_df)

train_result = fit_model.transform(train_df)
test_result = fit_model.transform(test_df)

# Evaluate Prediction Result
evaluator = BinaryClassificationEvaluator(rawPredictionCol = "rawPrediction",
                                          labelCol = "toxic")
train_auc = evaluator.evaluate(train_result)
test_auc = evaluator.evaluate(test_result)

accuracy = []
for result in [train_result, test_result]:
  TN = result.filter('prediction = 0 AND toxic = prediction').count()
  TP = result.filter('prediction = 1 AND toxic = prediction').count()
  FN = result.filter('prediction = 0 AND toxic = 1').count()
  FP = result.filter('prediction = 1 AND toxic = 0').count()

  #Accuracy measures the proportion of correct predictions
  accuracy.append((TN + TP) / (TN + TP + FN + FP))

print("Train AUC: {}\tTrain Accuracy: {}".format(train_auc, accuracy[0]))
print("Test AUC: {}\tTest Accuracy: {}".format(test_auc, accuracy[1]))
