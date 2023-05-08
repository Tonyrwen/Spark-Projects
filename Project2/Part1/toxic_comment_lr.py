from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import udf, when, col, min, max, avg, concat, lit, desc, substring, length
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import string, re, sys

if __name__ == "__main__":
  spark = SparkSession.builder.appName("Toxic Comment Classification")\
                              .getOrCreate()

  # load in train and test
  train_df = spark.read.format("csv")\
                        .options(header = 'true', inferschema = 'true', quote = '"', escape = '"', multiline = "true")\
                        .load(sys.argv[1])\
                        .select(col('id'), col('comment_text'), col('toxic'))
  test_df = spark.read.format("csv")\
                        .options(header = 'true', inferschema = 'true', quote = '"', escape = '"', multiline = "true")\
                        .load(sys.argv[2])\
                        .select(col('id'), col('comment_text'))
  test_label = spark.read.format("csv")\
                        .options(header = 'true', inferschema = 'true', quote = '"', escape = '"', multiline = "true")\
                        .load(sys.argv[3])\
                        .select(col('id'), col('toxic'))           
  test_df = test_df.join(test_label, ['id'], 'inner')

  # clean text
  cleanTextUDF = udf(lambda text: "".join(i for i in text.replace('\n', ' ').lower() if i not in string.punctuation),
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
    
  print('\n**************************************************\n')
  print("Train AUC: {}\tTrain Accuracy: {}".format(train_auc, accuracy[0]))
  print("Test AUC: {}\tTest Accuracy: {}".format(test_auc, accuracy[1]))
  print('\n**************************************************')
  
  spark.stop()
