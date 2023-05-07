from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import col
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml.classification import LogisticRegressionTrainingSummary
from pyspark.sql import SparkSession
import sys
from operator import itemgetter

spark = SparkSession.builder.appName("Project2Part2").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load the dataset
heart_df = spark.read.option("header", True).option("inferSchema", True).csv(sys.argv[1])

# Drop the 'education' column
heart_df = heart_df.drop('education')

heart_df = heart_df.replace(["NA", "NaN"], [None, None])
heart_df = heart_df.na.drop()


heart_df.describe().show()

heart_df = heart_df.withColumn("cigsPerDay", col("cigsPerDay").cast("double"))
heart_df = heart_df.withColumn("BPMeds", col("BPMeds").cast("double"))
heart_df = heart_df.withColumn("totChol", col("totChol").cast("double"))
heart_df = heart_df.withColumn("BMI", col("BMI").cast("double"))
heart_df = heart_df.withColumn("heartRate", col("heartRate").cast("double"))
heart_df = heart_df.withColumn("glucose", col("glucose").cast("double"))


heart_df.printSchema()
assembler = VectorAssembler(inputCols=[c for c in heart_df.columns if c != "TenYearCHD"], outputCol="features")
data = assembler.transform(heart_df)

train, test = data.randomSplit([0.7, 0.3])

lr = LogisticRegression(featuresCol="features", labelCol="TenYearCHD")

paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator().setLabelCol("TenYearCHD"),
                          numFolds=5)
print(train)

model = crossval.fit(train)

best_model = model.bestModel  # Get the best model from the CrossValidatorModel object
training_summary = best_model.summary  # Get the training summary from the best model
training_summary.predictions.describe().show()


predictions = best_model.transform(test)

evaluator = BinaryClassificationEvaluator(labelCol="TenYearCHD")
auc = evaluator.evaluate(predictions)

predictions.printSchema()

overall_risk = (predictions.agg({"prediction": "avg"}).collect()[0][0]) * 100
factors = dict(zip(heart_df.columns[:-1], best_model.coefficients))
relevant_factors = sorted(factors.items(), key=itemgetter(1), reverse=True)
i = 1
for factor, coefficient in relevant_factors:
    print(f"{factor}: {coefficient:.4f}%")
    i += 1

print()
print(f"The overall risk is {overall_risk:.4f}%")

# Print the AUC
print("AUC:", auc)