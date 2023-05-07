 #!/usr/bin/python

import sys
from pyspark.sql import SparkSession
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler,VectorIndexer,OneHotEncoder,StringIndexer
from pyspark.ml import Pipeline
import pyspark.sql.functions as funcs


if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("part_3").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    original_columns = ["age", "workClass", "fnlwgt", "education", "educationNum", "maritalStatus", "occupation", "relationship", "race", "sex", "capitalGain", "capitalLoss", "hoursPerWeek", "nativeCountry", "Labels"]

    final_columns = ["age", "workClass", "educationNum", "maritalStatus", "occupation","relationship", "race", "sex","hoursPerWeek", "nativeCountry","Labels"]

    # Load training data
    #train_data = spark.read.csv("adult_data.csv",inferSchema=True,header=False)
    train_data = spark.read.csv(sys.argv[1], inferSchema=True, header=False)


    #Adding Columns
    column_names = original_columns
    train_data = train_data.toDF(*column_names)

    #Removing Columns no longer needed
    train_data = train_data.select(final_columns)


    #Load test data
    #test_data = spark.sparkContext.textFile("adult_test.csv").map(lambda line : line.split(","))
    test_data = spark.sparkContext.textFile(sys.argv[2]).map(lambda line: line.split(","))
    test_data = test_data.filter( lambda line : len(line) >2 )
    test_data = test_data.toDF()
    test_data = test_data.toDF(*column_names)

    #Removing Columns no longer needed
    test_data = test_data.select(final_columns)

    def clean_data(data_set):

        #Removing spaces
        for colname in final_columns:
            data_set = data_set.withColumn(colname, funcs.trim(funcs.col(colname)))

        #Removing ?
        nativeCountryCol = 'nativeCountry'
        data_set = data_set.withColumn(nativeCountryCol, funcs.when(funcs.col(nativeCountryCol) == "?", "Other").otherwise(funcs.col(nativeCountryCol)))

        #Replacing <=50K to == 0 & >50K == 1
        income_label_Col = 'Labels'
        data_set = data_set.withColumn(income_label_Col, funcs.when(funcs.col(income_label_Col).contains("<=50K"), 0).otherwise(1))

        #Converting values to interger
        data_set = data_set.withColumn(income_label_Col, funcs.col(income_label_Col).cast("int"))

        int_Cols = ['educationNum', 'age', 'hoursPerWeek']
        for int_Col in int_Cols:
            data_set = data_set.withColumn(int_Col, funcs.col(int_Col).cast("int"))

        # Removing ? from feature workClass & occupation
        workCols = ['workClass', 'occupation']
        for colname in workCols:
            data_set = data_set.withColumn(colname, funcs.when(funcs.col(colname) == "?", None).otherwise(
                funcs.col(colname)))
        data_set = data_set.na.drop()

        return data_set



    #Cleaning data sets (Train & Test data)
    train_data = clean_data(train_data)
    #print("----------This is train data for 0 values: ----------")
    #train_data.filter(train_data['Labels'] == 0).show()
    #print("----------This is train data for 1 values:----------")
    #train_data.filter(train_data['Labels'] == 1).show()


    test_data = clean_data(test_data)
    #print("----------This is test data for 0 values: ----------")
    #test_data.filter(test_data['Labels'] == 0).show()
    #print("----------This is test data for 1 values:----------")
    #test_data.filter(test_data['Labels'] == 1).show()


    def get_indexer_encoder(fieldName):
        #Converting categorical data to indexer
        indexer = StringIndexer(inputCol=fieldName, outputCol=fieldName+'Index')

        #Converting indexer data to encoder
        encoder = OneHotEncoder(inputCol=fieldName+'Index', outputCol=fieldName+'Vec')
        return indexer,encoder


    #Calling function to get indexer & encoder
    workClass_indexer,workClass_encoder = get_indexer_encoder('workClass')
    maritalStatus_indexer, maritalStatus_encoder = get_indexer_encoder('maritalStatus')
    occupation_indexer, occupation_encoder = get_indexer_encoder('occupation')
    relationship_indexer, relationship_encoder = get_indexer_encoder('relationship')
    race_indexer, race_encoder = get_indexer_encoder('race')
    sex_indexer, sex_encoder = get_indexer_encoder('sex')
    nativeCountry_indexer, nativeCountry_encoder = get_indexer_encoder('nativeCountry')

    #Converting educationNum in to OneHotEncoder since value is already numeric
    educationNum_encoder = OneHotEncoder(inputCol='educationNum', outputCol='educationNumVec')

    #Creating assembler to get all feature
    assembler = VectorAssembler(inputCols=["age", "workClassVec", "educationNumVec", "maritalStatusVec", "occupationVec","relationshipVec", "raceVec", "sexVec","hoursPerWeek", "nativeCountryVec"], outputCol='Features')

    #Calling ML LogisticRegression model
    log_reg_income = LogisticRegression(featuresCol='Features', labelCol='Labels')

    #Pipeline that executes indexers, encoder and Logistic Regression model
    pipeline = Pipeline(stages = [workClass_indexer,maritalStatus_indexer,occupation_indexer,relationship_indexer,race_indexer,sex_indexer,nativeCountry_indexer,
                                  workClass_encoder,maritalStatus_encoder,occupation_encoder,relationship_encoder,race_encoder,sex_encoder,nativeCountry_encoder,educationNum_encoder,
                                  assembler,log_reg_income])

    #Fitting data to train data set
    fit_model = pipeline.fit(train_data)
    results = fit_model.transform(test_data)

    #Getting final prediction vs Labels
    my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='Labels')

    results.filter(results['Labels'] == 0).select('Labels','prediction').show(100)
    #Metrics values
    AUC = my_eval.evaluate(results)
    print(f'----------The ACU result is:  {AUC}----------')


    TN = results.filter('prediction = 0 AND Labels = prediction').count()
    TP = results.filter('prediction = 1 AND Labels = prediction').count()
    FN = results.filter('prediction = 0 AND Labels = 1').count()
    FP = results.filter('prediction = 1 AND Labels = 0').count()

    #Accuracy measures the proportion of correct predictions
    accuracy = (TN + TP) / (TN + TP + FN + FP)
    print()
    print()
    print()
    print(f'----------The Accuracy result is:   {accuracy}----------')

    spark.stop()




