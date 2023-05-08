 #!/usr/bin/python

import sys
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier
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

    #Calling ML models
    rf_income = RandomForestClassifier(featuresCol='Features', labelCol='Labels')
    dt_income = DecisionTreeClassifier(featuresCol='Features', labelCol='Labels')

    #Pipeline that executes indexers, encoder and Logistic Regression model
    rf_pipeline = Pipeline(stages = [workClass_indexer,maritalStatus_indexer,occupation_indexer,relationship_indexer,race_indexer,sex_indexer,nativeCountry_indexer,
                                  workClass_encoder,maritalStatus_encoder,occupation_encoder,relationship_encoder,race_encoder,sex_encoder,nativeCountry_encoder,educationNum_encoder,
                                  assembler,rf_income])
    dt_pipeline = Pipeline(stages = [workClass_indexer,maritalStatus_indexer,occupation_indexer,relationship_indexer,race_indexer,sex_indexer,nativeCountry_indexer,
                                  workClass_encoder,maritalStatus_encoder,occupation_encoder,relationship_encoder,race_encoder,sex_encoder,nativeCountry_encoder,educationNum_encoder,
                                  assembler,dt_income])

    #Fitting data to train data set
    rf_fit_model = rf_pipeline.fit(train_data)
    dt_fit_model = dt_pipeline.fit(train_data)
    rf_results = rf_fit_model.transform(test_data)
    dt_results = dt_fit_model.transform(test_data)

    #Getting final prediction vs Labels
    my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='Labels')

    #Metrics values
    rf_AUC = my_eval.evaluate(rf_results)
    dt_AUC = my_eval.evaluate(dt_results)

    rf_TN = rf_results.filter('prediction = 0 AND Labels = prediction').count()
    rf_TP = rf_results.filter('prediction = 1 AND Labels = prediction').count()
    rf_FN = rf_results.filter('prediction = 0 AND Labels = 1').count()
    rf_FP = rf_results.filter('prediction = 1 AND Labels = 0').count()
    
    dt_TN = dt_results.filter('prediction = 0 AND Labels = prediction').count()
    dt_TP = dt_results.filter('prediction = 1 AND Labels = prediction').count()
    dt_FN = dt_results.filter('prediction = 0 AND Labels = 1').count()
    dt_FP = dt_results.filter('prediction = 1 AND Labels = 0').count()

    #Accuracy measures the proportion of correct predictions
    rf_accuracy = (rf_TN + rf_TP) / (rf_TN + rf_TP + rf_FN + rf_FP)
    dt_accuracy = (dt_TN + dt_TP) / (dt_TN + dt_TP + dt_FN + dt_FP)


    print("\n**************************************************\n")
    print(f'Random Forest Test AUC is:\t{rf_AUC}')
    print(f'Random Forest Test Accuracy is:\t{rf_accuracy}')
    print()
    print(f'Decision Tree Test AUC is:\t{dt_AUC}')
    print(f'Decision Tree Test Accuracy is:\t{dt_accuracy}')
    print("\n**************************************************\n")

    spark.stop()




