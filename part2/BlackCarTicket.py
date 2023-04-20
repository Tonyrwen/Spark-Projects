#!/usr/bin/python

import sys
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

Street = ["34510", "10030", "34050"]
colors = ["Black", "BLK", "BK", "BK.", "BLAC", "BK/","BCK","BLK.","B LAC","BC"]


if __name__ == "__main__":
    file = str(sys.argv[1]).strip()
    spark = SparkSession.builder\
        .appName("BlackCarTicket").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Data 
    violations = spark.read.csv(file, inferSchema=True, header=True)

    #Filtering base on condition
    violations=violations.select('Vehicle Color','Street Code1','Street Code2','Street Code3').na.drop()

    #Total black car violation with in streets ["34510", "10030", "34050"]
    yes_count = violations.filter(violations['Vehicle Color'].isin(colors) & (violations['Street Code1'].isin(Street)) | (violations['Street Code2'].isin(Street)) |(violations['Street Code3'].isin(Street))).count()
    #Total Violation Records
    total_count = violations.select('Vehicle Color').count()
    
    #print(f'Yes value is: {yes_count}')
    #print(f'Total Value is: {total_count}')
    
    #Probality calculation
    final_probalility = yes_count/total_count

    print("The Probability of Black vehicle parking illegally is:",final_probalility)

        
        
   