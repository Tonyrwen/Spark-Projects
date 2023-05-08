#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part4/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part4/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part4/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../adult_data.csv  /part4/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../adult_test.csv /part4/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 Census_Income_P4.py hdfs://$SPARK_MASTER:9000/part4/input/adult_data.csv hdfs://$SPARK_MASTER:9000/part4/input/adult_test.csv
../../stop.sh

