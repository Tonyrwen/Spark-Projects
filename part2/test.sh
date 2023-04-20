#!/bin/bash
source ../env.sh
../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part1/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part1/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking_violations_2023.csv /part1/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 BlackCarTicket.py hdfs://$SPARK_MASTER:9000/part1/input/
../stop.sh
