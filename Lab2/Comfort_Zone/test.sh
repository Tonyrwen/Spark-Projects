#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /comfort_zone/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /comfort_zone/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal shot_logs.csv /comfort_zone/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./comfort_zone.py hdfs://$SPARK_MASTER:9000/comfort_zone/input/
../../stop.sh
