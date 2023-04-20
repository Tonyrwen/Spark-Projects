#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /ticket_prob/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ticket_prob/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../parking-violation-2022.csv /ticket_prob/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_prob.py hdfs://$SPARK_MASTER:9000/ticket_prob/input/
../../stop.sh
