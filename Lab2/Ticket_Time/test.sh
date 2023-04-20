#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../parking-violation-2022.csv /ticket_time/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/
../../stop.sh
