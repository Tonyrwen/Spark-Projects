#!/bin/bash
source ../../env.sh
../../start.sh

/usr/local/hadoop/bin/hdfs dfs -rm -r /toxic_comment/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /comfort_zone/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal train.csv /toxic_comment/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal test.csv /toxic_comment/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal test_labels.csv /toxic_comment/input/

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./toxic_comment_lr.py hdfs://$SPARK_MASTER:9000/toxic_comment/input/

../../stop.sh
