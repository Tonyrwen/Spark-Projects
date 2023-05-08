#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /project2_part2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /project2_part2/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /project2_part2/input/

/usr/local/hadoop/bin/hdfs dfs -copyFromLocal framingham.csv /project2_part2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part2_code.py hdfs://$SPARK_MASTER:9000/project2_part2/input/
../../stop.sh
