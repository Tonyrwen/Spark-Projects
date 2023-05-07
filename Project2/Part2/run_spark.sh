#!/bin/bash
source /spark-examples/env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /project2_part2/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /project2_part2/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /project2_part2/input/

/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /project2/data/framingham.csv /project2_part2/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part2_code.py /project2_part2/input/