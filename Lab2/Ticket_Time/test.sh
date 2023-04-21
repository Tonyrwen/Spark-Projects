#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../parking-violation-2023.csv /ticket_time/input/
echo "\n\nParallelism 2:\n"
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=2
echo "\n\nParallelism 3:\n"
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=3
echo "\n\nParallelism 4:\n"
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=4
echo "\n\nParallelism 5:\n"
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=5
../../stop.sh
