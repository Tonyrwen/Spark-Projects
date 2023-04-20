#!/bin/bash
source ../env.sh
../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /part3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking_violations_2023.csv  /part3/input/

echo "****************************************** 2 *****************************************************************************************************"

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 hour.py hdfs://$SPARK_MASTER:9000/part3/input/ --conf spark.default.parallelism=2

echo "****************************************** 3 *****************************************************************************************************"

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 hour.py hdfs://$SPARK_MASTER:9000/part3/input/ --conf spark.default.parallelism=3

echo "****************************************** 4 *****************************************************************************************************"

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 hour.py hdfs://$SPARK_MASTER:9000/part3/input/ --conf spark.default.parallelism=4

echo "****************************************** 5 *****************************************************************************************************"

/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 hour.py hdfs://$SPARK_MASTER:9000/part3/input/ --conf spark.default.parallelism=5



/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /part3/output/
../stop.sh
