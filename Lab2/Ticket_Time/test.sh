#!/bin/bash
source ../../env.sh
../../start.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ticket_time/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../parking-violation-2023.csv /ticket_time/input/
echo
echo
echo "Parallelism 2:"
echo
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=2
end=$(date +%s)
runtime=$((end-start))
echo
echo "Parallelism 2 Runtime: $runtime seconds"
echo
echo
echo "Parallelism 3:"
echo
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=3
end=$(date +%s)
runtime=$((end-start))
echo
echo "Parallelism 3 Runtime: $runtime seconds"
echo
echo
echo "Parallelism 4:"
echo
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=4
end=$(date +%s)
runtime=$((end-start))
echo
echo "Parallelism 4 Runtime: $runtime seconds"
echo
echo
echo "Parallelism 5:"
echo
start=$(date +%s)
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ticket_time.py hdfs://$SPARK_MASTER:9000/ticket_time/input/ --conf spark.default.parallelism=5
end=$(date +%s)
runtime=$((end-start))
echo
echo "Parallelism 5 Runtime: $runtime seconds"
../../stop.sh
