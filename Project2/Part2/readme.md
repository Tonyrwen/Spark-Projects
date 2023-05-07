# Big Data Project 2 Part 2

Files: 
- part2_code.py: Contains the code for Part 2 of the project.
- run_spark.sh: Contains the script that runs the part2_code.py file in the cluster.

To run this lab:
1. If you have not started HDFS and Spark yet: run start.sh from the /mapreduce-test/ folder to start the Hadoop clusters, and run start.sh from the /spark-examples/ folder to start Spark.
2. Run the run_spark.sh script, which will run the python code and print the answer to the terminal.

Output:
- This program will run a logistic regression on the heart disease framingham.csv dataset using Spark on the cluster. It will output the coefficients of the risk factors, predictions, overall risk, and the area under the curve score (AUC).