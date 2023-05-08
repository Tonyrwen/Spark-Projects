# Spark-Projects

+ Setups for `CISC5950Project2.zip`:
  1. upload to vm via ssh
  <br> `scp <localZipFilePath> <userName>@<externalIP>:<vmDestinationPath>`
  2. `unzip CISC5950Project2.zip`
  3. `cd CISC5950Project2`
  4. `chmod +x env.sh start.sh stop.sh`
  5. `chmod +x Project2/Part1/test.sh Project2/Part2/test.sh Project2/Part3/test.sh Project2/Part4/test.sh`

+ Requirements:
  + `python3 -m pip install pandas`
  
+ Parts:
  > Go to any of the sections and `bash test.sh`
  <br>When prompted, enter the internal ip of your manager vm
  + Part 1: Toxic Comment Logistic Regression
  + Part 2: Heart Disease Logistic Regression
  + Part 3: Census Income Logistic Regression
  + Part 4: Census Income Random Forest and Decision Tree

+ The Video Demos are located under their respective subdirectories as `Part#.mp4` files.  
  
