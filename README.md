# Spark-Projects

+ Setups for `CISC5950Lab2.zip`:
  1. upload to vm via ssh
  <br> `scp <localZipFilePath> <userName>@<externalIP>:<vmDestinationPath>`
  2. `unzip CISC5950Lab2.zip`
  3. `cd CISC5950Lab2`
  4. `chmod +x env.sh start.sh stop.sh`
  5. `chmod +x Lab2/Comfort_Zone/test.sh Lab2/Ticket_Prob/test.sh Lab2/Ticket_Time/test.sh`
  6. `cd Lab2`

+ Requirements:
  + `python3 -m pip install pandas`
  
+ Parts:
  > Go to any of the sections and `bash test.sh`
  <br>When prompted, enter the internal ip of your manager vm
  + Comfort_Zone == Part 1
  + Ticket_Probability == Part 2
  + Ticket_Time == Part 3
  
  
