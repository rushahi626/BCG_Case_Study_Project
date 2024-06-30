Case Study on analysis of US accidents or crashes

Case Study:

Dataset:

Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below analytics.


Analytics: a. Application should perform below analysis and store the results for each analysis.

Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
Analysis 2: How many two wheelers are booked for crashes?
Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
Analysis 5: Which state has highest number of accidents in which females are not involved?
Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


Expected Output:

Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)
Code should be properly organized in folders as a project.
Input data sources and output should be config driven
Code should be strictly developed using Data Frame APIs (Do not use Spark SQL)
Share the entire project as zip or link to project in GitHub repo.


Runbook
Clone the repo and follow these steps:

Test Enviriment
Tested on window VS Code

Steps:
Go to the Project Directory: $ cd 'BCG Sprk Project'
run pip install command to import all dependencies listed on setup.py
pip install .

Run main.py file using Spark Submit
spark-submit main.py
