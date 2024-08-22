# Airflow-AWS-Project
Within this repository, I created an ETL job via Airflow and AWS to retrieve OpenWeather API data on a daily basis.


# Approach
I wanted to retrieve the weather for New York on a daily basis, so I created this Airflow project that can orchestrate an ETL job via AWS (Ubuntu EC2 Instance).
This project pulls data from the OpenWeather API, transforms the data in my Python script, and persists the data to an S3 output bucket. 
These tasks are all managed on my Weather DAG within Airflow

Note: Due to the pwd module being only available on Linux, I will be providing my python code for the project. 
The rest of the configurations (EC2 Instance calibration, Ubunut instance configuration, and dependencies installation)are local to my machine.
Thank you to TupleSpectra for showing me how to execute this project!


