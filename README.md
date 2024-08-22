# Airflow-AWS-Project
Within this repository, I created an ETL job via Airflow and AWS to retrieve OpenWeather API data on a daily basis.


# Approach
I wanted to retrieve the weather for New York on a daily basis, so I created this Airflow project that can orchestrate an ETL job via AWS (Ubuntu EC2 Instance).
This project pulls data from the OpenWeather API, transforms the data in my Python script, and persists the data to an S3 output bucket. 
These tasks are all managed on my Weather DAG within Airflow

Note: Due to the pwd module being only available on Linux, I will be providing my python code for the project. 
The rest of the configurations (EC2 Instance calibration, Ubunut instance configuration, and dependencies installation)are local to my machine.
Thank you to TupleSpectra for showing me how to execute this project!


# Technical steps for DAG
1. My first task makes sure to establish a proper connection with OpenWeather API. This is done by using my personal API key and city name within the link.
2. For the second task, we extract the data from the API by using the same endpoint to pull our data in JSON format.
3. For the third task, we perform our transformations on the data (Converting Kelving to Fahrenheit, concatenating time of records, etc.)
4. We chain these tasks together and establish connection with our EC2 instance using a remote SSH connection and to our AWS account/S3 bucket using Access key/Secret Access key IDs.
5. Run our DAG on Airflow, which will run the code and output the data to our S3 bucket.