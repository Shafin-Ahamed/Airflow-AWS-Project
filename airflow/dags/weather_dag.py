from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = round(kelvin_to_fahrenheit(data["main"]["temp"]),2)
    feels_like_farenheit= round(kelvin_to_fahrenheit(data["main"]["feels_like"]),2)
    min_temp_farenheit = round(kelvin_to_fahrenheit(data["main"]["temp_min"]),2)
    max_temp_farenheit = round(kelvin_to_fahrenheit(data["main"]["temp_max"]),2)
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIATCKAOAQPEJ5SGU5R", "secret": "HIN9grpfwGlhgN92gjzVsBc7AmfV8E9o/kM2rGEU", "token": "FwoGZXIvYXdzEJf//////////wEaDOVOAvdFkcE8i6o/2yJqf5TjZAhc09GFgEMrQ6Wevp4Q4/mqMrf3KDAHDkSJPqz2CuhTiZ+e2Id3Rsuop3kWpfX0Dt8qxtOl1sMaPd9gy001Kx9tJ0eN8W5a/SXw2VpVmYUZApajYePePBJ1vvFUbBWKRcyhyFj/TyiC75y2BjIog7HiyrWOymb8EfHhLsak57/IBzMJfgMPw66A2WEzZESZCZO6uWXr2g=="}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_NewYork_' + dt_string
    df_data.to_csv(f"s3://openweather-api-bucket/{dt_string}.csv", index=False, storage_options=aws_credentials)


default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,8,21),
    'email': ['ashafin372@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=New York&APPID=b0898ff45bdf68a624a2b5791f647f63'
        )

        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=New York&APPID=b0898ff45bdf68a624a2b5791f647f63',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

