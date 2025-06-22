"""
DAG to collect weather data from Open-Meteo API
"""
from datetime import datetime, timedelta
import os
import json
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# List of cities with stores (based on stores.csv data)
CITIES = {
    'Hanoi': {'latitude': 21.0285, 'longitude': 105.8542, 'region': 'North'},
    'Ho Chi Minh': {'latitude': 10.8231, 'longitude': 106.6297, 'region': 'South'},
    'Danang': {'latitude': 16.0544, 'longitude': 108.2022, 'region': 'Central'},
    'Hai Phong': {'latitude': 20.8449, 'longitude': 106.6881, 'region': 'North'},
    'Can Tho': {'latitude': 10.0452, 'longitude': 105.7469, 'region': 'South'},
    'Hue': {'latitude': 16.4637, 'longitude': 107.5909, 'region': 'Central'},
    'Thai Nguyen': {'latitude': 21.5942, 'longitude': 105.8482, 'region': 'North'},
    'Nha Trang': {'latitude': 12.2388, 'longitude': 109.1967, 'region': 'Central'},
    'Vung Tau': {'latitude': 10.3460, 'longitude': 107.0843, 'region': 'South'},
    'Bac Ninh': {'latitude': 21.1214, 'longitude': 106.1151, 'region': 'North'},
}

def fetch_weather_data(city, lat, lon, ds, **kwargs):
    """
    Fetch weather data from Open-Meteo API
    """
    date = ds
    
    # Create API URL to get weather data
    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        f"&daily=temperature_2m_max,temperature_2m_min,rain_sum,precipitation_hours"
        f"&timezone=Asia/Ho_Chi_Minh"
        f"&start_date={date}&end_date={date}"
    )
    
    # Call API and get data
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Extract weather data
        weather_data = {
            'date': date,
            'city': city,
            'region': CITIES[city]['region'],
            'temperature_max': data['daily']['temperature_2m_max'][0],
            'temperature_min': data['daily']['temperature_2m_min'][0],
            'rain_sum': data['daily']['rain_sum'][0],
            'precipitation_hours': data['daily']['precipitation_hours'][0]
        }
        
        # Create storage directory if it doesn't exist
        output_dir = f"/opt/airflow/data/raw/weather/{date}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save data as JSON
        with open(f"{output_dir}/{city.lower().replace(' ', '_')}.json", 'w') as f:
            json.dump(weather_data, f)
        
        return f"Successfully fetched weather data for {city} on {date}"
    else:
        return f"Failed to fetch weather data for {city}. Status code: {response.status_code}"

def combine_daily_weather_data(ds, **kwargs):
    """
    Combine all weather data from cities on a single day into one CSV file
    """
    date = ds
    weather_dir = f"/opt/airflow/data/raw/weather/{date}"
    
    if not os.path.exists(weather_dir):
        return f"No weather data found for {date}"
    
    # Read all JSON files in the directory
    all_data = []
    for file in os.listdir(weather_dir):
        if file.endswith('.json'):
            with open(os.path.join(weather_dir, file), 'r') as f:
                city_data = json.load(f)
                all_data.append(city_data)
    
    if all_data:
        # Convert to DataFrame and save as CSV
        weather_df = pd.DataFrame(all_data)
        
        # Create processed directory if it doesn't exist
        processed_dir = "/opt/airflow/data/processed/weather"
        os.makedirs(processed_dir, exist_ok=True)
        
        # Save as CSV
        output_file = f"{processed_dir}/{date}.csv"
        weather_df.to_csv(output_file, index=False)
        
        return f"Successfully combined weather data for {date} into {output_file}"
    else:
        return f"No weather data found for {date}"

# Define DAG
with DAG(
    'weather_data_collector',
    default_args=default_args,
    description='Collect weather data from Open-Meteo API',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['weather'],
) as dag:
    
    # Create tasks to collect weather data for each city
    weather_tasks = []
    for city, coords in CITIES.items():
        task = PythonOperator(
            task_id=f'fetch_weather_{city.lower().replace(" ", "_")}',
            python_callable=fetch_weather_data,
            op_kwargs={
                'city': city,
                'lat': coords['latitude'],
                'lon': coords['longitude'],
            },
        )
        weather_tasks.append(task)
    
    # Task to combine all weather data
    combine_task = PythonOperator(
        task_id='combine_weather_data',
        python_callable=combine_daily_weather_data,
    )
    
    # Set up flow: fetch_weather_tasks >> combine_task
    weather_tasks >> combine_task