import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import requests
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'Igor',
    'depends_on_past': False,
    'email': ['igor.enenberg@develeap.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=10),
}

CITIES = [
    "New York", "Los Angeles", "Chicago", "Miami", "Seattle",
    "Denver", "Boston", "San Francisco", "Dallas", "Atlanta"
]

def fetch_weather_data(**context):
    """
    Fetch weather data for multiple cities and store in XCom
    We're simulating API calls to avoid actual API key requirements
    """
    logger.info("Starting weather data collection")
    
    # In a real scenario, we would call a weather API like OpenWeatherMap
    # Here we'll simulate the data
    weather_data = []
    
    for city in CITIES:
        # Generate random weather data for demonstration
        temp = round(random.uniform(5, 35), 1)  # temperature in Celsius
        humidity = random.randint(30, 95)  # humidity percentage
        wind_speed = round(random.uniform(0, 30), 1)  # wind speed in km/h
        precipitation = round(random.uniform(0, 100), 1)  # precipitation chance in %
        
        weather_data.append({
            "city": city,
            "temperature": temp,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "precipitation": precipitation,
            "timestamp": datetime.now().isoformat()
        })
    
    logger.info(f"Collected weather data for {len(weather_data)} cities")

    context['ti'].xcom_push(key='weather_data', value=json.dumps(weather_data))
    return len(weather_data)

def process_weather_data(**context):
    """
    Process the weather data to compute statistics and insights
    """
    logger.info("Starting weather data processing")
    
    # Get the weather data from XCom
    ti = context['ti']
    weather_data_str = ti.xcom_pull(task_ids='fetch_weather_data', key='weather_data')
    weather_data = json.loads(weather_data_str)
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(weather_data)
    
    # Compute basic statistics
    avg_temp = df['temperature'].mean()
    max_temp_city = df.loc[df['temperature'].idxmax()]['city']
    min_temp_city = df.loc[df['temperature'].idxmin()]['city']
    avg_humidity = df['humidity'].mean()
    
    # Categorize cities by temperature ranges
    df['temp_category'] = pd.cut(
        df['temperature'],
        bins=[0, 10, 20, 30, 100],
        labels=['Cold', 'Mild', 'Warm', 'Hot']
    )
    
    category_counts = df['temp_category'].value_counts().to_dict()
    
    # Compute rain probability by humidity (simplified model)
    df['rain_probability'] = df['humidity'] * df['precipitation'] / 100
    
    # Create processed results
    results = {
        "average_temperature": round(avg_temp, 1),
        "hottest_city": max_temp_city,
        "coldest_city": min_temp_city,
        "average_humidity": round(avg_humidity, 1),
        "temperature_categories": category_counts,
        "cities_by_rain_probability": df[['city', 'rain_probability']].sort_values(
            by='rain_probability', ascending=False
        ).head(3).to_dict('records')
    }
    
    logger.info(f"Processed weather data into {len(results)} result categories")

    ti.xcom_push(key='processed_weather_data', value=json.dumps(results))
    return True

def generate_weather_report(**context):
    """
    Generate a human-readable weather report based on the processed data
    """
    logger.info("Starting weather report generation")

    ti = context['ti']
    processed_data_str = ti.xcom_pull(task_ids='process_weather_data', key='processed_weather_data')
    processed_data = json.loads(processed_data_str)

    report = f"""
    DAILY WEATHER REPORT - {datetime.now().strftime('%Y-%m-%d')}
    =========================================================
    
    TEMPERATURE SUMMARY:
    - Average Temperature: {processed_data['average_temperature']}°C
    - Hottest City: {processed_data['hottest_city']}
    - Coldest City: {processed_data['coldest_city']}
    
    HUMIDITY:
    - Average Humidity: {processed_data['average_humidity']}%
    
    TEMPERATURE CATEGORIES:
    """
    
    for category, count in processed_data['temperature_categories'].items():
        report += f"- {category}: {count} cities\n    "
    
    report += """
    RAIN FORECAST:
    Cities with highest chance of rain:
    """
    
    for city_data in processed_data['cities_by_rain_probability']:
        report += f"- {city_data['city']}: {round(city_data['rain_probability'] * 100, 1)}% chance\n    "
    
    report += """
    =========================================================
    End of Report
    """
    
    logger.info("Weather report generated successfully")

    ti.xcom_push(key='weather_report', value=report)
    logger.info(f"Weather Report:\n{report}")
    return True

def send_notifications(**context):
    """
    Simulate sending weather alerts based on thresholds
    """
    logger.info("Starting notification service")

    ti = context['ti']
    processed_data_str = ti.xcom_pull(task_ids='process_weather_data', key='processed_weather_data')
    weather_data_str = ti.xcom_pull(task_ids='fetch_weather_data', key='weather_data')
    
    processed_data = json.loads(processed_data_str)
    weather_data = json.loads(weather_data_str)
    
    # Define notification thresholds
    TEMP_HIGH_THRESHOLD = 30  # Celsius
    TEMP_LOW_THRESHOLD = 5    # Celsius
    RAIN_THRESHOLD = 70       # Percentage
    
    # Check for cities exceeding thresholds
    notifications = []
    
    for city_data in weather_data:
        city = city_data['city']
        temp = city_data['temperature']
        precip = city_data['precipitation']
        
        if temp > TEMP_HIGH_THRESHOLD:
            notifications.append(f"HEAT ALERT: {city} is experiencing high temperatures of {temp}°C")
        
        if temp < TEMP_LOW_THRESHOLD:
            notifications.append(f"COLD ALERT: {city} is experiencing low temperatures of {temp}°C")
        
        if precip > RAIN_THRESHOLD:
            notifications.append(f"RAIN ALERT: {city} has a high chance of precipitation ({precip}%)")
    
    # In a real scenario, we would send these notifications via email, SMS, etc.
    if notifications:
        logger.info(f"Sending {len(notifications)} weather alerts")
        for notification in notifications:
            logger.info(f"ALERT: {notification}")
    else:
        logger.info("No weather alerts to send")

    ti.xcom_push(key='weather_alerts', value=json.dumps(notifications))
    return len(notifications)

dag = DAG(
    dag_id='weather_analytics',
    default_args=default_args,
    description='A DAG that collects, processes, and reports on weather data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'analytics', 'example2'],
)

fetch_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    provide_context=True,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_weather_report',
    python_callable=generate_weather_report,
    provide_context=True,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='send_notifications',
    python_callable=send_notifications,
    provide_context=True,
    dag=dag,
)

fetch_task >> process_task >> [report_task, notify_task]
