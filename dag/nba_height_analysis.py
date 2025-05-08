import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=10),
}

# S3/MinIO configuration
S3_CONN_ID = 'minio_s3'
BUCKET_NAME = 'workshop-data'

# Data source
NBA_HEIGHTS_URL = 'https://www.openintro.org/data/csv/nba_heights.csv'

def extract_nba_data(**context):
    """
    Extract NBA player height data from external URL with robust error handling
    and upload raw data to MinIO
    """
    logger.info("Starting NBA data extraction")
    
    # Set headers to avoid HTTP 406 errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/csv,application/csv,text/plain,application/octet-stream,*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.openintro.org/'
    }
    
    try:
        # Download data with retry logic (Airflow will handle retries based on default_args)
        logger.info(f"Downloading NBA height data from {NBA_HEIGHTS_URL}")
        response = requests.get(NBA_HEIGHTS_URL, headers=headers)
        response.raise_for_status()  # Raise exception for 4XX/5XX responses
        
        # Quick validation of the data
        data = response.text
        if not data or len(data) < 100:  # Basic check that we got something reasonable
            raise ValueError("Downloaded data appears too small or empty")
        
        # Convert to DataFrame for validation and future tasks
        df = pd.read_csv(StringIO(data))
        logger.info(f"Successfully downloaded data: {len(df)} rows, {df.columns.tolist()} columns")
        
        # Upload raw data to MinIO
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        raw_key = f'nba/raw/nba_heights_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        logger.info(f"Uploading raw data to MinIO: s3://{BUCKET_NAME}/{raw_key}")
        s3_hook.load_string(
            string_data=data,
            key=raw_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        
        # Store the S3 path for downstream tasks
        context['ti'].xcom_push(key='raw_data_s3_path', value=f"{BUCKET_NAME}/{raw_key}")
        context['ti'].xcom_push(key='raw_data_rows', value=len(df))
        
        logger.info("NBA data extraction completed successfully")
        return raw_key
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading NBA data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in data extraction: {str(e)}")
        raise

def clean_nba_data(**context):
    """
    Clean and transform the NBA player height data:
    - Create full name column
    - Convert heights to metric units
    - Categorize players by position based on height
    - Sort players by height
    - Log statistics about tallest/shortest players
    - Upload cleaned data to MinIO
    """
    logger.info("Starting NBA data cleaning")
    
    # Get the raw data path from the previous task
    ti = context['ti']
    raw_data_s3_path = ti.xcom_pull(task_ids='extract_nba_data', key='raw_data_s3_path')
    
    if not raw_data_s3_path:
        raise ValueError("Raw data path not found in XCom")
    
    # Download raw data from MinIO
    bucket_name, key = raw_data_s3_path.split('/', 1)
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    raw_data = s3_hook.read_key(key=key, bucket_name=bucket_name)
    
    # Convert to dataframe
    df = pd.read_csv(StringIO(raw_data))
    logger.info(f"Downloaded raw data: {len(df)} rows")
    
    # 1. Create full player name column
    df['player_name'] = df['first_name'] + ' ' + df['last_name']
    
    # 2. Convert heights to metric units
    # First convert height from feet-inches format to inches, then to meters and cm
    df['height_inches'] = df['height'].apply(lambda x: 
        int(x.split('-')[0]) * 12 + int(x.split('-')[1]) if isinstance(x, str) and '-' in x else None)
    
    # Convert to meters
    df['height_m'] = df['height_inches'].apply(lambda x: round(x * 0.0254, 2) if pd.notnull(x) else None)
    
    # Convert to centimeters
    df['height_cm'] = df['height_inches'].apply(lambda x: round(x * 2.54, 1) if pd.notnull(x) else None)
    
    # 3. Categorize players by position based on height
    # Define the position categories based on height in meters
    df['position_category'] = pd.cut(
        df['height_m'],
        bins=[0, 1.90, 2.00, 2.10, 3.0],
        labels=['Point Guard/Shooting Guard', 'Small Forward', 'Power Forward', 'Center']
    )
    
    # 4. Sort players by height and reset index
    df = df.sort_values(by='height_m', ascending=False).reset_index(drop=True)
    
    # 5. Log statistics about tallest and shortest players
    tallest_player = df.iloc[0]
    shortest_player = df.iloc[-1]
    
    logger.info(f"Tallest player: {tallest_player['player_name']} - {tallest_player['height_m']}m ({tallest_player['height_cm']}cm)")
    logger.info(f"Shortest player: {shortest_player['player_name']} - {shortest_player['height_m']}m ({shortest_player['height_cm']}cm)")
    
    logger.info(f"Average height: {df['height_m'].mean():.2f}m ({df['height_cm'].mean():.1f}cm)")
    logger.info(f"Median height: {df['height_m'].median():.2f}m ({df['height_cm'].median():.1f}cm)")
    
    # Get distribution by position category
    position_counts = df['position_category'].value_counts()
    for position, count in position_counts.items():
        logger.info(f"Position category: {position} - {count} players")
    
    # 6. Upload cleaned data to MinIO
    cleaned_key = f'nba/cleaned/nba_heights_cleaned_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    logger.info(f"Uploading cleaned data to MinIO: s3://{BUCKET_NAME}/{cleaned_key}")
    cleaned_csv = df.to_csv(index=False)
    s3_hook.load_string(
        string_data=cleaned_csv,
        key=cleaned_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    
    # Store the cleaned data path for downstream tasks
    context['ti'].xcom_push(key='cleaned_data_s3_path', value=f"{BUCKET_NAME}/{cleaned_key}")
    context['ti'].xcom_push(key='position_categories', value=position_counts.to_dict())
    context['ti'].xcom_push(key='player_count', value=len(df))
    
    logger.info("NBA data cleaning completed successfully")
    return cleaned_key

# Define the DAG
dag = DAG(
    dag_id='nba_height_analysis',
    default_args=default_args,
    description='A pipeline to analyze NBA player heights',
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
    tags=['nba', 'analytics', 'workshop'],
)

# Task for data extraction
extract_task = PythonOperator(
    task_id='extract_nba_data',
    python_callable=extract_nba_data,
    provide_context=True,
    dag=dag,
)

# Task for data cleaning
clean_task = PythonOperator(
    task_id='clean_nba_data',
    python_callable=clean_nba_data,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> clean_task 