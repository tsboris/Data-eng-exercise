import os
import logging
import requests
import pandas as pd
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

# Define task dependencies (will add more tasks later)
extract_task 