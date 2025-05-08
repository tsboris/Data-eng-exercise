import os
import logging
import requests
import pandas as pd
import numpy as np
import json
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
    
    # Print column names to debug
    logger.info(f"Available columns: {df.columns.tolist()}")
    
    # 1. Create full player name column
    if 'first_name' in df.columns and 'last_name' in df.columns:
        df['player_name'] = df['first_name'] + ' ' + df['last_name']
    else:
        # If first_name/last_name not available, try to use name column if it exists
        if 'name' in df.columns:
            df['player_name'] = df['name']
        else:
            logger.warning("Could not create player_name - missing required columns")
            df['player_name'] = 'Unknown'
    
    # 2. Convert heights to metric units
    # First identify the height column - it might be 'height', 'height_feet', etc.
    height_column = None
    possible_height_columns = ['height', 'h', 'height_feet_inches', 'player_height']
    
    for col in possible_height_columns:
        if col in df.columns:
            height_column = col
            break
    
    if height_column:
        logger.info(f"Using '{height_column}' as the height column")
        
        # Check the format of the first few values to determine conversion approach
        sample_heights = df[height_column].dropna().head(5).tolist()
        logger.info(f"Sample height values: {sample_heights}")
        
        # Determine if heights are in feet-inches format (e.g., "6-2") or just inches
        if any(isinstance(h, str) and '-' in h for h in sample_heights if isinstance(h, str)):
            # Heights are in feet-inches format
            df['height_inches'] = df[height_column].apply(
                lambda x: int(x.split('-')[0]) * 12 + int(x.split('-')[1]) 
                if isinstance(x, str) and '-' in x else None
            )
        elif any(isinstance(h, (int, float)) for h in sample_heights):
            # Heights might already be in inches
            df['height_inches'] = df[height_column]
        else:
            # Fallback - try to convert to float first
            try:
                df['height_inches'] = pd.to_numeric(df[height_column], errors='coerce')
            except:
                logger.warning(f"Could not convert {height_column} to numeric values")
                df['height_inches'] = None
    else:
        # If no height column is found, create empty columns
        logger.warning("No height column found in the dataset")
        df['height_inches'] = None
    
    # Convert to meters and centimeters
    df['height_m'] = df['height_inches'].apply(lambda x: round(x * 0.0254, 2) if pd.notnull(x) else None)
    df['height_cm'] = df['height_inches'].apply(lambda x: round(x * 2.54, 1) if pd.notnull(x) else None)
    
    # 3. Categorize players by position based on height (only for players with height)
    if df['height_m'].notna().any():
        df['position_category'] = pd.cut(
            df['height_m'],
            bins=[0, 1.90, 2.00, 2.10, 3.0],
            labels=['Point Guard/Shooting Guard', 'Small Forward', 'Power Forward', 'Center']
        )
    else:
        df['position_category'] = None
    
    # 4. Sort players by height and reset index (only if we have height data)
    if df['height_m'].notna().any():
        df = df.sort_values(by='height_m', ascending=False).reset_index(drop=True)
    
    # 5. Log statistics about tallest and shortest players
    if df['height_m'].notna().any():
        tallest_player = df.loc[df['height_m'].idxmax()]
        shortest_player = df.loc[df['height_m'].idxmin()]
        
        logger.info(f"Tallest player: {tallest_player['player_name']} - {tallest_player['height_m']}m ({tallest_player['height_cm']}cm)")
        logger.info(f"Shortest player: {shortest_player['player_name']} - {shortest_player['height_m']}m ({shortest_player['height_cm']}cm)")
        
        logger.info(f"Average height: {df['height_m'].mean():.2f}m ({df['height_cm'].mean():.1f}cm)")
        logger.info(f"Median height: {df['height_m'].median():.2f}m ({df['height_cm'].median():.1f}cm)")
        
        # Get distribution by position category
        position_counts = df['position_category'].value_counts()
        for position, count in position_counts.items():
            logger.info(f"Position category: {position} - {count} players")
    else:
        logger.warning("No height data available for statistics")
    
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
    context['ti'].xcom_push(key='position_categories', value=position_counts.to_dict() if df['height_m'].notna().any() else {})
    context['ti'].xcom_push(key='player_count', value=len(df))
    
    logger.info("NBA data cleaning completed successfully")
    return cleaned_key

def analyze_and_visualize_data(**context):
    """
    Analyze and visualize the cleaned NBA player height data:
    - Calculate statistics by position category
    - Generate ASCII histograms for height distribution
    - Create text-based box plots for position categories
    - Identify top 5 tallest and shortest players
    - Generate a comprehensive JSON report
    - Upload results to MinIO
    """
    logger.info("Starting NBA data analysis and visualization")
    
    # Helper function to convert numpy types to native Python types
    def convert_numpy_types(obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (list, tuple)):
            return [convert_numpy_types(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: convert_numpy_types(value) for key, value in obj.items()}
        else:
            return obj
    
    # Get the cleaned data path from the previous task
    ti = context['ti']
    cleaned_data_s3_path = ti.xcom_pull(task_ids='clean_nba_data', key='cleaned_data_s3_path')
    
    if not cleaned_data_s3_path:
        raise ValueError("Cleaned data path not found in XCom")
    
    # Download cleaned data from MinIO
    bucket_name, key = cleaned_data_s3_path.split('/', 1)
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    cleaned_data = s3_hook.read_key(key=key, bucket_name=bucket_name)
    
    # Convert to dataframe
    df = pd.read_csv(StringIO(cleaned_data))
    logger.info(f"Downloaded cleaned data: {len(df)} rows")
    logger.info(f"Available columns: {df.columns.tolist()}")
    
    # Check if we have height data
    if not ('height_m' in df.columns and 'height_cm' in df.columns):
        logger.warning("Height data columns missing. Analysis will be limited.")
    
    has_height_data = ('height_m' in df.columns and df['height_m'].notna().any() and 
                       'height_cm' in df.columns and df['height_cm'].notna().any())
    
    if not has_height_data:
        logger.warning("No valid height data found. Analysis will be limited.")
    
    # 1. Calculate statistics by position category
    position_stats_dict = {}
    
    if has_height_data and 'position_category' in df.columns and df['position_category'].notna().any():
        # Filter only rows with valid height data and position category
        valid_df = df.dropna(subset=['height_m', 'height_cm', 'position_category'])
        
        if len(valid_df) > 0:
            position_stats = valid_df.groupby('position_category')[['height_m', 'height_cm']].agg([
                'count', 'mean', 'median', 'min', 'max', 'std'
            ]).reset_index()
            
            # Convert the multi-level column structure to a simpler format
            for position in position_stats['position_category']:
                pos_data = position_stats[position_stats['position_category'] == position]
                position_stats_dict[position] = {
                    'count': int(pos_data[('height_m', 'count')].values[0]),
                    'mean_m': round(float(pos_data[('height_m', 'mean')].values[0]), 2),
                    'mean_cm': round(float(pos_data[('height_cm', 'mean')].values[0]), 1),
                    'median_m': round(float(pos_data[('height_m', 'median')].values[0]), 2),
                    'median_cm': round(float(pos_data[('height_cm', 'median')].values[0]), 1),
                    'min_m': round(float(pos_data[('height_m', 'min')].values[0]), 2),
                    'min_cm': round(float(pos_data[('height_cm', 'min')].values[0]), 1),
                    'max_m': round(float(pos_data[('height_m', 'max')].values[0]), 2),
                    'max_cm': round(float(pos_data[('height_cm', 'max')].values[0]), 1),
                    'std_m': round(float(pos_data[('height_m', 'std')].values[0]), 3),
                    'std_cm': round(float(pos_data[('height_cm', 'std')].values[0]), 1)
                }
        else:
            logger.warning("No valid data for position statistics")
    else:
        logger.warning("Missing required columns for position statistics")
    
    # 2. Generate ASCII-based histogram for player heights distribution
    ascii_histogram = []
    ascii_histogram.append("Height Distribution Histogram (cm)")
    ascii_histogram.append("=" * 50)
    
    bin_edges = np.array([0])
    hist_data = np.array([0])
    
    if has_height_data:
        # Get only valid height data for histogram
        valid_heights = df['height_cm'].dropna()
        
        if len(valid_heights) > 0:
            # Calculate histogram data
            hist_data, bin_edges = np.histogram(valid_heights, bins=10)
            max_bar_width = 50  # Maximum width of the ASCII bar
            
            # Create the ASCII histogram
            if len(hist_data) > 0 and np.max(hist_data) > 0:  # Check that we have valid data
                for i in range(len(hist_data)):
                    # Handle potential divide by zero or NaN
                    if np.isnan(hist_data[i]) or np.max(hist_data) == 0:
                        bar_width = 0
                    else:
                        bar_width = int((hist_data[i] / np.max(hist_data)) * max_bar_width)
                    
                    bar = '#' * bar_width
                    bin_range = f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}cm"
                    ascii_histogram.append(f"{bin_range.ljust(15)} | {bar} ({hist_data[i]})")
            else:
                ascii_histogram.append("No valid height data for histogram")
        else:
            ascii_histogram.append("No valid height data for histogram")
    else:
        ascii_histogram.append("Height data missing - cannot generate histogram")
    
    ascii_histogram_str = '\n'.join(ascii_histogram)
    logger.info(f"Generated ASCII histogram:\n{ascii_histogram_str}")
    
    # 3. Create text-based box plots for height differences across position categories
    box_plots = []
    box_plots.append("Height Box Plots by Position Category (cm)")
    box_plots.append("=" * 60)
    
    if has_height_data and 'position_category' in df.columns and df['position_category'].notna().any():
        # Check each position category
        for position in df['position_category'].dropna().unique():
            heights = df[df['position_category'] == position]['height_cm'].dropna()
            
            if len(heights) < 3:  # Need at least a few data points for a meaningful box plot
                box_plots.append(f"{position.ljust(25)} | Insufficient data for box plot (n={len(heights)})")
                continue
                
            q1, median, q3 = heights.quantile([0.25, 0.5, 0.75])
            iqr = q3 - q1
            lower_whisker = max(heights.min(), q1 - 1.5 * iqr)
            upper_whisker = min(heights.max(), q3 + 1.5 * iqr)
            
            # Ensure we don't divide by zero with a very narrow range
            plot_range = upper_whisker - lower_whisker
            if plot_range <= 0:
                plot_range = 1  # Set a minimum range to avoid division by zero
            
            # Scale the box plot to fit within a reasonable width
            scale = 40 / plot_range
            
            # Ensure our calculations don't result in negative indices
            left_pad = max(0, int((lower_whisker - heights.min()) * scale))
            box_start = max(0, int((q1 - heights.min()) * scale))
            median_pos = max(0, int((median - heights.min()) * scale))
            box_end = max(0, int((q3 - heights.min()) * scale))
            right_pad = max(0, int((heights.max() - upper_whisker) * scale))
            
            # Create the box plot line (with defensive coding)
            line = ' ' * left_pad + '|' + '-' * max(0, (box_start - left_pad))
            line += '|' + '-' * max(0, (median_pos - box_start)) + '+' + '-' * max(0, (box_end - median_pos)) + '|'
            line += '-' * right_pad + '|'
            
            # Add position label and stats
            box_plots.append(f"{position.ljust(25)} | {heights.min():.1f}cm {'-' * 10} {median:.1f}cm {'-' * 10} {heights.max():.1f}cm")
            box_plots.append(f"{' ' * 25} | {line}")
            box_plots.append(f"{' ' * 25} | n={len(heights)}, mean={heights.mean():.1f}cm, std={heights.std():.1f}cm")
            box_plots.append("-" * 60)
    else:
        box_plots.append("Position or height data missing - cannot generate box plots")
    
    box_plots_str = '\n'.join(box_plots)
    logger.info(f"Generated box plots:\n{box_plots_str}")
    
    # 4. Identify top 5 tallest and shortest players
    tallest_players = []
    shortest_players = []
    
    if has_height_data and 'player_name' in df.columns:
        # Get valid player data (players with name and height)
        valid_players = df.dropna(subset=['player_name', 'height_m']).copy()
        
        if len(valid_players) > 0:
            # Get top 5 tallest
            tallest = valid_players.sort_values('height_m', ascending=False).head(min(5, len(valid_players)))
            tallest_players = tallest[['player_name', 'height_m', 'height_cm', 'position_category']].to_dict('records')
            
            # Get top 5 shortest
            shortest = valid_players.sort_values('height_m').head(min(5, len(valid_players)))
            shortest_players = shortest[['player_name', 'height_m', 'height_cm', 'position_category']].to_dict('records')
            
            # Log the top players
            logger.info("Top 5 tallest players:")
            for idx, player in enumerate(tallest_players, 1):
                logger.info(f"{idx}. {player['player_name']} - {player['height_m']}m ({player['height_cm']}cm)")
            
            logger.info("Top 5 shortest players:")
            for idx, player in enumerate(shortest_players, 1):
                logger.info(f"{idx}. {player['player_name']} - {player['height_m']}m ({player['height_cm']}cm)")
        else:
            logger.warning("No valid player data for top/shortest players list")
    else:
        logger.warning("Missing player_name or height_m columns")
    
    # 5. Generate a comprehensive JSON report with all analysis results
    # Safely calculate basic statistics
    height_stats = {}
    if has_height_data:
        valid_heights_m = df['height_m'].dropna()
        valid_heights_cm = df['height_cm'].dropna()
        
        if len(valid_heights_m) > 0 and len(valid_heights_cm) > 0:
            height_stats = {
                'mean_m': round(float(valid_heights_m.mean()), 2),
                'mean_cm': round(float(valid_heights_cm.mean()), 1),
                'median_m': round(float(valid_heights_m.median()), 2),
                'median_cm': round(float(valid_heights_cm.median()), 1),
                'min_m': round(float(valid_heights_m.min()), 2),
                'min_cm': round(float(valid_heights_cm.min()), 1),
                'max_m': round(float(valid_heights_m.max()), 2),
                'max_cm': round(float(valid_heights_cm.max()), 1),
                'std_m': round(float(valid_heights_m.std()), 3),
                'std_cm': round(float(valid_heights_cm.std()), 1)
            }
    
    # Create a complete analysis report
    analysis_report = {
        'total_players': int(len(df)),
        'players_with_height_data': int(df['height_m'].notna().sum()),
        'height_stats': height_stats,
        'position_stats': position_stats_dict,
        'tallest_players': tallest_players,
        'shortest_players': shortest_players,
        'histogram_data': {
            'bin_edges': bin_edges.tolist(),
            'frequency': hist_data.tolist()
        },
        'ascii_visualizations': {
            'histogram': ascii_histogram,
            'box_plots': box_plots
        },
        'timestamp': datetime.now().isoformat()
    }
    
    # Convert any numpy types to native Python types
    analysis_report = convert_numpy_types(analysis_report)
    
    # 6. Upload analysis results to MinIO
    analysis_key = f'nba/analysis/nba_height_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    logger.info(f"Uploading analysis results to MinIO: s3://{BUCKET_NAME}/{analysis_key}")
    s3_hook.load_string(
        string_data=json.dumps(analysis_report, indent=2),
        key=analysis_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    
    # Store the analysis data path for dashboard generation
    context['ti'].xcom_push(key='analysis_data_s3_path', value=f"{BUCKET_NAME}/{analysis_key}")
    context['ti'].xcom_push(key='ascii_histogram', value=ascii_histogram_str)
    context['ti'].xcom_push(key='box_plots', value=box_plots_str)
    
    logger.info("NBA data analysis and visualization completed successfully")
    return analysis_key

def generate_dashboard(**context):
    """
    Generate an HTML dashboard with text-based visualizations and tables:
    - Format data in readable tables
    - Apply modern styling with responsive design
    - Upload the dashboard HTML to MinIO
    """
    logger.info("Starting NBA dashboard generation")
    
    # Get data from previous tasks via XCom
    ti = context['ti']
    analysis_data_s3_path = ti.xcom_pull(task_ids='analyze_and_visualize_data', key='analysis_data_s3_path')
    ascii_histogram = ti.xcom_pull(task_ids='analyze_and_visualize_data', key='ascii_histogram')
    box_plots = ti.xcom_pull(task_ids='analyze_and_visualize_data', key='box_plots')
    
    if not analysis_data_s3_path:
        raise ValueError("Analysis data path not found in XCom")
    
    # Download analysis data from MinIO
    bucket_name, key = analysis_data_s3_path.split('/', 1)
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    analysis_data_str = s3_hook.read_key(key=key, bucket_name=bucket_name)
    analysis_data = json.loads(analysis_data_str)
    
    # Generate HTML dashboard
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>NBA Player Height Analysis Dashboard</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f8fa;
            }}
            h1, h2, h3 {{
                color: #2c3e50;
            }}
            .dashboard-header {{
                background-color: #2c3e50;
                color: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                text-align: center;
            }}
            .stats-container {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }}
            .stat-card {{
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }}
            .visualization {{
                background-color: white;
                border-radius: 8px;
                padding: 20px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                overflow-x: auto;
            }}
            .data-table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }}
            .data-table th {{
                background-color: #2c3e50;
                color: white;
                padding: 12px;
                text-align: left;
            }}
            .data-table tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            .data-table td {{
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}
            pre {{
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 5px;
                overflow-x: auto;
                font-family: 'Courier New', Courier, monospace;
                font-size: 14px;
                line-height: 1.5;
            }}
            .message {{
                background-color: #f8d7da;
                color: #721c24;
                padding: 10px;
                border-radius: 5px;
                margin-bottom: 15px;
            }}
            @media (max-width: 768px) {{
                .stats-container {{
                    grid-template-columns: 1fr;
                }}
            }}
            .footer {{
                text-align: center;
                margin-top: 40px;
                color: #7f8c8d;
                font-size: 14px;
            }}
        </style>
    </head>
    <body>
        <div class="dashboard-header">
            <h1>NBA Player Height Analysis Dashboard</h1>
            <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="stat-card">
            <h2>Overview</h2>
            <p>Total players analyzed: <strong>{analysis_data.get('total_players', 'N/A')}</strong></p>
            <p>Players with height data: <strong>{analysis_data.get('players_with_height_data', 'N/A')}</strong></p>
    """
    
    # Add height stats if available
    if 'height_stats' in analysis_data and analysis_data['height_stats']:
        height_stats = analysis_data['height_stats']
        html_content += f"""
            <p>Average height: <strong>{height_stats.get('mean_m', 'N/A')}m ({height_stats.get('mean_cm', 'N/A')}cm)</strong></p>
        """
        
        # Add tallest/shortest player info if available
        if 'tallest_players' in analysis_data and analysis_data['tallest_players']:
            tallest = analysis_data['tallest_players'][0]
            html_content += f"""
            <p>Tallest player: <strong>{tallest.get('player_name', 'N/A')} - {tallest.get('height_m', 'N/A')}m</strong></p>
            """
        else:
            html_content += """
            <p>Tallest player: <strong>Data not available</strong></p>
            """
            
        if 'shortest_players' in analysis_data and analysis_data['shortest_players']:
            shortest = analysis_data['shortest_players'][0]
            html_content += f"""
            <p>Shortest player: <strong>{shortest.get('player_name', 'N/A')} - {shortest.get('height_m', 'N/A')}m</strong></p>
            """
        else:
            html_content += """
            <p>Shortest player: <strong>Data not available</strong></p>
            """
    else:
        html_content += """
            <p class="message">Height statistics not available</p>
        """
    
    html_content += """
        </div>
    """
    
    # Add position categories if available
    if 'position_stats' in analysis_data and analysis_data['position_stats']:
        html_content += """
        <h2>Position Categories</h2>
        <div class="stats-container">
        """
        
        # Add position category stats
        for position, stats in analysis_data['position_stats'].items():
            html_content += f"""
                <div class="stat-card">
                    <h3>{position}</h3>
                    <p>Players: <strong>{stats.get('count', 'N/A')}</strong></p>
                    <p>Average height: <strong>{stats.get('mean_m', 'N/A')}m ({stats.get('mean_cm', 'N/A')}cm)</strong></p>
                    <p>Height range: <strong>{stats.get('min_m', 'N/A')}m - {stats.get('max_m', 'N/A')}m</strong></p>
                </div>
            """
        
        html_content += """
        </div>
        """
    else:
        html_content += """
        <div class="stat-card">
            <h2>Position Categories</h2>
            <p class="message">Position category data not available</p>
        </div>
        """
    
    # Add histogram
    html_content += """
        <div class="visualization">
            <h2>Height Distribution</h2>
            <pre>
    """
    
    # Add ASCII histogram
    html_content += ascii_histogram if ascii_histogram else "Height distribution data not available"
    
    html_content += """
            </pre>
        </div>
        
        <div class="visualization">
            <h2>Height by Position (Box Plots)</h2>
            <pre>
    """
    
    # Add box plots
    html_content += box_plots if box_plots else "Box plot data not available"
    
    html_content += """
            </pre>
        </div>
    """
    
    # Add tallest players table if available
    if 'tallest_players' in analysis_data and analysis_data['tallest_players']:
        html_content += """
        <h2>Top 5 Tallest Players</h2>
        <div class="visualization">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Player Name</th>
                        <th>Height (m)</th>
                        <th>Height (cm)</th>
                        <th>Position Category</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add tallest players
        for idx, player in enumerate(analysis_data['tallest_players'], 1):
            position = player.get('position_category', 'N/A')
            html_content += f"""
                        <tr>
                            <td>{idx}</td>
                            <td>{player.get('player_name', 'N/A')}</td>
                            <td>{player.get('height_m', 'N/A')}</td>
                            <td>{player.get('height_cm', 'N/A')}</td>
                            <td>{position}</td>
                        </tr>
            """
        
        html_content += """
                </tbody>
            </table>
        </div>
        """
    else:
        html_content += """
        <div class="visualization">
            <h2>Top 5 Tallest Players</h2>
            <p class="message">Tallest player data not available</p>
        </div>
        """
    
    # Add shortest players table if available
    if 'shortest_players' in analysis_data and analysis_data['shortest_players']:
        html_content += """
        <h2>Top 5 Shortest Players</h2>
        <div class="visualization">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Rank</th>
                        <th>Player Name</th>
                        <th>Height (m)</th>
                        <th>Height (cm)</th>
                        <th>Position Category</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add shortest players
        for idx, player in enumerate(analysis_data['shortest_players'], 1):
            position = player.get('position_category', 'N/A')
            html_content += f"""
                        <tr>
                            <td>{idx}</td>
                            <td>{player.get('player_name', 'N/A')}</td>
                            <td>{player.get('height_m', 'N/A')}</td>
                            <td>{player.get('height_cm', 'N/A')}</td>
                            <td>{position}</td>
                        </tr>
            """
        
        html_content += """
                </tbody>
            </table>
        </div>
        """
    else:
        html_content += """
        <div class="visualization">
            <h2>Top 5 Shortest Players</h2>
            <p class="message">Shortest player data not available</p>
        </div>
        """
    
    html_content += """
        <div class="footer">
            <p>NBA Player Height Analysis - Data Engineering Pipeline Workshop</p>
        </div>
    </body>
    </html>
    """
    
    # Upload dashboard HTML to MinIO
    dashboard_key = f'nba/dashboard/nba_height_dashboard_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html'
    
    logger.info(f"Uploading dashboard HTML to MinIO: s3://{BUCKET_NAME}/{dashboard_key}")
    s3_hook.load_string(
        string_data=html_content,
        key=dashboard_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    
    # Store the dashboard URL
    dashboard_url = f"s3://{BUCKET_NAME}/{dashboard_key}"
    context['ti'].xcom_push(key='dashboard_url', value=dashboard_url)
    
    logger.info(f"NBA dashboard generation completed successfully. Dashboard available at: {dashboard_url}")
    return dashboard_key

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

# Task for data analysis and visualization
analyze_task = PythonOperator(
    task_id='analyze_and_visualize_data',
    python_callable=analyze_and_visualize_data,
    provide_context=True,
    dag=dag,
)

# Task for dashboard generation
dashboard_task = PythonOperator(
    task_id='generate_dashboard',
    python_callable=generate_dashboard,
    provide_context=True,
    dag=dag,
)

# Define task dependencies
extract_task >> clean_task >> analyze_task >> dashboard_task 