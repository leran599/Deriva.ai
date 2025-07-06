from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.dates import days_ago
from airflow.models.param import Param

import gzip
import pandas as pd
import os
import io
import json
from datetime import datetime, timedelta

URL_PATH = "/pub/data/ghcn/daily/by_year/"
YEARS = [2020, 2021, 2022, 2023]
OUT_DIR = "/opt/airflow/output"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'execution_timeout': timedelta(minutes=10)  # 10 minute timeout for each task
}

with DAG(
    dag_id="weather_data_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Ingest NOAA weather data, transform, and emit JSON summaries.",
) as dag:

    @task
    def list_files():
        hook = HttpHook(http_conn_id='noaa_http', method='GET')
        response = hook.run("/pub/data/ghcn/daily/by_year/")
        print(f"Response status: {response.status_code}")
        print(f"Response length: {len(response.text)}")
        print(f"Response preview: {response.text[:500]}")
        
        # Parse HTML to extract .csv.gz filenames
        import re
        pattern = r'href="(\d{4}\.csv\.gz)"'
        links = re.findall(pattern, response.text)
        print(f"Raw HTML sample: {response.text[response.text.find('2020.csv.gz')-20:response.text.find('2020.csv.gz')+20]}")
        print(f"All links found: {links}")
        print(f"Looking for years: {YEARS}")
        
        filtered = [f"{year}.csv.gz" for year in YEARS if f"{year}.csv.gz" in links]
        print(f"Filtered links: {filtered}")
        
        return filtered

    @task
    def download_and_process(file_name: str, **context):
        print(f"Starting download of {file_name}")
        
        # Use Airflow's HttpHook but with better error handling
        hook = HttpHook(method="GET", http_conn_id='noaa_http')
        
        # Get the connection to construct URL for logging
        conn = hook.get_connection(hook.http_conn_id)
        # Fix the URL construction - use the host directly if it already contains protocol
        if conn.host.startswith('http'):
            base_url = conn.host
        else:
            base_url = f"{conn.schema or 'http'}://{conn.host}"

        full_url = base_url + URL_PATH + file_name
        print(f"Downloading from: {full_url}")
        
        # Use HttpHook with timeout but handle large files carefully
        try:
            r = hook.run(URL_PATH + file_name, 
                        extra_options={'timeout': 600})  # 10 minute timeout
            
            print(f"Download completed for {file_name}, size: {len(r.content)} bytes")
            buf = io.BytesIO(r.content)
            
            # Initialize variables for aggregation
            total_rows = 0
            unique_stations = set()
            all_dates = []
            element_counts = {}
            tmax_values = []
            tmin_values = []
            
            # Define the essential weather measurements we want to capture
            essential_measurements = ['TMAX', 'TMIN', 'PRCP']
            
            # Process in chunks of 10,000 rows
            chunk_size = 10000
            print(f"Processing {file_name} in chunks of {chunk_size} rows...")
            
            with gzip.open(buf, 'rt') as f:
                for chunk_num, chunk_df in enumerate(pd.read_csv(f, header=None, 
                                                                names=["station", "date", "element", "value", "mflag", "qflag", "sflag", "obs_time"],
                                                                chunksize=chunk_size)):
                    
                    total_rows += len(chunk_df)
                    unique_stations.update(chunk_df["station"].unique())
                    all_dates.extend(chunk_df["date"].unique())
                    
                    # Count only essential elements
                    chunk_elements = chunk_df["element"].value_counts()
                    for element, count in chunk_elements.items():
                        if element in essential_measurements:
                            element_counts[element] = element_counts.get(element, 0) + count
                    
                    # Collect temperature values
                    tmax_chunk = chunk_df[chunk_df["element"] == "TMAX"]["value"]
                    tmin_chunk = chunk_df[chunk_df["element"] == "TMIN"]["value"]
                    
                    if len(tmax_chunk) > 0:
                        tmax_values.extend(tmax_chunk.tolist())
                    if len(tmin_chunk) > 0:
                        tmin_values.extend(tmin_chunk.tolist())
                    
                    if chunk_num % 10 == 0:
                        print(f"Processed chunk {chunk_num + 1}, total rows so far: {total_rows}")
            
            print(f"Processing completed for {file_name}, total rows: {total_rows}")
            
        except Exception as e:
            print(f"Error downloading {file_name}: {str(e)}")
            raise

        # Calculate stats from chunked data
        unique_stations_count = len(unique_stations)
        
        # Convert dates to datetime for range calculation
        date_objects = [pd.to_datetime(str(date), format="%Y%m%d") for date in all_dates]
        min_date = min(date_objects).strftime("%Y-%m-%d")
        max_date = max(date_objects).strftime("%Y-%m-%d")

        # Calculate temperature averages
        tmax = (sum(tmax_values) / len(tmax_values)) / 10 if tmax_values else 0
        tmin = (sum(tmin_values) / len(tmin_values)) / 10 if tmin_values else 0

        result = {
            "file_name": file_name,
            "processed_at": datetime.utcnow().isoformat(),
            "total_records": total_rows,
            "unique_stations": unique_stations_count,
            "date_range": {"start": min_date, "end": max_date},
            "measurement_counts": element_counts,
            "temperature_stats": {"avg_max_c": tmax, "avg_min_c": tmin}
        }

        output_path = os.path.join(OUT_DIR, file_name.replace(".csv.gz", ".json"))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)

        return f"Written to {output_path}"

    file_list = list_files()
    _ = download_and_process.expand(file_name=file_list)