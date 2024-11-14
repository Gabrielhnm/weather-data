from airflow.models import Variable
from airflow.decorators import dag, task
import pendulum
from datetime import timedelta, datetime, timezone
import pandas as pd
import requests
import tempfile
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from zoneinfo import ZoneInfo
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

API_KEY=Variable.get("api_key")
lat=Variable.get("lat")
lon=Variable.get("lon")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='weather_pipeline', 
    description="Weather data using Taskflow API",
    default_args = default_args,
    start_date=pendulum.datetime(2024,11,12,13,tz="America/Sao_Paulo"),
    schedule_interval='*/15 * * * *') 

def weather_etl():

    @task()
    def fetch_data():
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric&lang=pt_br"

        response = requests.get(url)
        data = response.json()
        return data
    
    @task()
    def transform_data(data):
        df_dados = {
            'lon': [data['coord']['lon']],
            'lat': [data['coord']['lat']],
            'weather_description': [data['weather'][0]['description']],
            'temp': [data['main']['temp']],
            'feels_like': [data['main']['feels_like']],
            'temp_min': [data['main']['temp_min']],
            'temp_max': [data['main']['temp_max']],
            'pressure': [data['main']['pressure']],
            'humidity': [data['main']['humidity']],
            'wind_speed': [data['wind']['speed']],
            'wind_deg': [data['wind']['deg']],
            'clouds_all': [data['clouds']['all']],
            'dt': [datetime.fromtimestamp(data['dt']).astimezone(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')],
            'sys_country': [data['sys']['country']],
            'sunrise': [datetime.fromtimestamp(data['sys']['sunrise']).astimezone(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')],
            'sunset': [datetime.fromtimestamp(data['sys']['sunset']).astimezone(ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d %H:%M:%S')],
            'city_name': [data['name']],
        }
    
        df = pd.DataFrame(df_dados)
        return df
    
    @task()
    def upload_to_gcs(df):
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', encoding='latin1', suffix='.csv') as temp_file:
            # Save DataFrame to temporary file with UTF-8 encoding
            df.to_csv(temp_file.name, index=False, encoding='latin1')
            
            # Use GCSHook instead of direct client
            gcs_hook = GCSHook(gcp_conn_id='gc_connect')  # Use your connection ID
            
            # Upload file
            bucket_name = "weather_testegb"
            current_time = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%Y%m%d_%H%M%S")
            blob_name = f"weather_data/weather_{current_time}.csv"
            
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=blob_name,
                filename=temp_file.name
            )
            print(temp_file)
            print(f"File uploaded to gs://{bucket_name}/{blob_name}")
        return blob_name

    @task()
    def load_to_bigquery(blob_name):
        # Get BigQuery Hook using the connection ID
        bq_hook = BigQueryHook(gcp_conn_id='gc_connect')
        client = bq_hook.get_client()
        
        table_id = 'weather.clima_nilo'
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        
        uri = f'gs://weather_testegb/{blob_name}'
        
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config
        )
        
        # Wait for the job to complete
        load_job.result()
        
        # Get the table and print the total rows
        table = client.get_table(table_id)
        print(f"Loaded {load_job.output_rows} rows into {table_id}")
        print(f"Total rows in table: {table.num_rows}")


    dados = fetch_data()
    df = transform_data(dados)
    blob_name = upload_to_gcs(df)
    load_to_bigquery(blob_name)

weather_pipeline = weather_etl()


