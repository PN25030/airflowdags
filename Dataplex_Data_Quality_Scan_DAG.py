import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateDataQualityScanOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'dataplex_dq_scan_creator',
    default_args=default_args,
    schedule_interval=None,  # Manually triggered or set as needed
    catchup=False,
) as dag:
    # Retrieve Airflow variables
    gcs_location = "gs://mybucket/folder"  # You can also use Variable.get() to fetch from Airflow variables
    lake_id = Variable.get("demo-lake")
    zone_id = Variable.get("demo-zone")
    asset_id = Variable.get("demo-curated-asset")

    # Task to list YAML files in the GCS bucket
    list_yaml_files = GCSListObjectsOperator(
        task_id='list_yaml_files',
        bucket=gcs_location.replace('gs://', '').split('/')[0],
        prefix=gcs_location.replace('gs://', '').split('/', 1)[1] + '/',
        delimiter='/',
        match_glob='*.yaml'
    )

    # Function to process YAML files and create/update Dataplex Data Quality scans
    def create_dq_scans(file_names, **context):
        from google.cloud import storage

        # Initialize GCS client
        storage_client = storage.Client()
        bucket_name = gcs_location.replace('gs://', '').split('/')[0]
        bucket = storage_client.bucket(bucket_name)

        processed_files = []
        for file_name in file_names:
            # Skip if file doesn't end with .yaml
            if not file_name.endswith('.yaml'):
                continue

            # Parse filename
            filename_parts = os.path.splitext(os.path.basename(file_name))[0].split('__')
            
            # Determine if it's a star_ file
            is_star_file = 'star_' in file_name

            # Ensure we have enough parts
            if len(filename_parts) < 3:
                print(f"Skipping invalid filename: {file_name}")
                continue

            # Extract details
            project_id = filename_parts[0]
            dataset = filename_parts[1]
            table_name = filename_parts[2]

            # Get schedule from filename if it's a star file
            schedule = filename_parts[3] if is_star_file and len(filename_parts) > 3 else None

            # Download YAML file content
            blob = bucket.blob(file_name)
            yaml_content = blob.download_as_text()

            # Create Dataplex Data Quality Scan
            create_scan = DataplexCreateOrUpdateDataQualityScanOperator(
                task_id=f'create_dq_scan_{os.path.splitext(os.path.basename(file_name))[0]}',
                project_id=project_id,
                region='us-central1',  # Adjust as needed
                lake_id=lake_id,
                zone_id=zone_id,
                asset_id=asset_id,
                body=yaml_content,
                data_quality_scan_id=f'{table_name}_dq_scan'
            )

            # Execute the scan creation
            create_scan.execute(context)

            # Optional: Print schedule if present
            if schedule:
                print(f"Schedule for {file_name}: {schedule}")

            processed_files.append(file_name)

        return processed_files

    # Create a PythonOperator to process the YAML files
    process_dq_scans = PythonOperator(
        task_id='process_dq_scans',
        python_callable=create_dq_scans,
        op_args=[list_yaml_files.output],
        provide_context=True
    )

    # Set task dependencies
    list_yaml_files >> process_dq_scans
