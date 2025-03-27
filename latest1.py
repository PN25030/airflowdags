from airflow import DAG
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateScanOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago
import yaml

PROJECT_ID = "your-project-id"
LOCATION = "your-location"
ZONE_ID = "your-zone-id"
ENTITY_ID = "projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

SCAN_ID = "auto-dq-scan"
GCS_BUCKET_NAME = "your-bucket-name"
GCS_OBJECT_NAME = "path/to/dq_rules.yaml"
LOCAL_FILE_PATH = "/tmp/dq_rules.yaml"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

def load_yaml_to_dict(file_path):
    """Reads YAML file from local path and returns a dictionary."""
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

with DAG(
    "dataplex_auto_dq_scan_with_yaml",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Step 1: Download YAML from GCS to local
    download_yaml = GCSToLocalFilesystemOperator(
        task_id="download_dq_yaml",
        bucket=GCS_BUCKET_NAME,
        object_name=GCS_OBJECT_NAME,
        filename=LOCAL_FILE_PATH,
    )

    # Step 2: Load the YAML and create the scan
    def create_scan_body():
        dq_rules = load_yaml_to_dict(LOCAL_FILE_PATH)
        return {
            "description": "Automated Data Quality Scan using YAML",
            "displayName": "AutoDQScanWithYAML",
            "data": {
                "entity": ENTITY_ID.format(
                    project_id=PROJECT_ID, dataset_id="your_dataset", table_id="your_table"
                ),
            },
            "scanSpec": {
                "dataQualitySpec": {
                    "rules": dq_rules,
                    "samplingPercent": 50
                }
            }
        }

    create_scan = DataplexCreateOrUpdateScanOperator(
        task_id="create_auto_dq_scan",
        project_id=PROJECT_ID,
        region=LOCATION,
        scan_id=SCAN_ID,
        body=create_scan_body(),
        validate_only=False,
    )

    download_yaml >> create_scan
