from airflow import DAG
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateScanOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "your-project-id"
LOCATION = "your-location"
ZONE_ID = "your-zone-id"
ENTITY_ID = "projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

SCAN_ID = "auto-dq-scan"
BODY = {
    "description": "Automated Data Quality Scan for BigQuery Table",
    "displayName": "AutoDQScan",
    "data": {
        "entity": ENTITY_ID.format(
            project_id=PROJECT_ID, dataset_id="your_dataset", table_id="your_table"
        ),
    },
    "scanSpec": {
        "dataQualitySpec": {
            "rules": [
                {
                    "rowConditionExpectation": {
                        "sqlExpression": "column_name IS NOT NULL"
                    },
                    "description": "Check for NULL values",
                    "dimension": "completeness"
                },
                {
                    "rangeExpectation": {
                        "column": "numeric_column",
                        "minValue": "0",
                        "maxValue": "100"
                    },
                    "description": "Ensure values are within range",
                    "dimension": "accuracy"
                }
            ],
            "samplingPercent": 50
        }
    }
}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    "dataplex_auto_dq_scan",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_scan = DataplexCreateOrUpdateScanOperator(
        task_id="create_auto_dq_scan",
        project_id=PROJECT_ID,
        region=LOCATION,
        scan_id=SCAN_ID,
        body=BODY,
        validate_only=False,
    )
