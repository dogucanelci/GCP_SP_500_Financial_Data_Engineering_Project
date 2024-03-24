import os
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

#from cosmos.config import ProfileConfig, ProjectConfig
#from pathlib import Path
#from cosmos.airflow.task_group import DbtTaskGroup
#from include.dbt.cosmos_config import DBT_CONFIG,DBT_PROJECT_CONFIG
#from cosmos.constants import LoadMode
#from cosmos.config import ProjectConfig, RenderConfig


# Python dosyasını çalıştıracak işlem
def extract_raw_data():
    # Dosyanın bulunduğu dizin
    DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))
    print(DAG_FOLDER)
    file_path = os.path.join(DAG_FOLDER, "dag_test1.py")
    # Dosyayı çalıştırma
    exec(open(file_path).read())

with DAG(dag_id='SP500',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    extract_raw_data = PythonOperator(
    task_id='extract_raw_data',
    python_callable=extract_raw_data,
    dag=dag,
    )


    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='./out.csv',
        dst='raw/raw_data.csv',
        bucket='raw_ingested_data',
        gcp_conn_id='google_cloud_storage_default',
        mime_type='text/csv',
        dag=dag,
    )

    create_SP500_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_SP500_dataset',
        dataset_id="stock_market_project4",
        gcp_conn_id="bigquery_default",
    )

#    transform_model = DbtTaskGroup(
#            group_id='transform_model',
#            project_config=DBT_PROJECT_CONFIG,
#            profile_config=DBT_CONFIG,
#            render_config=RenderConfig(
#                load_method=LoadMode.DBT_LS,
#                select=['path:models/transform']
#            )
#        )
    

#    transform_report = DbtTaskGroup(
#            group_id='transform_report',
#            project_config=DBT_PROJECT_CONFIG,
#            profile_config=DBT_CONFIG,
#            render_config=RenderConfig(
#                load_method=LoadMode.DBT_LS,
#                select=['path:models/report']
#            )
#        )
#
    projectid = 'gcp-data-engin-project-no4'
    schema_name = 'stock_market_project4'
    table_name = 'raw_sp500_companies'
    schema = [
        {"name": "Stock_Code", "type": "STRING"},
        {"name": "Quarter_Date", "type": "STRING"},
        {"name": "Gross Profit", "type": "FLOAT"},
        {"name": "Cost Of Revenue", "type": "FLOAT"},
        {"name": "Total Revenue", "type": "FLOAT"},
        {"name": "Net Income", "type": "FLOAT"},
        {"name": "close_date", "type": "STRING"},
        {"name": "Price", "type": "FLOAT"},
        {"name": "Company Name", "type": "STRING"},
        {"name": "GICS Sector", "type": "STRING"},
        {"name": "GICS Sub Industry", "type": "STRING"},
        {"name": "Founded", "type": "STRING"},
        {"name": "State", "type": "STRING"},
        {"name": "Province", "type": "STRING"}]

    create_empty_table_bq = BigQueryCreateEmptyTableOperator(
    task_id='create_empty_table_bq',
        dataset_id='stock_market_project4',
        table_id='raw_sp500_companies',
        schema_fields=schema,
        gcp_conn_id="bigquery_default",
        google_cloud_storage_conn_id="gcp",)


    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
    	bucket='raw_ingested_data',
    	source_objects=['raw/raw_data.csv'],
    	destination_project_dataset_table=f"{projectid}.{schema_name}.{table_name}",
    	schema_fields=schema,
    	source_format="CSV",
    	field_delimiter=",",
    	max_bad_records="0",
        gcp_conn_id="bigquery_default",
        write_disposition="WRITE_TRUNCATE",
    )
#                'destinationTable': {
#                    'projectId': 'gcp-data-engin-project-no4',
#                    'datasetId': 'stock_market_project4',
#                    'tableId': 'dim_company'
#                },
    dim_company_stored_procedure = BigQueryInsertJobOperator(
        task_id='dim_company_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "dim_company`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

    dim_location_stored_procedure = BigQueryInsertJobOperator(
        task_id='dim_location_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "dim_location`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

    dim_datetime_stored_procedure = BigQueryInsertJobOperator(
        task_id='dim_datetime_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "dim_datetime`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

    dim_sector_stored_procedure = BigQueryInsertJobOperator(
        task_id='dim_sector_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "dim_sector`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

    fct_stock_info_stored_procedure = BigQueryInsertJobOperator(
        task_id='fct_stock_info_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "fct_stock_info`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

    ads_stock_info_stored_procedure = BigQueryInsertJobOperator(
        task_id='ads_stock_info_stored_procedure',
        configuration={
            'query': {
                "query": "CALL `" + 'gcp-data-engin-project-no4' + "." + 'stored_procedure_dataset' + "." + "ads_stock_info`();",
                'useLegacySql': False,
                'allowLargeResults': True,
            }
        },
        gcp_conn_id='bigquery_default',
        dag=dag
    )

extract_raw_data >> upload_csv_to_gcs >> create_SP500_dataset >> create_empty_table_bq >> gcs_to_bq >> \
    [dim_company_stored_procedure,dim_location_stored_procedure,dim_datetime_stored_procedure,dim_sector_stored_procedure, \
     fct_stock_info_stored_procedure] >> ads_stock_info_stored_procedure




#{"name": "Stock_Code", "type": "STRING"},
#{"name": "Quarter_Date", "type": "STRING"},
#{"name": "Gross Profit", "type": "FLOAT"},
#{"name": "Cost Of Revenue", "type": "FLOAT"},
#{"name": "Total Revenue", "type": "FLOAT"},
#{"name": "Net Income", "type": "FLOAT"},
#{"name": "close_date", "type": "STRING"},
#{"name": "Company Name", "type": "STRING"},
#{"name": "Price", "type": "FLOAT"},
#{"name": "GICS Sector", "type": "STRING"},
#{"name": "GICS Sub Industry", "type": "STRING"},
#{"name": "Founded", "type": "STRING"},
#{"name": "State", "type": "STRING"},
#{"name": "Province", "type": "STRING"}

