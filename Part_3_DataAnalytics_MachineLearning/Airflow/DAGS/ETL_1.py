from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from process_functions import dataframe_to_parquet_and_upload, get_json_data, get_rest_data, etl_g_2, category_creation_ds 
from process_functions import attributes_ds_creation, prices_ds_creation, coord_ds_create, reviews_datasets, preprocess_reviews
# Define DAG details
default_args = {
    'owner': 'Admin',
    'start_date': days_ago(1),  # Start the DAG from one day ago
}

def exec2():
        pass

with DAG(
    dag_id='ETL_1',
    schedule_interval=None, # Run the DAG manually
    catchup=False,
) as dag:

    etl_google_1 = PythonOperator(
        task_id='Get-data-from-Google-restaurants',
        python_callable= get_json_data,
        op_args=['metadata-sitios']
    )

    rest_data = PythonOperator(
        task_id = 'Preprocess-data-from-google-restaurants',
        python_callable = get_rest_data,
    )

    etl_google_restaurants = PythonOperator(
        task_id = 'Filter-data-from-google-restaurants',
        python_callable = etl_g_2,
    )

    upload_dfg_sites = PythonOperator(
        task_id = 'export-to-google-bucket',
        python_callable = dataframe_to_parquet_and_upload,
        op_args = ['/opt/airflow/datasets/OUT/dfg_sites.parquet', 'dfg_rest.parquet']
    )
    
    category_ds_creation = PythonOperator(
        task_id = 'Creation-of-category-ds',
        python_callable = category_creation_ds
    )

    upload_category_ds = PythonOperator(
        task_id = 'Upload-category_ds-to-cloud',
        python_callable = dataframe_to_parquet_and_upload,
        op_args = ['/opt/airflow/datasets/OUT/dfg_site_categories.parquet', 'dfg_site_categories.parquet'],
    )

    attributes_creation = PythonOperator(
        task_id = 'Creation-of-attributes-ds',
        python_callable = attributes_ds_creation
    )

    upload_attributes_ds = PythonOperator(
        task_id = 'Upload-attributes_ds-to-cloud',
        python_callable = dataframe_to_parquet_and_upload,
        op_args = ["/opt/airflow/datasets/OUT/dfg_attributes.parquet", 'dfg_attributes.parquet'],
    )

    prices_ds = PythonOperator(
        task_id = 'Create-prices-ds-creation',
        python_callable = prices_ds_creation
    )

    upload_prices_ds = PythonOperator(
        task_id = 'Upload-prices-ds',
        python_callable = dataframe_to_parquet_and_upload,
        op_args = ["/opt/airflow/datasets/OUT/dfg_rest_prices_by_zip.parquet", 'dfg_rest_prices_by_zip.parquet'],
    )

    create_coord = PythonOperator(
        task_id = 'create-coordinates-ds',
        python_callable = coord_ds_create
    )

    upload_coords = PythonOperator(
        task_id = 'Upload-coords-ds',
        python_callable = dataframe_to_parquet_and_upload,
        op_args = ['/opt/airflow/datasets/OUT/dfg_rest_coord.parquet', 'dfg_rest_coord.parquet'],
    )

    rev_dataset = PythonOperator(
        task_id = 'first-iteraton-on-reviews-datasets',
        python_callable = reviews_datasets,
    )

    prep_dataset = PythonOperator(
        task_id = 'Preprocess-reviews-datasets',
        python_callable = preprocess_reviews,
    )


etl_google_1 >> rest_data >> etl_google_restaurants >> upload_dfg_sites
upload_dfg_sites >> category_ds_creation >> upload_category_ds
upload_dfg_sites >> attributes_creation >> upload_attributes_ds
upload_dfg_sites >> prices_ds >> upload_prices_ds
upload_dfg_sites >> create_coord >> upload_coords
upload_dfg_sites >> rev_dataset >> prep_dataset