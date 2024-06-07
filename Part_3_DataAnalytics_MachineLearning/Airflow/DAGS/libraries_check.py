from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator


# Default arguments for the DAG
default_args = {
    'owner': 'Admin',
    'start_date': days_ago(1),  # Start the DAG from one day ago
}

# Define the DAG
with DAG(
    dag_id='check_library_availability',
    default_args=default_args,
    schedule_interval=None,  # Run manually
) as dag:
    def check_libraries():
        import importlib, sys
        print("Hello Admin")
        libraries = ["pandas", "os", "numpy", "matplotlib", "seaborn", "scipy", "vaderSentiment", "datetime", "json", "gzip", "pickle", "importlib"]
        for library in libraries:
            try:
                importlib.import_module(library)
                print(f"{library} is already installed (assuming compatible version for Python {sys.version})")
            except ModuleNotFoundError:
                print(f"{library} is not installed. You might need to rebuild the Airflow image with the libraries pre-installed.")


    # Task to check libraries
    check_libraries_task = PythonOperator(
        task_id='check_libraries_task',
        python_callable=check_libraries,
        provide_context=True,
    )

# Set the task flow
check_libraries_task
