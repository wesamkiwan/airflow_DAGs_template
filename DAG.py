from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


# Default arguments for the DAG
default_args = {
    'owner': 'wesam',
    'start_date': days_ago(0),
    'email': ['wesam@anymail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Apache Airflow Final Assignment',
)

# Create a task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command1='echo "extracting \'.tgz\' file.."',
    bash_command2='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)
# Create a task to extract data from csv file
extract_data_from_csv=BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d \',\' -f 1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)
# Create a task to extract data from tsv file
extract_data_from_tsv=BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5-7 tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)
# Create a task to extract data from fixed width file
extract_data_from_fixed_width=BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk "NF{print $(NF-1),$NF}"  OFS="\t"  payment-data.txt > fixed_width_data.csv',
    dag=dag,
)
# Create a task to consolidate data extracted from previous tasks
consolidate_data=BashOperator(
    task_id='consolidate_data',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)
# create task to Transform and load the data
transform_data=BashOperator(
    task_id='transform_data',
    bash_command='tr [:lower:] [:upper:] < extracted_data.csv > transformed_data.csv',
    dag=dag,
)
# Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data