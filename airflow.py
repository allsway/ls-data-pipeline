from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

'''
input arguments for downloading S3 data 
and Spark jobs
REMARK: 
Replace `srcDir` and `redditFile` as the full paths containing your PySpark scripts
and location of the Reddit file will be stored respectively 
'''
redditFile = os.getcwd() + '/data/RC-s3-2007-10'
python_files_dir = '/home/ls-data/'

## Define the DAG object
default_args = {
    'owner': 'insight-dan',
    'depends_on_past': False,
    'start_date': datetime(2016, 10, 15),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('s3_livestories_dag', default_args=default_args, schedule_interval=timedelta(1))

# Tasks that will be triggered
get_data = BashOperator(
    task_id='get-s3-data-task',
    bash_command='python ' + python_files_dir + 'get_s3_data.py config.txt' 
    dag=dag)

#task to compute number of unique authors
get_metrics = BashOperator(
    task_id='calculate-metrics',
    bash_command='spark-submit ' + python_files_dir + 'calculate_metrics.py '  ,
    dag=dag)
#Specify that this task depends on the downloadData task
get_metrics.set_upstream(get_data)

#task to compute average upvotes
averageUpvotes = BashOperator(
    task_id='average-upvotes',
    bash_command=sparkSubmit + ' ' + srcDir + 'pyspark/averageUpvote.py ' + redditFile,
    dag=dag)

averageUpvotes.set_upstream(downloadData)

