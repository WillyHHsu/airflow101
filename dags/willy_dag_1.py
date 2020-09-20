from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import random 

args = {
	
	'owner': 'villy'
	,'start_date': days_ago(1)
}

dag = DAG(dag_id = 'willy-dag_1'
	,default_args = args
	,schedule_interval = None)

def run_this(**context):
	received_value = context['ti'].xco,_push(key='random_value')
	print(str(received_value))

def always_fail(**context):
	if random.random()>0.4:
		raise Exception('Error')
	else:
		print('ok')

def push_to_xcom(**context):
	random_value = random.random()
	context['ti'].xco,_push(key='random_value', value=random_value)
	print('Done')


with dag:
	run_this_task = PythonOperator(
		task_id = 'run_this_id'
		,python_callable = always_fail
		,provide_context = True
		,retries = 10
		,retry_delay = timedelta(seconds = 1)
	)


	run_this_task2 = PythonOperator(
		task_id = 'run_this_id_2'
		,python_callable = run_this
		,provide_context = True
	)	

	run_this_task3 = PythonOperator(
		task_id = 'run_this_id_3'
		,python_callable = run_this
		,provide_context = True
	)	

	run_this_task >> run_this_task2