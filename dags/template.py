from datetime import datetime, date, timedelta

import airflow
from airflow.configuration import conf
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    return 'Whatever you return gets printed in the logs'

### DAG setting ###
default_args = {
    'owner': 'stan',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 6, 15, 31),
    'email': ['hyokyun.park@gmail.com', 'gyrbsdl18@naver.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    #'trigger_rule': u'all_success'
}

dag = DAG(
    'template',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    description='DAG for classting summary jobs',
    schedule_interval="31 16 * * *",
)
### BASH setting ###
scripts_folder = "/home/hadoop/dev/spark-submit-examples"
default_spark_command = f"cd {scripts_folder}" + " && ./runner.sh {{ params.class_name }} '{{ ds_nodash }} 1'"

aws_s3 = "/usr/bin/aws s3 "
### TASKs ###
log_exists = BashOperator(
    task_id='utc9_log_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/logs_{{ ds_nodash }}/_SUCCESS",
    params={'s3_dir': 'classting-archive/utc9/classting-client-log'},
    dag=dag,
)
#UTC9ClientLog = BashOperator(
#    task_id="UTC9ClientLog",
#    bash_command=default_spark_command,
#    params={'class_name': 'com.classting.UTC9ClientLog'},
#    dag=dag,
#)

show_me_the_context = PythonOperator(
    task_id='show_me_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

# branching
log_exists >> show_me_the_context

if __name__ == "__main__":
    print(__file__)
