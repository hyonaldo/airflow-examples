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
    'start_date': datetime(2019, 6, 6),
    'email': ['hyokyun.park@gmail.com', 'gyrbsdl18@naver.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    #'trigger_rule': u'all_success'
}

dag = DAG(
    'client-log-batch',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    description='DAG for classting summary jobs',
    schedule_interval="31 15 * * *",
)
### BASH setting ###
scripts_folder = "/home/hadoop/dev/spark-submit-examples"
default_spark_command = f"cd {scripts_folder}" + " && ./runner.sh {{ params.class_name }} '{{ tomorrow_ds_nodash }} 1'"

aws_s3 = "/usr/bin/aws s3 "
### TASKs ###
# check input
client_log_exists = BashOperator(
    task_id='client-log_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/logs_{{ tomorrow_ds_nodash }}",
    params={'s3_dir': 's3://classting-client-log'},
    dag=dag,
)
utc9_log_exists = BashOperator(
    task_id='utc9_log_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/logs_{{ tomorrow_ds_nodash }}/_SUCCESS",
    params={'s3_dir': 'classting-archive/utc9/classting-client-log'},
    dag=dag,
)
pay_rdb_exists = BashOperator(
    task_id='pay_rdb_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/{{ tomorrow_ds_nodash }}/pay.parquet",
    params={'s3_dir': 's3://classting-rds'},
    dag=dag,
)
active_user30d_exists = BashOperator(
    task_id='active_user30d_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/{{ tomorrow_ds_nodash }}/_SUCCESS",
    params={'s3_dir': 's3://classting-archive/active_log/user30d'},
    dag=dag,
)
active_user1d_exists = BashOperator(
    task_id='active_user1d_exists',
    bash_command=f"{aws_s3} ls " + " {{ params.s3_dir }}/{{ tomorrow_ds_nodash }}/_SUCCESS",
    params={'s3_dir': 's3://classting-archive/active_log/user1d'},
    dag=dag,
)

# spark jobs
UTC9ClientLog = BashOperator(
    task_id="UTC9ClientLog",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.UTC9ClientLog'},
    dag=dag,
)
ActiveLogUser = BashOperator(
    task_id="ActiveLogUser",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.ActiveLogUser'},
    dag=dag,
)
ActiveLogClass = BashOperator(
    task_id="ActiveLogClass",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.ActiveLogClass'},
    dag=dag,
)
RevenueMonthly = BashOperator(
    task_id="RevenueMonthly",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.RevenueMonthly'},
    dag=dag,
)
RevenueDaily = BashOperator(
    task_id="RevenueDaily",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.RevenueDaily'},
    dag=dag,
)
StatsAccumClass = BashOperator(
    task_id="StatsAccumClass",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.StatsAccumClass'},
    dag=dag,
)
StatsAccumUser = BashOperator(
    task_id="StatsAccumUser",
    bash_command=default_spark_command,
    params={'class_name': 'com.classting.StatsAccumUser'},
    dag=dag,
)

# branching tasks
client_log_exists >> UTC9ClientLog >> utc9_log_exists >> [ActiveLogUser,ActiveLogClass]
ActiveLogUser >> [active_user1d_exists,active_user30d_exists]
[pay_rdb_exists, active_user30d_exists] >> RevenueMonthly
[pay_rdb_exists, active_user1d_exists] >> RevenueDaily
utc9_log_exists >> [StatsAccumClass,StatsAccumUser]

if __name__ == "__main__":
    print(__file__)
    print(default_spark_command)
