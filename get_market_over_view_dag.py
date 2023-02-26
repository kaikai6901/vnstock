import airflow
from airflow.models import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
import datetime as dt
from datetime import timedelta
from airflow.operators.python import PythonOperator

import pendulum
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

pathProject = '/home/ubuntu/vnstock/'

with DAG(
    "get_market_overview",
    start_date=dt.datetime(2023, 1, 1, tzinfo=local_tz),
    schedule='5 12,15 * * 1-5'
) as dag:
    def ssh_operator(task_id, command):
        return SSHOperator(
            ssh_hook=SSHHook('aws_ssh'),
            task_id=task_id,
            command=command,
            do_xcom_push=False,
            trigger_rule='all_done'
        )
    process = ssh_operator('check_git', "scripts/check_git.sh")
    process = process >> ssh_operator('push_market_overview', "python3 " + pathProject + "/get_market_overview.py")
    process