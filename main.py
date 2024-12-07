from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import random
import time

# Налаштування DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'medal_count_dag',
    default_args=default_args,
    description='DAG for counting medals',
    schedule_interval=timedelta(days=1),
)

# SQL-запит для створення таблиці
create_table_sql = """
CREATE TABLE IF NOT EXISTS medal_count (
    id SERIAL PRIMARY KEY,
    medal_type VARCHAR(50),
    count INTEGER,
    created_at TIMESTAMP
);
"""

# Функція для вибору випадкового типу медалі та запуску відповідного завдання
def choose_and_count_medal():
    medal_type = random.choice(['Bronze', 'Silver', 'Gold'])
    if medal_type == 'Bronze':
        count_bronze()
    elif medal_type == 'Silver':
        count_silver()
    else:
        count_gold()

# Функції для підрахунку кількості записів для кожного типу медалі
def count_bronze():
    hook = MySqlHook(mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi')
    count_sql = "SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze';"
    count = hook.get_first(count_sql)[0]
    insert_sql = "INSERT INTO medal_count (medal_type, count, created_at) VALUES (%s, %s, %s);"
    hook.run(insert_sql, parameters=('Bronze', count, datetime.now()))

def count_silver():
    hook = MySqlHook(mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi')
    count_sql = "SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver';"
    count = hook.get_first(count_sql)[0]
    insert_sql = "INSERT INTO medal_count (medal_type, count, created_at) VALUES (%s, %s, %s);"
    hook.run(insert_sql, parameters=('Silver', count, datetime.now()))

def count_gold():
    hook = MySqlHook(mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi')
    count_sql = "SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold';"
    count = hook.get_first(count_sql)[0]
    insert_sql = "INSERT INTO medal_count (medal_type, count, created_at) VALUES (%s, %s, %s);"
    hook.run(insert_sql, parameters=('Gold', count, datetime.now()))

# Функція для затримки виконання
def wait():
    time.sleep(35)

# Функція для перевірки віку останнього запису
def check_recent_record():
    hook = MySqlHook(mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi')
    check_sql = "SELECT MAX(created_at) FROM medal_count;"
    latest_record_time = hook.get_first(check_sql)[0]
    if latest_record_time:
        time_diff = datetime.now() - latest_record_time
        return time_diff.total_seconds() <= 30
    return False

# Завдання DAG
start_task = EmptyOperator(task_id='start', dag=dag)

create_table_task = MySqlOperator(
    task_id='create_table',
    sql=create_table_sql,
    mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi',
    dag=dag,
)

choose_and_count_medal_task = PythonOperator(
    task_id='choose_and_count_medal',
    python_callable=choose_and_count_medal,
    dag=dag,
)

wait_task = PythonOperator(
    task_id='wait',
    python_callable=wait,
    dag=dag,
)

check_recent_record_task = PythonSensor(
    task_id='check_recent_record',
    python_callable=check_recent_record,
    timeout=60,
    poke_interval=5,
    dag=dag,
)

# Порядок завдань
start_task >> create_table_task >> choose_and_count_medal_task
choose_and_count_medal_task >> wait_task >> check_recent_record_task
