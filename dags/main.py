from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.sensors.sql import SqlSensor
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
    tags=["VVD"],
)

# SQL-запит для створення таблиці
create_table_sql = """
CREATE TABLE IF NOT EXISTS medal_count_vvd (
    id SERIAL PRIMARY KEY,
    medal_type VARCHAR(50),
    count INTEGER,
    created_at TIMESTAMP
);
"""

calculate_bronze_sql = """
INSERT INTO medal_count_vvd (medal_type, count, created_at)
VALUES ('Bronze', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Bronze'), NOW());
"""

calculate_silver_sql = """
INSERT INTO medal_count_vvd (medal_type, count, created_at)
VALUES ('Silver', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Silver'), NOW());
"""

calculate_gold_sql = """
INSERT INTO medal_count_vvd (medal_type, count, created_at)
VALUES ('Gold', (SELECT COUNT(*) FROM olympic_dataset.athlete_event_results WHERE medal = 'Gold'), NOW());
"""

# Функція для вибору випадкового типу медалі
def pick_medal(ti):
    medal_type = random.choice(['Bronze', 'Silver', 'Gold'])
    ti.xcom_push(key='medal_type', value=medal_type)
    return medal_type

# Розгалуження
def check_medal_type(ti):
    medal_type = ti.xcom_pull(key='medal_type', task_ids='pick_medal')
    if medal_type == 'Bronze':
        return 'calc_bronze_task'
    if medal_type == 'Silver':
        return 'calc_silver_task'
    if medal_type == 'Gold':
        return 'calc_gold_task'

def wait():
    time.sleep(5)

# Завдання DAG

create_table_task = MySqlOperator(
    task_id='create_table',
    sql=create_table_sql,
    mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi',
    dag=dag,
)

choose_medal_task = PythonOperator(
    task_id='pick_medal',
    python_callable=pick_medal,
    dag=dag,
)

check_medal_type_task = BranchPythonOperator(
    task_id='pick_medal_task',
    python_callable=check_medal_type,
    provide_context=True,
    dag=dag,
)

calculate_bronze_task = MySqlOperator(
    task_id='calc_bronze_task',
    sql=calculate_bronze_sql,
    mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi',
    dag=dag,
)

calculate_silver_task = MySqlOperator(
    task_id='calc_silver_task',
    sql=calculate_silver_sql,
    mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi',
    dag=dag,
)

calculate_gold_task = MySqlOperator(
    task_id='calc_gold_task',
    sql=calculate_gold_sql,
    mysql_conn_id='goit_mysql_db_volodymyrdubrovskyi',
    dag=dag,
)

generate_delay = PythonOperator(
    task_id='generate_delay',
    python_callable=wait,
    dag=dag,
    trigger_rule='one_success',
)

check_for_correctness = SqlSensor(
    task_id='check_recent_record',
    conn_id='goit_mysql_db_volodymyrdubrovskyi',
    sql="""
    SELECT COUNT(*) 
    FROM medal_count_vvd 
    WHERE created_at >= NOW() - INTERVAL 30 SECOND;
    """,
    timeout=60,
    poke_interval=5,
    mode='poke',
    dag=dag,
    trigger_rule='one_success',
)

# Порядок завдань
create_table_task >> choose_medal_task >> check_medal_type_task
check_medal_type_task >> [calculate_bronze_task, calculate_silver_task, calculate_gold_task]
[calculate_bronze_task, calculate_silver_task, calculate_gold_task] >> generate_delay
generate_delay >> check_for_correctness
