from airflow import DAG
from get_candle_data_operator import GetCandleDataOperator
from save_db_operator import SaveDBOperator


default_args = {
    'owner': 'admin',
    'depends_on_past': False,
}

with DAG(
    dag_id='api_dag',
    default_args=default_args,
    catchup=False,
) as dag:
    get_data_task = GetCandleDataOperator(task_id='get_candle_data_task')
    save_db_task = SaveDBOperator(task_id='save_db_task')

    get_data_task >> save_db_task
