from airflow import DAG
from api_airflow_dag.prep_data_operator import PrepDataOperator
from api_airflow_dag.valid_data_operator import ValidDataOperator
from api_airflow_dag.get_candle_data_operator import GetCandleDataOperator
from api_airflow_dag.save_db_operator import SaveDBOperator
from datetime import datetime
from helpers import DEFAULT_ARGS
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


def _choose_branch():
    return


with DAG(
    dag_id='api_dag',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 11, 20),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:
    trigger_training = TriggerDagRunOperator(
        task_id="trigger_training",
        trigger_dag_id="train_dag",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    get_data_task = GetCandleDataOperator(task_id='get_candle_data_task', stock_name='AAPL')
    valid_data_task = ValidDataOperator(task_id='valid_data_task')
    prep_data_task = PrepDataOperator(task_id='prep_data_task')
    save_db_task = SaveDBOperator(task_id='save_db_task')

    get_data_task >> valid_data_task >> prep_data_task >> save_db_task >> trigger_training
