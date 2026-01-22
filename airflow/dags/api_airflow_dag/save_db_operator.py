from airflow.sdk import BaseOperator
from database import save_stock
from file_exchange import read_file, delete_file

import jsonpickle


class SaveDBOperator(BaseOperator):
    def execute(self, context):
        ti = context['ti']
        candle_file_name = ti.xcom_pull(task_ids='prep_data_task', key='prepared_candle_file_name')

        candle_data = read_file(candle_file_name)

        candle = jsonpickle.decode(candle_data)

        save_stock(candle_data=candle)

        delete_file(candle_file_name)

        return
