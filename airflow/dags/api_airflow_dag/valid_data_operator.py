from airflow.sdk import BaseOperator
from logger import get_logger
from file_exchange import read_file, delete_file

import logging
import jsonpickle


class ValidDataOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def execute(
            self,
            context,
            logger: logging.Logger = get_logger(__name__)
    ) -> None:
        ti = context['ti']
        candle_file_name = ti.xcom_pull(task_ids='get_candle_data_task', key='candle_file_name')

        candle_data = read_file(candle_file_name)

        candle = jsonpickle.decode(candle_data)

        try:
            _validate_candle(candle)
        except ValueError as e:
            delete_file(candle_file_name)

            raise e

        ti.xcom_push(key='candle_file_name', value=candle_file_name)


def _validate_candle(candle: dict[str, str | int]) -> None:
    for field in candle.keys():
        if candle[field] is None:
            raise ValueError('Невалидный формат данных свечи цен: цена не задана')

        if not isinstance(candle[field], float):
            raise ValueError('Невалидный формат данных свечи цен: цена не является дробным числом')

        if candle[field] <= 0:
            raise ValueError('Невалидный формат данных свечи цен: цена не положительна')
