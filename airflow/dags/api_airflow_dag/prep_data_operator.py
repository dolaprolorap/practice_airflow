import logging
import jsonpickle

from airflow.sdk import BaseOperator

from domain import CandleDto
from file_exchange import read_file, delete_file, save_file
from logger import get_logger


class PrepDataOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(
            self,
            context,
            logger: logging.Logger = get_logger(__name__)
    ) -> None:
        ti = context['ti']
        candle_file_name = ti.xcom_pull(task_ids='valid_data_task', key='candle_file_name')

        candle_data = read_file(candle_file_name)
        candle = jsonpickle.decode(candle_data)
        delete_file(candle_file_name)

        prepared_candle = _prepare_data(candle)

        prepared_candle_file_name = save_file(
            file_name=prepared_candle.stock_name,
            content=jsonpickle.encode(prepared_candle)
        )

        ti.xcom_push(key='prepared_candle_file_name', value=prepared_candle_file_name)


def _prepare_data(candle: dict[str, str | int]) -> CandleDto:
    return CandleDto(
        stock_name=candle['stock_name'],
        current_price=candle['current_price'],
        open_price_of_day=candle['open_price_of_day'],
        timestamp=candle['timestamp'],
    )
