from airflow.sdk import BaseOperator
from logger import get_logger
from typing import Any
from finnhub.exceptions import FinnhubAPIException, FinnhubRequestException
from file_exchange import save_file
from stocks_api import get_stocks_data

import logging
import jsonpickle


class GetCandleDataOperator(BaseOperator):
    def __init__(
            self,
            stock_name: str,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self._stock_name = stock_name

    def execute(
            self,
            context,
            logger: logging.Logger = get_logger(__name__),
    ) -> Any:
        logger.info('Считываем данные о цене акций {}...'.format(self._stock_name))

        try:
            response = get_stocks_data(self._stock_name)
        except (FinnhubAPIException, FinnhubRequestException) as e:
            logger.error('Ошибка считывания стоимости акции: {}'.format(e))

            return

        data = {
            'stock_name': self._stock_name,
            'current_price': response['c'],
            'highest_price_of_day': response['h'],
            'lowest_price_of_day': response['l'],
            'open_price_of_day': response['o'],
            'timestamp': response['t'],
        }

        file_name = save_file(self._stock_name, jsonpickle.encode(data))

        ti = context['ti']
        ti.xcom_push(key='candle_file_name', value=file_name)

        return data
