from helpers.airflow_variables import AirflowVariables
from airflow.models import Variable
from finnhub.exceptions import FinnhubAPIException, FinnhubRequestException

import finnhub
import random


_finnhub_token_name = AirflowVariables.FINNHUB_TOKEN_NAME.value

_finnhub_client = finnhub.Client(api_key=Variable.get(_finnhub_token_name))


_DATA_CORRUPT_CHANCE = 0.9


def get_stocks_data(
        stock_name: str,
) -> dict:
    try:
        data = _finnhub_client.quote(stock_name)

        data['o'] = data['o'] * (random.random() * 0.2 + 0.8)
        data['c'] = data['o'] * (random.random() * 0.3 + 0.7)

        if random.random() > _DATA_CORRUPT_CHANCE:
            data['c'] = None

        return data

    except (FinnhubAPIException, FinnhubRequestException) as e:
        raise ValueError('Ошибка считывания стоимости акции: {}'.format(e))
