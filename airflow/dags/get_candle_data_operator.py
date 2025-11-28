from airflow.sdk import BaseOperator
from airflow.models import Variable
from helpers.airflow_variables import AirflowVariables
from helpers.candle_data import CandleDTO

import finnhub


class GetCandleDataOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        finnhub_token_name = AirflowVariables.FINNHUB_TOKEN_NAME.value

        self.finnhub_client = finnhub.Client(api_key=Variable.get(finnhub_token_name))

    def execute(self, context):
        print('Считываем данные о цене акций Apple...')

        response = self.finnhub_client.quote('AAPL')

        data = CandleDTO(
            stock_name='AAPL',
            current_price=response['c'],
            highest_price_of_day=response['h'],
            lowest_price_of_day=response['l'],
            open_price_of_day=response['o'],
            timestamp=response['t'],
        )

        ti = context['ti']
        ti.xcom_push(key='candle_data', value=data)

        return data
