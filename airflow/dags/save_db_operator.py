from airflow.sdk import BaseOperator
from helpers.candle_data import CandleDTO

import sqlite3


class SaveDBOperator(BaseOperator):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._db_name = 'stocks.sqlite'

    def execute(self, context):
        self._init_db()

        ti = context['ti']
        candle = ti.xcom_pull(task_ids='get_candle_data_task', key='candle_data')

        return self._save(candle)

    def _init_db(self):
        con = sqlite3.connect(self._db_name)
        con.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                name TEXT,
                current_price REAL,
                highest_price_of_day REAL,
                lowest_price_of_day REAL,
                open_price_of_day REAL,
                timestamp INTEGER
            )
        ''')
        con.commit()
        con.close()

    def _save(self, candle_data: CandleDTO):
        con = sqlite3.connect(self._db_name)
        values = (
            candle_data.stock_name,
            candle_data.current_price,
            candle_data.highest_price_of_day,
            candle_data.lowest_price_of_day,
            candle_data.open_price_of_day,
            candle_data.timestamp
        )
        con.execute('INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?)', values)
        con.commit()
        con.close()
