from airflow.sdk import BaseOperator
from database import get_unused_stocks, save_model_results
from domain import CandleDto
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from logger import get_logger

import datetime
import logging
import numpy as np


class TrainModelOperator(BaseOperator):
    def __init__(
            self,
            logger: logging.Logger = get_logger(__name__),
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self._logger = logger

    def execute(self, context):
        self._logger.info('Началось обучение модели...')

        train_data = get_unused_stocks()

        data = _dto_to_array(train_data)

        X = data[:, 0]
        y = data[:, 1]

        train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.2, random_state=42)

        model = LinearRegression()
        model.fit(train_X.reshape(-1, 1), train_y)
        pred = model.predict(test_X.reshape(-1, 1))

        for (pred_record, test_y_record) in zip(pred.tolist(), test_y.tolist()):
            batch_number = save_model_results(
                date_time=datetime.datetime.now(),
                y_pred=pred_record,
                y_true=test_y_record,
            )

        self._logger.info('Обучение модели завершено, данные сохранены в БД под номеро {}'.format(batch_number))

        ti = context['ti']
        ti.xcom_push(key='batch_name', value=batch_number)


def _dto_to_array(data: list[CandleDto]) -> np.array:
    data_array = []

    for i in data:
        data_array.append(
            list(i.__dict__.values())[1:]
        )

    return np.array(data_array)
