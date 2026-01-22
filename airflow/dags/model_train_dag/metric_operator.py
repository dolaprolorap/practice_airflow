from logger import get_logger
from airflow.sdk import BaseOperator
from database import save_metrics
from sklearn.metrics import mean_squared_error
from domain import Metric

import datetime
import numpy as np
import logging
import database


class MetricOperator(BaseOperator):
    def __init__(
            self,
            logger: logging.Logger = get_logger(__name__),
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self._logger = logger

    def execute(self, context):
        ti = context['ti']
        batch_number = ti.xcom_pull(task_ids='train_model_operator', key='batch_name')

        data = database.get_model_results_by_batch_num(batch_number=batch_number)

        data = np.array(data)

        metrics = mean_squared_error(data[:, 1], data[:, 0])

        self._logger.info('Получены метрики от батча {}: MSE {}'.format(batch_number, metrics))

        save_metrics(
            metric=Metric(
                datetime=datetime.datetime.now(),
                value=metrics,
                metric_name='MSE',
                batch_num=batch_number,
            )
        )
