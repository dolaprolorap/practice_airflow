from logger import get_logger
from airflow.sdk import BaseOperator
from database import get_metrics
from domain import Metric
from matplotlib import pyplot as plt
from settings import Settings

import datetime
import numpy as np
import logging
import os


class VisualizeOperator(BaseOperator):
    def __init__(
            self,
            logger: logging.Logger = get_logger(__name__),
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self._logger = logger

    def execute(self, context):
        metrics = get_metrics()

        results = _dto_to_array(metrics)

        values = results[:, 0]
        batches = results[:, 1]

        settings = Settings()

        self._logger.info('Отображение графика...')

        os.makedirs(
            os.path.join(os.path.dirname(__file__), settings.visualisation_path),
            exist_ok=True
        )

        plt.figure(figsize=(10, 5))
        plt.plot(batches, values, marker="o", linestyle="-")
        plt.xlabel("Номер батча")
        plt.ylabel("Метрика")
        plt.title("MSE по батчам")
        plt.grid(True, alpha=0.3)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(os.path.join(os.path.dirname(__file__), settings.visualisation_path), f"values_by_batch_{timestamp}.png")

        plt.tight_layout()
        plt.savefig(file_path, dpi=100, bbox_inches="tight")
        plt.close()

        self._logger.info('Закончено отображение...')


def _dto_to_array(data: list[Metric]) -> np.ndarray:
    data_array = []

    for i in data:
        lst = list(i.__dict__.values())

        data_array.append(
            [lst[1], lst[3]]
        )

    return np.array(data_array)
