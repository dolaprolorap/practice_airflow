from dataclasses import dataclass
from datetime import datetime


@dataclass
class Metric:
    datetime: datetime
    value: float
    metric_name: str
    batch_num: int
