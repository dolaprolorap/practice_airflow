from dataclasses import dataclass


@dataclass
class CandleDto:
    stock_name: str
    current_price: float
    open_price_of_day: float
    timestamp: int
