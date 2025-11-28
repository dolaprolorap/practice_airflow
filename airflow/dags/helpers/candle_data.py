from dataclasses import dataclass


@dataclass
class CandleDTO:
    stock_name: str
    current_price: float
    highest_price_of_day: float
    lowest_price_of_day: float
    open_price_of_day: float
    timestamp: int
