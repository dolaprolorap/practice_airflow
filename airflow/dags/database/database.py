from sqlalchemy.orm import Session, DeclarativeBase, mapped_column
from sqlalchemy import String, Float, Integer, Boolean, create_engine, select, DateTime, update
from domain import CandleDto, Metric
from datetime import datetime
from sqlalchemy import select, func

from .config import Config


_config = Config()

_DB_DSN = "postgresql+psycopg2://{}:{}@{}/{}".format(
    _config.stocks_db_user,
    _config.stocks_db_password,
    _config.stocks_db_host,
    _config.stocks_db_name,
)

_engine = create_engine(_DB_DSN)


class _Base(DeclarativeBase):
    pass


class _Stocks(_Base):
    __tablename__ = 'stocks'

    id = mapped_column(Integer(), primary_key=True)
    stock_name = mapped_column(String(10))
    current_price = mapped_column(Float())
    open_price_of_day = mapped_column(Float())
    timestamp = mapped_column(Integer())
    is_used = mapped_column(Boolean(), default=False)


class _Metrics(_Base):
    __tablename__ = 'metrics'

    id = mapped_column(Integer(), primary_key=True)
    datetime = mapped_column(DateTime())
    value = mapped_column(Float())
    metric_name = mapped_column(String(10))
    batch_num = mapped_column(Integer())


class _ModelResults(_Base):
    __tablename__ = 'model_results'

    id = mapped_column(Integer(), primary_key=True)
    datetime = mapped_column(DateTime())
    y_pred = mapped_column(Float())
    y_true = mapped_column(Float())
    batch_num = mapped_column(Integer())


_Base.metadata.create_all(_engine)


def _connection(method):
    def wrapper(*args, **kwargs):
        with Session(_engine) as session:
            try:
                return method(*args, session=session, **kwargs)
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.close()

    return wrapper


@_connection
def save_stock(session: Session, candle_data: CandleDto):
    candle = _Stocks(
        stock_name=candle_data.stock_name,
        current_price=candle_data.current_price,
        open_price_of_day=candle_data.open_price_of_day,
        timestamp=candle_data.timestamp,
    )

    session.add(candle)

    session.commit()


@_connection
def get_unused_stocks(session: Session) -> list[CandleDto]:
    stmt = select(_Stocks).where(_Stocks.is_used == False)
    result = session.execute(stmt)

    stocks = []

    for stock in result.scalars().all():
        stocks.append(
            CandleDto(
                stock_name=stock.stock_name,
                current_price=stock.current_price,
                open_price_of_day=stock.open_price_of_day,
                timestamp=stock.timestamp,
            )
        )

    stmt = update(_Stocks).where(_Stocks.is_used == False).values(is_used=True)
    session.execute(stmt)

    return stocks


@_connection
def save_model_results(session: Session, date_time: datetime, y_pred: float, y_true: float) -> int:
    last_batch_num = _get_last_batch_num()

    model_results = _ModelResults(
        datetime=date_time,
        y_pred=y_pred,
        y_true=y_true,
        batch_num=last_batch_num + 1,
    )

    session.add(model_results)

    session.commit()

    return last_batch_num + 1


@_connection
def get_model_results_by_batch_num(session: Session, batch_number: int) -> list[list]:
    stmt = select(_ModelResults).where(_ModelResults.batch_num == batch_number)
    result = session.execute(stmt)

    return [[res.y_pred, res.y_true] for res in result.scalars().all()]


@_connection
def save_metrics(session: Session, metric: Metric) -> None:
    metric_record = _Metrics(
        datetime=metric.datetime,
        value=metric.value,
        metric_name=metric.metric_name,
        batch_num=metric.batch_num,
    )

    session.add(metric_record)

    session.commit()


@_connection
def get_metrics(session: Session) -> list[Metric]:
    stmt = select(_Metrics)
    result = session.execute(stmt)

    metrics = []

    for metric in result.scalars().all():
        metrics.append(
            Metric(
                datetime=metric.datetime,
                value=metric.value,
                metric_name=metric.metric_name,
                batch_num=metric.batch_num,
            )
        )

    return metrics


@_connection
def _get_last_batch_num(session: Session) -> int:
    stmt = select(func.max(_ModelResults.batch_num))

    val = session.execute(stmt).scalar_one_or_none()

    if val is None:
        return 0

    return val
