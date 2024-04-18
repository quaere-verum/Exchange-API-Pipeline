from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import json
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
import sqlalchemy as sa
import time
from typing import Any, Callable


class DataHandler:
    def __init__(self, table: Any) -> None:
        self.table = table
        self.last_updated = None

    def update(self, data: dict, session: Any) -> None:
        if self.last_updated != data['timestamp']:
            kline = self.table(**data)
            session.add(kline)
            session.commit()
            self.last_updated = data['timestamp']


def create_kline_table(table_name: str, base: Any) -> Any:
    class Kline(base):
        __tablename__ = table_name
        timestamp: Mapped[int] = mapped_column(primary_key=True)
        close: Mapped[float]
        base_asset_volume: Mapped[float]
        quote_asset_volume: Mapped[float]
        taker_buy_base_asset_volume: Mapped[float]
        taker_buy_quote_asset_volume: Mapped[float]
        nr_trades: Mapped[int]
    return Kline


def kline_stream_handler(session: Any, table: Any) -> Callable[[str, str], None]:
    data_handler = DataHandler(table)

    def message_handler(_, message):
        info = json.loads(message)['data']
        data = {'timestamp': info['k']['T']//1000 + 1,
                'close': info['k']['c'],
                'base_asset_volume': info['k']['v'],
                'quote_asset_volume': info['k']['q'],
                'taker_buy_base_asset_volume': info['k']['V'],
                'taker_buy_quote_asset_volume': info['k']['Q'],
                'nr_trades': info['k']['n']
                }
        data_handler.update(data, session)

    return message_handler


def stream_data(duration: int, interval: str, session: Any, symbol: str, table: Any) -> None:
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(session=session, table=table),
                                       is_combined=True)
    client.kline(symbol=symbol, interval=interval)
    time.sleep(duration)
    client.stop()


def stream_to_db(symbol: str, table_name: str, duration: int, interval: str,
                 connection_string: str, replace_existing: bool = True) -> None:
    db = sa.create_engine(connection_string)
    Session = sessionmaker(bind=db)
    Base = declarative_base(metadata=sa.MetaData(schema='TickerData'))
    Kline = create_kline_table(table_name=table_name, base=Base)
    if replace_existing:
        Base.metadata.drop_all(db)
    Base.metadata.create_all(db)
    with Session() as session:
        stream_data(duration=duration, interval=interval, session=session, symbol=symbol, table=Kline)
