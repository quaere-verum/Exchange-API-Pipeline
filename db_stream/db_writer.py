from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import json
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
import sqlalchemy as sa
import time
from typing import Any, Callable, List, Dict, Iterable


class TableHandler:
    def __init__(self, tables: Dict[str, Any]) -> None:
        self.tables = tables
        self.last_updated = dict(zip(tables.keys(), [None for _ in tables]))

    def update(self, data: dict, symbol: str, session: Any) -> None:
        if self.last_updated[symbol.lower()] != data['timestamp']:
            kline = self.tables[symbol](**data)
            session.add(kline)
            session.commit()
            self.last_updated[symbol.lower()] = data['timestamp']


def create_kline_table(table_name: str) -> Any:
    Base = declarative_base(metadata=sa.MetaData(schema='TickerData'), class_registry=dict())

    class Kline(Base):
        __tablename__ = table_name
        timestamp: Mapped[int] = mapped_column(primary_key=True)
        close: Mapped[float]
        base_asset_volume: Mapped[float]
        quote_asset_volume: Mapped[float]
        taker_buy_base_asset_volume: Mapped[float]
        taker_buy_quote_asset_volume: Mapped[float]
        nr_trades: Mapped[int]
    return Kline


def kline_stream_handler(session: Any, table_handler: TableHandler) -> Callable[[str, str], None]:

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
        table_handler.update(data=data, symbol=info['s'].lower(), session=session)

    return message_handler


def stream_data(duration: int, interval: str, session: Any, symbols: Iterable, tables: Any) -> None:
    table_handler = TableHandler(tables)
    refresh_seconds = 3600
    nr_refreshes = duration//refresh_seconds
    remaining_time = duration % refresh_seconds
    for _ in range(nr_refreshes):
        client = SpotWebsocketStreamClient(on_message=kline_stream_handler(session=session,
                                                                           table_handler=table_handler),
                                           is_combined=True)
        for symbol in symbols:
            client.kline(symbol=symbol, interval=interval)
        time.sleep(refresh_seconds)
        client.stop()
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(session=session,
                                                                       table_handler=table_handler),
                                       is_combined=True)
    for symbol in symbols:
        client.kline(symbol=symbol, interval=interval)
    time.sleep(remaining_time)
    client.stop()


def stream_to_db(symbols_to_table_names: Dict[str, str], duration: int, interval: str,
                 connection_string: str, replace_existing: bool = True) -> None:
    db = sa.create_engine(connection_string)
    Session = sessionmaker(bind=db)
    tables = {symbol.lower(): create_kline_table(table_name=symbols_to_table_names[symbol]) for symbol in
              symbols_to_table_names}
    if replace_existing:
        for table in tables.values():
            table.metadata.drop_all(db)
    for table in tables.values():
        table.metadata.create_all(db)
    with Session() as session:
        stream_data(duration=duration, interval=interval, session=session,
                    symbols=symbols_to_table_names.keys(), tables=tables)
