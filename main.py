from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import json
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
import sqlalchemy as sa
import time
import os
SQL_PASSWORD = os.environ['SQL_PASSWORD']

connection_string = f"postgresql://localhost/Crypto?user=postgres&password={SQL_PASSWORD}"
db = sa.create_engine(connection_string)
Session = sessionmaker(bind=db)
Base = declarative_base(metadata=sa.MetaData(schema='TickerData'))
symbol = 'ethusdt'


class Kline(Base):
    __tablename__ = symbol
    timestamp: Mapped[int] = mapped_column(primary_key=True)
    close: Mapped[float]
    base_asset_volume: Mapped[float]
    quote_asset_volume: Mapped[float]
    taker_buy_base_asset_volume: Mapped[float]
    taker_buy_quote_asset_volume: Mapped[float]
    nr_trades: Mapped[int]


def kline_stream_handler(session):
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
        kline = Kline(**data)
        session.add(kline)
        session.commit()
    return message_handler


def stream_data(duration, interval, session):
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(session), is_combined=True)
    client.kline(symbol=symbol, interval=interval)
    time.sleep(duration)
    client.stop()


def main():
    duration = 180
    interval = '1s'
    replace_existing = True
    if replace_existing:
        Base.metadata.drop_all(db)
    Base.metadata.create_all(db)
    with Session() as session:
        stream_data(duration=duration, interval=interval, session=session)


if __name__ == '__main__':
    main()
