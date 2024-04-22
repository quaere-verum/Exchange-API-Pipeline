import numpy as np
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from data_handling import DataHandler
from config import TradingModel
import json
import time
import os
from typing import Callable, List
DATAFOLDER = os.environ['DATAFOLDER']


def kline_stream_handler(data_handler: DataHandler) -> Callable[[str, str], None]:

    def message_handler(_, message: str) -> None:
        info = json.loads(message)['data']
        # close, base_asset_vol, quote_asset_vol, taker_buy_asset_vol, quote_buy_asset_vol, nr_trades
        data = np.array([info['k']['c'],
                        info['k']['v'],
                        info['k']['q'],
                        info['k']['V'],
                        info['k']['Q'],
                        info['k']['n']])
        updated = data_handler.update(timestamp=info['k']['T'], symbol=info['s'].lower(), data=data)
        if updated:
            data_handler.trade()

    return message_handler


def algo_trading(duration: int, interval: str, symbols: List[str], websocket_refresh_seconds: int = 43200) -> None:
    data_handler = DataHandler(TradingModel(), symbols=symbols, interval=interval)
    nr_refreshes = duration // websocket_refresh_seconds
    remaining_time = duration % websocket_refresh_seconds
    for _ in range(nr_refreshes):
        client = SpotWebsocketStreamClient(on_message=kline_stream_handler(data_handler=data_handler),
                                           is_combined=True)
        for symbol in symbols:
            client.kline(symbol=symbol, interval=interval)
        time.sleep(duration)
        client.stop()
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(data_handler=data_handler),
                                       is_combined=True)
    for symbol in symbols:
        client.kline(symbol=symbol, interval=interval)
    time.sleep(remaining_time)
    client.stop()


if __name__ == '__main__':
    duration = 10
    interval = '1s'
    symbols = ['ethusdt', 'btcusdt']
    algo_trading(duration=duration,
                 interval=interval,
                 symbols=symbols)
    
