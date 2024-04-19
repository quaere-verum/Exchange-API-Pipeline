import numpy as np
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from data_handling import DataHandler
from config import TradingModel, trading_logic
import json
import time
import os
from typing import Any, Callable, List
DATAFOLDER = os.environ['DATAFOLDER']


def kline_stream_handler(symbols: List[str], interval: str) -> Callable[[str, str], None]:
    data_handler = DataHandler(TradingModel(), symbols=symbols, interval=interval)

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
            prediction = data_handler.predict()
            trading_logic(prediction)

    return message_handler


def algo_trading(duration: int, interval: str, symbols: List[str]) -> None:
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(symbols, interval), is_combined=True)
    for symbol in symbols:
        client.kline(symbol=symbol, interval=interval)
    time.sleep(duration)
    client.stop()


if __name__ == '__main__':
    duration = 10
    interval = '1s'
    symbols = ['ethusdt', 'btcusdt']
    algo_trading(duration=duration,
                 interval=interval,
                 symbols=symbols)
    
