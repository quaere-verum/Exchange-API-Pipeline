import numpy as np
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from feature_calculation import FeatureCalculator
from config import TradingModel
import json
import time
import os
from typing import Any, Callable
DATAFOLDER = os.environ['DATAFOLDER']


class DataHandler:
    def __init__(self, model: Any) -> None:
        self.model = model
        self.ticker_array = np.zeros(shape=model.params['ticker_shape'], dtype=np.float32)
        self.features_array = np.zeros(shape=model.params['features_shape'], dtype=np.float32)
        self.n_updates = 0
        self.feature_calculator = FeatureCalculator(features=model.features, 
                                                    n_features=model.n_features,
                                                    use_multiprocessing=False)
        self.last_timestamp = None

    def update(self, timestamp: int, data: np.ndarray) -> bool:
        if timestamp == self.last_timestamp:
            return False
        else:
            self.last_timestamp = timestamp
            self.n_updates += 1
            self.ticker_array[0:-1, :] = self.ticker_array[1:, :]
            self.ticker_array[-1] = data
            features = self.feature_calculator.calculate_features(self.ticker_array)
            self.features_array[0:-1, :] = self.features_array[1:, :]
            self.features_array[-1] = features
            return True

    def predict(self) -> Any:
        return self.model.predict(self.features_array)


def kline_stream_handler() -> Callable[[str, str], None]:
    data_handler = DataHandler(TradingModel())

    def message_handler(_, message: str) -> None:
        info = json.loads(message)['data']
        # close, base_asset_vol, quote_asset_vol, taker_buy_asset_vol, quote_buy_asset_vol, nr_trades
        data = np.array([info['k']['c'],
                        info['k']['v'],
                        info['k']['q'],
                        info['k']['V'],
                        info['k']['Q'],
                        info['k']['n']])
        updated = data_handler.update(timestamp=info['k']['T'], data=data)
        if updated:
            prediction = data_handler.predict()
            # Implement trade execution based on model prediction
            print(prediction)

    return message_handler


def algo_trading(duration: int, interval: str, symbol: str) -> None:
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(), is_combined=True)
    client.kline(symbol=symbol, interval=interval)
    time.sleep(duration)
    client.stop()


if __name__ == '__main__':
    duration = 10
    interval = '1s'
    symbol = 'ethusdt'
    algo_trading(duration=duration,
                 interval=interval,
                 symbol=symbol)
    
