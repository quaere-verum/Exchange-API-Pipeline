import numpy as np
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import json
import time
import os
DATAFOLDER = os.environ['DATAFOLDER']


class DataHandler:
    def __init__(self, ticker_shape, features_shape, features):
        self.ticker_array = np.zeros(shape=ticker_shape, dtype=np.float32)
        self.features_array = np.zeros(shape=features_shape, dtype=np.float32)
        self.features = features
        self.n_updates = 0

    def update(self, data):
        self.n_updates += 1
        self.ticker_array[0:-1, :] = self.ticker_array[1:, :]
        self.ticker_array[-1] = data
        features = self.calc_features()
        self.features_array[0:-1, :] = self.features_array[1:, :]
        self.features_array[-1] = features

    def calc_features(self):
        features = np.zeros(self.features_array.shape[1])
        for feature in self.features:
            features[self.features[feature]['pos']] = self.features[feature]['function'](self.ticker_array)
        return features


def kline_stream_handler(params):
    data_handler = DataHandler(ticker_shape=(params['ticker_length'], 6),
                               features_shape=(params['features_length'], len(params['features'])),
                               features=params['features'])

    def message_handler(_, message):
        info = json.loads(message)['data']
        # close, base_asset_vol, quote_asset_vol, taker_buy_asset_vol, quote_buy_asset_vol, nr_trades
        data = np.array([info['k']['c'],
                        info['k']['v'],
                        info['k']['q'],
                        info['k']['V'],
                        info['k']['Q'],
                        info['k']['n']])
        data_handler.update(data)

    return message_handler


def algo_trading(duration, interval, symbol, params):
    client = SpotWebsocketStreamClient(on_message=kline_stream_handler(params=params), is_combined=True)
    client.kline(symbol=symbol, interval=interval)
    time.sleep(duration)
    client.stop()


if __name__ == '__main__':
    duration = 10
    interval = '1s'
    symbol = 'ethusdt'
    params = {
        'ticker_length': 5,
        'features_length': 2,
        'features': {'mean': {'function': lambda x: np.mean(x[:, 0]), 'pos': 0},
                     'std': {'function': lambda x: np.std(x[:, 0]), 'pos': 1}}
    }
    algo_trading(duration=duration,
                 interval=interval,
                 symbol=symbol,
                 params=params)
    