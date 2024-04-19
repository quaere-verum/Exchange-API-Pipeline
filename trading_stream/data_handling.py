import numpy as np
from typing import Any, List
from concurrent.futures import ProcessPoolExecutor
import requests
from datetime import datetime, timedelta
import time


class DataHandler:
    def __init__(self, model: Any, symbols: List[str], interval: str, use_multiprocessing: bool = False) -> None:
        self.model = model
        self.symbols = symbols
        self.interval = interval
        self.features = model.features
        self.use_multiprocessing = use_multiprocessing
        self.n_features = None

        self.ticker_history_length = model.params['ticker_history_length']
        self.features_history_length = model.params['features_history_length']
        self.ticker_array = np.zeros(shape=(self.ticker_history_length, 6*len(symbols)), dtype=np.float32)
        self._set_n_features()
        self.features_array = np.zeros(shape=(self.features_history_length,
                                              self.n_features), dtype=np.float32)
        self.n_updates = 0
        valid_model = self._validate_model()
        if not valid_model:
            raise ValueError('Model configured incorrectly.')
        self._initialise_arrays()
        self.last_timestamp = {symbol: 0 for symbol in self.symbols}

    def _validate_model(self) -> bool:
        try:
            self.calculate_features(self.ticker_array)
            self.model.trade(self.features_array)
            return True
        except:
            return False

    def _set_n_features(self):
        n_features = 0
        for feature in self.features:
            result = feature(self.ticker_array)
            if isinstance(result, (float, int, np.float32, np.int32, np.float64, np.int64)):
                n_features += 1
            else:
                if isinstance(result, np.ndarray):
                    assert len(result.shape) == 1, 'Feature must be scalar or 1-dimensional'
                n_features += len(result)
        self.n_features = n_features

    def _initialise_arrays(self) -> None:
        now = datetime.now()
        start = None
        past = int(self.interval[0:-1])*self.ticker_history_length + self.features_array.shape[0] - 1
        match self.interval[-1]:
            case 's':
                start = now - timedelta(seconds=past)
            case 'm':
                start = now - timedelta(minutes=past)
            case 'h':
                start = now - timedelta(hours=past)
        ticker_storage = np.zeros(shape=(self.ticker_history_length + self.features_array.shape[0] - 1,
                                         6*len(self.symbols)))
        for k, symbol in enumerate(self.symbols):
            klines = np.array(requests.get(url="https://api.binance.com/api/v3/klines",
                                           params={'symbol': symbol.upper(),
                                                   'interval': self.interval,
                                                   'startTime': int(1e3*time.mktime(start.timetuple())),
                                                   'endTime': int(1e3*time.mktime(now.timetuple()))}).json())
            # close, base_asset_vol, quote_asset_vol, taker_buy_asset_vol, quote_buy_asset_vol, nr_trades
            ticker_storage[:, 6*k:6*(k+1)] = klines[:, [4, 5, 7, 9, 10, 8]].astype(np.float32)
        for k in range(self.features_array.shape[0]):
            ticker_data = ticker_storage[k:k+self.ticker_history_length, :]
            self.features_array[k] = self.calculate_features(ticker_data)

    def calculate_features(self, ticker_data: np.ndarray) -> np.ndarray:
        features = np.zeros(self.n_features)
        if self.use_multiprocessing:
            futures = []
            with ProcessPoolExecutor() as executor:
                for feature in self.features:
                    futures.append(executor.submit(feature, ticker_data))
            ind = 0
            for future in futures:
                result = future.result()
                if isinstance(result, (float, int)):
                    features[ind] = result
                    ind += 1
                else:
                    features[ind:ind+len(result)] = result
                    ind += len(result)
        else:
            ind = 0
            for feature in self.features:
                result = feature(ticker_data)
                if isinstance(result, (float, int, np.float32, np.int32, np.float64, np.int64)):
                    features[ind] = result
                    ind += 1
                else:
                    features[ind:ind+len(result)] = result
                    ind += len(result)
        return features

    def update(self, timestamp: int, symbol: str, data: np.ndarray) -> bool:
        if timestamp == self.last_timestamp[symbol]:
            return False
        else:
            index = self.symbols.index(symbol)
            self.last_timestamp[symbol] = timestamp
            self.ticker_array[0:-1][:, index*6:(index+1)*6] = self.ticker_array[1:][:, index*6:(index+1)*6]
            self.ticker_array[-1, index*6:(index+1)*6] = data
            if np.min(list(self.last_timestamp.values())) == timestamp:
                features = self.calculate_features(self.ticker_array)
                self.features_array[0:-1, :] = self.features_array[1:, :]
                self.features_array[-1] = features
                self.n_updates += 1
                return True
            else:
                return False

    def trade(self) -> None:
        self.model.trade(self.features_array)


if __name__ == '__main__':
    from config import TradingModel
    test = DataHandler(model=TradingModel(), symbols=['ethusdt', 'btcusdt'], interval='1m')
