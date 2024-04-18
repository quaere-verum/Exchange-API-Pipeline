import numpy as np
from typing import Dict, Callable, Union, Any, List
from concurrent.futures import ProcessPoolExecutor


class DataHandler:
    def __init__(self, model: Any) -> None:
        self.model = model
        self.ticker_array = np.zeros(shape=model.params['ticker_shape'], dtype=np.float32)
        self.features_array = np.zeros(shape=model.params['features_shape'], dtype=np.float32)
        self.n_updates = 0
        self.feature_calculator = FeatureCalculator(features=model.features,
                                                    use_multiprocessing=False)
        self.feature_calculator.get_n_features(self.ticker_array)
        valid_model = self._validate_model()
        if not valid_model:
            raise ValueError('Model configured incorrectly.')
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

    def _validate_model(self) -> bool:
        try:
            self.feature_calculator.calculate_features(self.ticker_array)
            self.model.predict(self.features_array)
            return True
        except:
            return False

    def predict(self) -> Any:
        return self.model.predict(self.features_array)


class FeatureCalculator:
    def __init__(self, features: List[Callable[[np.ndarray], Union[np.ndarray, float]]],
                 use_multiprocessing: bool = False) -> None:
        self.features = features
        self.n_features = None
        self.use_multiprocessing = use_multiprocessing

    def __len__(self) -> int:
        return self.n_features

    def get_n_features(self, ticker_data):
        self.n_features = 0
        for feature in self.features:
            result = feature(ticker_data)
            if isinstance(result, (float, int, np.float32, np.int32, np.float64, np.int64):
                self.n_features += 1
            else:
                self.n_features += len(result)

    def calculate_features(self, ticker_data: np.ndarray) -> np.ndarray:
        features = np.zeros(len(self))
        if self.use_multiprocessing:
            futures = []
            with ProcessPoolExecutor() as executor:
                for feature in self.features:
                    futures.append(executor.submit(feature, ticker_data))
            for k, future in enumerate(futures):
                result = future.result()
                if isinstance(result, (float, int)):
                    features[k] = result
                else:
                    features[k:k+len(result)] = result
        else:
            for k, feature in enumerate(self.features):
                result = feature(ticker_data)
                if isinstance(result, (float, int, np.float32, np.int32, np.float64, np.int64)):
                    features[k] = result
                else:
                    features[k:k+len(result)] = result
        return features
