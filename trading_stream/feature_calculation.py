from concurrent.futures import ProcessPoolExecutor
import numpy as np
from typing import Dict, Callable, Union, Any, List


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
