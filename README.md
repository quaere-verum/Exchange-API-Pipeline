# Exchange-API-Pipeline
## db_stream
Connects to the Binance websocket API to obtain a live data feed, 
which is written to the database specified by the connection string.

## trading_stream
Connects to the Binance websocket API to obtain a live data feed,
processes the input via a user-defined model, and executes trades based on the trading logic specified by the
user-defined model.

# How to use
## db_stream
Obtain a connection string for the database you wish to stream data to, save it in main.py and execute the script.
Duration is in seconds.
## trading_stream
Configure your trading model in config.py. This includes setting the API endpoint, specifying API key and secret key,
and defining a TradingModel class which implements your trading logic in a .trade method, which takes the features
you've added, and asset prices, as input. Pass a list of symbols from which your features are calculated to
algo_trading in algo_execution.py, then run the script. Make sure the symbols are passed in the same order that
your features expect them to be in.
