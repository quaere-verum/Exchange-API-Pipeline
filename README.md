# Exchange-API-Pipeline
## db_stream
Connects to the Binance websocket API to obtain a live data feed, 
which is written to the database specified by the connection string.

## trading_stream
Connects to the Binance websocket API to obtain a live data feed,
processes the input via a user-defined model, and executes trades based on the result.

# To Do
- Implement example model & corresponding trading logic.
