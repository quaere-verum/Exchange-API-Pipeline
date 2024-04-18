from db_stream.db_writer import stream_to_db
import os
SQL_PASSWORD = os.environ['SQL_PASSWORD']


def main():
    duration = 60*60*24
    interval = '1m'
    connection_string = f"postgresql://localhost/Crypto?user=postgres&password={SQL_PASSWORD}"
    replace_existing = True
    symbols = ['ethusdt', 'btcusdt']
    symbols_to_table_names = dict(zip(symbols, [symbol + '_' + interval for symbol in symbols]))
    stream_to_db(symbols_to_table_names=symbols_to_table_names,
                 duration=duration,
                 interval=interval,
                 connection_string=connection_string,
                 replace_existing=replace_existing)


if __name__ == '__main__':
    main()
