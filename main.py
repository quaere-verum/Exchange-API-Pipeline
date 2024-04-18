from db_stream.db_writer import stream_to_db
import os
SQL_PASSWORD = os.environ['SQL_PASSWORD']


def main():
    duration = 24*60*60
    interval = '1m'
    connection_string = f"postgresql://localhost/Crypto?user=postgres&password={SQL_PASSWORD}"
    replace_existing = False
    symbol = 'ethusdt'
    table_name = symbol + '_' + interval
    stream_to_db(symbol=symbol,
                 table_name=table_name,
                 duration=duration,
                 interval=interval,
                 connection_string=connection_string,
                 replace_existing=replace_existing)


if __name__ == '__main__':
    main()
