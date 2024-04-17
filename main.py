from db_stream.db_writer import stream_to_db
import os
SQL_PASSWORD = os.environ['SQL_PASSWORD']


def main():
    duration = 30
    interval = '1s'
    connection_string = f"postgresql://localhost/Crypto?user=postgres&password={SQL_PASSWORD}"
    replace_existing = True
    stream_to_db(symbol='ethusdt',
                 duration=duration,
                 interval=interval,
                 connection_string=connection_string,
                 replace_existing=replace_existing)


if __name__ == '__main__':
    main()
