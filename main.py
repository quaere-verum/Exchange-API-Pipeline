from db_stream.db_writer import stream_to_db


def main():
    duration = 10
    interval = '1s'
    replace_existing = True
    stream_to_db(duration=duration,
                 interval=interval,
                 replace_existing=replace_existing)


if __name__ == '__main__':
    main()
