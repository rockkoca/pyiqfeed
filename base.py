#! /usr/bin/env python3
# coding=utf-8
from my_functions import *
import concurrent.futures
import multiprocessing


def get_indexes():
    """Get level 1 quotes and trades for ticker for seconds seconds."""

    listener = IndexWatcher
    quote_conn = MyQuote(name="pyiqfeed-index")
    quote_listener = listener("index Listener")
    quote_conn.add_listener(quote_listener)
    print('get_indexes started')

    with iq.ConnConnector([quote_conn]) as connector:
        all_fields = sorted(list(iq.QuoteConn.quote_msg_map.keys()))
        quote_conn.select_update_fieldnames(all_fields)
        for ticker in IndexWatcher.symbol_map.keys():
            quote_conn.watch(ticker)
            time.sleep(1)

        while quote_conn.reader_running():
            time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pyiqfeed example code")
    parser.add_argument('-l', action="store_true", dest='level_1',
                        help="Run Level 1 Quotes")
    parser.add_argument('-r', action="store_true", dest='regional_quotes',
                        help="Run Regional Quotes")
    parser.add_argument('-t', action="store_true", dest='trade_updates',
                        help="Run Trades Only Quotes")
    parser.add_argument('-i', action="store_true", dest='interval_data',
                        help="Run interval data")
    parser.add_argument("-a", action='store_true', dest='admin_socket',
                        help="Run Administrative Connection")
    parser.add_argument("-k", action='store_true', dest='historical_tickdata',
                        help="Get historical tickdata")
    parser.add_argument("-b", action='store_true', dest='historical_bars',
                        help="Get historical bar-data")
    parser.add_argument("-d", action='store_true', dest='historical_daily_data',
                        help="Get historical daily data")
    parser.add_argument("-f", action='store_true', dest='reference_data',
                        help="Get reference data")
    parser.add_argument("-c", action='store_true', dest='lookups_and_chains',
                        help="Lookups and Chains")
    parser.add_argument("-n", action='store_true', dest='news',
                        help="News related stuff")
    results = parser.parse_args()

    launch_service()
    # print(results)
    # get_level_1_quotes_and_trades(ticker="AMD", seconds=1)
    # get_tickdata(ticker="AMD", max_ticks=10000, num_days=4)
    # get_historical_bar_data(ticker="AMD",
    #                             bar_len=60,
    #                             bar_unit='s',
    #                             num_bars=100)
    # get_daily_data(ticker="AMD", num_days=10)
    # get_live_interval_bars(ticker="AMD", bar_len=600, seconds=30)

    # We can use a with statement to ensure threads are cleaned up promptly
    # TODO Fix MongoClient opened before fork. Create MongoClient
    # set_interval(check_connection, 5)

    # wait 10 till the service is started
    threading.Timer(30, check_connection).start()

    pool = {}
    #
    bars = threading.Timer(5, get_indexes, [{}, 30, 1, True])

    while 1:
        time.sleep(1)
