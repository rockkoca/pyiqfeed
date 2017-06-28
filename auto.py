#! /usr/bin/env python3
# coding=utf-8
from my_functions import *

import concurrent.futures
import multiprocessing
import ujson
import threading
from get_stocks import *

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

    # launch_service()

    # We can use a with statement to ensure threads are cleaned up promptly
    # TODO Fix MongoClient opened before fork. Create MongoClient
    # set_interval(check_connection, 3)
    pool = {}
    # update_mongo = UpdateMongo()
    # stocks = update_mongo.get_symbols()
    chart_invs = {

    }

    # get sp500 stocks
    # stocks = get_sp500()

    update_mongo = UpdateMongo()
    stocks = update_mongo.get_symbols()
    print(stocks)
    bars = threading.Timer(5, get_live_multi_interval_bars, [stocks, 30, 1, False])
    bars.start()

    lv1 = threading.Timer(5, get_level_1_multi_quotes_and_trades, [stocks, 1, False])
    lv1.start()

    while 1:
        time.sleep(1)
