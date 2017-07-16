#! /usr/bin/env python3
# coding=utf-8
from my_functions import *
import concurrent.futures
import multiprocessing

update_mongo = UpdateMongo()
db = update_mongo.get_db()


def search_mongo(ty: str):
    if ty == 'ins':
        return db.instruments.find()
    if ty == 'orders':
        return db.orders.find({'cancel': {'$ne': None}})
    if ty == 'pos':
        return db.nonzero_positions.find()
        # return db.orders.find({'symbol': 'AMD', 'cancel': {'$ne': None}})


def sync_mongo():
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        ins = executor.submit(search_mongo, ty='ins')
        orders = executor.submit(search_mongo, ty='orders')
        positions = executor.submit(search_mongo, ty='pos')
        try:
            ins = ins.result()
            orders = orders.result()
            positions = positions.result()
        except Exception as exc:
            print(f'exceptions when syncing mongo {exc}')
        else:
            for instrument in ins:
                update_mongo.mongo_cache['ins'][instrument['symbol']] = instrument
                update_mongo.mongo_cache['ins_to_symbol'][instrument['url']] = instrument
            for order in orders:
                update_mongo.mongo_cache['orders'][order['instrument']] = order
            for pos in positions:
                update_mongo.mongo_cache['pos'][pos['instrument']] = pos


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
    # threading.Timer(30, check_connection).start()
    set_interval(sync_mongo, .15)

    pool = {}
    #
    lv1 = threading.Timer(1, get_level_1_multi_quotes_and_trades, [{}, 1, True])
    lv1.start()

    lv2 = threading.Timer(1, get_level_2_multi_quotes_and_trades, [{}, 1, True])
    lv2.start()

    while 1:
        time.sleep(5)
