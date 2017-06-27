#! /usr/bin/env python3
# coding=utf-8
from my_functions import *
import robinhood.Robinhood as RB
from robinhood.credentials import *
import concurrent.futures
import multiprocessing
import ujson
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
    stocks = get_sp500()

    trader = RB.Robinhood()
    trader.login(username=Credential.get_username(), password=Credential.get_password())

    if multiprocessing.cpu_count() > 200:
        pool_executor = concurrent.futures.ProcessPoolExecutor(max_workers=len(stocks) * 5)
        print('Using multi process', multiprocessing.cpu_count(), 'cores')
    else:
        pool_executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(stocks) * 5)
        print('Using multi threading', multiprocessing.cpu_count(), 'cores')

    with pool_executor as executor:
        # executor.submit(get_administrative_messages, 1)


        def launch_futures(future_name: str, real=True, **kwargs) -> None:
            # global stocks
            # stocks = update_mongo.get_symbols()

            def stop():
                if future_name in pool and pool[future_name].running():
                    pool[future_name].cancel()
                    # print('stop {} '.format(future_name))
                else:
                    pool[future_name] = executor.submit(str, future_name)
                    pool[future_name].cancel()
                    # print('skip {} bar'.format(stock))

            pre = future_name[:3]
            name = future_name[4:]
            if pre == 'bar':
                # pool[key] = executor.submit(get_live_interval_bars, ticker=name,
                #                             bar_len=stocks[name]['auto'].get('chart_len', 60),
                #                             seconds=60)

                if stocks[name]['auto'].get('chart', 0):
                    update_inv = False
                    bar_len = stocks[name]['auto'].get('chart_inv', 30)
                    last_bar_len = chart_invs.get(future_name, 0)
                    if bar_len != chart_invs.get(future_name, 0):
                        if chart_invs.get(future_name, 0):
                            print("unwatch {} @ {}".format(name, chart_invs.get(future_name, bar_len)))
                        update_inv = True
                        chart_invs[future_name] = bar_len

                    # print(stocks[name])
                    if update_inv or future_name not in pool or not pool[future_name].running():
                        print('watch bar ' + name + " : " + str(bar_len))
                        pool[future_name] = executor.submit(get_live_interval_bars, ticker=name,
                                                            bar_len=bar_len,
                                                            seconds=1, auto_unwatch=False)
                else:
                    stop()

            elif pre == 'lv1':
                if stocks[name]['auto'].get('lv1', 0):
                    if future_name not in pool or not pool[future_name].running():
                        print('watch lv1 ' + name)
                        pool[future_name] = executor.submit(get_level_1_quotes_and_trades, ticker=name,
                                                            seconds=1, auto_unwatch=False)
                else:
                    stop()
            elif pre == 'lv2':
                print(pre, ' has not been implemented yet!')
                pass

            else:
                print(pre, 'what the hell is this?????')
                pass

        while 1:
            # launch_service()
            # print(results)
            # pre fix is always 3 characters
            i = 0

            for stock in stocks.keys():

                bar = combine_name('bar', stock)
                launch_futures(bar, True, bar_len=30)
                lv1 = combine_name('lv1', stock)
                launch_futures(lv1, True)
                if i % 10 == 0:
                    time.sleep(5)

            time_cost = 0
            counter = 0
            limit = 50
            start = time.time()
            while 1:
                # stocks = update_mongo.get_symbols()
                for key, future in pool.items():
                    # if not future.running():
                    # check if user changed the status of the char or lvs

                    try:
                        t = threading.Timer(.05, launch_futures, [key])
                        t.start()

                    except Exception as e:
                        print('{} crashed and restarted'.format(key), e)


                counter += 1
                if counter == limit:
                    end = time.time()
                    time_cost += end - start
                    print('each loop used {} seconds'.format(time_cost / limit - .5))
                    counter = 0
                    time_cost = 0
                    start = time.time()
                time.sleep(.5)  # give the mongo little break





                # if results.level_1:
                #     print(get_level_1_quotes_and_trades(ticker="AMD", seconds=30))
                # if results.regional_quotes:
                #     get_regional_quotes(ticker="AMD", seconds=120)
                # if results.trade_updates:
                #     get_trades_only(ticker="AMD", seconds=30)
                # if results.interval_data:
                #     get_live_interval_bars(ticker="AMD", bar_len=5, seconds=30)
                # if results.admin_socket:
                #     get_administrative_messages(seconds=30)
                # if results.historical_tickdata:
                #     get_tickdata(ticker="AMD", max_ticks=100, num_days=4)
                # if results.historical_bars:
                #     get_historical_bar_data(ticker="AMD",
                #                             bar_len=60,
                #                             bar_unit='s',
                #                             num_bars=100)
                # if results.historical_daily_data:
                #     get_daily_data(ticker="AMD", num_days=10)
                # if results.reference_data:
                #     get_reference_data()
                # if results.lookups_and_chains:
                #     get_ticker_lookups("SPH9GBM1")
                #     get_equity_option_chain("AMD")
                #     get_futures_chain("@VX")
                #     get_futures_spread_chain("@VX")
                #     get_futures_options_chain("CL")
                # if results.news:
                #     get_news()
