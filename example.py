#! /usr/bin/env python3
# coding=utf-8
from my_functions import *
import concurrent.futures

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

    pool = {}
    stocks = update_mongo.get_symbols()
    chart_invs = {

    }

    def combine_name(p: str, n: str) -> str:
        return "{}:{}".format(p, n)


    def launch_futures(future_name: str, real=True, **kwargs) -> None:
        # global stocks
        # stocks = update_mongo.get_symbols()

        # used to keep the connection
        stocks['TOPS'] = {
            'auto': {
                'chart': 1,
                'chart_inv': 300
            }
        }

        def stop():
            if future_name in pool and pool[future_name].running():
                pool[future_name].cancel()
                print('stop {} '.format(future_name))
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
                                                        seconds=6)
            else:
                stop()

        elif pre == 'lv1':
            if stocks[name]['auto'].get('lv1', 0):
                if future_name not in pool or not pool[future_name].running():
                    print('watch lv1 ' + name)
                    pool[future_name] = executor.submit(get_level_1_quotes_and_trades, ticker=name,
                                                        seconds=1)
            else:
                stop()
        elif pre == 'lv2':
            print(pre, ' has not been implemented yet!')
            pass

        else:
            print(pre, 'what the hell is this?????')
            pass


    # We can use a with statement to ensure threads are cleaned up promptly
    # TODO Fix MongoClient opened before fork. Create MongoClient
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(stocks) * 5) as executor:
        launch_futures('bar:TOPS')  # KEEP A RUNNING CONNECTION
        # get_live_interval_bars(ticker='DRYS',
        #                        bar_len=60,
        #                        seconds=6)

        while 1:
            # launch_service()
            # print(results)
            # pre fix is always 3 characters

            for stock in stocks.keys():
                bar = combine_name('bar', stock)
                launch_futures(bar, stocks[stock]['auto'].get('chart', 0), bar_len=60)

                lv1 = combine_name('lv1', stock)
                launch_futures(lv1, stocks[stock]['auto'].get('chart', 0))

            while 1:
                stocks = update_mongo.get_symbols()
                for key, future in pool.items():
                    # if not future.running():
                    # check if user changed the status of the char or lvs
                    launch_futures(key)

                    try:
                        pass
                    except Exception as e:
                        print(e)
                        launch_futures(key)
                        print('{} crashed and restarted'.format(key))

                time.sleep(1)
                # concurrent.futures.
                # for future in concurrent.futures.as_completed(pool):
                #     # print(future)
                #     try:
                #         data = future.result()
                #         print(future, data)
                #     except Exception as exc:
                #         print('generated an exception: %s' % exc)
                #         launch_service()
                #     else:
                #         print('%s' % data)





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
