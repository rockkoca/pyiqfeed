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
    print(results)
    # get_level_1_quotes_and_trades(ticker="AMD", seconds=1)
    # get_tickdata(ticker="AMD", max_ticks=10000, num_days=4)
    # get_historical_bar_data(ticker="AMD",
    #                             bar_len=60,
    #                             bar_unit='s',
    #                             num_bars=100)
    # get_daily_data(ticker="AMD", num_days=10)
    # get_live_interval_bars(ticker="AMD", bar_len=600, seconds=30)

    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:

        while 1:
            launch_service()
            print(results)
            pool = [
                executor.submit(get_live_interval_bars, ticker="AMD", bar_len=60, seconds=60),
                executor.submit(get_live_interval_bars, ticker="NVDA", bar_len=60, seconds=60),
                # executor.submit(get_daily_data, ticker="AMD", num_days=10),
                # executor.submit(get_level_1_quotes_and_trades, ticker="AMD", seconds=1)
            ]
            for future in concurrent.futures.as_completed(pool):
                try:
                    data = future.result()
                except Exception as exc:
                    print('generated an exception: %s' % exc)
                    launch_service()
                else:
                    print('%s' % data)





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
